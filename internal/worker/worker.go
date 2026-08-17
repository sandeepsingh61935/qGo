package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/sandy/jobqueue/internal/job"
	"github.com/sandy/jobqueue/internal/metrics"
	"github.com/sandy/jobqueue/internal/queue"
)

// Handler runs business logic for one job type.
type Handler interface {
	Handle(ctx context.Context, j *job.Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, j *job.Job) error

func (f HandlerFunc) Handle(ctx context.Context, j *job.Job) error {
	return f(ctx, j)
}

type Worker struct {
	q           *queue.Queue
	concurrency int
	handlers    map[string]Handler
	wg          sync.WaitGroup
}

func New(q *queue.Queue, concurrency int) *Worker {
	if concurrency <= 0 {
		concurrency = 4
	}
	return &Worker{
		q:           q,
		concurrency: concurrency,
		handlers:    make(map[string]Handler),
	}
}

func (w *Worker) RegisterHandler(jobType string, h Handler) {
	w.handlers[jobType] = h
}

// Run starts worker goroutines and a reaper; blocks until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	reaper := time.NewTicker(10 * time.Second)
	defer reaper.Stop()

	go w.reaperLoop(ctx, reaper)

	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.workerLoop(ctx)
	}
	w.wg.Wait()
}

func (w *Worker) workerLoop(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			w.processOne(ctx)
		}
	}
}

func (w *Worker) processOne(ctx context.Context) {
	d, err := w.q.Dequeue(ctx)
	if err != nil {
		slog.Error("dequeue failed", "error", err)
		time.Sleep(200 * time.Millisecond)
		return
	}
	if d == nil {
		time.Sleep(500 * time.Millisecond)
		return
	}

	j := d.Job
	raw := d.Raw

	handler, ok := w.handlers[j.Type]
	if !ok {
		slog.Error("unknown job type", "type", j.Type, "job_id", j.ID)
		_ = w.q.Ack(ctx, raw)
		_ = w.q.EnqueueDLQ(ctx, j, "unknown job type: "+j.Type)
		metrics.JobsFailedTotal.WithLabelValues(j.Type, metrics.DefaultQueue).Inc()
		return
	}

	start := time.Now()
	slog.Info("job started", "job_id", j.ID, "type", j.Type, "retry", j.RetryCount)

	err = safeHandle(ctx, handler, j)
	duration := time.Since(start)
	metrics.JobDurationSeconds.WithLabelValues(j.Type, metrics.DefaultQueue).Observe(duration.Seconds())

	if err != nil {
		slog.Error("job failed", "job_id", j.ID, "type", j.Type, "error", err, "retry", j.RetryCount)
		_ = w.q.Ack(ctx, raw)
		w.handleFailure(ctx, j, err)
		return
	}

	if err := w.q.Complete(ctx, j, raw); err != nil {
		slog.Error("complete failed", "job_id", j.ID, "error", err)
	}
	slog.Info("job completed", "job_id", j.ID, "type", j.Type, "duration_ms", duration.Milliseconds())
	metrics.JobsProcessedTotal.WithLabelValues(j.Type, metrics.DefaultQueue).Inc()
}

func safeHandle(ctx context.Context, h Handler, j *job.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			slog.Error("handler panicked", "job_id", j.ID, "panic", r)
		}
	}()
	return h.Handle(ctx, j)
}

func (w *Worker) handleFailure(ctx context.Context, j *job.Job, err error) {
	j.RetryCount++
	metrics.JobsRetriedTotal.WithLabelValues(j.Type, metrics.DefaultQueue).Inc()

	if j.RetryCount >= 3 {
		_ = w.q.EnqueueDLQ(ctx, j, err.Error())
		metrics.JobsFailedTotal.WithLabelValues(j.Type, metrics.DefaultQueue).Inc()
		slog.Info("job moved to DLQ", "job_id", j.ID, "type", j.Type)
		return
	}

	backoff := time.Duration(1<<uint(j.RetryCount)) * time.Second
	jitter := time.Duration(0)
	if backoff >= 2 {
		jitter = time.Duration(rand.Int63n(int64(backoff / 2)))
	}
	delay := backoff + jitter
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}

	_ = w.q.ReleaseIdempotency(ctx, j.IdempotencyKey)

	slog.Info("job scheduled for retry", "job_id", j.ID, "retry", j.RetryCount, "delay", delay.String())
	jobCopy := *j
	time.AfterFunc(delay, func() {
		if err := w.q.Requeue(context.Background(), &jobCopy); err != nil {
			slog.Error("requeue failed", "job_id", jobCopy.ID, "error", err)
		}
	})
}

func (w *Worker) reaperLoop(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.q.ReapStale(ctx)
			if err != nil {
				slog.Error("reaper error", "error", err)
			}
			if n > 0 {
				metrics.JobsReapedTotal.Add(float64(n))
			}
			main, proc, dlq, err := w.q.Depths(ctx)
			if err != nil {
				continue
			}
			metrics.QueueDepth.Set(float64(main))
			metrics.ProcessingDepth.Set(float64(proc))
			metrics.DLQDepth.Set(float64(dlq))
			if lag, err := w.q.OldestJobAge(ctx); err == nil {
				metrics.QueueLagSeconds.Set(lag)
			}
		}
	}
}
