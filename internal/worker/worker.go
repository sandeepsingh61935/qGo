package worker

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/sandy/jobqueue/internal/job"
	"github.com/sandy/jobqueue/internal/metrics"
	"github.com/sandy/jobqueue/internal/queue"
)

type Handler interface {
	Handle(ctx context.Context, j *job.Job) error
}

type Worker struct {
	q            *queue.Queue
	concurrency  int
	handlers     map[string]Handler
	reaperTicker *time.Ticker
	wg           sync.WaitGroup
	sem          chan struct{}
}

func New(q *queue.Queue, concurrency int) *Worker {
	if concurrency <= 0 {
		concurrency = 4
	}
	return &Worker{
		q:           q,
		concurrency: concurrency,
		handlers:    make(map[string]Handler),
		sem:         make(chan struct{}, concurrency),
	}
}

func (w *Worker) RegisterHandler(jobType string, h Handler) {
	w.handlers[jobType] = h
}

func (w *Worker) Run(ctx context.Context) {
	w.reaperTicker = time.NewTicker(10 * time.Second)
	defer w.reaperTicker.Stop()

	go w.reaperLoop(ctx)

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
	w.sem <- struct{}{}
	defer func() { <-w.sem }()

	select {
	case <-ctx.Done():
		return
	default:
	}

	j, err := w.q.DequeueWithIdempotency(ctx, "")
	if err != nil {
		slog.Error("dequeue failed", "error", err)
		time.Sleep(100 * time.Millisecond)
		return
	}
	if j == nil {
		time.Sleep(500 * time.Millisecond)
		return
	}

	handler, ok := w.handlers[j.Type]
	if !ok {
		slog.Error("unknown job type", "type", j.Type, "job_id", j.ID)
		w.q.EnqueueDLQ(ctx, j, "unknown job type: "+j.Type)
		metrics.JobsFailedTotal.WithLabelValues(j.Type).Inc()
		return
	}

	metrics.JobsProcessingDepth.Inc()
	start := time.Now()
	slog.Info("job started", "job_id", j.ID, "type", j.Type, "retry", j.RetryCount)

	err = handler.Handle(ctx, j)
	duration := time.Since(start)
	metrics.JobsProcessingDepth.Dec()
	metrics.JobDurationSeconds.WithLabelValues(j.Type).Observe(duration.Seconds())

	if err != nil {
		slog.Error("job failed", "job_id", j.ID, "type", j.Type, "error", err, "retry", j.RetryCount)
		w.handleFailure(ctx, j, err)
		return
	}

	if err := w.q.Complete(ctx, j.ID, j.IdempotencyKey); err != nil {
		slog.Error("complete failed", "job_id", j.ID, "error", err)
	}
	slog.Info("job completed", "job_id", j.ID, "type", j.Type, "duration_ms", duration.Milliseconds())
	metrics.JobsProcessedTotal.WithLabelValues(j.Type).Inc()
}

func (w *Worker) handleFailure(ctx context.Context, j *job.Job, err error) {
	j.RetryCount++
	metrics.JobsRetriedTotal.WithLabelValues(j.Type).Inc()

	if j.RetryCount >= 3 {
		w.q.EnqueueDLQ(ctx, j, err.Error())
		metrics.JobsFailedTotal.WithLabelValues(j.Type).Inc()
		metrics.DLQDepth.Inc()
		slog.Info("job moved to DLQ", "job_id", j.ID, "type", j.Type)
		return
	}

	backoff := time.Duration(1<<j.RetryCount) * time.Second
	jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
	delay := backoff + jitter
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}

	slog.Info("job scheduled for retry", "job_id", j.ID, "retry", j.RetryCount, "delay", delay)
	time.AfterFunc(delay, func() {
		if err := w.q.Requeue(context.Background(), j); err != nil {
			slog.Error("requeue failed", "job_id", j.ID, "error", err)
		}
	})
}

func (w *Worker) reaperLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.reaperTicker.C:
			n, err := w.q.ReapStale(ctx)
			if err != nil {
				slog.Error("reaper error", "error", err)
			}
			if n > 0 {
				slog.Info("reaper requeued stale jobs", "count", n)
				metrics.JobsReapedTotal.Add(float64(n))
			}
			main, proc, dlq, _ := w.q.Depths(ctx)
			metrics.QueueDepth.Set(float64(main))
			metrics.ProcessingDepth.Set(float64(proc))
			metrics.DLQDepth.Set(float64(dlq))
			if lag, err := w.q.OldestJobAge(ctx); err == nil {
				metrics.QueueLagSeconds.Set(lag)
			}
		}
	}
}