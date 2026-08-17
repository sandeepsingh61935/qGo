package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandy/jobqueue/internal/job"
)

const (
	processingSuffix  = ":processing"
	dlqSuffix         = ":dlq"
	idempotencyPrefix = "idemp:"
	completedPrefix   = "done:"
)

// ErrIdempotencyConflict is returned when an idempotency key was already used at enqueue.
var ErrIdempotencyConflict = errors.New("idempotency key already exists")

// Dequeued is a job plus the exact Redis payload sitting in the processing list.
// Complete/Ack must use Raw so LREM matches.
type Dequeued struct {
	Job *job.Job
	Raw []byte
}

type Queue struct {
	client            *redis.Client
	name              string
	visibilityTimeout time.Duration
}

func New(client *redis.Client, name string, visibilityTimeoutSec int) *Queue {
	if name == "" {
		name = "jobs"
	}
	if visibilityTimeoutSec <= 0 {
		visibilityTimeoutSec = 30
	}
	return &Queue{
		client:            client,
		name:              name,
		visibilityTimeout: time.Duration(visibilityTimeoutSec) * time.Second,
	}
}

func (q *Queue) mainKey() string       { return q.name }
func (q *Queue) processingKey() string { return q.name + processingSuffix }
func (q *Queue) dlqKey() string        { return q.name + dlqSuffix }

// Enqueue pushes a job. IdempotencyKey SETNX rejects duplicates for 24h.
func (q *Queue) Enqueue(ctx context.Context, j *job.Job) error {
	if j.IdempotencyKey == "" {
		j.IdempotencyKey = j.ID
	}

	ok, err := q.client.SetNX(ctx, idempotencyPrefix+j.IdempotencyKey, j.ID, 24*time.Hour).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ErrIdempotencyConflict
	}

	data, err := j.Marshal()
	if err != nil {
		_ = q.client.Del(ctx, idempotencyPrefix+j.IdempotencyKey).Err()
		return err
	}
	if err := q.client.RPush(ctx, q.mainKey(), data).Err(); err != nil {
		_ = q.client.Del(ctx, idempotencyPrefix+j.IdempotencyKey).Err()
		return err
	}
	return nil
}

// Dequeue moves one job main → processing, stamps visibility deadline, returns exact raw.
// Empty queue → (nil, nil).
func (q *Queue) Dequeue(ctx context.Context) (*Dequeued, error) {
	raw, err := q.client.RPopLPush(ctx, q.mainKey(), q.processingKey()).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	j, err := job.Unmarshal(raw)
	if err != nil {
		// Poison payload: drop from processing into DLQ-shaped entry.
		_ = q.client.LRem(ctx, q.processingKey(), 1, raw).Err()
		return nil, fmt.Errorf("corrupt job payload: %w", err)
	}

	// Skip if already completed under this idempotency key (reaper requeue race).
	if j.IdempotencyKey != "" {
		n, err := q.client.Exists(ctx, completedPrefix+j.IdempotencyKey).Result()
		if err != nil {
			return nil, err
		}
		if n > 0 {
			_ = q.client.LRem(ctx, q.processingKey(), 1, raw).Err()
			return nil, nil
		}
	}

	j.VisibilityDeadline = time.Now().UTC().Add(q.visibilityTimeout)
	updated, err := j.Marshal()
	if err != nil {
		return nil, err
	}

	// Replace processing entry with stamped job so reaper sees deadline.
	pipe := q.client.Pipeline()
	pipe.LRem(ctx, q.processingKey(), 1, raw)
	pipe.LPush(ctx, q.processingKey(), updated)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	return &Dequeued{Job: j, Raw: updated}, nil
}

// Complete removes the processing entry and marks the key as done (24h).
func (q *Queue) Complete(ctx context.Context, j *job.Job, raw []byte) error {
	if len(raw) == 0 {
		var err error
		raw, err = j.Marshal()
		if err != nil {
			return err
		}
	}
	if err := q.client.LRem(ctx, q.processingKey(), 1, raw).Err(); err != nil {
		return err
	}
	if j.IdempotencyKey != "" {
		if err := q.client.Set(ctx, completedPrefix+j.IdempotencyKey, j.ID, 24*time.Hour).Err(); err != nil {
			return err
		}
	}
	return nil
}

// Ack removes a processing entry without marking completed (retry / DLQ path).
func (q *Queue) Ack(ctx context.Context, raw []byte) error {
	return q.client.LRem(ctx, q.processingKey(), 1, raw).Err()
}

// ReleaseIdempotency clears the enqueue lock so a retry can re-enter.
func (q *Queue) ReleaseIdempotency(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	return q.client.Del(ctx, idempotencyPrefix+key).Err()
}

// Requeue puts a job back on the main queue.
func (q *Queue) Requeue(ctx context.Context, j *job.Job) error {
	j.VisibilityDeadline = time.Time{}
	data, err := j.Marshal()
	if err != nil {
		return err
	}
	if j.IdempotencyKey != "" {
		_ = q.client.Set(ctx, idempotencyPrefix+j.IdempotencyKey, j.ID, 24*time.Hour).Err()
	}
	return q.client.LPush(ctx, q.mainKey(), data).Err()
}

// EnqueueDLQ records a permanent failure.
func (q *Queue) EnqueueDLQ(ctx context.Context, j *job.Job, errMsg string) error {
	entry := map[string]any{
		"job":       j,
		"error":     errMsg,
		"failed_at": time.Now().UTC(),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err := q.client.RPush(ctx, q.dlqKey(), data).Err(); err != nil {
		return err
	}
	if j.IdempotencyKey != "" {
		_ = q.client.Del(ctx, idempotencyPrefix+j.IdempotencyKey).Err()
	}
	return nil
}

func (q *Queue) Depths(ctx context.Context) (main, processing, dlq int64, err error) {
	pipe := q.client.Pipeline()
	mainCmd := pipe.LLen(ctx, q.mainKey())
	procCmd := pipe.LLen(ctx, q.processingKey())
	dlqCmd := pipe.LLen(ctx, q.dlqKey())
	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	return mainCmd.Val(), procCmd.Val(), dlqCmd.Val(), nil
}

func (q *Queue) OldestJobAge(ctx context.Context) (float64, error) {
	// RPUSH → oldest is index 0 (head).
	val, err := q.client.LIndex(ctx, q.mainKey(), 0).Bytes()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	j, err := job.Unmarshal(val)
	if err != nil {
		return 0, err
	}
	if j.CreatedAt.IsZero() {
		return 0, nil
	}
	return time.Since(j.CreatedAt).Seconds(), nil
}

// ReapStale requeues jobs whose visibility deadline has passed.
func (q *Queue) ReapStale(ctx context.Context) (int, error) {
	jobs, err := q.client.LRange(ctx, q.processingKey(), 0, -1).Result()
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	reaped := 0
	for _, jdata := range jobs {
		j, err := job.Unmarshal([]byte(jdata))
		if err != nil {
			continue
		}
		if j.VisibilityDeadline.IsZero() || now.Before(j.VisibilityDeadline) {
			continue
		}
		if err := q.client.LRem(ctx, q.processingKey(), 1, jdata).Err(); err != nil {
			slog.Error("reaper lrem failed", "error", err, "job_id", j.ID)
			continue
		}
		if err := q.Requeue(ctx, j); err != nil {
			slog.Error("reaper requeue failed", "error", err, "job_id", j.ID)
			continue
		}
		reaped++
		slog.Info("reaper requeued stale job", "job_id", j.ID, "type", j.Type)
	}
	return reaped, nil
}

func (q *Queue) String() string {
	return fmt.Sprintf("queue(%s, vt=%s)", q.name, q.visibilityTimeout)
}
