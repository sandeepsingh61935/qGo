package queue

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandy/jobqueue/internal/job"
)

const (
	mainQueueKey      = "jobs"
	processingQueueKey = "jobs:processing"
	dlqKey            = "jobs:dlq"
	idempotencyPrefix = "idemp:"
)

var dequeueScript = redis.NewScript(`
local main_key = KEYS[1]
local processing_key = KEYS[2]
local idemp_prefix = KEYS[3]
local visibility_timeout = tonumber(ARGV[1])
local job_id = ARGV[2]
local idemp_key = ARGV[3]

local job = redis.call('RPOPLPUSH', main_key, processing_key)
if not job then
	return nil
end

local job_data = cjson.decode(job)
local deadline = redis.call('TIME')
deadline = deadline[1] + visibility_timeout
job_data.visibility_deadline = deadline

local idemp_full_key = idemp_prefix .. idemp_key
local idemp_set = redis.call('SET', idemp_full_key, '1', 'EX', visibility_timeout + 86400, 'NX')
if not idemp_set then
	redis.call('LREM', processing_key, 1, job)
	return {err='IDEMPOTENCY_CONFLICT'}
end

redis.call('EXPIRE', processing_key, visibility_timeout + 10)
return cjson.encode(job_data)
`)

type Queue struct {
	client           *redis.Client
	name             string
	visibilityTimeout time.Duration
}

func New(client *redis.Client, name string, visibilityTimeoutSec int) *Queue {
	return &Queue{
		client:            client,
		name:              name,
		visibilityTimeout: time.Duration(visibilityTimeoutSec) * time.Second,
	}
}

func (q *Queue) Enqueue(ctx context.Context, j *job.Job) error {
	if j.IdempotencyKey == "" {
		j.IdempotencyKey = j.ID
	}
	data, err := j.Marshal()
	if err != nil {
		return err
	}
	return q.client.RPush(ctx, q.name, data).Err()
}

func (q *Queue) Dequeue(ctx context.Context) (*job.Job, error) {
	idempKey := "auto:" + uuid.NewV7().String() // placeholder, actual key passed from worker
	res, err := dequeueScript.Run(ctx, q.client, []string{
		q.name,
		processingQueueKey,
		idempotencyPrefix,
	}, int(q.visibilityTimeout.Seconds()), "", idempKey).Text()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var j job.Job
	if err := json.Unmarshal([]byte(res), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func (q *Queue) DequeueWithIdempotency(ctx context.Context, idempKey string) (*job.Job, error) {
	res, err := dequeueScript.Run(ctx, q.client, []string{
		q.name,
		processingQueueKey,
		idempotencyPrefix,
	}, int(q.visibilityTimeout.Seconds()), "", idempKey).Text()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var j job.Job
	if err := json.Unmarshal([]byte(res), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func (q *Queue) Complete(ctx context.Context, jobID, idempKey string) error {
	pipe := q.client.Pipeline()
	pipe.LRem(ctx, processingQueueKey, 1, jobID)
	pipe.Del(ctx, idempotencyPrefix+idempKey)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *Queue) Requeue(ctx context.Context, j *job.Job) error {
	data, err := j.Marshal()
	if err != nil {
		return err
	}
	return q.client.LPush(ctx, q.name, data).Err()
}

func (q *Queue) EnqueueDLQ(ctx context.Context, j *job.Job, errMsg string) error {
	dlqEntry := map[string]any{
		"job":        j,
		"error":      errMsg,
		"failed_at":  time.Now().UTC(),
	}
	data, _ := json.Marshal(dlqEntry)
	return q.client.RPush(ctx, dlqKey, data).Err()
}

func (q *Queue) Depths(ctx context.Context) (main, processing, dlq int64, err error) {
	pipe := q.client.Pipeline()
	mainCmd := pipe.LLen(ctx, q.name)
	procCmd := pipe.LLen(ctx, processingQueueKey)
	dlqCmd := pipe.LLen(ctx, dlqKey)
	_, err = pipe.Exec(ctx)
	return mainCmd.Val(), procCmd.Val(), dlqCmd.Val(), err
}

func (q *Queue) OldestJobAge(ctx context.Context) (float64, error) {
	val, err := q.client.LIndex(ctx, q.name, 0).Bytes()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var j job.Job
	if err := json.Unmarshal(val, &j); err != nil {
		return 0, err
	}
	return time.Since(j.CreatedAt).Seconds(), nil
}

func (q *Queue) ReapStale(ctx context.Context) (int, error) {
	// Scan processing list for expired visibility deadlines
	jobs, err := q.client.LRange(ctx, processingQueueKey, 0, -1).Result()
	if err != nil {
		return 0, err
	}
	reaped := 0
	now := time.Now().Unix()
	for _, jdata := range jobs {
		var j job.Job
		if err := json.Unmarshal([]byte(jdata), &j); err != nil {
			continue
		}
		if !j.VisibilityDeadline.IsZero() && now > j.VisibilityDeadline.Unix() {
			// Move back to main queue
			if err := q.client.LMove(ctx, processingQueueKey, q.name, "RIGHT", "LEFT").Err(); err != nil {
				slog.Error("reaper move failed", "error", err)
				continue
			}
			// Clean up idempotency key
			if j.IdempotencyKey != "" {
				q.client.Del(ctx, idempotencyPrefix+j.IdempotencyKey)
			}
			reaped++
		}
	}
	return reaped, nil
}