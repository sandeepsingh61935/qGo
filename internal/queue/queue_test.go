package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandy/jobqueue/internal/job"
)

func TestEnqueueDequeueComplete(t *testing.T) {
	rdb, name := testRedis(t)
	q := New(rdb, name, 30)
	ctx := context.Background()

	j := job.New("echo", []byte(`{"msg":"hi"}`))
	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}
	d, err := q.Dequeue(ctx)
	if err != nil || d == nil {
		t.Fatalf("dequeue: d=%v err=%v", d, err)
	}
	if d.Job.ID != j.ID {
		t.Fatalf("id %s != %s", d.Job.ID, j.ID)
	}
	if err := q.Complete(ctx, d.Job, d.Raw); err != nil {
		t.Fatal(err)
	}
	main, proc, dlq, err := q.Depths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if main != 0 || proc != 0 || dlq != 0 {
		t.Fatalf("depths main=%d proc=%d dlq=%d", main, proc, dlq)
	}
}

func TestIdempotencyConflict(t *testing.T) {
	rdb, name := testRedis(t)
	q := New(rdb, name, 30)
	ctx := context.Background()

	key := name + "-idemp"
	a := job.New("echo", []byte(`{}`), job.WithIdempotencyKey(key))
	b := job.New("echo", []byte(`{}`), job.WithIdempotencyKey(key))
	if err := q.Enqueue(ctx, a); err != nil {
		t.Fatal(err)
	}
	err := q.Enqueue(ctx, b)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestRetryThenDLQ(t *testing.T) {
	rdb, name := testRedis(t)
	q := New(rdb, name, 30)
	ctx := context.Background()

	j := job.New("fail", []byte(`{}`))
	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}
	d, err := q.Dequeue(ctx)
	if err != nil || d == nil {
		t.Fatalf("dequeue: %v %v", d, err)
	}
	if err := q.Ack(ctx, d.Raw); err != nil {
		t.Fatal(err)
	}
	if err := q.EnqueueDLQ(ctx, d.Job, "boom"); err != nil {
		t.Fatal(err)
	}
	_, _, dlq, err := q.Depths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if dlq != 1 {
		t.Fatalf("dlq=%d want 1", dlq)
	}
}

func TestReapStale(t *testing.T) {
	rdb, name := testRedis(t)
	q := New(rdb, name, 1) // 1s visibility
	ctx := context.Background()

	j := job.New("echo", []byte(`{}`))
	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}
	d, err := q.Dequeue(ctx)
	if err != nil || d == nil {
		t.Fatalf("dequeue: %v %v", d, err)
	}
	// Force deadline into the past on the processing entry.
	d.Job.VisibilityDeadline = time.Now().UTC().Add(-time.Second)
	raw, err := d.Job.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.LSet(ctx, name+processingSuffix, 0, raw).Err(); err != nil {
		t.Fatal(err)
	}

	n, err := q.ReapStale(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reaped=%d want 1", n)
	}
	main, proc, _, err := q.Depths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if main != 1 || proc != 0 {
		t.Fatalf("main=%d proc=%d", main, proc)
	}
}
