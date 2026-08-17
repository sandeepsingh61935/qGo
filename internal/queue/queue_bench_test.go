package queue

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/sandy/jobqueue/internal/job"
)

func BenchmarkEnqueue(b *testing.B) {
	rdb, name := testRedis(b)
	q := New(rdb, name, 30)
	ctx := context.Background()
	payload := []byte(`{"msg":"bench"}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := job.New("bench.enqueue", payload)
		if err := q.Enqueue(ctx, j); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDequeue(b *testing.B) {
	rdb, name := testRedis(b)
	q := New(rdb, name, 30)
	ctx := context.Background()
	payload := []byte(`{"msg":"bench"}`)

	// Pre-fill so Dequeue never hits empty-queue path during the timed loop.
	for i := 0; i < b.N; i++ {
		j := job.New("bench.dequeue", payload)
		if err := q.Enqueue(ctx, j); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := q.Dequeue(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if d == nil {
			b.Fatal("empty queue during benchmark")
		}
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	rdb, name := testRedis(b)
	q := New(rdb, name, 30)
	ctx := context.Background()
	payload := []byte(`{"msg":"bench"}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := job.New("bench.roundtrip", payload)
		if err := q.Enqueue(ctx, j); err != nil {
			b.Fatal(err)
		}
		d, err := q.Dequeue(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if d == nil {
			b.Fatal("empty queue during roundtrip")
		}
		if err := q.Complete(ctx, d.Job, d.Raw); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnqueueParallel(b *testing.B) {
	rdb, name := testRedis(b)
	q := New(rdb, name, 30)
	ctx := context.Background()
	payload := []byte(`{"msg":"bench"}`)
	var n atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := n.Add(1)
			j := job.New("bench.parallel", payload, job.WithIdempotencyKey(
				fmt.Sprintf("par-%d-%d", id, b.N),
			))
			if err := q.Enqueue(ctx, j); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkJobMarshal(b *testing.B) {
	j := job.New("bench.marshal", []byte(`{"to":"a@b.c","subject":"hi"}`), job.WithPriority(5))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := j.Marshal(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJobUnmarshal(b *testing.B) {
	j := job.New("bench.unmarshal", []byte(`{"to":"a@b.c","subject":"hi"}`), job.WithPriority(5))
	raw, err := j.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := job.Unmarshal(raw); err != nil {
			b.Fatal(err)
		}
	}
}
