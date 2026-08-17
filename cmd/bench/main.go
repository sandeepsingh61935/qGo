// Command bench runs load scenarios against a live Redis and prints a
// portfolio-friendly performance report (throughput + latency percentiles).
//
//	REDIS_ADDR=localhost:6379 go run ./cmd/bench
//	go run ./cmd/bench -jobs 20000 -producers 8 -consumers 8
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandy/jobqueue/internal/job"
	"github.com/sandy/jobqueue/internal/queue"
)

func main() {
	var (
		redisAddr   = flag.String("redis", env("REDIS_ADDR", "localhost:6379"), "Redis address")
		jobs        = flag.Int("jobs", 10_000, "jobs per scenario (enqueue/roundtrip target)")
		producers   = flag.Int("producers", runtime.NumCPU(), "concurrent producers")
		consumers   = flag.Int("consumers", runtime.NumCPU(), "concurrent consumers")
		payloadSize = flag.Int("payload-bytes", 64, "approx JSON payload body size")
		outPath     = flag.String("json", "", "optional path to write machine-readable results")
	)
	flag.Parse()

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:         *redisAddr,
		Password:     os.Getenv("REDIS_PASSWORD"),
		PoolSize:     *producers + *consumers + 8,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "redis ping failed (%s): %v\n", *redisAddr, err)
		fmt.Fprintf(os.Stderr, "start redis first, e.g.\n  podman run -d --name qgo-redis -p 6379:6379 redis:7-alpine\n")
		os.Exit(1)
	}

	payload := makePayload(*payloadSize)
	runID := time.Now().UTC().Format("20060102T150405Z")
	report := Report{
		GeneratedAt: time.Now().UTC(),
		RedisAddr:   *redisAddr,
		GoVersion:   runtime.Version(),
		GOMAXPROCS:  runtime.GOMAXPROCS(0),
		NumCPU:      runtime.NumCPU(),
		Config: Config{
			Jobs:         *jobs,
			Producers:    *producers,
			Consumers:    *consumers,
			PayloadBytes: len(payload),
		},
		Scenarios: make([]ScenarioResult, 0, 4),
	}

	fmt.Println("qGo performance bench")
	fmt.Println("=====================")
	fmt.Printf("redis=%s  jobs=%d  producers=%d  consumers=%d  payload=%dB  go=%s  procs=%d\n\n",
		*redisAddr, *jobs, *producers, *consumers, len(payload), runtime.Version(), runtime.GOMAXPROCS(0))

	// 1) Sequential enqueue latency distribution
	{
		name := fmt.Sprintf("bench-%s-enq", runID)
		q := queue.New(rdb, name, 30)
		defer flushQueue(rdb, name)
		res := runEnqueue(ctx, q, *jobs, 1, payload)
		res.Name = "enqueue_sequential"
		report.Scenarios = append(report.Scenarios, res)
		printScenario(res)
	}

	// 2) Parallel enqueue
	{
		name := fmt.Sprintf("bench-%s-enqp", runID)
		q := queue.New(rdb, name, 30)
		defer flushQueue(rdb, name)
		res := runEnqueue(ctx, q, *jobs, *producers, payload)
		res.Name = "enqueue_parallel"
		report.Scenarios = append(report.Scenarios, res)
		printScenario(res)
	}

	// 3) Parallel dequeue (pre-filled)
	{
		name := fmt.Sprintf("bench-%s-deq", runID)
		q := queue.New(rdb, name, 30)
		defer flushQueue(rdb, name)
		if err := seed(ctx, q, *jobs, payload); err != nil {
			fatalf("seed: %v", err)
		}
		res := runDequeue(ctx, q, *jobs, *consumers)
		res.Name = "dequeue_parallel"
		report.Scenarios = append(report.Scenarios, res)
		printScenario(res)
	}

	// 4) End-to-end: producers + consumers (enqueue → dequeue → complete)
	{
		name := fmt.Sprintf("bench-%s-e2e", runID)
		q := queue.New(rdb, name, 30)
		defer flushQueue(rdb, name)
		res := runE2E(ctx, q, *jobs, *producers, *consumers, payload)
		res.Name = "e2e_produce_consume"
		report.Scenarios = append(report.Scenarios, res)
		printScenario(res)
	}

	printSummary(report)

	if *outPath != "" {
		raw, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fatalf("marshal report: %v", err)
		}
		if err := os.WriteFile(*outPath, raw, 0o644); err != nil {
			fatalf("write json: %v", err)
		}
		fmt.Printf("\nwrote %s\n", *outPath)
	}
}

type Config struct {
	Jobs         int `json:"jobs"`
	Producers    int `json:"producers"`
	Consumers    int `json:"consumers"`
	PayloadBytes int `json:"payload_bytes"`
}

type Report struct {
	GeneratedAt time.Time        `json:"generated_at"`
	RedisAddr   string           `json:"redis_addr"`
	GoVersion   string           `json:"go_version"`
	GOMAXPROCS  int              `json:"gomaxprocs"`
	NumCPU      int              `json:"num_cpu"`
	Config      Config           `json:"config"`
	Scenarios   []ScenarioResult `json:"scenarios"`
}

type ScenarioResult struct {
	Name          string   `json:"name"`
	Ops           int      `json:"ops"`
	Errors        int64    `json:"errors"`
	DurationSec   float64  `json:"duration_sec"`
	ThroughputOps float64  `json:"throughput_ops_per_sec"`
	Latency       Latency  `json:"latency_ms"`
	Process       *Latency `json:"process_latency_ms,omitempty"` // dequeue→complete only (e2e)
}

type Latency struct {
	Min  float64 `json:"min"`
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

func runEnqueue(ctx context.Context, q *queue.Queue, n, workers int, payload []byte) ScenarioResult {
	if workers < 1 {
		workers = 1
	}
	lat := make([]time.Duration, n)
	var errCount atomic.Int64
	var idx atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(idx.Add(1) - 1)
				if i >= n {
					return
				}
				j := job.New("bench.enqueue", payload)
				t0 := time.Now()
				err := q.Enqueue(ctx, j)
				lat[i] = time.Since(t0)
				if err != nil {
					errCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	return finish("enqueue", n, errCount.Load(), elapsed, lat)
}

func runDequeue(ctx context.Context, q *queue.Queue, n, workers int) ScenarioResult {
	if workers < 1 {
		workers = 1
	}
	lat := make([]time.Duration, n)
	var errCount atomic.Int64
	var done atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// Claim a slot first so we don't over-dequeue.
				i := done.Add(1) - 1
				if i >= int64(n) {
					return
				}
				t0 := time.Now()
				for {
					d, err := q.Dequeue(ctx)
					if err != nil {
						errCount.Add(1)
						lat[i] = time.Since(t0)
						break
					}
					if d != nil {
						lat[i] = time.Since(t0)
						break
					}
					// Brief spin on empty (should be rare when pre-filled).
					time.Sleep(50 * time.Microsecond)
					if time.Since(t0) > 10*time.Second {
						errCount.Add(1)
						lat[i] = time.Since(t0)
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	return finish("dequeue", n, errCount.Load(), elapsed, lat)
}

type latPair struct {
	sojourn time.Duration // CreatedAt → Complete (includes queue wait)
	process time.Duration // Dequeue start → Complete (queue path only)
}

func runE2E(ctx context.Context, q *queue.Queue, n, producers, consumers int, payload []byte) ScenarioResult {
	if producers < 1 {
		producers = 1
	}
	if consumers < 1 {
		consumers = 1
	}

	latCh := make(chan latPair, n)
	var produced atomic.Int64
	var completed atomic.Int64
	var errCount atomic.Int64

	prodCtx, cancelProd := context.WithCancel(ctx)
	defer cancelProd()
	consCtx, cancelCons := context.WithCancel(ctx)
	defer cancelCons()

	start := time.Now()
	var prodWG, consWG sync.WaitGroup

	for w := 0; w < producers; w++ {
		prodWG.Add(1)
		go func() {
			defer prodWG.Done()
			for {
				i := produced.Add(1)
				if i > int64(n) {
					return
				}
				j := job.New("bench.e2e", payload)
				if err := q.Enqueue(prodCtx, j); err != nil {
					errCount.Add(1)
				}
			}
		}()
	}

	for w := 0; w < consumers; w++ {
		consWG.Add(1)
		go func() {
			defer consWG.Done()
			for {
				if completed.Load() >= int64(n) {
					return
				}
				select {
				case <-consCtx.Done():
					return
				default:
				}
				t0 := time.Now()
				d, err := q.Dequeue(consCtx)
				if err != nil {
					if consCtx.Err() != nil {
						return
					}
					errCount.Add(1)
					continue
				}
				if d == nil {
					time.Sleep(100 * time.Microsecond)
					continue
				}
				// Handler is intentionally no-op: measure queue path, not business logic.
				if err := q.Complete(consCtx, d.Job, d.Raw); err != nil {
					errCount.Add(1)
					continue
				}
				c := completed.Add(1)
				latCh <- latPair{
					sojourn: time.Since(d.Job.CreatedAt),
					process: time.Since(t0),
				}
				if c >= int64(n) {
					cancelCons()
					return
				}
			}
		}()
	}

	prodWG.Wait()
	// Wait until all jobs completed or timeout.
	deadline := time.After(2 * time.Minute)
	for completed.Load() < int64(n) {
		select {
		case <-deadline:
			cancelCons()
			errCount.Add(int64(n) - completed.Load())
			goto done
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
done:
	cancelCons()
	consWG.Wait()
	close(latCh)
	elapsed := time.Since(start)

	sojourn := make([]time.Duration, 0, n)
	process := make([]time.Duration, 0, n)
	for p := range latCh {
		sojourn = append(sojourn, p.sojourn)
		process = append(process, p.process)
	}
	ops := int(completed.Load())
	res := finish("e2e", ops, errCount.Load(), elapsed, sojourn)
	proc := summarize(process)
	res.Process = &proc
	return res
}

func seed(ctx context.Context, q *queue.Queue, n int, payload []byte) error {
	for i := 0; i < n; i++ {
		j := job.New("bench.seed", payload)
		if err := q.Enqueue(ctx, j); err != nil {
			return err
		}
	}
	return nil
}

func finish(name string, ops int, errs int64, elapsed time.Duration, samples []time.Duration) ScenarioResult {
	_ = name
	sec := elapsed.Seconds()
	tps := 0.0
	if sec > 0 {
		tps = float64(ops) / sec
	}
	return ScenarioResult{
		Ops:           ops,
		Errors:        errs,
		DurationSec:   sec,
		ThroughputOps: tps,
		Latency:       summarize(samples),
	}
}

func summarize(samples []time.Duration) Latency {
	if len(samples) == 0 {
		return Latency{}
	}
	ns := make([]float64, len(samples))
	var sum float64
	for i, d := range samples {
		ms := float64(d.Nanoseconds()) / 1e6
		ns[i] = ms
		sum += ms
	}
	sort.Float64s(ns)
	return Latency{
		Min:  ns[0],
		P50:  percentile(ns, 50),
		P95:  percentile(ns, 95),
		P99:  percentile(ns, 99),
		Max:  ns[len(ns)-1],
		Mean: sum / float64(len(ns)),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	// Nearest-rank.
	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func printScenario(s ScenarioResult) {
	fmt.Printf("## %s\n", s.Name)
	fmt.Printf("  ops=%d  errors=%d  duration=%.3fs  throughput=%.0f ops/s\n",
		s.Ops, s.Errors, s.DurationSec, s.ThroughputOps)
	label := "latency_ms"
	if s.Process != nil {
		label = "sojourn_ms (create→done)"
	}
	fmt.Printf("  %s: min=%.3f  p50=%.3f  p95=%.3f  p99=%.3f  max=%.3f  mean=%.3f\n",
		label, s.Latency.Min, s.Latency.P50, s.Latency.P95, s.Latency.P99, s.Latency.Max, s.Latency.Mean)
	if s.Process != nil {
		fmt.Printf("  process_ms (dequeue→done): min=%.3f  p50=%.3f  p95=%.3f  p99=%.3f  max=%.3f  mean=%.3f\n",
			s.Process.Min, s.Process.P50, s.Process.P95, s.Process.P99, s.Process.Max, s.Process.Mean)
	}
	fmt.Println()
}

func printSummary(r Report) {
	fmt.Println("Summary (copy for README / resume)")
	fmt.Println("----------------------------------")
	fmt.Printf("| scenario | ops/s | p50 ms | p95 ms | p99 ms |\n")
	fmt.Printf("|----------|------:|-------:|-------:|-------:|\n")
	for _, s := range r.Scenarios {
		fmt.Printf("| %s | %.0f | %.2f | %.2f | %.2f |\n",
			s.Name, s.ThroughputOps, s.Latency.P50, s.Latency.P95, s.Latency.P99)
		if s.Process != nil {
			fmt.Printf("| %s (process only) | — | %.2f | %.2f | %.2f |\n",
				s.Name, s.Process.P50, s.Process.P95, s.Process.P99)
		}
	}
	fmt.Println()
	fmt.Println("Notes: local Redis; e2e sojourn includes queue wait under producer burst;")
	fmt.Println("process = dequeue→complete with no-op handler. Single-host lab numbers, not multi-AZ SLOs.")
}

func makePayload(n int) []byte {
	if n < 8 {
		n = 8
	}
	body := make([]byte, n)
	for i := range body {
		body[i] = 'a' + byte(i%26)
	}
	raw, _ := json.Marshal(map[string]string{"data": string(body)})
	return raw
}

func flushQueue(rdb *redis.Client, name string) {
	ctx := context.Background()
	_ = rdb.Del(ctx, name, name+":processing", name+":dlq").Err()
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
