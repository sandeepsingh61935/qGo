package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/sandy/jobqueue/internal/api"
	"github.com/sandy/jobqueue/internal/metrics"
	"github.com/sandy/jobqueue/internal/queue"
	"github.com/sandy/jobqueue/internal/worker"
)

func main() {
	mode := flag.String("mode", "api", "run mode: api or worker")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(env("LOG_LEVEL", "INFO")),
	}))
	slog.SetDefault(logger)

	cfg := loadConfig()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           0,
		PoolSize:     cfg.WorkerConcurrency + 5,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("redis ping failed", "error", err, "addr", cfg.RedisAddr)
		os.Exit(1)
	}

	q := queue.New(rdb, cfg.QueueName, cfg.VisibilityTimeout)
	metrics.Init(cfg.QueueName)

	switch *mode {
	case "api":
		runAPI(ctx, cfg, q)
	case "worker":
		runWorker(ctx, cfg, q)
	default:
		slog.Error("invalid mode", "mode", *mode)
		os.Exit(1)
	}
}

func runAPI(ctx context.Context, cfg *config, q *queue.Queue) {
	h := api.New(q)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /enqueue", h.Enqueue)
	mux.HandleFunc("GET /health", h.Health)
	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("api server starting", "port", cfg.Port, "redis", cfg.RedisAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("api server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("api shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func runWorker(ctx context.Context, cfg *config, q *queue.Queue) {
	// Expose Prometheus on workers separately (process-local counters).
	if cfg.MetricsPort != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("GET /metrics", promhttp.Handler())
			mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"status":"healthy","role":"worker"}`))
			})
			addr := ":" + cfg.MetricsPort
			slog.Info("worker metrics listening", "addr", addr)
			if err := http.ListenAndServe(addr, mux); err != nil {
				slog.Error("worker metrics server error", "error", err)
			}
		}()
	}

	w := worker.New(q, cfg.WorkerConcurrency)
	w.RegisterHandler("email.send", &EmailHandler{})
	w.RegisterHandler("echo", &EchoHandler{})

	slog.Info("worker starting",
		"concurrency", cfg.WorkerConcurrency,
		"queue", cfg.QueueName,
		"visibility_timeout", cfg.VisibilityTimeout,
	)
	w.Run(ctx)
	slog.Info("worker stopped")
}

type config struct {
	RedisAddr         string
	RedisPassword     string
	QueueName         string
	VisibilityTimeout int
	WorkerConcurrency int
	Port              string
	MetricsPort       string
}

func loadConfig() *config {
	concurrency := envInt("WORKER_CONCURRENCY", 0)
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
		if concurrency < 1 {
			concurrency = 4
		}
	}
	return &config{
		RedisAddr:         env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     env("REDIS_PASSWORD", ""),
		QueueName:         env("QUEUE_NAME", "jobs"),
		VisibilityTimeout: envInt("VISIBILITY_TIMEOUT", 30),
		WorkerConcurrency: concurrency,
		Port:              env("PORT", "8080"),
		MetricsPort:       env("METRICS_PORT", "9091"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "DEBUG", "debug":
		return slog.LevelDebug
	case "WARN", "warn":
		return slog.LevelWarn
	case "ERROR", "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
