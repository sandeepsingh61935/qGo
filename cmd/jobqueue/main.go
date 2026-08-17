package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
		Level: slog.LevelInfo,
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
		slog.Error("redis ping failed", "error", err)
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
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	go func() {
		slog.Info("api server starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("api server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("api shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func runWorker(ctx context.Context, cfg *config, q *queue.Queue) {
	w := worker.New(q, cfg.WorkerConcurrency)
	slog.Info("worker starting", "concurrency", cfg.WorkerConcurrency)
	w.Run(ctx)
	slog.Info("worker stopped")
}

type config struct {
	RedisAddr          string `envconfig:"REDIS_ADDR" default:"localhost:6379"`
	RedisPassword      string `envconfig:"REDIS_PASSWORD" default:""`
	QueueName          string `envconfig:"QUEUE_NAME" default:"jobs"`
	VisibilityTimeout  int    `envconfig:"VISIBILITY_TIMEOUT" default:"30"`
	WorkerConcurrency  int    `envconfig:"WORKER_CONCURRENCY" default:"0"`
	Port               string `envconfig:"PORT" default:"8080"`
}

func loadConfig() *config {
	var cfg config
	// envconfig.Process would go here; inline defaults for brevity
	if cfg.WorkerConcurrency == 0 {
		cfg.WorkerConcurrency = 4 // fallback
	}
	return &cfg
}