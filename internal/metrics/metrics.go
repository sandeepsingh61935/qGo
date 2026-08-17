package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JobsEnqueuedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_enqueued_total",
		Help: "Total number of jobs enqueued",
	}, []string{"type", "queue"})

	JobsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_processed_total",
		Help: "Total number of jobs processed successfully",
	}, []string{"type", "queue"})

	JobsFailedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_failed_total",
		Help: "Total number of jobs moved to DLQ",
	}, []string{"type", "queue"})

	JobsRetriedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_retried_total",
		Help: "Total number of job retries",
	}, []string{"type", "queue"})

	JobsReapedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_reaped_total",
		Help: "Total number of stale jobs reaped by visibility timeout",
	})

	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_depth",
		Help: "Current number of jobs in main queue",
	})

	ProcessingDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "processing_depth",
		Help: "Current number of jobs in processing",
	})

	DLQDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dlq_depth",
		Help: "Current number of jobs in dead-letter queue",
	})

	JobDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "job_duration_seconds",
		Help:    "Job processing duration in seconds",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60},
	}, []string{"type", "queue"})

	QueueLagSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_lag_seconds",
		Help: "Age of oldest job in queue (seconds)",
	})
)

func Init(queueName string) {
	// Labels are set per-metric at emission site
	_ = queueName // placeholder for future per-queue labeling
}