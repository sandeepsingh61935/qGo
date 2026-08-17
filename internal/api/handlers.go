package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/sandy/jobqueue/internal/job"
	"github.com/sandy/jobqueue/internal/metrics"
	"github.com/sandy/jobqueue/internal/queue"
)

type Handler struct {
	q *queue.Queue
}

func New(q *queue.Queue) *Handler {
	return &Handler{q: q}
}

type EnqueueRequest struct {
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Priority        int             `json:"priority,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
}

type EnqueueResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

type HealthResponse struct {
	Status             string  `json:"status"`
	QueueDepth         int64   `json:"queue_depth"`
	ProcessingDepth    int64   `json:"processing_depth"`
	OldestJobAgeSeconds float64 `json:"oldest_job_age_seconds"`
	DLQDepth           int64   `json:"dlq_depth"`
}

func (h *Handler) Enqueue(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		http.Error(w, "type required", http.StatusBadRequest)
		return
	}

	idempKey := req.IdempotencyKey
	if idempKey == "" {
		idempKey = uuid.NewV7().String()
	}

	j := job.New(req.Type, req.Payload, job.WithPriority(req.Priority), job.WithIdempotencyKey(idempKey))
	if err := h.q.Enqueue(r.Context(), j); err != nil {
		slog.Error("enqueue failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	metrics.JobsEnqueuedTotal.WithLabelValues(req.Type).Inc()
	metrics.QueueDepth.Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(EnqueueResponse{JobID: j.ID, Status: "queued"})
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	main, proc, dlq, err := h.q.Depths(r.Context())
	if err != nil {
		slog.Error("health depth failed", "error", err)
		http.Error(w, "redis error", http.StatusServiceUnavailable)
		return
	}
	lag, _ := h.q.OldestJobAge(r.Context())

	status := "healthy"
	if err != nil {
		status = "unhealthy"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(HealthResponse{
		Status:              status,
		QueueDepth:          main,
		ProcessingDepth:     proc,
		OldestJobAgeSeconds: lag,
		DLQDepth:            dlq,
	})
}