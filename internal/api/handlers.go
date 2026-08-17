package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

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
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type EnqueueResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

type HealthResponse struct {
	Status              string  `json:"status"`
	QueueDepth          int64   `json:"queue_depth"`
	ProcessingDepth     int64   `json:"processing_depth"`
	OldestJobAgeSeconds float64 `json:"oldest_job_age_seconds"`
	DLQDepth            int64   `json:"dlq_depth"`
}

func (h *Handler) Enqueue(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		http.Error(w, `{"error":"type required"}`, http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	j := job.New(req.Type, req.Payload, job.WithPriority(req.Priority), job.WithIdempotencyKey(req.IdempotencyKey))
	if err := h.q.Enqueue(r.Context(), j); err != nil {
		if errors.Is(err, queue.ErrIdempotencyConflict) {
			http.Error(w, `{"error":"idempotency key already exists"}`, http.StatusConflict)
			return
		}
		slog.Error("enqueue failed", "error", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	metrics.JobsEnqueuedTotal.WithLabelValues(req.Type, metrics.DefaultQueue).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(EnqueueResponse{JobID: j.ID, Status: "queued"})
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	main, proc, dlq, err := h.q.Depths(r.Context())
	if err != nil {
		slog.Error("health depth failed", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy"})
		return
	}
	lag, _ := h.q.OldestJobAge(r.Context())

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(HealthResponse{
		Status:              "healthy",
		QueueDepth:          main,
		ProcessingDepth:     proc,
		OldestJobAgeSeconds: lag,
		DLQDepth:            dlq,
	})
}
