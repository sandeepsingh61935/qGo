package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandy/jobqueue/internal/job"
)

// EmailHandler is a demo handler for type "email.send".
// Payload: {"to":"...","subject":"..."} — logs delivery (no real SMTP).
type EmailHandler struct{}

func (h *EmailHandler) Handle(ctx context.Context, j *job.Job) error {
	var p struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}
	if len(j.Payload) > 0 {
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return fmt.Errorf("invalid email payload: %w", err)
		}
	}
	if p.To == "" {
		return fmt.Errorf("email payload missing 'to'")
	}

	// Simulate work.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}

	slog.Info("email sent",
		"job_id", j.ID,
		"to", p.To,
		"subject", p.Subject,
		"retry", j.RetryCount,
	)
	return nil
}

// EchoHandler echoes payload for type "echo" — useful for smoke tests.
type EchoHandler struct{}

func (h *EchoHandler) Handle(ctx context.Context, j *job.Job) error {
	slog.Info("echo job", "job_id", j.ID, "payload", string(j.Payload))
	return nil
}
