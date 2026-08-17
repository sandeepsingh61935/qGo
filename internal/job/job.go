package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Job is the unit of work stored in Redis lists as JSON.
type Job struct {
	ID                 string          `json:"id"`
	Type               string          `json:"type"`
	Payload            json.RawMessage `json:"payload"`
	Priority           int             `json:"priority,omitempty"`
	IdempotencyKey     string          `json:"idempotency_key,omitempty"`
	RetryCount         int             `json:"retry_count"`
	CreatedAt          time.Time       `json:"created_at"`
	VisibilityDeadline time.Time       `json:"visibility_deadline,omitempty"`
}

// New builds a job with a UUIDv7 id and UTC created_at.
func New(jobType string, payload []byte, opts ...Option) *Job {
	j := &Job{
		ID:         mustV7(),
		Type:       jobType,
		Payload:    payload,
		RetryCount: 0,
		CreatedAt:  time.Now().UTC(),
	}
	for _, opt := range opts {
		opt(j)
	}
	if j.IdempotencyKey == "" {
		j.IdempotencyKey = j.ID
	}
	return j
}

// NewID returns a new UUIDv7 string.
func NewID() string {
	return mustV7()
}

func mustV7() string {
	id, err := uuid.NewV7()
	if err != nil {
		// Extremely unlikely; fall back to v4 so the process never panics on id gen.
		return uuid.NewString()
	}
	return id.String()
}

type Option func(*Job)

func WithPriority(p int) Option {
	return func(j *Job) { j.Priority = p }
}

func WithIdempotencyKey(key string) Option {
	return func(j *Job) {
		if key != "" {
			j.IdempotencyKey = key
		}
	}
}

func (j *Job) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

func Unmarshal(data []byte) (*Job, error) {
	var j Job
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}
	return &j, nil
}
