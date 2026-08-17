package job

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Priority        int             `json:"priority,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	RetryCount      int             `json:"retry_count"`
	CreatedAt       time.Time       `json:"created_at"`
	VisibilityDeadline time.Time    `json:"visibility_deadline,omitempty"`
}

func New(jobType string, payload []byte, opts ...Option) *Job {
	j := &Job{
		ID:             uuid.NewV7().String(),
		Type:           jobType,
		Payload:        payload,
		RetryCount:     0,
		CreatedAt:      time.Now().UTC(),
	}
	for _, opt := range opts {
		opt(j)
	}
	return j
}

type Option func(*Job)

func WithPriority(p int) Option {
	return func(j *Job) { j.Priority = p }
}

func WithIdempotencyKey(key string) Option {
	return func(j *Job) { j.IdempotencyKey = key }
}

func (j *Job) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

func Unmarshal(data []byte) (*Job, error) {
	var j Job
	err := json.Unmarshal(data, &j)
	return &j, err
}