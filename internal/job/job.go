// Package job defines the core Job type and related status constants.
package job

import (
	"fmt"
	"time"
)

// Status represents the lifecycle state of a job.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusRetrying   Status = "retrying"
)

// Job holds all metadata for a single unit of work submitted to the queue.
type Job struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Payload     map[string]interface{} `json:"payload"`
	Status      Status                 `json:"status"`
	Retries     int                    `json:"retries"`
	MaxRetries  int                    `json:"max_retries"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Error       string                 `json:"error,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
}

// New creates a new Job with default retry settings (max 3 retries).
func New(id, jobType string, payload map[string]interface{}) *Job {
	now := time.Now().UTC()
	return &Job{
		ID:         id,
		Type:       jobType,
		Payload:    payload,
		Status:     StatusPending,
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// String implements the Stringer interface for logging.
func (j *Job) String() string {
	return fmt.Sprintf("Job{id=%s type=%s status=%s retries=%d/%d}",
		j.ID, j.Type, j.Status, j.Retries, j.MaxRetries)
}

// CanRetry reports whether the job has remaining retry attempts.
func (j *Job) CanRetry() bool {
	return j.Retries < j.MaxRetries
}
