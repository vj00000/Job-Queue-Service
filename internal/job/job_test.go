package job_test

import (
	"testing"
	"time"

	"github.com/vjy0/job-queue-service/internal/job"
)

func TestNew(t *testing.T) {
	before := time.Now().UTC()
	j := job.New("id-1", "email", map[string]interface{}{"to": "a@b.com"})
	after := time.Now().UTC()

	if j.ID != "id-1" {
		t.Errorf("ID: got %q want %q", j.ID, "id-1")
	}
	if j.Type != "email" {
		t.Errorf("Type: got %q want %q", j.Type, "email")
	}
	if j.Status != job.StatusPending {
		t.Errorf("Status: got %q want %q", j.Status, job.StatusPending)
	}
	if j.MaxRetries != 3 {
		t.Errorf("MaxRetries: got %d want 3", j.MaxRetries)
	}
	if j.Retries != 0 {
		t.Errorf("Retries: got %d want 0", j.Retries)
	}
	if j.CreatedAt.Before(before) || j.CreatedAt.After(after) {
		t.Errorf("CreatedAt %v not in [%v, %v]", j.CreatedAt, before, after)
	}
}

func TestCanRetry(t *testing.T) {
	tests := []struct {
		name       string
		retries    int
		maxRetries int
		want       bool
	}{
		{"no retries yet",       0, 3, true},
		{"one retry used",       1, 3, true},
		{"two retries used",     2, 3, true},
		{"max retries reached",  3, 3, false},
		{"over max retries",     4, 3, false},
		{"zero max retries",     0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			j := &job.Job{Retries: tc.retries, MaxRetries: tc.maxRetries}
			if got := j.CanRetry(); got != tc.want {
				t.Errorf("CanRetry()=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestString(t *testing.T) {
	j := job.New("abc", "notify", nil)
	s := j.String()
	if s == "" {
		t.Error("String() returned empty string")
	}
}
