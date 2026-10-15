package store_test

import (
	"sync"
	"testing"

	"github.com/vjy0/job-queue-service/internal/job"
	"github.com/vjy0/job-queue-service/internal/store"
)

func newJob(id string) *job.Job {
	return job.New(id, "test", map[string]interface{}{"k": "v"})
}

// ── Create ────────────────────────────────────────────────────────────────────

func TestCreate_Success(t *testing.T) {
	s := store.New()
	j := newJob("j1")
	if err := s.Create(j); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	if s.Count() != 1 {
		t.Errorf("Count: got %d want 1", s.Count())
	}
}

func TestCreate_Duplicate(t *testing.T) {
	s := store.New()
	j := newJob("dup")
	_ = s.Create(j)
	if err := s.Create(j); err == nil {
		t.Fatal("Create: expected error for duplicate ID, got nil")
	}
}

// ── Get ───────────────────────────────────────────────────────────────────────

func TestGet_Existing(t *testing.T) {
	s := store.New()
	j := newJob("g1")
	_ = s.Create(j)

	got, err := s.Get("g1")
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if got.ID != "g1" {
		t.Errorf("Get: ID mismatch got=%q want=%q", got.ID, "g1")
	}
}

func TestGet_NotFound(t *testing.T) {
	s := store.New()
	if _, err := s.Get("missing"); err == nil {
		t.Fatal("Get: expected error for missing ID, got nil")
	}
}

// Get returns a copy; mutating it must not affect the stored value.
func TestGet_ReturnsCopy(t *testing.T) {
	s := store.New()
	_ = s.Create(newJob("cp"))

	got, _ := s.Get("cp")
	got.Type = "mutated"

	got2, _ := s.Get("cp")
	if got2.Type == "mutated" {
		t.Error("Get: returned live pointer instead of a copy")
	}
}

// ── Update ────────────────────────────────────────────────────────────────────

func TestUpdate_Success(t *testing.T) {
	s := store.New()
	_ = s.Create(newJob("u1"))

	err := s.Update("u1", func(j *job.Job) {
		j.Status = job.StatusCompleted
	})
	if err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}

	got, _ := s.Get("u1")
	if got.Status != job.StatusCompleted {
		t.Errorf("Update: status not persisted, got %q", got.Status)
	}
}

func TestUpdate_NotFound(t *testing.T) {
	s := store.New()
	if err := s.Update("nope", func(j *job.Job) {}); err == nil {
		t.Fatal("Update: expected error for missing ID, got nil")
	}
}

// ── List ──────────────────────────────────────────────────────────────────────

func TestList(t *testing.T) {
	s := store.New()
	for _, id := range []string{"a", "b", "c"} {
		_ = s.Create(newJob(id))
	}
	jobs := s.List()
	if len(jobs) != 3 {
		t.Errorf("List: got %d want 3", len(jobs))
	}
}

// ── Concurrency (race detector) ───────────────────────────────────────────────

func TestStore_ConcurrentReadWrite(t *testing.T) {
	s := store.New()
	// Pre-populate so readers have data.
	for i := 0; i < 20; i++ {
		_ = s.Create(newJob(string(rune('a' + i))))
	}

	var wg sync.WaitGroup
	// 10 concurrent readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.List()
		}()
	}
	// 10 concurrent updaters
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_ = s.Update(id, func(j *job.Job) {
				j.Status = job.StatusProcessing
			})
		}(string(rune('a' + i)))
	}
	wg.Wait()
}
