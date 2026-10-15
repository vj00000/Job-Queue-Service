// Package store provides a thread-safe in-memory registry of all jobs,
// guarded by a sync.RWMutex so concurrent reads do not block each other.
package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/vjy0/job-queue-service/internal/job"
)

// Store is a thread-safe, in-memory job registry.
// Write operations (Create, Update) acquire a full lock;
// reads (Get, List) acquire only a read lock.
type Store struct {
	mu   sync.RWMutex
	jobs map[string]*job.Job
}

// New returns an initialised Store.
func New() *Store {
	return &Store{jobs: make(map[string]*job.Job)}
}

// Create adds j to the store. Returns an error if the ID already exists.
func (s *Store) Create(j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[j.ID]; exists {
		return fmt.Errorf("job %s already exists", j.ID)
	}
	// Store a copy to prevent external mutation.
	cp := *j
	s.jobs[j.ID] = &cp
	return nil
}

// Update atomically applies fn to the job identified by id.
// The function fn receives a mutable pointer; any changes are persisted.
func (s *Store) Update(id string, fn func(*job.Job)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %s not found", id)
	}
	fn(j)
	j.UpdatedAt = time.Now().UTC()
	return nil
}

// Get returns a snapshot copy of the job (not a live pointer).
func (s *Store) Get(id string) (*job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, ok := s.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %s not found", id)
	}
	cp := *j
	return &cp, nil
}

// List returns snapshot copies of every job in insertion-independent order.
func (s *Store) List() []*job.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*job.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		cp := *j
		out = append(out, &cp)
	}
	return out
}

// Count returns the total number of stored jobs.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.jobs)
}
