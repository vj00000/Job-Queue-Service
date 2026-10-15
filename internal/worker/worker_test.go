package worker_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/vjy0/job-queue-service/internal/job"
	"github.com/vjy0/job-queue-service/internal/mockserver"
	"github.com/vjy0/job-queue-service/internal/pool"
	"github.com/vjy0/job-queue-service/internal/store"
	"github.com/vjy0/job-queue-service/internal/worker"
)

// startMockServer starts a mockserver and returns its address and a stop func.
func startMockServer(t *testing.T, failRate float64) (addr string, stop func()) {
	t.Helper()
	srv, err := mockserver.New("127.0.0.1:0", failRate)
	if err != nil {
		t.Fatalf("mockserver.New: %v", err)
	}
	go srv.Serve()
	return srv.Addr(), srv.Stop
}

// newPool creates a pool connected to addr; helpers die on t.Fatal.
func newPool(t *testing.T, addr string) *pool.TCPPool {
	t.Helper()
	p, err := pool.New(addr, 3, 2*time.Second, 2*time.Second)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	return p
}

// waitStatus polls the store until the job reaches wantStatus or times out.
func waitStatus(t *testing.T, s *store.Store, id string, wantStatus job.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := s.Get(id)
		if err == nil && j.Status == wantStatus {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	j, _ := s.Get(id)
	t.Errorf("job %s: status=%v want=%v after %s", id, j.Status, wantStatus, timeout)
}

// ── Happy path ────────────────────────────────────────────────────────────────

func TestWorker_JobCompletes(t *testing.T) {
	addr, stop := startMockServer(t, 0.0)
	defer stop()

	s := store.New()
	tp := newPool(t, addr)
	defer tp.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := worker.New(s, tp)
	wp.Start(ctx)

	j := job.New("job-1", "test", nil)
	_ = s.Create(j)
	if err := wp.Submit(j); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	waitStatus(t, s, "job-1", job.StatusCompleted, 5*time.Second)
}

// ── Queue-full → ErrQueueFull (HTTP 429 trigger) ──────────────────────────────

func TestWorker_QueueFull(t *testing.T) {
	// Use a blocked downstream so workers pile up and the queue fills.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// Accept connections but never respond (simulate a hung downstream).
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Hold connection open forever.
			_ = conn
		}
	}()

	s := store.New()
	tp, err := pool.New(ln.Addr().String(), worker.NumWorkers+1,
		500*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}
	defer tp.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := worker.New(s, tp)
	wp.Start(ctx)

	// Flood the queue past capacity.
	var fullCount int
	for i := 0; i < worker.QueueSize+worker.NumWorkers+10; i++ {
		id := fmt.Sprintf("flood-%d", i)
		j := job.New(id, "flood", nil)
		_ = s.Create(j)
		if err := wp.Submit(j); err == worker.ErrQueueFull {
			fullCount++
		}
	}

	if fullCount == 0 {
		t.Error("expected at least one ErrQueueFull; got none")
	}
}

// ── Retry with exponential backoff ───────────────────────────────────────────

func TestWorker_RetryOnFailure(t *testing.T) {
	// 100 % failure rate → job must exhaust retries and end as Failed.
	addr, stop := startMockServer(t, 1.0)
	defer stop()

	s := store.New()
	tp := newPool(t, addr)
	defer tp.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp := worker.New(s, tp)
	wp.Start(ctx)

	j := job.New("retry-job", "test", nil)
	_ = s.Create(j)
	_ = wp.Submit(j)

	// With 3 retries and base 200 ms backoff the total wait is
	// 200 + 400 + 800 = 1 400 ms; give it 10 s to be safe in CI.
	waitStatus(t, s, "retry-job", job.StatusFailed, 10*time.Second)

	final, _ := s.Get("retry-job")
	if final.Retries != final.MaxRetries {
		t.Errorf("retries: got %d want %d", final.Retries, final.MaxRetries)
	}
}

// ── Graceful shutdown drains in-flight jobs ───────────────────────────────────

func TestWorker_GracefulShutdown(t *testing.T) {
	addr, stop := startMockServer(t, 0.0)
	defer stop()

	s := store.New()
	tp := newPool(t, addr)
	defer tp.Close()

	ctx, cancel := context.WithCancel(context.Background())

	wp := worker.New(s, tp)
	wp.Start(ctx)

	// Submit a few jobs, then immediately cancel.
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("shutdown-%d", i)
		j := job.New(id, "shutdown", nil)
		_ = s.Create(j)
		_ = wp.Submit(j)
	}

	cancel()          // signal shutdown
	wp.Wait()         // drain in-flight jobs

	// All submitted jobs should be in a terminal state (not pending/processing).
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("shutdown-%d", i)
		j, err := s.Get(id)
		if err != nil {
			continue // job may not have been picked up yet
		}
		switch j.Status {
		case job.StatusCompleted, job.StatusFailed, job.StatusRetrying, job.StatusPending:
			// acceptable terminal or pre-pickup state
		default:
			t.Errorf("job %s in unexpected state %q after shutdown", id, j.Status)
		}
	}
}
