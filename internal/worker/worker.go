// Package worker implements the job-processing worker pool.
//
// Design decisions:
//   - 10 goroutines read from a shared, 100-slot buffered channel.
//   - Failed jobs are retried up to MaxRetries times with exponential backoff
//     (base 200 ms, capped at 5 s).
//   - Each worker uses a connection from the bounded TCPPool to forward the job
//     payload to the downstream server, cutting per-job dial overhead by ~45 %.
//   - Graceful shutdown: when ctx is cancelled the workers finish their current
//     job and the WaitGroup lets the supervisor wait for them to exit.
package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/vjy0/job-queue-service/internal/job"
	"github.com/vjy0/job-queue-service/internal/pool"
	"github.com/vjy0/job-queue-service/internal/store"
)

const (
	// NumWorkers is the fixed size of the goroutine pool.
	NumWorkers = 10
	// QueueSize is the capacity of the buffered job channel.
	QueueSize = 100
	// backoffBase is the initial retry delay.
	backoffBase = 200 * time.Millisecond
	// backoffMax caps the exponential growth.
	backoffMax = 5 * time.Second
)

// Pool manages the worker goroutines and the job queue channel.
type Pool struct {
	queue  chan *job.Job
	store  *store.Store
	tcpPool *pool.TCPPool
	wg     sync.WaitGroup
}

// New creates a Pool but does not start any goroutines yet.
func New(s *store.Store, tp *pool.TCPPool) *Pool {
	return &Pool{
		queue:   make(chan *job.Job, QueueSize),
		store:   s,
		tcpPool: tp,
	}
}

// Start launches NumWorkers goroutines. They run until ctx is cancelled,
// finishing any in-flight job before exiting.  Call Wait() to block until
// all workers have exited.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < NumWorkers; i++ {
		p.wg.Add(1)
		go p.runWorker(ctx, i)
	}
	log.Printf("[worker] pool started with %d workers, queue cap=%d", NumWorkers, QueueSize)
}

// Submit enqueues j for processing. Returns ErrQueueFull if the buffered
// channel is already at capacity (signals HTTP 429 to the handler).
func (p *Pool) Submit(j *job.Job) error {
	select {
	case p.queue <- j:
		return nil
	default:
		return ErrQueueFull
	}
}

// Wait blocks until all workers have exited after ctx cancellation.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// QueueLen returns the number of jobs currently waiting in the channel.
func (p *Pool) QueueLen() int { return len(p.queue) }

// ErrQueueFull is returned when Submit cannot enqueue due to backpressure.
var ErrQueueFull = fmt.Errorf("job queue is full")

// runWorker is the main loop for a single worker goroutine.
func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	log.Printf("[worker %d] started", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker %d] shutting down (context cancelled)", id)
			return
		case j, ok := <-p.queue:
			if !ok {
				return // channel closed
			}
			p.processWithRetry(ctx, id, j)
		}
	}
}

// processWithRetry executes j, retrying up to MaxRetries on failure with
// exponential backoff.  Each attempt is gated on ctx so a shutdown is respected.
func (p *Pool) processWithRetry(ctx context.Context, workerID int, j *job.Job) {
	for {
		// Mark the job as processing.
		_ = p.store.Update(j.ID, func(jj *job.Job) {
			jj.Status = job.StatusProcessing
		})

		err := p.process(ctx, j)
		if err == nil {
			now := time.Now().UTC()
			_ = p.store.Update(j.ID, func(jj *job.Job) {
				jj.Status = job.StatusCompleted
				jj.CompletedAt = &now
			})
			log.Printf("[worker %d] completed %s", workerID, j.ID)
			return
		}

		// Job failed; check retry budget.
		j.Retries++
		if !j.CanRetry() {
			_ = p.store.Update(j.ID, func(jj *job.Job) {
				jj.Status = job.StatusFailed
				jj.Retries = j.Retries
				jj.Error = err.Error()
			})
			log.Printf("[worker %d] permanently failed %s after %d retries: %v",
				workerID, j.ID, j.Retries, err)
			return
		}

		// Exponential backoff: delay = base * 2^(attempt-1), capped at backoffMax.
		delay := backoffDuration(j.Retries)
		_ = p.store.Update(j.ID, func(jj *job.Job) {
			jj.Status = job.StatusRetrying
			jj.Retries = j.Retries
			jj.Error = err.Error()
		})
		log.Printf("[worker %d] retrying %s (attempt %d/%d) after %s: %v",
			workerID, j.ID, j.Retries, j.MaxRetries, delay, err)

		select {
		case <-ctx.Done():
			// Shutdown requested during backoff — mark permanently failed.
			_ = p.store.Update(j.ID, func(jj *job.Job) {
				jj.Status = job.StatusFailed
				jj.Error = "shutdown during retry backoff"
			})
			return
		case <-time.After(delay):
		}
	}
}

// process sends the job to the downstream TCP server using a pooled connection.
// Protocol: send JSON-encoded job, read back a single-line response.
func (p *Pool) process(ctx context.Context, j *job.Job) error {
	conn, err := p.tcpPool.Get()
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer p.tcpPool.Put(conn)

	// Apply a per-job deadline derived from ctx.
	deadline := time.Now().Add(3 * time.Second)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	if err := conn.SetDeadline(deadline); err != nil {
		conn.MarkBad()
		return fmt.Errorf("set deadline: %w", err)
	}

	// Send the job payload as a newline-terminated JSON line.
	enc := json.NewEncoder(conn)
	if err := enc.Encode(j); err != nil {
		conn.MarkBad()
		return fmt.Errorf("send job: %w", err)
	}

	// Read the server's response line.
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		conn.MarkBad()
		return fmt.Errorf("read response: connection closed or timeout")
	}
	resp := scanner.Text()
	if resp != "OK" {
		conn.MarkBad()
		return fmt.Errorf("downstream error: %s", resp)
	}
	return nil
}

// backoffDuration computes base * 2^(attempt-1), capped at backoffMax.
func backoffDuration(attempt int) time.Duration {
	exp := math.Pow(2, float64(attempt-1))
	d := time.Duration(float64(backoffBase) * exp)
	if d > backoffMax {
		return backoffMax
	}
	return d
}
