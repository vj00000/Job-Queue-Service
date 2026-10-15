package pool_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/vjy0/job-queue-service/internal/pool"
)

// startEchoServer starts a TCP server that handles PING/PONG and echoes OK.
// Returns the address and a stop function.
func startEchoServer(t *testing.T, failAfter int) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	count := 0
	mu := sync.Mutex{}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					_ = c.SetDeadline(time.Now().Add(2 * time.Second))
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					msg := string(buf[:n])
					mu.Lock()
					count++
					shouldFail := failAfter > 0 && count > failAfter
					mu.Unlock()

					if msg == "PING\n" {
						_, _ = fmt.Fprint(c, "PONG\n")
					} else if shouldFail {
						_, _ = fmt.Fprint(c, "ERR\n")
					} else {
						_, _ = fmt.Fprint(c, "OK\n")
					}
				}
			}(conn)
		}
	}()

	return ln.Addr().String(), func() { _ = ln.Close() }
}

// TestPool_GetPut verifies that basic Get/Put round-trips work.
func TestPool_GetPut(t *testing.T) {
	addr, stop := startEchoServer(t, 0)
	defer stop()

	p, err := pool.New(addr, 3, time.Second, time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	c, err := p.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Put(c)

	idle, inFlight, _ := p.Stats()
	if inFlight != 0 {
		t.Errorf("in-flight after Put: got %d want 0", inFlight)
	}
	if idle == 0 {
		t.Error("no idle connections after Put")
	}
}

// TestPool_MaxConns ensures the pool never exceeds its size.
func TestPool_MaxConns(t *testing.T) {
	addr, stop := startEchoServer(t, 0)
	defer stop()

	const max = 3
	p, err := pool.New(addr, max, time.Second, time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	conns := make([]*pool.Conn, max)
	for i := 0; i < max; i++ {
		c, err := p.Get()
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		conns[i] = c
	}

	_, inFlight, total := p.Stats()
	if total > max {
		t.Errorf("total=%d exceeds max=%d", total, max)
	}
	if inFlight != max {
		t.Errorf("in-flight: got %d want %d", inFlight, max)
	}

	for _, c := range conns {
		p.Put(c)
	}
}

// TestPool_MarkBad verifies that bad connections are evicted and replaced.
func TestPool_MarkBad(t *testing.T) {
	addr, stop := startEchoServer(t, 0)
	defer stop()

	p, err := pool.New(addr, 2, time.Second, time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	c, err := p.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	c.MarkBad()
	p.Put(c) // should be evicted

	// Next Get must succeed with a fresh connection.
	c2, err := p.Get()
	if err != nil {
		t.Fatalf("Get after bad eviction: %v", err)
	}
	p.Put(c2)
}

// TestPool_GetFromIdle verifies that connections returned via Put are reused.
func TestPool_GetFromIdle(t *testing.T) {
	addr, stop := startEchoServer(t, 0)
	defer stop()

	p, err := pool.New(addr, 3, time.Second, time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	// Get then Put to put something back in the idle list.
	c, err := p.Get()
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	p.Put(c)

	// Second Get should reuse the idle connection (same underlying pool).
	c2, err := p.Get()
	if err != nil {
		t.Fatalf("second Get (idle reuse): %v", err)
	}
	p.Put(c2)
}

// TestPool_GetFromEmpty verifies Get dials fresh when there are no idle conns.
func TestPool_GetFromEmpty(t *testing.T) {
	addr, stop := startEchoServer(t, 0)
	defer stop()

	// size-1 pool initialised eagerly with 0 connections (no pre-fill).
	p, err := pool.New(addr, 1, time.Second, time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	c, err := p.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Put(c)
}

// TestPool_Close verifies that Close cleans up idle connections.
func TestPool_Close(t *testing.T) {
	addr, stop := startEchoServer(t, 0)
	defer stop()

	p, err := pool.New(addr, 2, time.Second, time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Close()

	// After Close, idle list is empty.
	idle, _, _ := p.Stats()
	if idle != 0 {
		t.Errorf("idle after Close: got %d want 0", idle)
	}
}

// TestPool_ConcurrentAccess stress-tests the pool under concurrent load.
func TestPool_ConcurrentAccess(t *testing.T) {
	addr, stop := startEchoServer(t, 0)
	defer stop()

	p, err := pool.New(addr, 5, time.Second, time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	const goroutines = 20
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.Get()
			if err != nil {
				t.Errorf("Get: %v", err)
				return
			}
			time.Sleep(5 * time.Millisecond)
			p.Put(c)
		}()
	}
	wg.Wait()

	_, inFlight, _ := p.Stats()
	if inFlight != 0 {
		t.Errorf("in-flight after all Put: got %d want 0", inFlight)
	}
}
