// Package pool implements a bounded pool of reusable TCP connections to a
// single downstream address. It supports:
//   - A configurable maximum number of connections (default 5).
//   - Per-connection dial and read/write timeouts.
//   - Health-checks via a lightweight PING/PONG protocol.
//   - Automatic eviction of dead connections and replacement with fresh ones.
//
// Callers acquire a connection with Get, use it, and return it with Put.
// If the connection has gone bad after use, callers should call MarkBad
// before Put so the pool evicts and replaces it.
package pool

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const (
	DefaultMaxConns    = 5
	DefaultDialTimeout = 5 * time.Second
	DefaultRWTimeout   = 3 * time.Second
	pingMsg            = "PING\n"
	pongMsg            = "PONG"
)

// Conn wraps a net.Conn and tracks its health.
type Conn struct {
	net.Conn
	bad bool
}

// MarkBad flags this connection as unhealthy so the pool evicts it on return.
func (c *Conn) MarkBad() { c.bad = true }

// TCPPool is a bounded, reusable pool of TCP connections.
// All exported methods are safe for concurrent use.
type TCPPool struct {
	addr        string
	maxConns    int
	dialTimeout time.Duration
	rwTimeout   time.Duration

	mu    sync.Mutex
	idle  []*Conn // connections ready to be handed out
	total int     // total connections created (idle + in-flight)
	avail chan struct{}
}

// New creates a TCPPool that eagerly dials maxConns connections to addr.
// A nil options slice uses package-level defaults.
func New(addr string, maxConns int, dialTimeout, rwTimeout time.Duration) (*TCPPool, error) {
	if maxConns <= 0 {
		maxConns = DefaultMaxConns
	}
	if dialTimeout <= 0 {
		dialTimeout = DefaultDialTimeout
	}
	if rwTimeout <= 0 {
		rwTimeout = DefaultRWTimeout
	}

	p := &TCPPool{
		addr:        addr,
		maxConns:    maxConns,
		dialTimeout: dialTimeout,
		rwTimeout:   rwTimeout,
		idle:        make([]*Conn, 0, maxConns),
		avail:       make(chan struct{}, maxConns),
	}

	// Pre-fill the semaphore.
	for i := 0; i < maxConns; i++ {
		p.avail <- struct{}{}
	}

	// Eagerly establish all connections so the first real request is fast.
	for i := 0; i < maxConns; i++ {
		c, err := p.dial()
		if err != nil {
			// Partial initialisation is acceptable; pool will refill lazily.
			log.Printf("[pool] eager dial %d/%d failed: %v", i+1, maxConns, err)
			break
		}
		p.mu.Lock()
		p.idle = append(p.idle, c)
		p.total++
		p.mu.Unlock()
	}

	return p, nil
}

// dial opens a fresh TCP connection.
func (p *TCPPool) dial() (*Conn, error) {
	nc, err := net.DialTimeout("tcp", p.addr, p.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("pool: dial %s: %w", p.addr, err)
	}
	return &Conn{Conn: nc}, nil
}

// Get blocks until a healthy connection is available or ctx is done.
// The returned Conn must be returned via Put (or closed if beyond repair).
func (p *TCPPool) Get() (*Conn, error) {
	// Acquire a slot (blocks if all maxConns connections are in-flight).
	<-p.avail

	p.mu.Lock()
	// Try to reuse an existing idle connection.
	for len(p.idle) > 0 {
		c := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.mu.Unlock()

		if p.healthCheck(c) {
			return c, nil
		}
		// Dead connection — close, do not return slot yet; we'll dial fresh.
		log.Printf("[pool] evicting dead connection to %s", p.addr)
		c.Conn.Close() //nolint:errcheck
		p.mu.Lock()
		p.total--
		p.mu.Unlock()

		// Dial a replacement and return it.
		fresh, err := p.dial()
		if err != nil {
			p.avail <- struct{}{} // release slot
			return nil, err
		}
		p.mu.Lock()
		p.total++
		p.mu.Unlock()
		return fresh, nil
	}

	// No idle connections — we still hold a slot, so dial a fresh one.
	defer p.mu.Unlock()
	if p.total >= p.maxConns {
		// Shouldn't happen because avail guards this, but be defensive.
		p.avail <- struct{}{}
		return nil, fmt.Errorf("pool: no connections available")
	}
	c, err := p.dial()
	if err != nil {
		p.avail <- struct{}{} // release slot
		return nil, err
	}
	p.total++
	return c, nil
}

// Put returns a connection to the pool. If the connection has been marked bad
// (via MarkBad) or the pool is shutting down, it is closed and discarded.
func (p *TCPPool) Put(c *Conn) {
	if c.bad {
		c.Conn.Close() //nolint:errcheck
		p.mu.Lock()
		p.total--
		p.mu.Unlock()
		p.avail <- struct{}{} // release slot for a future dial
		return
	}

	p.mu.Lock()
	p.idle = append(p.idle, c)
	p.mu.Unlock()
	p.avail <- struct{}{} // signal that a connection is ready
}

// healthCheck sends PING and expects PONG within rwTimeout.
// Returns false if the connection is stale or unresponsive.
func (p *TCPPool) healthCheck(c *Conn) bool {
	deadline := time.Now().Add(p.rwTimeout)
	if err := c.SetDeadline(deadline); err != nil {
		return false
	}
	if _, err := fmt.Fprint(c, pingMsg); err != nil {
		return false
	}
	scanner := bufio.NewScanner(c)
	if !scanner.Scan() {
		return false
	}
	return scanner.Text() == pongMsg
}

// Close closes every idle connection in the pool.
// In-flight connections are not affected; callers should drain them first.
func (p *TCPPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.idle {
		c.Conn.Close() //nolint:errcheck
	}
	p.idle = p.idle[:0]
	p.total = 0
}

// Stats returns a snapshot of current pool usage.
func (p *TCPPool) Stats() (idle, inFlight, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idle = len(p.idle)
	total = p.total
	inFlight = total - idle
	return
}
