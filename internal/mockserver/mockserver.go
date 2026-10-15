// Package mockserver implements a lightweight TCP server that acts as the
// downstream dependency for the job-queue-service.
//
// Protocol (newline-terminated text):
//   - "PING\n"   → responds "PONG\n"   (health-check, used by the TCP pool)
//   - <JSON job> → responds "OK\n"     (simulates processing; random 50 ms delay)
//   - "ERROR\n"  → responds "ERR downstream error\n" (for testing retries)
package mockserver

import (
	"bufio"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"
)

// Server is a mock TCP downstream.
type Server struct {
	ln       net.Listener
	addr     string
	failRate float64 // fraction [0,1] of jobs that fail (for retry testing)
}

// New creates a Server listening on addr.
// failRate ∈ [0,1] controls what fraction of jobs return ERR (useful in tests).
func New(addr string, failRate float64) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{ln: ln, addr: ln.Addr().String(), failRate: failRate}, nil
}

// Addr returns the actual listening address (useful when port 0 is used).
func (s *Server) Addr() string { return s.addr }

// Serve accepts connections until Stop is called.
func (s *Server) Serve() {
	log.Printf("[mockserver] listening on %s (fail_rate=%.0f%%)", s.addr, s.failRate*100)
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			log.Printf("[mockserver] accept error: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// Stop closes the listener, causing Serve to return.
func (s *Server) Stop() { _ = s.ln.Close() }

// handleConn reads lines from conn and responds to each one.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()

		switch {
		case line == "PING":
			// Health-check from the TCP pool.
			_, _ = conn.Write([]byte("PONG\n"))

		case line == "ERROR":
			// Explicit error trigger used in integration tests.
			_, _ = conn.Write([]byte("ERR downstream error\n"))

		default:
			// Assume it's a JSON-encoded job.
			// Simulate variable processing time (10–60 ms).
			time.Sleep(time.Duration(10+rand.Intn(50)) * time.Millisecond)

			if rand.Float64() < s.failRate {
				_, _ = conn.Write([]byte("ERR simulated failure\n"))
			} else {
				_, _ = conn.Write([]byte("OK\n"))
			}
		}
	}
}
