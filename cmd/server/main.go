// Command server is the main entrypoint for the job-queue-service.
//
// Environment variables:
//
//	LISTEN_ADDR      HTTP listen address (default: ":8080")
//	DOWNSTREAM_ADDR  TCP address of the mock downstream (default: ":9090")
//	POOL_SIZE        Number of TCP connections in the pool (default: 5)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/vjy0/job-queue-service/internal/handler"
	"github.com/vjy0/job-queue-service/internal/pool"
	"github.com/vjy0/job-queue-service/internal/store"
	"github.com/vjy0/job-queue-service/internal/worker"
)

func main() {
	listenAddr    := envOr("LISTEN_ADDR", ":8080")
	downstreamAddr := envOr("DOWNSTREAM_ADDR", ":9090")
	poolSize      := envInt("POOL_SIZE", pool.DefaultMaxConns)

	log.Printf("[main] starting job-queue-service listen=%s downstream=%s pool=%d",
		listenAddr, downstreamAddr, poolSize)

	// ── Stores ──────────────────────────────────────────────────────────────
	jobStore := store.New()

	// ── TCP connection pool to downstream ───────────────────────────────────
	tcpPool, err := pool.New(downstreamAddr, poolSize,
		pool.DefaultDialTimeout, pool.DefaultRWTimeout)
	if err != nil {
		log.Fatalf("[main] failed to create TCP pool: %v", err)
	}
	defer tcpPool.Close()

	// ── Worker pool ─────────────────────────────────────────────────────────
	// ctx is cancelled on SIGINT/SIGTERM to trigger graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerPool := worker.New(jobStore, tcpPool)
	workerPool.Start(ctx)

	// ── HTTP server ──────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	h := handler.New(jobStore, workerPool, tcpPool)
	h.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ── Graceful shutdown ────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("[main] received signal %v — initiating graceful shutdown", sig)

		// 1. Stop accepting new HTTP requests.
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutCancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("[main] HTTP shutdown error: %v", err)
		}

		// 2. Cancel worker context so goroutines stop picking up new jobs.
		cancel()

		// 3. Wait for in-flight workers to complete their current job.
		workerPool.Wait()

		log.Println("[main] all workers drained — goodbye")
	}()

	log.Printf("[main] HTTP server listening on %s", listenAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[main] HTTP server error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
