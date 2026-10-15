// Package handler wires the HTTP layer to the worker pool and job store.
//
// Routes:
//
//	POST /jobs          – enqueue a new job; 429 when queue is full
//	GET  /jobs          – list all jobs
//	GET  /jobs/{id}     – fetch a single job by ID
//	GET  /health        – liveness + pool metrics
package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/vjy0/job-queue-service/internal/job"
	"github.com/vjy0/job-queue-service/internal/pool"
	"github.com/vjy0/job-queue-service/internal/store"
	"github.com/vjy0/job-queue-service/internal/worker"
)

// Handler holds the shared dependencies injected at construction time.
type Handler struct {
	store   *store.Store
	pool    *worker.Pool
	tcpPool *pool.TCPPool
}

// New returns a fully initialised Handler.
func New(s *store.Store, wp *worker.Pool, tp *pool.TCPPool) *Handler {
	return &Handler{store: s, pool: wp, tcpPool: tp}
}

// RegisterRoutes attaches all HTTP routes to mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/jobs", h.jobsRouter)
	mux.HandleFunc("/jobs/", h.jobByID)
	mux.HandleFunc("/health", h.health)
}

// jobsRouter dispatches /jobs to the correct sub-handler.
func (h *Handler) jobsRouter(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createJob(w, r)
	case http.MethodGet:
		h.listJobs(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// POST /jobs
// ──────────────────────────────────────────────────────────────────────────────

type createJobRequest struct {
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

// createJob decodes the request, persists the job, and submits it to the pool.
// Returns 429 when the worker queue is at capacity (backpressure).
func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "field 'type' is required")
		return
	}

	id := generateID()
	j := job.New(id, req.Type, req.Payload)

	if err := h.store.Create(j); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := h.pool.Submit(j); err != nil {
		// Backpressure: queue is full → HTTP 429 Too Many Requests.
		writeError(w, http.StatusTooManyRequests,
			"server is busy, queue at capacity — retry later")
		return
	}

	log.Printf("[handler] enqueued %s (type=%s)", j.ID, j.Type)
	writeJSON(w, http.StatusAccepted, j)
}

// ──────────────────────────────────────────────────────────────────────────────
// GET /jobs
// ──────────────────────────────────────────────────────────────────────────────

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs := h.store.List()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"count": len(jobs),
		"jobs":  jobs,
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// GET /jobs/{id}
// ──────────────────────────────────────────────────────────────────────────────

func (h *Handler) jobByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract ID from path: /jobs/{id}
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing job ID")
		return
	}

	j, err := h.store.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// ──────────────────────────────────────────────────────────────────────────────
// GET /health
// ──────────────────────────────────────────────────────────────────────────────

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	idle, inFlight, total := h.tcpPool.Stats()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ok",
		"queue": map[string]int{
			"length":   h.pool.QueueLen(),
			"capacity": worker.QueueSize,
		},
		"tcp_pool": map[string]int{
			"idle":      idle,
			"in_flight": inFlight,
			"total":     total,
		},
		"jobs_stored": h.store.Count(),
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// generateID produces a compact, time-ordered unique ID (no external deps).
func generateID() string {
	return time.Now().UTC().Format("20060102150405.000000000")
}
