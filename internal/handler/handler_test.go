package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vjy0/job-queue-service/internal/handler"
	"github.com/vjy0/job-queue-service/internal/job"
	"github.com/vjy0/job-queue-service/internal/mockserver"
	"github.com/vjy0/job-queue-service/internal/pool"
	"github.com/vjy0/job-queue-service/internal/store"
	"github.com/vjy0/job-queue-service/internal/worker"
)

// setupHandler boots the full stack against a mock downstream and
// returns a configured *http.ServeMux and a teardown function.
func setupHandler(t *testing.T) (*http.ServeMux, func()) {
	t.Helper()

	srv, err := mockserver.New("127.0.0.1:0", 0.0)
	if err != nil {
		t.Fatalf("mockserver.New: %v", err)
	}
	go srv.Serve()

	tp, err := pool.New(srv.Addr(), 3, time.Second, time.Second)
	if err != nil {
		t.Fatalf("pool.New: %v", err)
	}

	s := store.New()
	ctx, cancel := context.WithCancel(context.Background())
	wp := worker.New(s, tp)
	wp.Start(ctx)

	h := handler.New(s, wp, tp)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	teardown := func() {
		cancel()
		wp.Wait()
		tp.Close()
		srv.Stop()
	}
	return mux, teardown
}

func postJob(mux http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/jobs",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// ── POST /jobs ────────────────────────────────────────────────────────────────

func TestCreateJob_Accepted(t *testing.T) {
	mux, teardown := setupHandler(t)
	defer teardown()

	w := postJob(mux, `{"type":"email","payload":{"to":"x@y.com"}}`)
	if w.Code != http.StatusAccepted {
		t.Errorf("status: got %d want %d — body: %s", w.Code, http.StatusAccepted, w.Body)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := resp["id"]; !ok {
		t.Error("response missing 'id' field")
	}
}

func TestCreateJob_MissingType(t *testing.T) {
	mux, teardown := setupHandler(t)
	defer teardown()

	w := postJob(mux, `{"payload":{}}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d want %d", w.Code, http.StatusBadRequest)
	}
}

func TestCreateJob_InvalidJSON(t *testing.T) {
	mux, teardown := setupHandler(t)
	defer teardown()

	w := postJob(mux, `not json`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d want %d", w.Code, http.StatusBadRequest)
	}
}

// ── GET /jobs ─────────────────────────────────────────────────────────────────

func TestListJobs(t *testing.T) {
	mux, teardown := setupHandler(t)
	defer teardown()

	// Submit two jobs first.
	postJob(mux, `{"type":"a"}`)
	postJob(mux, `{"type":"b"}`)

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d want %d", w.Code, http.StatusOK)
	}
	var resp map[string]interface{}
	_ = json.NewDecoder(w.Body).Decode(&resp)
	count, ok := resp["count"].(float64)
	if !ok || count < 2 {
		t.Errorf("count: got %v want >=2", resp["count"])
	}
}

// ── GET /jobs/{id} ────────────────────────────────────────────────────────────

func TestGetJobByID_Found(t *testing.T) {
	mux, teardown := setupHandler(t)
	defer teardown()

	wr := postJob(mux, `{"type":"lookup"}`)
	var created job.Job
	_ = json.NewDecoder(wr.Body).Decode(&created)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d want %d", w.Code, http.StatusOK)
	}
}

func TestGetJobByID_NotFound(t *testing.T) {
	mux, teardown := setupHandler(t)
	defer teardown()

	req := httptest.NewRequest(http.MethodGet, "/jobs/does-not-exist", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status: got %d want %d", w.Code, http.StatusNotFound)
	}
}

// ── GET /health ───────────────────────────────────────────────────────────────

func TestHealth(t *testing.T) {
	mux, teardown := setupHandler(t)
	defer teardown()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d want %d", w.Code, http.StatusOK)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("status field: got %v want 'ok'", resp["status"])
	}
}

// ── Method not allowed ────────────────────────────────────────────────────────

func TestMethodNotAllowed(t *testing.T) {
	mux, teardown := setupHandler(t)
	defer teardown()

	req := httptest.NewRequest(http.MethodDelete, "/jobs", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status: got %d want %d", w.Code, http.StatusMethodNotAllowed)
	}
}
