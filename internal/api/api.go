package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"distributedjobplatform/internal/executor"
	"distributedjobplatform/internal/observability"
	"distributedjobplatform/internal/storage/postgres"
)

const maxBodyBytes = 64 * 1024

var queueName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
var idempotencyKey = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type Store interface {
	CreateWithOptions(context.Context, postgres.CreateParams) (postgres.Job, bool, error)
	Get(context.Context, string) (postgres.Job, error)
	List(context.Context, int) ([]postgres.Job, error)
	Cancel(context.Context, string) (postgres.Job, error)
	Replay(context.Context, string, int) (postgres.Job, error)
	Attempts(context.Context, string) ([]postgres.Attempt, error)
	ListQueues(context.Context) ([]postgres.QueueInfo, error)
	ListWorkers(context.Context) ([]postgres.WorkerInfo, error)
	MetricSnapshot(context.Context) (observability.Snapshot, error)
}

type Server struct {
	Store   Store
	Ready   func(context.Context) error
	Metrics *observability.Metrics
	Log     *slog.Logger
}

func (s Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", s.create)
	mux.HandleFunc("GET /v1/jobs", s.list)
	mux.HandleFunc("GET /v1/jobs/{id}", s.get)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("POST /v1/jobs/{id}/replay", s.replay)
	mux.HandleFunc("GET /v1/jobs/{id}/attempts", s.attempts)
	mux.HandleFunc("GET /v1/queues", s.queues)
	mux.HandleFunc("GET /v1/workers", s.workers)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.ready)
	if s.Metrics != nil {
		metricsHandler := s.Metrics.Handler()
		mux.Handle("GET /metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			snapshot, err := s.Store.MetricSnapshot(ctx)
			if err != nil {
				http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
				return
			}
			s.Metrics.SetSnapshot(snapshot)
			metricsHandler.ServeHTTP(w, r)
		}))
	}
	return mux
}

func (s Server) logger() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (s Server) create(w http.ResponseWriter, r *http.Request) {
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var request struct {
		Queue          string          `json:"queue"`
		Type           string          `json:"type"`
		Payload        json.RawMessage `json:"payload"`
		Priority       int             `json:"priority"`
		ScheduledAt    *time.Time      `json:"scheduled_at"`
		MaxAttempts    *int            `json:"max_attempts"`
		IdempotencyKey string          `json:"idempotency_key"`
	}
	if err := dec.Decode(&request); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request exceeds 65536 bytes")
		} else {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		}
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request must contain one JSON object")
		return
	}
	if request.Queue == "" {
		request.Queue = "default"
	}
	if !queueName.MatchString(request.Queue) {
		writeError(w, http.StatusBadRequest, "queue must be 1-64 letters, digits, _ or -")
		return
	}
	if err := executor.Validate(request.Type, request.Payload); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if request.Priority < 0 || request.Priority > 100 {
		writeError(w, http.StatusBadRequest, "priority must be between 0 and 100")
		return
	}
	maxAttempts := 3
	if request.MaxAttempts != nil {
		maxAttempts = *request.MaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > 20 {
		writeError(w, http.StatusBadRequest, "max_attempts must be between 1 and 20")
		return
	}
	if request.IdempotencyKey != "" && (len(request.IdempotencyKey) > 128 || !idempotencyKey.MatchString(request.IdempotencyKey)) {
		writeError(w, http.StatusBadRequest, "idempotency_key must be 1-128 letters, digits, . _ : or -")
		return
	}
	params := postgres.CreateParams{
		Queue: request.Queue, Type: request.Type, Payload: request.Payload,
		Priority: request.Priority, ScheduledAt: request.ScheduledAt,
		MaxAttempts: maxAttempts, IdempotencyKey: request.IdempotencyKey,
	}
	if params.IdempotencyKey != "" {
		var payload any
		if err := json.Unmarshal(params.Payload, &payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid payload")
			return
		}
		var scheduled any
		if params.ScheduledAt != nil {
			scheduled = params.ScheduledAt.UTC()
		}
		canonical, err := json.Marshal(struct {
			Queue       string `json:"queue"`
			Type        string `json:"type"`
			Payload     any    `json:"payload"`
			Priority    int    `json:"priority"`
			ScheduledAt any    `json:"scheduled_at"`
			MaxAttempts int    `json:"max_attempts"`
		}{params.Queue, params.Type, payload, params.Priority, scheduled, params.MaxAttempts})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not hash request")
			return
		}
		sum := sha256.Sum256(canonical)
		params.RequestHash = hex.EncodeToString(sum[:])
	}
	job, created, err := s.Store.CreateWithOptions(r.Context(), params)
	if errors.Is(err, postgres.ErrIdempotencyConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not create job")
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+job.ID)
	if created {
		s.logger().Info("job enqueued", "job_id", job.ID, "queue", job.Queue,
			"job_type", job.Type, "priority", job.Priority)
		writeJSON(w, http.StatusCreated, job)
	} else {
		writeJSON(w, http.StatusOK, job)
	}
}

func (s Server) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}
	job, err := s.Store.Get(r.Context(), id)
	if errors.Is(err, postgres.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s Server) list(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	jobs, err := s.Store.List(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s Server) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}
	job, err := s.Store.Cancel(r.Context(), id)
	if errors.Is(err, postgres.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if errors.Is(err, postgres.ErrNotCancellable) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not cancel job")
		return
	}
	s.logger().Info("job cancellation requested", "job_id", job.ID, "queue", job.Queue,
		"job_type", job.Type, "status", job.Status)
	writeJSON(w, http.StatusOK, job)
}

func (s Server) replay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}
	additional := 1
	if r.ContentLength != 0 {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var body struct {
			AdditionalAttempts *int `json:"additional_attempts"`
		}
		if err := dec.Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid replay request")
			return
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			writeError(w, http.StatusBadRequest, "replay request must contain one object")
			return
		}
		if body.AdditionalAttempts != nil {
			additional = *body.AdditionalAttempts
		}
	}
	if additional < 1 || additional > 20 {
		writeError(w, http.StatusBadRequest, "additional_attempts must be between 1 and 20")
		return
	}
	job, err := s.Store.Replay(r.Context(), id, additional)
	if errors.Is(err, postgres.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if errors.Is(err, postgres.ErrNotReplayable) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not replay job")
		return
	}
	s.logger().Info("job replayed", "job_id", job.ID, "queue", job.Queue,
		"job_type", job.Type, "replay_count", job.ReplayCount)
	writeJSON(w, http.StatusOK, job)
}

func (s Server) attempts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}
	if _, err := s.Store.Get(r.Context(), id); errors.Is(err, postgres.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read job")
		return
	}
	attempts, err := s.Store.Attempts(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read attempts")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"attempts": attempts})
}

func (s Server) queues(w http.ResponseWriter, r *http.Request) {
	queues, err := s.Store.ListQueues(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list queues")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": queues})
}

func (s Server) workers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.Store.ListWorkers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list workers")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

func (s Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if s.Ready == nil || s.Ready(ctx) != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
