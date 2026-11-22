// Package api is the control plane's REST interface.
//
// The API only records intent: it validates a request, writes desired state to
// the store and pokes the reconciler. It never talks to Kubernetes itself, so a
// slow or broken cluster can't make the API slow or broken.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/store"
)

// Notifier is called with a service ID after its desired state changes.
type Notifier func(id string)

// Actions are imperative operations an engine may support (e.g. failover).
type Actions interface {
	Failover(ctx context.Context, id string) error
}

// Server serves /v1.
type Server struct {
	store   store.Store
	notify  Notifier
	actions Actions
	log     *slog.Logger
}

// New builds a Server. notify and actions may be nil.
func New(st store.Store, notify Notifier, actions Actions, log *slog.Logger) *Server {
	if notify == nil {
		notify = func(string) {}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Server{store: st, notify: notify, actions: actions, log: log}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/services", s.create)
	mux.HandleFunc("GET /v1/services", s.list)
	mux.HandleFunc("GET /v1/services/{id}", s.get)
	mux.HandleFunc("PATCH /v1/services/{id}", s.patch)
	mux.HandleFunc("DELETE /v1/services/{id}", s.delete)
	mux.HandleFunc("GET /v1/services/{id}/events", s.events)
	mux.HandleFunc("POST /v1/services/{id}/failover", s.failover)
	return mux
}

// CreateRequest is the body of POST /v1/services.
type CreateRequest struct {
	Name     string              `json:"name"`
	Engine   model.Engine        `json:"engine"`
	Version  string              `json:"version,omitempty"`
	Replicas int                 `json:"replicas,omitempty"`
	Kong     *model.KongSpec     `json:"kong,omitempty"`
	Postgres *model.PostgresSpec `json:"postgres,omitempty"`
}

// PatchRequest is the body of PATCH /v1/services/{id}. Nil fields are left
// alone; a non-nil Kong or Postgres block replaces the whole block.
type PatchRequest struct {
	ResourceVersion *int64              `json:"resourceVersion,omitempty"` // optional optimistic-concurrency guard
	Version         *string             `json:"version,omitempty"`
	Replicas        *int                `json:"replicas,omitempty"`
	Kong            *model.KongSpec     `json:"kong,omitempty"`
	Postgres        *model.PostgresSpec `json:"postgres,omitempty"`
}

const maxBody = 1 << 20

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if !decode(w, r, &req) {
		return
	}
	svc := &model.Service{
		Name:   req.Name,
		Engine: req.Engine,
		Spec: model.Spec{
			Version:  req.Version,
			Replicas: req.Replicas,
			Kong:     req.Kong,
			Postgres: req.Postgres,
		},
	}
	if svc.Spec.Version == "" {
		svc.Spec.Version = model.DefaultVersion(svc.Engine)
	}
	if svc.Spec.Replicas == 0 {
		svc.Spec.Replicas = 1
	}
	if err := svc.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.store.Create(r.Context(), svc); err != nil {
		s.storeErr(w, err)
		return
	}
	_ = s.store.AddEvent(r.Context(), svc.ID, model.Event{Type: model.EventNormal, Reason: "Created", Message: fmt.Sprintf("%s service %q requested (version %s, %d replicas)", svc.Engine, svc.Name, svc.Spec.Version, svc.Spec.Replicas)})
	s.notify(svc.ID)
	writeJSON(w, http.StatusCreated, svc)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	svcs, err := s.store.List(r.Context())
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": svcs})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	svc, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, svc)
}

func (s *Server) patch(w http.ResponseWriter, r *http.Request) {
	var req PatchRequest
	if !decode(w, r, &req) {
		return
	}
	id := r.PathValue("id")
	// Without a client-supplied resourceVersion, retry read-modify-write on
	// conflict so a concurrent status write by the reconciler doesn't fail the call.
	attempts := 5
	if req.ResourceVersion != nil {
		attempts = 1
	}
	for i := 0; i < attempts; i++ {
		svc, err := s.store.Get(r.Context(), id)
		if err != nil {
			s.storeErr(w, err)
			return
		}
		if svc.Deleting {
			writeErr(w, http.StatusConflict, errors.New("service is being deleted"))
			return
		}
		if req.ResourceVersion != nil {
			svc.ResourceVersion = *req.ResourceVersion
		}
		changes := applyPatch(svc, &req)
		if err := svc.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		err = s.store.UpdateSpec(r.Context(), svc)
		if errors.Is(err, store.ErrConflict) && i < attempts-1 {
			continue
		}
		if err != nil {
			s.storeErr(w, err)
			return
		}
		_ = s.store.AddEvent(r.Context(), id, model.Event{Type: model.EventNormal, Reason: "SpecChanged", Message: changes})
		s.notify(id)
		writeJSON(w, http.StatusOK, svc)
		return
	}
}

func applyPatch(svc *model.Service, req *PatchRequest) string {
	msg := ""
	add := func(f string, a ...any) {
		if msg != "" {
			msg += "; "
		}
		msg += fmt.Sprintf(f, a...)
	}
	if req.Version != nil && *req.Version != svc.Spec.Version {
		add("version %s -> %s", svc.Spec.Version, *req.Version)
		svc.Spec.Version = *req.Version
	}
	if req.Replicas != nil && *req.Replicas != svc.Spec.Replicas {
		add("replicas %d -> %d", svc.Spec.Replicas, *req.Replicas)
		svc.Spec.Replicas = *req.Replicas
	}
	if req.Kong != nil {
		add("kong config replaced (%d routes, %d plugins)", len(req.Kong.Routes), len(req.Kong.Plugins))
		svc.Spec.Kong = req.Kong
	}
	if req.Postgres != nil {
		add("postgres config replaced (%d settings, %d extensions)", len(req.Postgres.Settings), len(req.Postgres.Extensions))
		svc.Spec.Postgres = req.Postgres
	}
	if msg == "" {
		msg = "no-op patch"
	}
	return msg
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	for i := 0; i < 5; i++ {
		svc, err := s.store.Get(r.Context(), id)
		if err != nil {
			s.storeErr(w, err)
			return
		}
		if svc.Deleting {
			writeJSON(w, http.StatusAccepted, svc)
			return
		}
		svc.Deleting = true
		err = s.store.UpdateSpec(r.Context(), svc)
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err != nil {
			s.storeErr(w, err)
			return
		}
		_ = s.store.AddEvent(r.Context(), id, model.Event{Type: model.EventNormal, Reason: "DeleteRequested", Message: "teardown scheduled"})
		s.notify(id)
		writeJSON(w, http.StatusAccepted, svc)
		return
	}
	writeErr(w, http.StatusConflict, store.ErrConflict)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	ev, err := s.store.Events(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": ev})
}

func (s *Server) failover(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	svc, err := s.store.Get(r.Context(), id)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if svc.Engine != model.EnginePostgres {
		writeErr(w, http.StatusBadRequest, errors.New("failover is only supported for postgres services"))
		return
	}
	if s.actions == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("failover is not available on this control plane"))
		return
	}
	if err := s.actions.Failover(r.Context(), id); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	svc, _ = s.store.Get(r.Context(), id)
	writeJSON(w, http.StatusAccepted, svc)
}

func (s *Server) storeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err)
	case errors.Is(err, store.ErrAlreadyExists):
		writeErr(w, http.StatusConflict, errors.New("a service with that name already exists"))
	case errors.Is(err, store.ErrConflict):
		writeErr(w, http.StatusConflict, err)
	default:
		s.log.Error("store error", "err", err)
		writeErr(w, http.StatusInternalServerError, errors.New("internal error"))
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
