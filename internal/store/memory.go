package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

// Memory is an in-process Store for tests and single-node demos.
type Memory struct {
	mu       sync.RWMutex
	services map[string]*model.Service
	events   map[string][]model.Event
	now      func() time.Time
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		services: map[string]*model.Service{},
		events:   map[string][]model.Event{},
		now:      time.Now,
	}
}

// NewID returns a short random service ID.
func NewID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "svc-" + hex.EncodeToString(b)
}

// clone deep-copies through JSON so callers never share maps or slices with the store.
func clone(s *model.Service) *model.Service {
	b, _ := json.Marshal(s)
	var out model.Service
	_ = json.Unmarshal(b, &out)
	return &out
}

func (m *Memory) Create(_ context.Context, s *model.Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.services {
		if existing.Name == s.Name {
			return ErrAlreadyExists
		}
	}
	now := m.now().UTC()
	s.ID = NewID()
	s.ResourceVersion = 1
	s.Generation = 1
	s.CreatedAt, s.UpdatedAt = now, now
	if s.Status.Phase == "" {
		s.Status.Phase = model.PhasePending
	}
	m.services[s.ID] = clone(s)
	return nil
}

func (m *Memory) Get(_ context.Context, id string) (*model.Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.services[id]
	if !ok {
		return nil, ErrNotFound
	}
	return clone(s), nil
}

func (m *Memory) List(_ context.Context) ([]*model.Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*model.Service, 0, len(m.services))
	for _, s := range m.services {
		out = append(out, clone(s))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt) || (out[i].CreatedAt.Equal(out[j].CreatedAt) && out[i].Name < out[j].Name)
	})
	return out, nil
}

func (m *Memory) UpdateSpec(_ context.Context, s *model.Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.services[s.ID]
	if !ok {
		return ErrNotFound
	}
	if cur.ResourceVersion != s.ResourceVersion {
		return ErrConflict
	}
	next := clone(cur)
	next.Spec = clone(s).Spec
	next.Deleting = s.Deleting
	next.ResourceVersion++
	next.Generation++
	next.UpdatedAt = m.now().UTC()
	m.services[s.ID] = next
	*s = *clone(next)
	return nil
}

func (m *Memory) UpdateStatus(_ context.Context, s *model.Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.services[s.ID]
	if !ok {
		return ErrNotFound
	}
	if cur.ResourceVersion != s.ResourceVersion {
		return ErrConflict
	}
	next := clone(cur)
	next.Status = s.Status
	next.ResourceVersion++
	next.UpdatedAt = m.now().UTC()
	m.services[s.ID] = next
	*s = *clone(next)
	return nil
}

func (m *Memory) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.services[id]; !ok {
		return ErrNotFound
	}
	delete(m.services, id)
	delete(m.events, id)
	return nil
}

func (m *Memory) AddEvent(_ context.Context, id string, e model.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.services[id]; !ok {
		return ErrNotFound
	}
	if e.Time.IsZero() {
		e.Time = m.now().UTC()
	}
	ev := append(m.events[id], e)
	if len(ev) > MaxEvents {
		ev = ev[len(ev)-MaxEvents:]
	}
	m.events[id] = ev
	return nil
}

func (m *Memory) Events(_ context.Context, id string) ([]model.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.services[id]; !ok {
		return nil, ErrNotFound
	}
	return append([]model.Event(nil), m.events[id]...), nil
}
