package store

import (
	"errors"
	"sync"
	"testing"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

func newSvc(name string) *model.Service {
	return &model.Service{Name: name, Engine: model.EngineKong, Spec: model.Spec{Version: "3.9", Replicas: 1}}
}

func TestMemoryOptimisticConcurrency(t *testing.T) {
	ctx := t.Context()
	m := NewMemory()
	s := newSvc("a")
	if err := m.Create(ctx, s); err != nil {
		t.Fatal(err)
	}
	a, _ := m.Get(ctx, s.ID)
	b, _ := m.Get(ctx, s.ID)

	a.Spec.Replicas = 2
	if err := m.UpdateSpec(ctx, a); err != nil {
		t.Fatal(err)
	}
	if a.ResourceVersion != 2 || a.Generation != 2 {
		t.Fatalf("want rv=2 gen=2, got rv=%d gen=%d", a.ResourceVersion, a.Generation)
	}
	b.Status.Phase = model.PhaseReady
	if err := m.UpdateStatus(ctx, b); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale status write: want ErrConflict, got %v", err)
	}
}

func TestMemoryStatusDoesNotBumpGeneration(t *testing.T) {
	ctx := t.Context()
	m := NewMemory()
	s := newSvc("a")
	_ = m.Create(ctx, s)
	s.Status.Phase = model.PhaseReady
	if err := m.UpdateStatus(ctx, s); err != nil {
		t.Fatal(err)
	}
	if s.Generation != 1 || s.ResourceVersion != 2 {
		t.Fatalf("want gen=1 rv=2, got gen=%d rv=%d", s.Generation, s.ResourceVersion)
	}
	// UpdateStatus must not let a caller sneak in a spec change.
	s.Spec.Replicas = 5
	_ = m.UpdateStatus(ctx, s)
	got, _ := m.Get(ctx, s.ID)
	if got.Spec.Replicas != 1 {
		t.Fatalf("status write changed spec")
	}
}

func TestMemoryReturnsCopies(t *testing.T) {
	ctx := t.Context()
	m := NewMemory()
	s := newSvc("a")
	s.Spec.Kong = &model.KongSpec{Routes: []model.Route{{Name: "r", Path: "/", Upstream: "http://x"}}}
	_ = m.Create(ctx, s)
	got, _ := m.Get(ctx, s.ID)
	got.Spec.Kong.Routes[0].Path = "/mutated"
	again, _ := m.Get(ctx, s.ID)
	if again.Spec.Kong.Routes[0].Path != "/" {
		t.Fatal("caller mutation leaked into the store")
	}
}

func TestMemoryEventsCapped(t *testing.T) {
	ctx := t.Context()
	m := NewMemory()
	s := newSvc("a")
	_ = m.Create(ctx, s)
	for i := 0; i < MaxEvents+25; i++ {
		_ = m.AddEvent(ctx, s.ID, model.Event{Reason: "x"})
	}
	ev, _ := m.Events(ctx, s.ID)
	if len(ev) != MaxEvents {
		t.Fatalf("want %d events, got %d", MaxEvents, len(ev))
	}
}

func TestMemoryConcurrentWritersOneWins(t *testing.T) {
	ctx := t.Context()
	m := NewMemory()
	s := newSvc("a")
	_ = m.Create(ctx, s)
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := *s // all start from rv=1
			c.Spec.Replicas = 2
			if err := m.UpdateSpec(ctx, &c); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("want exactly 1 winner from the same resourceVersion, got %d", wins)
	}
}

func TestMemoryDuplicateAndMissing(t *testing.T) {
	ctx := t.Context()
	m := NewMemory()
	_ = m.Create(ctx, newSvc("a"))
	if err := m.Create(ctx, newSvc("a")); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
	if _, err := m.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := m.Delete(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
