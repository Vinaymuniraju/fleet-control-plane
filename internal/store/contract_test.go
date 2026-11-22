package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

// Every Store implementation must pass the same contract.

func TestMemoryContract(t *testing.T) {
	runContract(t, func(t *testing.T) Store { return NewMemory() })
}

// TestPostgresContract runs when FLEET_TEST_DSN points at a scratch database,
// e.g. postgres://postgres:test@localhost:55432/postgres?sslmode=disable
func TestPostgresContract(t *testing.T) {
	dsn := os.Getenv("FLEET_TEST_DSN")
	if dsn == "" {
		t.Skip("FLEET_TEST_DSN not set")
	}
	runContract(t, func(t *testing.T) Store {
		p, err := OpenPostgres(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.pool.Exec(context.Background(), `TRUNCATE services CASCADE`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	})
}

func runContract(t *testing.T, mk func(t *testing.T) Store) {
	tests := map[string]func(t *testing.T, s Store){
		"optimistic concurrency":          testOptimisticConcurrency,
		"status does not bump generation": testStatusDoesNotBumpGeneration,
		"returns copies":                  testReturnsCopies,
		"events capped and ordered":       testEventsCapped,
		"concurrent writers, one wins":    testConcurrentWritersOneWins,
		"duplicate and missing":           testDuplicateAndMissing,
		"delete cascades events":          testDeleteCascades,
		"round trips spec and status":     testRoundTrip,
	}
	for name, fn := range tests {
		t.Run(name, func(t *testing.T) { fn(t, mk(t)) })
	}
}

func newSvc(name string) *model.Service {
	return &model.Service{Name: name, Engine: model.EngineKong, Spec: model.Spec{Version: "3.9", Replicas: 1}}
}

func testOptimisticConcurrency(t *testing.T, m Store) {
	ctx := t.Context()
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

func testStatusDoesNotBumpGeneration(t *testing.T, m Store) {
	ctx := t.Context()
	s := newSvc("a")
	_ = m.Create(ctx, s)
	s.Status.Phase = model.PhaseReady
	if err := m.UpdateStatus(ctx, s); err != nil {
		t.Fatal(err)
	}
	if s.Generation != 1 || s.ResourceVersion != 2 {
		t.Fatalf("want gen=1 rv=2, got gen=%d rv=%d", s.Generation, s.ResourceVersion)
	}
	s.Spec.Replicas = 5 // must not sneak in through a status write
	_ = m.UpdateStatus(ctx, s)
	got, _ := m.Get(ctx, s.ID)
	if got.Spec.Replicas != 1 {
		t.Fatal("status write changed spec")
	}
}

func testReturnsCopies(t *testing.T, m Store) {
	ctx := t.Context()
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

func testEventsCapped(t *testing.T, m Store) {
	ctx := t.Context()
	s := newSvc("a")
	_ = m.Create(ctx, s)
	for i := 0; i < MaxEvents+25; i++ {
		_ = m.AddEvent(ctx, s.ID, model.Event{Reason: "r", Message: string(rune('a' + i%26))})
	}
	ev, _ := m.Events(ctx, s.ID)
	if len(ev) != MaxEvents {
		t.Fatalf("want %d events, got %d", MaxEvents, len(ev))
	}
	if ev[len(ev)-1].Message != string(rune('a'+(MaxEvents+24)%26)) {
		t.Fatal("newest event must be last")
	}
	if err := m.AddEvent(ctx, "svc-missing", model.Event{Reason: "x"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("event for missing service: want ErrNotFound, got %v", err)
	}
}

func testConcurrentWritersOneWins(t *testing.T, m Store) {
	ctx := t.Context()
	s := newSvc("a")
	_ = m.Create(ctx, s)
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := *s // every writer read rv=1
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

func testDuplicateAndMissing(t *testing.T, m Store) {
	ctx := t.Context()
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
	ghost := newSvc("ghost")
	ghost.ID, ghost.ResourceVersion = "svc-ghost", 1
	if err := m.UpdateSpec(ctx, ghost); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing: want ErrNotFound, got %v", err)
	}
}

func testDeleteCascades(t *testing.T, m Store) {
	ctx := t.Context()
	s := newSvc("a")
	_ = m.Create(ctx, s)
	_ = m.AddEvent(ctx, s.ID, model.Event{Reason: "x"})
	if err := m.Delete(ctx, s.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Events(ctx, s.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("events after delete: want ErrNotFound, got %v", err)
	}
	// The name is free again.
	if err := m.Create(ctx, newSvc("a")); err != nil {
		t.Fatal(err)
	}
}

func testRoundTrip(t *testing.T, m Store) {
	ctx := t.Context()
	s := &model.Service{Name: "db", Engine: model.EnginePostgres, Spec: model.Spec{Version: "17-alpine", Replicas: 2,
		Postgres: &model.PostgresSpec{Settings: map[string]string{"work_mem": "8MB"}, Extensions: []string{"pgcrypto"}}}}
	_ = m.Create(ctx, s)
	s.Status = model.Status{Phase: model.PhaseReady, ReadyReplicas: 2, Primary: "pg-1", ReplicationLagBytes: 42, ObservedGeneration: 1}
	if err := m.UpdateStatus(ctx, s); err != nil {
		t.Fatal(err)
	}
	list, _ := m.List(ctx)
	if len(list) != 1 {
		t.Fatalf("list: %d", len(list))
	}
	g := list[0]
	if g.Spec.Postgres.Settings["work_mem"] != "8MB" || g.Status.Primary != "pg-1" || g.Status.ReplicationLagBytes != 42 || g.Engine != model.EnginePostgres {
		t.Fatalf("round trip lost data: %+v", g)
	}
}
