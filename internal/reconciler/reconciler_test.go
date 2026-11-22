package reconciler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/engine"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/store"
)

// fakeEngine plays the cluster: Apply makes obs match the spec; tests poke obs
// to simulate pods dying or someone editing objects by hand.
type fakeEngine struct {
	mu        sync.Mutex
	obs       engine.Observed
	applies   int
	applyErr  error
	deleted   bool
	deleteCnt int
}

func (f *fakeEngine) Apply(_ context.Context, svc *model.Service) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applies++
	if f.applyErr != nil {
		return f.applyErr
	}
	f.obs.Exists = true
	f.obs.Drift = nil
	f.obs.RunningVersion = svc.Spec.Version
	return nil
}

func (f *fakeEngine) Observe(context.Context, *model.Service) (engine.Observed, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.obs, nil
}

func (f *fakeEngine) Delete(context.Context, *model.Service) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCnt++
	return f.deleteCnt >= 2, nil // first call starts teardown, second sees it gone
}

func (f *fakeEngine) set(fn func(o *engine.Observed)) {
	f.mu.Lock()
	fn(&f.obs)
	f.mu.Unlock()
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

type rec struct {
	nopRecorder
	recovered []time.Duration
	drift     int
}

func (r *rec) Recovered(_ model.Engine, d time.Duration) { r.recovered = append(r.recovered, d) }
func (r *rec) DriftDetected(model.Engine)                { r.drift++ }

func setup(t *testing.T) (*Reconciler, *store.Memory, *fakeEngine, *clock, *rec, *model.Service) {
	t.Helper()
	st := store.NewMemory()
	fe := &fakeEngine{}
	ck := &clock{t: time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)}
	rc := &rec{}
	r := New(st, engine.Registry{model.EngineKong: fe}, Options{Now: ck.now, Recorder: rc})
	svc := &model.Service{Name: "edge", Engine: model.EngineKong, Spec: model.Spec{Version: "3.9", Replicas: 2}}
	if err := st.Create(t.Context(), svc); err != nil {
		t.Fatal(err)
	}
	return r, st, fe, ck, rc, svc
}

func reasons(t *testing.T, st store.Store, id string) string {
	t.Helper()
	ev, _ := st.Events(t.Context(), id)
	var out []string
	for _, e := range ev {
		out = append(out, e.Reason)
	}
	return strings.Join(out, ",")
}

func mustReconcile(t *testing.T, r *Reconciler, id string) time.Duration {
	t.Helper()
	d, err := r.Reconcile(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestProvisionToReady(t *testing.T) {
	r, st, fe, _, _, svc := setup(t)

	requeue := mustReconcile(t, r, svc.ID)
	got, _ := st.Get(t.Context(), svc.ID)
	if got.Status.Phase != model.PhaseProvisioning || requeue == 0 {
		t.Fatalf("want Provisioning with requeue, got %s requeue=%v", got.Status.Phase, requeue)
	}
	if got.Status.ObservedGeneration != 1 || fe.applies != 1 {
		t.Fatalf("want gen 1 applied once, got obsGen=%d applies=%d", got.Status.ObservedGeneration, fe.applies)
	}

	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 2 })
	requeue = mustReconcile(t, r, svc.ID)
	got, _ = st.Get(t.Context(), svc.ID)
	if got.Status.Phase != model.PhaseReady || requeue != 0 {
		t.Fatalf("want Ready without requeue, got %s requeue=%v", got.Status.Phase, requeue)
	}
	if fe.applies != 1 {
		t.Fatalf("steady state must not re-apply; applies=%d", fe.applies)
	}
}

func TestSpecChangeAppliesOnce(t *testing.T) {
	r, st, fe, _, _, svc := setup(t)
	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 2 })
	mustReconcile(t, r, svc.ID)

	cur, _ := st.Get(t.Context(), svc.ID)
	cur.Spec.Version = "3.10"
	_ = st.UpdateSpec(t.Context(), cur)
	mustReconcile(t, r, svc.ID)
	mustReconcile(t, r, svc.ID)
	got, _ := st.Get(t.Context(), svc.ID)
	if fe.applies != 2 || got.Status.ObservedGeneration != 2 || got.Status.RunningVersion != "3.10" {
		t.Fatalf("applies=%d obsGen=%d running=%s", fe.applies, got.Status.ObservedGeneration, got.Status.RunningVersion)
	}
}

func TestDriftIsHealedAndRecoveryTimed(t *testing.T) {
	r, st, fe, ck, rc, svc := setup(t)
	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 2 })
	mustReconcile(t, r, svc.ID)

	// Someone deletes the Deployment by hand.
	fe.set(func(o *engine.Observed) {
		o.Exists = false
		o.ReadyReplicas = 0
		o.Drift = []string{"deployment missing"}
	})
	mustReconcile(t, r, svc.ID)
	if fe.applies != 2 || rc.drift != 1 {
		t.Fatalf("drift not healed: applies=%d drift=%d", fe.applies, rc.drift)
	}
	got, _ := st.Get(t.Context(), svc.ID)
	if got.Status.Phase != model.PhaseDegraded {
		t.Fatalf("want Degraded while pods come back, got %s", got.Status.Phase)
	}

	ck.advance(7 * time.Second)
	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 2 })
	mustReconcile(t, r, svc.ID)
	got, _ = st.Get(t.Context(), svc.ID)
	if got.Status.Phase != model.PhaseReady {
		t.Fatalf("want Ready, got %s", got.Status.Phase)
	}
	if len(rc.recovered) != 1 || rc.recovered[0] != 7*time.Second {
		t.Fatalf("want one 7s recovery, got %v", rc.recovered)
	}
	if ev := reasons(t, st, svc.ID); !strings.Contains(ev, "DriftDetected,DriftCorrected") {
		t.Fatalf("events %s: want DriftDetected,DriftCorrected", ev)
	}
}

func TestPodLossDegradesThenRecovers(t *testing.T) {
	r, st, fe, ck, rc, svc := setup(t)
	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 2 })
	mustReconcile(t, r, svc.ID)

	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 1 })
	mustReconcile(t, r, svc.ID)
	got, _ := st.Get(t.Context(), svc.ID)
	if got.Status.Phase != model.PhaseDegraded {
		t.Fatalf("want Degraded, got %s", got.Status.Phase)
	}
	if fe.applies != 1 {
		t.Fatal("a pod restart is the Deployment's job; the control plane must not re-apply")
	}
	ck.advance(3 * time.Second)
	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 2 })
	mustReconcile(t, r, svc.ID)
	if len(rc.recovered) != 1 || rc.recovered[0] != 3*time.Second {
		t.Fatalf("recovery: %v", rc.recovered)
	}
}

func TestRolloutShowsUpdating(t *testing.T) {
	r, st, fe, _, _, svc := setup(t)
	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 2; o.Message = "rollout in progress: 1/2 updated" })
	mustReconcile(t, r, svc.ID)
	fe.set(func(o *engine.Observed) { o.Message = "" })
	mustReconcile(t, r, svc.ID)
	fe.set(func(o *engine.Observed) { o.Message = "rollout in progress: 1/2 updated" })
	mustReconcile(t, r, svc.ID)
	got, _ := st.Get(t.Context(), svc.ID)
	if got.Status.Phase != model.PhaseUpdating {
		t.Fatalf("want Updating, got %s", got.Status.Phase)
	}
}

func TestApplyFailureMarksFailedAndErrors(t *testing.T) {
	r, st, fe, _, _, svc := setup(t)
	fe.applyErr = errors.New("quota exceeded")
	if _, err := r.Reconcile(t.Context(), svc.ID); err == nil {
		t.Fatal("want error so the queue backs off")
	}
	got, _ := st.Get(t.Context(), svc.ID)
	if got.Status.Phase != model.PhaseFailed || got.Status.Message != "quota exceeded" {
		t.Fatalf("status: %+v", got.Status)
	}
	// Generation stays unobserved so the next pass retries the apply.
	if got.Status.ObservedGeneration != 0 {
		t.Fatalf("failed apply must not mark generation observed")
	}
}

func TestDeleteRemovesRowAfterTeardown(t *testing.T) {
	r, st, fe, _, _, svc := setup(t)
	mustReconcile(t, r, svc.ID)
	cur, _ := st.Get(t.Context(), svc.ID)
	cur.Deleting = true
	_ = st.UpdateSpec(t.Context(), cur)

	if d := mustReconcile(t, r, svc.ID); d == 0 {
		t.Fatal("teardown in progress should requeue")
	}
	got, _ := st.Get(t.Context(), svc.ID)
	if got.Status.Phase != model.PhaseDeleting {
		t.Fatalf("want Deleting, got %s", got.Status.Phase)
	}
	mustReconcile(t, r, svc.ID)
	if _, err := st.Get(t.Context(), svc.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("row should be gone, got %v", err)
	}
	if fe.deleteCnt != 2 {
		t.Fatalf("deleteCnt=%d", fe.deleteCnt)
	}
	// Reconciling a vanished id is a no-op.
	mustReconcile(t, r, svc.ID)
}

func TestRunLoopConverges(t *testing.T) {
	st := store.NewMemory()
	fe := &fakeEngine{}
	r := New(st, engine.Registry{model.EngineKong: fe}, Options{ConvergePoll: 20 * time.Millisecond, Resync: time.Hour})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.Run(ctx)

	svc := &model.Service{Name: "edge", Engine: model.EngineKong, Spec: model.Spec{Version: "3.9", Replicas: 1}}
	_ = st.Create(t.Context(), svc)
	r.Enqueue(svc.ID)
	time.Sleep(50 * time.Millisecond)
	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 1 })

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := st.Get(t.Context(), svc.ID)
		if got.Status.Phase == model.PhaseReady {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("loop did not converge to Ready")
}

func TestDriftHealedWhileReadyStillCountsAsRecovery(t *testing.T) {
	r, st, fe, _, rc, svc := setup(t)
	fe.set(func(o *engine.Observed) { o.ReadyReplicas = 2 })
	mustReconcile(t, r, svc.ID)

	// A ConfigMap edit: drift, but pods keep serving the old config.
	fe.set(func(o *engine.Observed) { o.Drift = []string{"configmap edited outside the control plane"} })
	mustReconcile(t, r, svc.ID)
	if len(rc.recovered) != 1 {
		t.Fatalf("want the healed drift recorded as one recovery, got %v", rc.recovered)
	}
	if ev := reasons(t, st, svc.ID); !strings.HasSuffix(ev, "DriftDetected,DriftCorrected,Recovered") {
		t.Fatalf("events: %s", ev)
	}
	// Nothing stale left behind for the next planned rollout to pick up.
	fe.set(func(o *engine.Observed) { o.Message = "rollout in progress: 1/2 updated" })
	mustReconcile(t, r, svc.ID)
	fe.set(func(o *engine.Observed) { o.Message = "" })
	mustReconcile(t, r, svc.ID)
	if len(rc.recovered) != 1 {
		t.Fatalf("a planned rollout must not be reported as a recovery: %v", rc.recovered)
	}
}
