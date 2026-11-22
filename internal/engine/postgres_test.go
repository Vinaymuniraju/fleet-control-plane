package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

// sqlStub answers psql calls by pod and statement substring and records them.
type sqlStub struct {
	mu      sync.Mutex
	answers map[string]string // "pod|substring" -> output
	calls   []string          // "pod|stmt"
}

func (s *sqlStub) run(_ context.Context, _, pod, _ string, stmts ...string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range stmts {
		s.calls = append(s.calls, pod+"|"+st)
	}
	last := stmts[len(stmts)-1]
	for k, v := range s.answers {
		p, sub, _ := strings.Cut(k, "|")
		if p == pod && strings.Contains(last, sub) {
			return v, nil
		}
	}
	return "", nil
}

func (s *sqlStub) called(pod, sub string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if strings.HasPrefix(c, pod+"|") && strings.Contains(c, sub) {
			return true
		}
	}
	return false
}

func pgSvc(replicas int) *model.Service {
	return &model.Service{ID: "svc-pg", Name: "orders", Engine: model.EnginePostgres, Generation: 1,
		Spec: model.Spec{Version: "17-alpine", Replicas: replicas, Postgres: &model.PostgresSpec{
			Settings:   map[string]string{"work_mem": "8MB", "max_connections": "80"},
			Extensions: []string{"pg_stat_statements"},
		}}}
}

func newPG(t *testing.T) (*Postgres, *fake.Clientset, *sqlStub, *time.Time) {
	t.Helper()
	c := fake.NewClientset()
	stub := &sqlStub{answers: map[string]string{}}
	now := time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)
	p := &Postgres{Kube: Kube{Client: c}, SQL: stub.run, FailoverAfter: 10 * time.Second, Now: func() time.Time { return now }}
	return p, c, stub, &now
}

// addPod fakes a StatefulSet pod.
func addPod(t *testing.T, c *fake.Clientset, ns, name string, ready bool) {
	t.Helper()
	st := corev1.ConditionFalse
	if ready {
		st = corev1.ConditionTrue
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"app": pgName}},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: st}}},
	}
	if _, err := c.CoreV1().Pods(ns).Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func setReady(t *testing.T, c *fake.Clientset, ns, name string, ready bool) {
	t.Helper()
	pod, _ := c.CoreV1().Pods(ns).Get(t.Context(), name, metav1.GetOptions{})
	pod.Status.Conditions[0].Status = map[bool]corev1.ConditionStatus{true: corev1.ConditionTrue, false: corev1.ConditionFalse}[ready]
	_, _ = c.CoreV1().Pods(ns).UpdateStatus(t.Context(), pod, metav1.UpdateOptions{})
}

func label(t *testing.T, c *fake.Clientset, ns, name string) string {
	t.Helper()
	pod, err := c.CoreV1().Pods(ns).Get(t.Context(), name, metav1.GetOptions{})
	if err != nil {
		return "<gone>"
	}
	return pod.Labels[RoleLabel]
}

func topo(t *testing.T, p *Postgres, ns string) topology {
	t.Helper()
	tp, err := p.topology(t.Context(), ns)
	if err != nil {
		t.Fatal(err)
	}
	return tp
}

func TestPostgresApplyObjects(t *testing.T) {
	p, c, _, _ := newPG(t)
	svc := pgSvc(2)
	if err := p.Apply(t.Context(), svc); err != nil {
		t.Fatal(err)
	}
	ns := Namespace(svc)
	sts, err := c.AppsV1().StatefulSets(ns).Get(t.Context(), pgName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(sts.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "max_connections=80") || strings.Contains(args, "work_mem") {
		t.Fatalf("restart-only settings belong on the command line, reloadable ones do not: %s", args)
	}
	rw, _ := c.CoreV1().Services(ns).Get(t.Context(), pgRW, metav1.GetOptions{})
	if rw.Spec.Selector[RoleLabel] != rolePrimary {
		t.Fatalf("pg-rw must select the primary role: %v", rw.Spec.Selector)
	}
	if tp := topo(t, p, ns); tp.Primary != "pg-0" || tp.Bootstrapped {
		t.Fatalf("initial topology: %+v", tp)
	}
	sec1, _ := c.CoreV1().Secrets(ns).Get(t.Context(), pgSecret, metav1.GetOptions{})

	// Re-apply must keep runtime topology and credentials untouched.
	_ = p.setTopology(t.Context(), ns, topology{Primary: "pg-1", Bootstrapped: true})
	if err := p.Apply(t.Context(), svc); err != nil {
		t.Fatal(err)
	}
	if tp := topo(t, p, ns); tp.Primary != "pg-1" {
		t.Fatal("Apply overwrote the topology after a failover")
	}
	sec2, _ := c.CoreV1().Secrets(ns).Get(t.Context(), pgSecret, metav1.GetOptions{})
	if sec1.StringData["password"] != sec2.StringData["password"] {
		t.Fatal("Apply rotated the database password")
	}
}

func TestPostgresApplyRefusesToScaleAwayThePrimary(t *testing.T) {
	p, _, _, _ := newPG(t)
	svc := pgSvc(2)
	_ = p.Apply(t.Context(), svc)
	_ = p.setTopology(t.Context(), Namespace(svc), topology{Primary: "pg-1", Bootstrapped: true})
	svc.Spec.Replicas = 1
	if err := p.Apply(t.Context(), svc); err == nil || !strings.Contains(err.Error(), "would remove the primary") {
		t.Fatalf("want refusal, got %v", err)
	}
}

func TestSettingStatements(t *testing.T) {
	stmts := settingStatements(map[string]string{"work_mem": "8MB"})
	joined := strings.Join(stmts, ";")
	if !strings.Contains(joined, "ALTER SYSTEM SET work_mem = '8MB'") ||
		!strings.Contains(joined, "ALTER SYSTEM RESET statement_timeout") ||
		strings.Contains(joined, "max_connections") ||
		stmts[len(stmts)-1] != "SELECT pg_reload_conf()" {
		t.Fatalf("unexpected statements: %v", stmts)
	}
}

func TestMaintainLabelsBootstrapsAndAppliesSettings(t *testing.T) {
	p, c, stub, _ := newPG(t)
	svc := pgSvc(2)
	_ = p.Apply(t.Context(), svc)
	ns := Namespace(svc)
	addPod(t, c, ns, "pg-0", true)
	addPod(t, c, ns, "pg-1", true)
	stub.answers["pg-0|pg_extension"] = "plpgsql"

	events, err := p.Maintain(t.Context(), svc)
	if err != nil {
		t.Fatal(err)
	}
	if label(t, c, ns, "pg-0") != rolePrimary || label(t, c, ns, "pg-1") != roleReplica {
		t.Fatal("role labels not set from topology")
	}
	if !topo(t, p, ns).Bootstrapped {
		t.Fatal("bootstrap not recorded")
	}
	if !stub.called("pg-0", "ALTER SYSTEM SET work_mem = '8MB'") || !stub.called("pg-1", "ALTER SYSTEM SET work_mem = '8MB'") {
		t.Fatal("settings must be applied on every ready pod")
	}
	if !stub.called("pg-0", `CREATE EXTENSION IF NOT EXISTS "pg_stat_statements"`) {
		t.Fatal("extension not created")
	}
	reasons := []string{}
	for _, e := range events {
		reasons = append(reasons, e.Reason)
	}
	if got := strings.Join(reasons, ","); got != "Bootstrapped,SettingsApplied,ExtensionCreated" {
		t.Fatalf("events: %s", got)
	}
	// Second pass is quiet.
	stub.answers["pg-0|pg_extension"] = "plpgsql,pg_stat_statements"
	events, _ = p.Maintain(t.Context(), svc)
	if len(events) != 0 {
		t.Fatalf("steady state produced events: %v", events)
	}
}

func TestAutomaticFailoverAfterGracePeriod(t *testing.T) {
	p, c, stub, now := newPG(t)
	svc := pgSvc(3)
	_ = p.Apply(t.Context(), svc)
	ns := Namespace(svc)
	addPod(t, c, ns, "pg-0", true)
	addPod(t, c, ns, "pg-1", true)
	addPod(t, c, ns, "pg-2", true)
	stub.answers["pg-0|pg_extension"] = "pg_stat_statements"
	_, _ = p.Maintain(t.Context(), svc) // bootstrap

	setReady(t, c, ns, "pg-0", false)
	stub.answers["pg-1|pg_last_wal_replay_lsn"] = "0/3000100"
	stub.answers["pg-2|pg_last_wal_replay_lsn"] = "0/3000200" // most caught up

	if ev, _ := p.Maintain(t.Context(), svc); len(ev) != 0 {
		t.Fatalf("must not fail over before the grace period: %v", ev)
	}
	*now = now.Add(11 * time.Second)
	ev, err := p.Maintain(t.Context(), svc)
	if err != nil {
		t.Fatal(err)
	}
	if tp := topo(t, p, ns); tp.Primary != "pg-2" {
		t.Fatalf("want pg-2 promoted (highest LSN), topology says %s", tp.Primary)
	}
	if !stub.called("pg-2", "pg_promote") || stub.called("pg-1", "pg_promote") {
		t.Fatal("wrong replica promoted")
	}
	if label(t, c, ns, "pg-2") != rolePrimary {
		t.Fatal("new primary not labelled")
	}
	if label(t, c, ns, "pg-0") != "<gone>" {
		t.Fatal("old primary pod must be deleted so it rejoins as a replica")
	}
	if len(ev) != 2 || ev[0].Reason != "PrimaryDown" || ev[1].Reason != "Failover" {
		t.Fatalf("events: %+v", ev)
	}
}

func TestNoFailoverWithSingleReplica(t *testing.T) {
	p, c, _, now := newPG(t)
	svc := pgSvc(1)
	_ = p.Apply(t.Context(), svc)
	ns := Namespace(svc)
	addPod(t, c, ns, "pg-0", true)
	_, _ = p.Maintain(t.Context(), svc)
	setReady(t, c, ns, "pg-0", false)
	*now = now.Add(time.Minute)
	if ev, err := p.Maintain(t.Context(), svc); err != nil || len(ev) != 0 {
		t.Fatalf("single node must wait for the pod to restart: %v %v", ev, err)
	}
}

func TestPlannedSwitchoverFencesAndWaitsForCatchUp(t *testing.T) {
	p, c, stub, _ := newPG(t)
	svc := pgSvc(2)
	_ = p.Apply(t.Context(), svc)
	ns := Namespace(svc)
	addPod(t, c, ns, "pg-0", true)
	addPod(t, c, ns, "pg-1", true)
	_, _ = p.Maintain(t.Context(), svc)
	stub.answers["pg-0|pg_current_wal_lsn"] = "0/5000000"
	stub.answers["pg-1|pg_last_wal_replay_lsn"] = "0/5000000"

	ev, err := p.Failover(t.Context(), svc, true)
	if err != nil {
		t.Fatal(err)
	}
	if !stub.called("pg-0", "default_transaction_read_only = on") {
		t.Fatal("old primary was not fenced read-only")
	}
	if topo(t, p, ns).Primary != "pg-1" || !strings.Contains(ev[0].Message, "data loss 0 bytes") {
		t.Fatalf("switchover result: %+v", ev)
	}
}

func TestPlannedSwitchoverAbortsWhenReplicaLags(t *testing.T) {
	p, c, stub, now := newPG(t)
	// Advance the clock on every call so the catch-up wait times out at once.
	base := *now
	calls := 0
	p.Now = func() time.Time { calls++; return base.Add(time.Duration(calls) * 10 * time.Second) }
	svc := pgSvc(2)
	_ = p.Apply(t.Context(), svc)
	ns := Namespace(svc)
	addPod(t, c, ns, "pg-0", true)
	addPod(t, c, ns, "pg-1", true)
	_, _ = p.Maintain(t.Context(), svc)
	stub.answers["pg-0|pg_current_wal_lsn"] = "0/5000000"
	stub.answers["pg-1|pg_last_wal_replay_lsn"] = "0/4000000"

	if _, err := p.Failover(t.Context(), svc, true); err == nil || !strings.Contains(err.Error(), "did not catch up") {
		t.Fatalf("want abort, got %v", err)
	}
	if topo(t, p, ns).Primary != "pg-0" || label(t, c, ns, "pg-0") != rolePrimary {
		t.Fatal("aborted switchover must leave the old primary in place and unfenced")
	}
	if !stub.called("pg-0", "RESET default_transaction_read_only") {
		t.Fatal("aborted switchover must lift the read-only fence")
	}
}

func TestPostgresObserveHealth(t *testing.T) {
	p, c, stub, _ := newPG(t)
	svc := pgSvc(3)
	_ = p.Apply(t.Context(), svc)
	ns := Namespace(svc)
	addPod(t, c, ns, "pg-0", true)
	sts, _ := c.AppsV1().StatefulSets(ns).Get(t.Context(), pgName, metav1.GetOptions{})
	sts.Status.ReadyReplicas, sts.Status.UpdatedReplicas = 3, 3
	_, _ = c.AppsV1().StatefulSets(ns).UpdateStatus(t.Context(), sts, metav1.UpdateOptions{})

	stub.answers["pg-0|pg_stat_replication"] = "false,1,4096"
	obs, err := p.Observe(t.Context(), svc)
	if err != nil {
		t.Fatal(err)
	}
	if obs.Message != "1/2 replicas streaming" || obs.LagBytes != 4096 || obs.Primary != "pg-0" {
		t.Fatalf("observation: %+v", obs)
	}
	stub.answers["pg-0|pg_stat_replication"] = "true,0,0"
	obs, _ = p.Observe(t.Context(), svc)
	if obs.Message != "primary pg-0 is still in recovery" {
		t.Fatalf("message: %q", obs.Message)
	}
}

func TestParseLSN(t *testing.T) {
	for in, want := range map[string]uint64{"0/3000148": 0x3000148, "1/0": 1 << 32, "16/B374D848": 0x16B374D848} {
		got, err := parseLSN(in)
		if err != nil || got != want {
			t.Errorf("parseLSN(%q) = %x, %v; want %x", in, got, err, want)
		}
		if formatLSN(got) != strings.ToUpper(in) {
			t.Errorf("formatLSN round trip: %s", formatLSN(got))
		}
	}
	if _, err := parseLSN("garbage"); err == nil {
		t.Error("want error")
	}
}
