package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	b, _ := io.ReadAll(rr.Body)
	return string(b)
}

func TestMetricsExposeServiceStateAndForget(t *testing.T) {
	m := New()
	svc := &model.Service{Name: "edge", Engine: model.EngineKong, Spec: model.Spec{Replicas: 2},
		Status: model.Status{Phase: model.PhaseReady, ReadyReplicas: 2}}
	m.ServiceState(svc)
	m.DriftDetected(model.EngineKong)
	m.Recovered(model.EngineKong, 3*time.Second)
	m.ReconcileDone(model.EngineKong, 10*time.Millisecond, nil)

	out := scrape(t, m)
	for _, want := range []string{
		`fleet_service_ready{engine="kong",service="edge"} 1`,
		`fleet_service_phase{engine="kong",phase="Ready",service="edge"} 1`,
		`fleet_service_phase{engine="kong",phase="Degraded",service="edge"} 0`,
		`fleet_drift_detected_total{engine="kong"} 1`,
		`fleet_recovery_seconds_count{engine="kong"} 1`,
		`fleet_reconcile_duration_seconds_count{engine="kong",result="ok"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
	m.Forget(svc)
	if strings.Contains(scrape(t, m), `service="edge"`) {
		t.Error("deleted service still exported")
	}
}
