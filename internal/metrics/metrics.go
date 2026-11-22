// Package metrics exports control-plane and fleet health to Prometheus.
//
// The numbers an on-call engineer and an SLA report both need: how long
// reconciles take, how often reality drifts from intent, how long services
// take to recover, and what state every service is in right now.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

var phases = []model.Phase{
	model.PhasePending, model.PhaseProvisioning, model.PhaseReady, model.PhaseUpdating,
	model.PhaseDegraded, model.PhaseFailed, model.PhaseDeleting,
}

// Metrics implements reconciler.Recorder.
type Metrics struct {
	reg             *prometheus.Registry
	reconcileDur    *prometheus.HistogramVec
	drift           *prometheus.CounterVec
	recovery        *prometheus.HistogramVec
	ready           *prometheus.GaugeVec
	readyReplicas   *prometheus.GaugeVec
	desiredReplicas *prometheus.GaugeVec
	phase           *prometheus.GaugeVec
	lag             *prometheus.GaugeVec
}

// New registers every metric on a fresh registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := &Metrics{
		reg: reg,
		reconcileDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fleet_reconcile_duration_seconds",
			Help:    "Time spent in one reconcile pass.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"engine", "result"}),
		drift: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fleet_drift_detected_total",
			Help: "Times live state was found to differ from desired state without a spec change.",
		}, []string{"engine"}),
		recovery: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fleet_recovery_seconds",
			Help:    "Time from a service leaving Ready (drift, pod loss, failover) until it is Ready again.",
			Buckets: []float64{0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89},
		}, []string{"engine"}),
		ready: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fleet_service_ready",
			Help: "1 if the service is Ready, else 0.",
		}, []string{"service", "engine"}),
		readyReplicas: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fleet_service_ready_replicas",
			Help: "Replicas passing readiness.",
		}, []string{"service", "engine"}),
		desiredReplicas: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fleet_service_desired_replicas",
			Help: "Replicas requested in the spec.",
		}, []string{"service", "engine"}),
		phase: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fleet_service_phase",
			Help: "1 for the service's current phase, 0 for the others.",
		}, []string{"service", "engine", "phase"}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fleet_postgres_replication_lag_bytes",
			Help: "Worst replica replay lag behind the primary, in bytes.",
		}, []string{"service"}),
	}
	reg.MustRegister(m.reconcileDur, m.drift, m.recovery, m.ready, m.readyReplicas, m.desiredReplicas, m.phase, m.lag)
	return m
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

func (m *Metrics) ReconcileDone(e model.Engine, d time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.reconcileDur.WithLabelValues(string(e), result).Observe(d.Seconds())
}

func (m *Metrics) DriftDetected(e model.Engine) { m.drift.WithLabelValues(string(e)).Inc() }

func (m *Metrics) Recovered(e model.Engine, d time.Duration) {
	m.recovery.WithLabelValues(string(e)).Observe(d.Seconds())
}

func (m *Metrics) ServiceState(s *model.Service) {
	e := string(s.Engine)
	ready := 0.0
	if s.Status.Phase == model.PhaseReady {
		ready = 1
	}
	m.ready.WithLabelValues(s.Name, e).Set(ready)
	m.readyReplicas.WithLabelValues(s.Name, e).Set(float64(s.Status.ReadyReplicas))
	m.desiredReplicas.WithLabelValues(s.Name, e).Set(float64(s.Spec.Replicas))
	for _, p := range phases {
		v := 0.0
		if p == s.Status.Phase {
			v = 1
		}
		m.phase.WithLabelValues(s.Name, e, string(p)).Set(v)
	}
	if s.Engine == model.EnginePostgres {
		m.lag.WithLabelValues(s.Name).Set(float64(s.Status.ReplicationLagBytes))
	}
}

// Forget drops a deleted service's series so dashboards don't show ghosts.
func (m *Metrics) Forget(s *model.Service) {
	e := string(s.Engine)
	m.ready.DeleteLabelValues(s.Name, e)
	m.readyReplicas.DeleteLabelValues(s.Name, e)
	m.desiredReplicas.DeleteLabelValues(s.Name, e)
	for _, p := range phases {
		m.phase.DeleteLabelValues(s.Name, e, string(p))
	}
	m.lag.DeleteLabelValues(s.Name)
}
