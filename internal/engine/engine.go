// Package engine turns a Service's desired state into Kubernetes objects and
// reads back what is actually running.
//
// Every engine is level-triggered and idempotent: Apply can be called any
// number of times and converges to the spec, whatever state it starts from.
// That is what lets the reconciler heal drift by simply applying again.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

// Observed is what an engine sees in the cluster for one Service.
type Observed struct {
	Exists         bool     // the namespace and workload exist
	ReadyReplicas  int      // pods passing readiness
	RunningVersion string   // image tag on the workload
	ConfigHash     string   // config hash on the pod template
	Endpoint       string   // in-cluster address clients use
	Primary        string   // Postgres: current primary pod
	LagBytes       int64    // Postgres: worst replica lag
	Drift          []string // human-readable differences from the spec
	Message        string   // anything else worth surfacing
}

// Engine provisions and inspects one kind of data service.
type Engine interface {
	// Apply creates or updates every object needed for svc to match its spec.
	Apply(ctx context.Context, svc *model.Service) error
	// Observe reports what is running and how it differs from the spec.
	Observe(ctx context.Context, svc *model.Service) (Observed, error)
	// Delete tears svc down. It returns true once nothing is left.
	Delete(ctx context.Context, svc *model.Service) (bool, error)
}

// Namespace is where a service's objects live: one namespace per service, so
// tenants are isolated and teardown is a single delete.
func Namespace(svc *model.Service) string { return "fleet-" + svc.Name }

// Labels every managed object carries.
func Labels(svc *model.Service) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "fleet-control-plane",
		"fleet.dev/service-id":         svc.ID,
		"fleet.dev/service-name":       svc.Name,
		"fleet.dev/engine":             string(svc.Engine),
	}
}

// Hash returns a short stable hash of b, used to roll pods on config change.
func Hash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:16]
}

// Annotation keys.
const (
	AnnoConfigHash = "fleet.dev/config-hash"
	AnnoGeneration = "fleet.dev/generation"
)

// Registry picks the engine for a Service.
type Registry map[model.Engine]Engine

// For returns the engine for svc or an error.
func (r Registry) For(svc *model.Service) (Engine, error) {
	e, ok := r[svc.Engine]
	if !ok {
		return nil, fmt.Errorf("no engine registered for %q", svc.Engine)
	}
	return e, nil
}
