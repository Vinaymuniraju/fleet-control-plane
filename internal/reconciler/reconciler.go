// Package reconciler drives every Service toward its spec.
//
// It is the same pattern Kubernetes controllers use: a rate-limited work queue
// keyed by service ID, so one service is never reconciled by two workers at
// once, failures back off exponentially, and a burst of events for the same
// service collapses into one reconcile. Keys are added by the API (spec
// changed), by cluster watches (something changed under us) and by a periodic
// resync (safety net for anything a watch missed).
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/engine"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
	"github.com/Vinaymuniraju/fleet-control-plane/internal/store"
)

// Recorder receives reconcile telemetry. The metrics package implements it.
type Recorder interface {
	ReconcileDone(engine model.Engine, d time.Duration, err error)
	DriftDetected(engine model.Engine)
	Recovered(engine model.Engine, d time.Duration)
	ServiceState(svc *model.Service)
	Forget(svc *model.Service)
}

type nopRecorder struct{}

func (nopRecorder) ReconcileDone(model.Engine, time.Duration, error) {}
func (nopRecorder) DriftDetected(model.Engine)                       {}
func (nopRecorder) Recovered(model.Engine, time.Duration)            {}
func (nopRecorder) ServiceState(*model.Service)                      {}
func (nopRecorder) Forget(*model.Service)                            {}

// Options tune the loop.
type Options struct {
	Workers      int           // parallel reconciles (different services)
	Resync       time.Duration // full re-scan interval
	ConvergePoll time.Duration // requeue delay while a service is not Ready
	Recorder     Recorder
	Log          *slog.Logger
	Now          func() time.Time
}

// Reconciler is the control loop.
type Reconciler struct {
	store   store.Store
	engines engine.Registry
	opts    Options
	queue   workqueue.TypedRateLimitingInterface[string]

	mu             sync.Mutex
	unhealthySince map[string]time.Time // first moment a Ready service stopped being Ready
}

// New builds a Reconciler. Call Run to start it.
func New(st store.Store, engines engine.Registry, opts Options) *Reconciler {
	if opts.Workers <= 0 {
		opts.Workers = 2
	}
	if opts.Resync <= 0 {
		opts.Resync = 30 * time.Second
	}
	if opts.ConvergePoll <= 0 {
		opts.ConvergePoll = 2 * time.Second
	}
	if opts.Recorder == nil {
		opts.Recorder = nopRecorder{}
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	limiter := workqueue.NewTypedItemExponentialFailureRateLimiter[string](500*time.Millisecond, 60*time.Second)
	return &Reconciler{
		store:          st,
		engines:        engines,
		opts:           opts,
		queue:          workqueue.NewTypedRateLimitingQueueWithConfig(limiter, workqueue.TypedRateLimitingQueueConfig[string]{Name: "services"}),
		unhealthySince: map[string]time.Time{},
	}
}

// Enqueue asks for id to be reconciled soon. Safe to call from anywhere.
func (r *Reconciler) Enqueue(id string) {
	if id != "" {
		r.queue.Add(id)
	}
}

// Run blocks until ctx is done.
func (r *Reconciler) Run(ctx context.Context) {
	defer r.queue.ShutDown()
	r.resync(ctx)
	for i := 0; i < r.opts.Workers; i++ {
		go func() {
			for r.next(ctx) {
			}
		}()
	}
	t := time.NewTicker(r.opts.Resync)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.resync(ctx)
		}
	}
}

func (r *Reconciler) resync(ctx context.Context) {
	svcs, err := r.store.List(ctx)
	if err != nil {
		r.opts.Log.Error("resync list failed", "err", err)
		return
	}
	for _, s := range svcs {
		r.queue.Add(s.ID)
	}
}

func (r *Reconciler) next(ctx context.Context) bool {
	id, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(id)
	requeue, err := r.Reconcile(ctx, id)
	switch {
	case err != nil:
		r.opts.Log.Warn("reconcile failed", "id", id, "err", err)
		r.queue.AddRateLimited(id)
	case requeue > 0:
		r.queue.Forget(id)
		r.queue.AddAfter(id, requeue)
	default:
		r.queue.Forget(id)
	}
	return true
}

// Reconcile runs one pass for id. It returns how soon to look again (0 = only
// on the next event or resync) or an error to retry with backoff.
func (r *Reconciler) Reconcile(ctx context.Context, id string) (time.Duration, error) {
	start := r.opts.Now()
	svc, err := r.store.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	eng, err := r.engines.For(svc)
	if err != nil {
		return 0, err
	}
	requeue, err := r.reconcile(ctx, svc, eng)
	r.opts.Recorder.ReconcileDone(svc.Engine, r.opts.Now().Sub(start), err)
	return requeue, err
}

func (r *Reconciler) reconcile(ctx context.Context, svc *model.Service, eng engine.Engine) (time.Duration, error) {
	if svc.Deleting {
		return r.teardown(ctx, svc, eng)
	}

	obs, err := eng.Observe(ctx, svc)
	if err != nil {
		return 0, fmt.Errorf("observe: %w", err)
	}

	specChanged := svc.Status.ObservedGeneration != svc.Generation
	drifted := !specChanged && svc.Status.ObservedGeneration > 0 && (len(obs.Drift) > 0 || !obs.Exists)
	if drifted {
		r.opts.Recorder.DriftDetected(svc.Engine)
		r.markUnhealthy(svc.ID)
		r.event(ctx, svc, model.EventWarning, "DriftDetected", strings.Join(obs.Drift, "; "))
	}

	if specChanged || !obs.Exists || len(obs.Drift) > 0 {
		if err := eng.Apply(ctx, svc); err != nil {
			r.event(ctx, svc, model.EventWarning, "ApplyFailed", err.Error())
			svc.Status.Phase = model.PhaseFailed
			svc.Status.Message = err.Error()
			svc.Status.LastReconciled = r.opts.Now().UTC()
			_ = r.writeStatus(ctx, svc)
			return 0, fmt.Errorf("apply: %w", err)
		}
		switch {
		case drifted:
			r.event(ctx, svc, model.EventNormal, "DriftCorrected", "re-applied generation "+fmt.Sprint(svc.Generation))
		case specChanged:
			r.event(ctx, svc, model.EventNormal, "Applied", fmt.Sprintf("generation %d applied", svc.Generation))
		}
		if obs, err = eng.Observe(ctx, svc); err != nil {
			return 0, fmt.Errorf("observe after apply: %w", err)
		}
	}

	prev := svc.Status.Phase
	svc.Status.ObservedGeneration = svc.Generation
	svc.Status.ReadyReplicas = obs.ReadyReplicas
	svc.Status.RunningVersion = obs.RunningVersion
	svc.Status.ConfigHash = obs.ConfigHash
	svc.Status.Endpoint = obs.Endpoint
	svc.Status.Primary = obs.Primary
	svc.Status.ReplicationLagBytes = obs.LagBytes
	svc.Status.Message = obs.Message
	svc.Status.Phase = phaseFor(svc, obs, prev)
	svc.Status.LastReconciled = r.opts.Now().UTC()

	if svc.Status.Phase != prev {
		r.onPhaseChange(ctx, svc, prev)
	}
	if err := r.writeStatus(ctx, svc); err != nil {
		return 0, err
	}
	r.opts.Recorder.ServiceState(svc)

	if svc.Status.Phase != model.PhaseReady {
		return r.opts.ConvergePoll, nil
	}
	return 0, nil
}

// phaseFor maps an observation to the user-facing phase.
func phaseFor(svc *model.Service, obs engine.Observed, prev model.Phase) model.Phase {
	wasServing := prev == model.PhaseReady || prev == model.PhaseDegraded || prev == model.PhaseUpdating
	switch {
	case !obs.Exists:
		return model.PhaseProvisioning
	case obs.ReadyReplicas >= svc.Spec.Replicas && obs.Message == "" && len(obs.Drift) == 0:
		return model.PhaseReady
	case strings.HasPrefix(obs.Message, "rollout in progress") && obs.ReadyReplicas > 0:
		return model.PhaseUpdating
	case wasServing:
		return model.PhaseDegraded
	default:
		return model.PhaseProvisioning
	}
}

func (r *Reconciler) onPhaseChange(ctx context.Context, svc *model.Service, prev model.Phase) {
	now := r.opts.Now()
	cur := svc.Status.Phase
	typ := model.EventNormal
	if cur == model.PhaseDegraded || cur == model.PhaseFailed {
		typ = model.EventWarning
		r.markUnhealthy(svc.ID)
	}
	msg := fmt.Sprintf("%s -> %s (%d/%d ready)", orPending(prev), cur, svc.Status.ReadyReplicas, svc.Spec.Replicas)
	if svc.Status.Message != "" {
		msg += ": " + svc.Status.Message
	}
	if cur == model.PhaseReady {
		r.mu.Lock()
		since, ok := r.unhealthySince[svc.ID]
		delete(r.unhealthySince, svc.ID)
		r.mu.Unlock()
		if ok {
			d := now.Sub(since)
			r.opts.Recorder.Recovered(svc.Engine, d)
			msg += fmt.Sprintf("; recovered in %.1fs", d.Seconds())
		}
	}
	r.event(ctx, svc, typ, "PhaseChanged", msg)
}

func (r *Reconciler) markUnhealthy(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.unhealthySince[id]; !ok {
		r.unhealthySince[id] = r.opts.Now()
	}
}

func (r *Reconciler) teardown(ctx context.Context, svc *model.Service, eng engine.Engine) (time.Duration, error) {
	done, err := eng.Delete(ctx, svc)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	if !done {
		if svc.Status.Phase != model.PhaseDeleting {
			svc.Status.Phase = model.PhaseDeleting
			r.event(ctx, svc, model.EventNormal, "Deleting", "tearing down namespace "+engine.Namespace(svc))
			if err := r.writeStatus(ctx, svc); err != nil {
				return 0, err
			}
		}
		return r.opts.ConvergePoll, nil
	}
	r.opts.Recorder.Forget(svc)
	r.mu.Lock()
	delete(r.unhealthySince, svc.ID)
	r.mu.Unlock()
	r.opts.Log.Info("service deleted", "id", svc.ID, "name", svc.Name)
	if err := r.store.Delete(ctx, svc.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
		return 0, err
	}
	return 0, nil
}

// writeStatus saves status; a conflict means the spec moved under us, so the
// caller's error makes the queue retry with fresh state.
func (r *Reconciler) writeStatus(ctx context.Context, svc *model.Service) error {
	if err := r.store.UpdateStatus(ctx, svc); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	return nil
}

func (r *Reconciler) event(ctx context.Context, svc *model.Service, typ model.EventType, reason, msg string) {
	r.opts.Log.Info("event", "service", svc.Name, "type", typ, "reason", reason, "msg", msg)
	_ = r.store.AddEvent(ctx, svc.ID, model.Event{Type: typ, Reason: reason, Message: msg, Time: r.opts.Now().UTC()})
}

func orPending(p model.Phase) model.Phase {
	if p == "" {
		return model.PhasePending
	}
	return p
}
