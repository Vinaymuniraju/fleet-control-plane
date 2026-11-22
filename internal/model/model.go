// Package model holds the resource types the control plane manages.
//
// A Service is one managed data-service instance (a Kong gateway or a Postgres
// cluster). Users own Spec (desired state); the reconciler owns Status
// (observed state). The split mirrors how Kubernetes objects work, so the
// control plane can always answer "what was asked for" and "what is running".
package model

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Engine names the kind of data service a Service runs.
type Engine string

const (
	EngineKong     Engine = "kong"
	EnginePostgres Engine = "postgres"
)

// Phase is the coarse lifecycle state shown to users.
type Phase string

const (
	PhasePending      Phase = "Pending"      // accepted, nothing applied yet
	PhaseProvisioning Phase = "Provisioning" // resources applied, not ready
	PhaseReady        Phase = "Ready"        // all replicas serving
	PhaseUpdating     Phase = "Updating"     // rolling out a new generation
	PhaseDegraded     Phase = "Degraded"     // below desired replicas, lagging, or drifted
	PhaseFailed       Phase = "Failed"       // apply failed; see Status.Message
	PhaseDeleting     Phase = "Deleting"
)

// Service is one managed instance.
type Service struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	Engine          Engine    `json:"engine"`
	Spec            Spec      `json:"spec"`
	Status          Status    `json:"status"`
	ResourceVersion int64     `json:"resourceVersion"`
	Generation      int64     `json:"generation"` // bumped on every spec change
	Deleting        bool      `json:"deleting,omitempty"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

// Spec is the desired state.
type Spec struct {
	Version  string        `json:"version"`  // engine image tag, e.g. "3.9" or "17-alpine"
	Replicas int           `json:"replicas"` // Kong: gateway pods. Postgres: 1 primary + (n-1) replicas
	Kong     *KongSpec     `json:"kong,omitempty"`
	Postgres *PostgresSpec `json:"postgres,omitempty"`
}

// KongSpec is the gateway configuration pushed to every data-plane pod.
type KongSpec struct {
	Routes  []Route  `json:"routes"`
	Plugins []Plugin `json:"plugins,omitempty"` // global plugins, e.g. rate-limiting
}

// Route maps a path prefix to an upstream URL.
type Route struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	Upstream string `json:"upstream"`
}

// Plugin is a global Kong plugin with its config.
type Plugin struct {
	Name   string         `json:"name"`
	Config map[string]any `json:"config,omitempty"`
}

// PostgresSpec is the database configuration.
type PostgresSpec struct {
	Settings   map[string]string `json:"settings,omitempty"`   // applied with ALTER SYSTEM
	Extensions []string          `json:"extensions,omitempty"` // CREATE EXTENSION IF NOT EXISTS
}

// Status is the observed state, written only by the reconciler.
type Status struct {
	Phase               Phase     `json:"phase"`
	Message             string    `json:"message,omitempty"`
	ReadyReplicas       int       `json:"readyReplicas"`
	ObservedGeneration  int64     `json:"observedGeneration"`
	ConfigHash          string    `json:"configHash,omitempty"`
	RunningVersion      string    `json:"runningVersion,omitempty"`
	Endpoint            string    `json:"endpoint,omitempty"`
	Primary             string    `json:"primary,omitempty"`             // Postgres: pod currently primary
	ReplicationLagBytes int64     `json:"replicationLagBytes,omitempty"` // Postgres: worst replica lag
	LastReconciled      time.Time `json:"lastReconciled,omitempty"`
}

// EventType separates routine events from ones that need attention.
type EventType string

const (
	EventNormal  EventType = "Normal"
	EventWarning EventType = "Warning"
)

// Event is one line in a Service's audit trail.
type Event struct {
	Time    time.Time `json:"time"`
	Type    EventType `json:"type"`
	Reason  string    `json:"reason"`
	Message string    `json:"message"`
}

var nameRE = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,30}[a-z0-9])?$`)

// MaxReplicas keeps a laptop cluster alive; a real fleet would size per plan.
const MaxReplicas = 5

// DefaultVersion is used when a create request leaves Version empty.
func DefaultVersion(e Engine) string {
	switch e {
	case EngineKong:
		return "3.9"
	case EnginePostgres:
		return "17-alpine"
	}
	return ""
}

// Validate checks a Service before it is stored.
func (s *Service) Validate() error {
	if !nameRE.MatchString(s.Name) {
		return fmt.Errorf("name %q must be lowercase letters, digits and '-', 1-32 chars, starting with a letter", s.Name)
	}
	switch s.Engine {
	case EngineKong:
		if s.Spec.Postgres != nil {
			return fmt.Errorf("kong service cannot carry a postgres spec")
		}
		if s.Spec.Kong != nil {
			if err := s.Spec.Kong.validate(); err != nil {
				return err
			}
		}
	case EnginePostgres:
		if s.Spec.Kong != nil {
			return fmt.Errorf("postgres service cannot carry a kong spec")
		}
		if s.Spec.Postgres != nil {
			if err := s.Spec.Postgres.validate(); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("engine must be %q or %q", EngineKong, EnginePostgres)
	}
	if s.Spec.Replicas < 1 || s.Spec.Replicas > MaxReplicas {
		return fmt.Errorf("replicas must be between 1 and %d", MaxReplicas)
	}
	if !regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`).MatchString(s.Spec.Version) {
		return fmt.Errorf("version %q is not a valid image tag", s.Spec.Version)
	}
	return nil
}

func (k *KongSpec) validate() error {
	seen := map[string]bool{}
	for i, r := range k.Routes {
		if !nameRE.MatchString(r.Name) {
			return fmt.Errorf("routes[%d].name %q is invalid", i, r.Name)
		}
		if seen[r.Name] {
			return fmt.Errorf("routes[%d].name %q is duplicated", i, r.Name)
		}
		seen[r.Name] = true
		if !strings.HasPrefix(r.Path, "/") {
			return fmt.Errorf("routes[%d].path must start with '/'", i)
		}
		u, err := url.Parse(r.Upstream)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("routes[%d].upstream must be an http(s) URL", i)
		}
	}
	for i, p := range k.Plugins {
		if _, ok := AllowedKongPlugins[p.Name]; !ok {
			return fmt.Errorf("plugins[%d]: %q is not an allowed plugin", i, p.Name)
		}
	}
	return nil
}

// AllowedKongPlugins is the plugin allow-list a managed offering would expose.
var AllowedKongPlugins = map[string]struct{}{
	"rate-limiting":        {},
	"cors":                 {},
	"request-transformer":  {},
	"response-transformer": {},
	"key-auth":             {},
	"prometheus":           {},
	"correlation-id":       {},
}

// PostgresSetting describes one tunable a user may change.
type PostgresSetting struct {
	NeedsRestart bool
}

// AllowedPostgresSettings is the GUC allow-list. Anything that could break the
// managed contract (e.g. wal_level, archive_command) stays platform-owned.
var AllowedPostgresSettings = map[string]PostgresSetting{
	"work_mem":                            {},
	"maintenance_work_mem":                {},
	"effective_cache_size":                {},
	"random_page_cost":                    {},
	"statement_timeout":                   {},
	"idle_in_transaction_session_timeout": {},
	"log_min_duration_statement":          {},
	"default_statistics_target":           {},
	"max_connections":                     {NeedsRestart: true},
	"shared_buffers":                      {NeedsRestart: true},
}

// AllowedPostgresExtensions ship in the stock postgres image.
var AllowedPostgresExtensions = map[string]struct{}{
	"pg_stat_statements": {},
	"pgcrypto":           {},
	"uuid-ossp":          {},
	"hstore":             {},
	"citext":             {},
	"pg_trgm":            {},
	"btree_gin":          {},
	"btree_gist":         {},
}

var settingValueRE = regexp.MustCompile(`^[A-Za-z0-9._ -]{1,32}$`)

func (p *PostgresSpec) validate() error {
	for k, v := range p.Settings {
		if _, ok := AllowedPostgresSettings[k]; !ok {
			return fmt.Errorf("setting %q is not user-tunable", k)
		}
		if !settingValueRE.MatchString(v) {
			return fmt.Errorf("setting %q has an invalid value", k)
		}
	}
	for _, e := range p.Extensions {
		if _, ok := AllowedPostgresExtensions[e]; !ok {
			return fmt.Errorf("extension %q is not allowed", e)
		}
	}
	return nil
}
