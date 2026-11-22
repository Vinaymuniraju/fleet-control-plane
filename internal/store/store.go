// Package store keeps desired state and the event log.
//
// Writes use optimistic concurrency: an update carries the ResourceVersion it
// read, and the store refuses it with ErrConflict if someone wrote in between.
// That is what lets the API and the reconciler write the same row without locks.
package store

import (
	"context"
	"errors"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrConflict      = errors.New("resource version conflict")
	ErrAlreadyExists = errors.New("already exists")
)

// Store is the control plane's source of truth.
type Store interface {
	// Create assigns ID, timestamps, ResourceVersion=1 and Generation=1.
	Create(ctx context.Context, s *model.Service) error
	Get(ctx context.Context, id string) (*model.Service, error)
	List(ctx context.Context) ([]*model.Service, error)
	// UpdateSpec replaces Spec/Deleting if s.ResourceVersion matches, and bumps
	// ResourceVersion and Generation. s is updated in place on success.
	UpdateSpec(ctx context.Context, s *model.Service) error
	// UpdateStatus replaces Status if s.ResourceVersion matches, bumping only
	// ResourceVersion. s is updated in place on success.
	UpdateStatus(ctx context.Context, s *model.Service) error
	// Delete removes the row and its events.
	Delete(ctx context.Context, id string) error
	AddEvent(ctx context.Context, id string, e model.Event) error
	// Events returns the newest-last event log, capped by the implementation.
	Events(ctx context.Context, id string) ([]model.Event, error)
}

// MaxEvents is how many events are kept per service.
const MaxEvents = 200
