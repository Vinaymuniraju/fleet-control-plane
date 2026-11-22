package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Postgres is the durable Store. Desired state survives control-plane
// restarts, and several fleetd replicas can share it safely because every
// write is a compare-and-swap on resource_version.
type Postgres struct {
	pool *pgxpool.Pool
}

// OpenPostgres connects, runs pending migrations and returns the store.
func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for i := 0; i < 30; i++ { // the database may still be starting
		if lastErr = pool.Ping(ctx); lastErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if lastErr != nil {
		pool.Close()
		return nil, fmt.Errorf("connect: %w", lastErr)
	}
	p := &Postgres{pool: pool}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return p, nil
}

// Close releases the pool.
func (p *Postgres) Close() { p.pool.Close() }

// migrate applies migrations/NNN_*.sql in order, once each, under an advisory
// lock so two control-plane replicas starting together don't race.
func (p *Postgres) migrate(ctx context.Context) error {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7238411)`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	for _, e := range entries {
		v, err := strconv.Atoi(strings.SplitN(e.Name(), "_", 2)[0])
		if err != nil {
			return fmt.Errorf("migration %s: bad name", e.Name())
		}
		var done bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, v).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, _ := migrations.ReadFile("migrations/" + e.Name())
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", e.Name(), err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, v); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

const cols = `id, name, engine, spec, status, resource_version, generation, deleting, created_at, updated_at`

func scan(row pgx.Row) (*model.Service, error) {
	var s model.Service
	var spec, status []byte
	if err := row.Scan(&s.ID, &s.Name, &s.Engine, &spec, &status, &s.ResourceVersion, &s.Generation, &s.Deleting, &s.CreatedAt, &s.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal(spec, &s.Spec); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(status, &s.Status); err != nil {
		return nil, err
	}
	s.CreatedAt, s.UpdatedAt = s.CreatedAt.UTC(), s.UpdatedAt.UTC()
	return &s, nil
}

func (p *Postgres) Create(ctx context.Context, s *model.Service) error {
	now := time.Now().UTC().Truncate(time.Microsecond)
	s.ID, s.ResourceVersion, s.Generation, s.CreatedAt, s.UpdatedAt = NewID(), 1, 1, now, now
	if s.Status.Phase == "" {
		s.Status.Phase = model.PhasePending
	}
	spec, _ := json.Marshal(s.Spec)
	status, _ := json.Marshal(s.Status)
	_, err := p.pool.Exec(ctx, `INSERT INTO services (`+cols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		s.ID, s.Name, s.Engine, spec, status, s.ResourceVersion, s.Generation, s.Deleting, s.CreatedAt, s.UpdatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrAlreadyExists
	}
	return err
}

func (p *Postgres) Get(ctx context.Context, id string) (*model.Service, error) {
	return scan(p.pool.QueryRow(ctx, `SELECT `+cols+` FROM services WHERE id = $1`, id))
}

func (p *Postgres) List(ctx context.Context) ([]*model.Service, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+cols+` FROM services ORDER BY created_at, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*model.Service{}
	for rows.Next() {
		s, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// cas runs a compare-and-swap UPDATE and maps "no row changed" to the right error.
func (p *Postgres) cas(ctx context.Context, s *model.Service, query string, args ...any) error {
	got, err := scan(p.pool.QueryRow(ctx, query, args...))
	if errors.Is(err, ErrNotFound) {
		if _, gerr := p.Get(ctx, s.ID); errors.Is(gerr, ErrNotFound) {
			return ErrNotFound
		}
		return ErrConflict
	}
	if err != nil {
		return err
	}
	*s = *got
	return nil
}

func (p *Postgres) UpdateSpec(ctx context.Context, s *model.Service) error {
	spec, _ := json.Marshal(s.Spec)
	return p.cas(ctx, s, `UPDATE services
		SET spec = $1, deleting = $2, resource_version = resource_version + 1, generation = generation + 1, updated_at = now()
		WHERE id = $3 AND resource_version = $4 RETURNING `+cols,
		spec, s.Deleting, s.ID, s.ResourceVersion)
}

func (p *Postgres) UpdateStatus(ctx context.Context, s *model.Service) error {
	status, _ := json.Marshal(s.Status)
	return p.cas(ctx, s, `UPDATE services
		SET status = $1, resource_version = resource_version + 1, updated_at = now()
		WHERE id = $2 AND resource_version = $3 RETURNING `+cols,
		status, s.ID, s.ResourceVersion)
}

func (p *Postgres) Delete(ctx context.Context, id string) error {
	tag, err := p.pool.Exec(ctx, `DELETE FROM services WHERE id = $1`, id) // events cascade
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) AddEvent(ctx context.Context, id string, e model.Event) error {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	_, err = tx.Exec(ctx, `INSERT INTO service_events (service_id, at, type, reason, message) VALUES ($1,$2,$3,$4,$5)`,
		id, e.Time, e.Type, e.Reason, e.Message)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" { // foreign key: no such service
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM service_events WHERE service_id = $1 AND id <= (
		SELECT id FROM service_events WHERE service_id = $1 ORDER BY id DESC OFFSET $2 LIMIT 1)`, id, MaxEvents); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *Postgres) Events(ctx context.Context, id string) ([]model.Event, error) {
	if _, err := p.Get(ctx, id); err != nil {
		return nil, err
	}
	rows, err := p.pool.Query(ctx, `SELECT at, type, reason, message FROM service_events WHERE service_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Event{}
	for rows.Next() {
		var e model.Event
		if err := rows.Scan(&e.Time, &e.Type, &e.Reason, &e.Message); err != nil {
			return nil, err
		}
		e.Time = e.Time.UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}
