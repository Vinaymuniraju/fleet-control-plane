# fleet-control-plane: build plan

A small control plane in Go that runs **managed data services** on Kubernetes,
the way Kong runs Dedicated Cloud Gateways and ClickHouse runs managed Postgres.
One core (API, desired-state store, reconciler, health, metrics) and pluggable
engines: `kong` (API gateways) and `postgres` (primary + streaming replica).

## Who this is for

| Target role | What it proves |
| --- | --- |
| Kong · Software Engineer 2, Managed Gateways | Go service that provisions, configures, upgrades and self-heals Kong gateway fleets on K8s; reconciler design; drift detection; recovery-time numbers behind an SLA story |
| ClickHouse · Senior Software Engineer, Postgres | Control plane that orchestrates Postgres clusters: replication, failover, config tuning (`ALTER SYSTEM`), extensions, lag health checks |

## Rules for the build loop

- Take the **first unchecked milestone**, finish it, run `go build ./... && go vet ./... && go test ./...`
  (and the milestone's own check), tick it here with a one-line note, then `git commit` locally.
- Never create the GitHub repo or push. That waits for Vinay's OK.
- Keep everything runnable on an 8 GB arm64 Mac: colima (4 CPU / 3 GB, Vinay approved) + one kind node. Keep pod resource requests small.
- If blocked on something only Vinay can do (a password, a GUI prompt, a decision), stop and ask.
- Stop the loop once M0–M8 are ticked. M9+ are stretch.

## Milestones

- [x] **M0 Toolchain**: Homebrew `go kind kubectl colima docker`; `colima start --cpu 4 --memory 3`; `kind create cluster --name fleet`; check `kubectl get nodes`.
- [x] **M1 Skeleton**: Go module `github.com/Vinaymuniraju/fleet-control-plane`; `cmd/fleetd` (server) + `cmd/fleetctl` (CLI); `internal/api` REST on stdlib `net/http` (`POST/GET/PATCH/DELETE /v1/services`); `internal/store` interface with in-memory impl; Makefile; unit tests for the API.
- [x] **M2 Kong engine**: `internal/engine` interface (`Apply`, `Observe`, `Delete`); Kong engine creates Namespace, ConfigMap (declarative `kong.yml`), Deployment (Kong OSS DB-less, N replicas), Service via client-go; observes readiness through the Admin API `/status`; unit tests with the fake clientset.
- [x] **M3 Reconciler**: periodic + event-triggered loop comparing desired vs observed; config hash annotation drives rolling config updates; image change drives version upgrades; drift detection (manual edit / deleted Deployment) and self-heal; per-service event log at `GET /v1/services/{id}/events`.
- [ ] **M4 Observability + chaos**: Prometheus metrics (`reconcile_duration_seconds`, `drift_detected_total`, `service_ready`, `recovery_seconds`); `deploy/` manifests for Prometheus + a Grafana dashboard JSON; `scripts/chaos.sh` kills pods / deletes Deployments and records time-to-recover; numbers go into README.
- [ ] **M5 Postgres engine**: primary + streaming replica (official `postgres:17` image, `pg_basebackup` init for the replica); health from `pg_stat_replication` lag; `PATCH` settings → `ALTER SYSTEM` + `pg_reload_conf()`; enable extensions; failover: promote replica with `pg_promote()` when the primary stays down, then re-point the Service.
- [ ] **M6 Postgres-backed state store**: the control plane keeps its own desired state in Postgres (`pgx`, migrations, optimistic concurrency via `resource_version`), selectable by flag; in-memory stays for tests.
- [ ] **M7 Tests + CI**: table-driven unit tests across api/store/engines/reconciler; `scripts/e2e.sh` on kind (create Kong service, route traffic, kill pod, assert recovery; create Postgres, write, failover, read); GitHub Actions running vet + tests (+ kind e2e job).
- [ ] **M8 README**: architecture diagram, quickstart, demo walkthrough, measured numbers, design decisions and trade-offs, "what production would need" (multi-cloud cells, SLOs, tenancy, upgrades), role mapping table.
- [ ] M9 (stretch) Postgres backups with WAL archiving to MinIO (S3) and point-in-time restore.
- [ ] M10 (stretch) Canary config rollout for Kong with automatic rollback on error-rate rise.

## Log

<!-- one line per finished milestone: date, milestone, what was verified -->
- 2026-11-23 M0: go 1.27.4, colima 4cpu/3GB, kind v1.37 node `fleet-control-plane` Ready.
- 2026-11-23 M1: REST API + in-memory store with optimistic concurrency; 16 tests + 13 validation subtests pass with -race; CLI smoke run (create, scale, list, events) OK.
- 2026-11-23 M2: Kong engine (namespace, ConfigMap, Deployment maxUnavailable=0, proxy/admin Services, PDB) + drift detection + Admin API route probe; fake-clientset tests cover apply, idempotency, config roll, 5 drift cases healed by re-apply, delete.
- 2026-11-23 M3: workqueue reconciler + label-filtered informers; on kind: gateway Ready in 23s, traffic 200 via Kong 3.9.5 with rate-limit headers; self-heal: Deployment deleted 3.7s, ConfigMap edit + manual scale reverted ~1s, pod kill 4.0s; delete tears down namespace.
