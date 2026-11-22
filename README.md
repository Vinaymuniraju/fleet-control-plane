# fleet-control-plane

[![ci](https://github.com/Vinaymuniraju/fleet-control-plane/actions/workflows/ci.yml/badge.svg)](https://github.com/Vinaymuniraju/fleet-control-plane/actions/workflows/ci.yml)

A small **control plane for managed data services**, written in Go. You ask it for a
Kong API gateway or a Postgres cluster through a REST API. It provisions the service on
Kubernetes, keeps it matching what you asked for, and puts it back together when
something breaks.

It is a miniature version of what runs behind managed offerings like Kong's Dedicated
Cloud Gateways or a managed Postgres service: the part between "customer clicks
create" and "pods are serving traffic".

```text
$ fleetctl create kong edge --replicas 2 --route echo=/echo=http://echo.default.svc:8080
$ fleetctl create postgres orders --replicas 2 --set work_mem=8MB --ext pg_stat_statements
$ fleetctl list
ID            NAME    ENGINE    VERSION    READY  PHASE  ENDPOINT
svc-49067046  edge    kong      3.9        2/2    Ready  http://kong-proxy.fleet-edge.svc:80
svc-a81c03fe  orders  postgres  17-alpine  2/2    Ready  postgres://postgres@pg-rw.fleet-orders.svc:5432/app
$ kubectl -n fleet-edge delete deployment kong      # break it on purpose
$ fleetctl events edge
01:38:46  Warning  DriftDetected   deployment missing
01:38:46  Normal   DriftCorrected  re-applied generation 1
01:38:50  Normal   PhaseChanged    Degraded -> Ready (2/2 ready); recovered in 3.7s
```

> **New to this?** [docs/PROJECT_JOURNEY.md](docs/PROJECT_JOURNEY.md) is a beginner-friendly
> walkthrough of the whole project: the goal, the design, how it was built, every problem
> hit along the way and how it was fixed, how it was tested, and what comes next.

## What it does

| | Kong engine | Postgres engine |
| --- | --- | --- |
| Provision | Namespace per service, DB-less Kong pods, declarative config in a ConfigMap, proxy/admin Services, PodDisruptionBudget | StatefulSet with PVCs, primary bootstrapped with `initdb`, replicas cloned with `pg_basebackup`, `pg-rw` / `pg-ro` Services |
| Configure | Routes and an allow-listed set of plugins; config changes roll pods with `maxUnavailable=0` | Allow-listed settings: reloadable ones via `ALTER SYSTEM` + `pg_reload_conf()` on every node, restart-only ones (`max_connections`, `shared_buffers`) on every pod's command line so standbys always match |
| Upgrade | Image tag change rolls pods one at a time, zero downtime | Image tag change rolls the StatefulSet |
| Verify | Readiness from Kubernetes **and** the Admin API: the data plane must actually serve the routes we configured | `pg_is_in_recovery()`, replicas streaming in `pg_stat_replication`, replay lag in bytes, settings applied, extensions created |
| Heal | Deleted or hand-edited Deployment / ConfigMap / Service is re-applied within a reconcile | Automatic failover when the primary stays down (most caught-up replica wins); old primary is fenced and re-cloned |
| Operate | Platform-owned Prometheus plugin on every gateway | Planned switchover via API: fence, wait for the replica to catch up, promote. No acknowledged write is lost |

## Architecture

```text
                 fleetctl / curl
                       |
                 REST API (/v1/services)        validate, record intent, never touch the cluster
                       |
         +-------------+--------------+
         |  store (Postgres or memory) |     desired spec + observed status + event log
         |  optimistic concurrency     |     every write is compare-and-swap on resourceVersion
         +-------------+--------------+
                       |  enqueue(id)
                       v
   informers --> rate-limited work queue --> reconcile(id)        one worker per service at a time,
   (labelled        (client-go)               |                   exponential backoff on errors
    objects only)                             |
                                   Observe -> Apply (if spec changed or drift) -> Maintain -> Observe
                                              |
                          +-------------------+-------------------+
                          |                                       |
                     Kong engine                            Postgres engine
              Deployment, ConfigMap, Services,      StatefulSet, Services, topology ConfigMap,
              PDB, Admin API probe                  role labels, ALTER SYSTEM, failover
                          |                                       |
                          +------------- Kubernetes --------------+
```

**API.** Only records intent. It validates the request (DNS-safe names, http(s)
upstreams, plugin, setting and extension allow-lists, values that can't inject SQL),
writes the desired spec and wakes the reconciler. A slow cluster can't make the API slow.

**Store.** Spec is the user's and status is the reconciler's, as in Kubernetes objects.
Every write carries the `resourceVersion` it read and fails with a conflict if someone
wrote in between, so the API and the reconciler never need a lock between them. The same
contract test suite runs against the in-memory store and the Postgres store (JSONB
columns, embedded migrations under an advisory lock, events capped per service).

**Reconciler.** A level-triggered loop, the same pattern as a Kubernetes controller. A
service is put on the work queue when its spec changes (API), when any object labelled
as ours changes (informers, so a deleted pod is noticed in milliseconds), and every 30 s
(resync, the safety net). Each pass:

1. **Observe** what is running and list every difference from the spec ("drift").
2. **Apply** if the spec changed or anything drifted. Engines are idempotent, so healing
   drift is just applying again.
3. **Maintain**: runtime work for engines that need it (Postgres role labels, settings,
   automatic failover).
4. Write **status** and a **phase** (`Provisioning`, `Ready`, `Updating`, `Degraded`,
   `Failed`, `Deleting`), record events, and time how long the service took to recover.

**Engines.** One namespace per service gives tenant isolation and one-call teardown.
Every object carries `fleet.dev/service-id`, which is how watches map back to a service.

### Postgres failover, step by step

Which pod is primary is runtime state, not spec. It lives in a `pg-topology` ConfigMap
and in each pod's `fleet.dev/role` label. `pg-rw` selects `role=primary`.

1. The primary pod has not been ready for `FailoverAfter` (10 s), and the cluster has a replica.
2. Ask every ready replica for `pg_last_wal_replay_lsn()` and pick the most caught-up one.
3. Write the new primary into `pg-topology` **before** promoting, so a restarting old
   primary already knows it was demoted.
4. `SELECT pg_promote()` on the candidate, then relabel it `primary`. `pg-rw` now points at it.
5. Label the old primary `fenced` and delete its pod. When it starts, its entrypoint sees
   primary data on disk while the topology names someone else, discards the data and
   re-clones from `pg-rw`. An old primary can never come back as a second writer.

A **planned switchover** (`POST /v1/services/{id}/failover`) first fences the healthy
primary: it is taken out of `pg-rw`, set `default_transaction_read_only`, client sessions
are terminated, and the replica must replay up to the primary's last WAL position
before it is promoted. If it doesn't catch up in 15 s the switchover is aborted and the
fence is lifted.

## Measured

All numbers are from a single-node kind cluster on a laptop (colima, 4 vCPU / 3 GB),
under 50 req/s of live traffic from inside the cluster. Full tables and notes:
[docs/chaos-results.md](docs/chaos-results.md). Reproduce with `make chaos` and `make e2e`.

**Kong gateway, 2 replicas**

| Fault | Back to Ready | Failed requests |
| --- | --- | --- |
| Kill 1 of 2 gateway pods | 3.2 s | 0 of 1,249 |
| Kill all gateway pods | 2.9 s | 0 of 1,479 |
| Scale the Deployment to 0 by hand | 3.8 s | 0 of 1,465 |
| Delete the proxy Service by hand | 0.1 s | 0 of 999 |
| Hand-edit the gateway ConfigMap | 0.1 s | 0 of 748 |
| Delete the Deployment by hand | 1.4 s | 36 of 1,454 ¹ |
| Add a route (rolling config change) | 7.4 s rollout | 0 of 1,495 |
| Upgrade Kong 3.8 → 3.9 (rolling) | 20.8 s rollout | 0 of 2,079 |

¹ The control plane re-created the Deployment in 1.4 s, but Kubernetes' garbage collector
was already removing the old pods, and PodDisruptionBudgets don't apply to garbage
collection. The real fix is to stop people deleting managed workloads (RBAC or an
admission policy), not a faster reconcile.

**Postgres, primary + 1 streaming replica**

| Fault | Result |
| --- | --- |
| Primary **hung** (postmaster frozen with SIGSTOP; pod still "Running") | Replica promoted at 14.0 s (10 s of that is the deliberate grace period), writes accepted at 14.2 s, old primary fenced and re-cloned, cluster Ready at 17.4 s. Every committed row present on the new primary. |
| Planned switchover | 0 bytes lost: the replica must replay to the primary's last LSN before promotion |
| Hand-delete the Deployment / ConfigMap / Service of any engine | Shown as a `Warning` event and the `fleet_drift_detected_total` metric, then reverted |

`scripts/e2e.sh` asserts all of this on every run (14 checks: traffic through Kong, drift
heal, replication, settings, extensions, switchover with data check, hung-primary
failover, rejoin, teardown).

## Run it

Needs Docker (or colima), kind, kubectl, Go and Python 3 (used by the scripts). Fits on an 8 GB laptop: give the Docker VM 3 GB.

```bash
make build            # bin/fleetd, bin/fleetctl
./scripts/up.sh       # kind cluster, fleetd + its state DB in-cluster, echo upstream, Prometheus, Grafana
kubectl -n fleet-system port-forward svc/fleetd 8080:8080 &
kubectl -n monitoring port-forward svc/grafana 3000:3000 &   # dashboard at http://localhost:3000

./bin/fleetctl create kong edge --replicas 2 --route echo=/echo=http://echo.default.svc:8080 --plugin rate-limiting
./bin/fleetctl create postgres orders --replicas 2 --set work_mem=8MB --ext pg_stat_statements
./bin/fleetctl list
./bin/fleetctl failover orders     # planned switchover
./bin/fleetctl events orders

make test             # unit tests (set FLEET_TEST_DSN=postgres://... to also run the Postgres store contract)
make e2e              # full end-to-end on kind
make chaos            # fault injection under load, writes docs/chaos-results.md
```

Or run `fleetd` on your laptop against the cluster in your kubeconfig: `./bin/fleetd`
(in-memory store) or `./bin/fleetd -store postgres -dsn postgres://...`.

### API

| Method | Path | |
| --- | --- | --- |
| `POST` | `/v1/services` | create (`name`, `engine`, `version`, `replicas`, `kong` or `postgres`) |
| `GET` | `/v1/services`, `/v1/services/{id}` | list, get (spec + status) |
| `PATCH` | `/v1/services/{id}` | change version, replicas, kong or postgres block; optional `resourceVersion` guard |
| `DELETE` | `/v1/services/{id}` | asynchronous teardown |
| `GET` | `/v1/services/{id}/events` | audit trail |
| `POST` | `/v1/services/{id}/failover` | Postgres planned switchover |
| `GET` | `/metrics` | Prometheus |

### Metrics

`fleet_reconcile_duration_seconds`, `fleet_drift_detected_total`,
`fleet_recovery_seconds`, `fleet_service_ready`, `fleet_service_phase`,
`fleet_service_ready_replicas` / `_desired_replicas`,
`fleet_postgres_replication_lag_bytes`, plus Kong's own `kong_http_requests_total` and
latency histograms from every gateway. `deploy/grafana-dashboard.json` puts them on one
screen.

## Design decisions and trade-offs

- **Intent in a database, not in CRDs.** A managed service's control plane usually sits
  outside the clusters it manages (one control plane, many data-plane clusters across
  regions and clouds), so desired state lives in Postgres and Kubernetes is only the
  execution layer. The reconciler is still the controller pattern: work queue,
  informers, idempotent apply.
- **Optimistic concurrency everywhere.** No row locks and no leader for writes: a stale
  write fails and is retried with fresh state. The contract test fires 20 concurrent
  writers at the same version and expects exactly one winner, on both stores.
- **Drift is reported, then healed.** Every out-of-band change becomes a `Warning` event
  and a metric before it is reverted, so on-call can see that someone ran `kubectl edit`
  in production.
- **The data plane must prove it.** A Kong pod passing its readiness probe isn't enough:
  the engine asks the Admin API how many routes it serves and stays `Degraded` until it
  matches. "Ready" means serving what the customer configured.
- **Platform-owned vs user-owned config.** Users get allow-lists (plugins, settings,
  extensions). `wal_level`, replication, `shared_preload_libraries` and the Prometheus
  plugin belong to the platform, because the SLA depends on them.
- **Never rotate a secret behind a running database.** Credentials are create-once.
- **Refuse unsafe operations.** Scaling Postgres down below the primary's ordinal is
  rejected with a clear error rather than deleting the primary.

## What production would need

- **Leader election** for `fleetd` replicas (Kubernetes Lease) and sharding of services
  across reconcilers once one process can't keep up.
- **Many clusters and clouds.** A cell per region or cloud with its own reconciler,
  driven from one global store, so one bad region can't take down the others.
- **Postgres:** `pg_rewind` instead of re-cloning a demoted primary, WAL archiving to
  object storage with point-in-time restore, synchronous replication as an option for
  zero-RPO failover, and connection pooling (PgBouncer) in front of `pg-rw`.
- **Kong:** canary config rollouts that watch the 5xx rate and roll back automatically,
  per-tenant TLS, and Konnect-style control/data plane separation instead of DB-less
  config files.
- **SLOs on the control plane itself:** time to provision, time to recover, and drift
  rate per engine, with alerts on the `fleet_*` metrics.
- **Garbage collection of orphans.** Today a namespace whose service row is gone is left
  alone on purpose. Deleting what you don't recognise is how control planes wipe out
  customers when the state store is pointed at the wrong database.

## Layout

```text
cmd/fleetd        control plane: API + reconciler + metrics
cmd/fleetctl      CLI
cmd/fleet-load    load generator used by chaos and e2e
internal/api      REST handlers and validation
internal/model    Service, Spec, Status, Event, allow-lists
internal/store    Store interface, memory and Postgres implementations, shared contract tests
internal/reconciler work queue, informers, phases, recovery timing
internal/engine   Kong and Postgres engines, Kubernetes helpers, exec
internal/metrics  Prometheus collectors
deploy/           fleetd + state DB, echo upstream, Prometheus + Grafana
scripts/          up.sh, e2e.sh, chaos.sh
docs/             project journey, chaos results, original build plan
```
