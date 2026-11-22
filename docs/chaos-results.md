# Chaos results

Measured by `scripts/chaos.sh` on 2026-11-22 against a single-node kind cluster (colima, 4 vCPU / 3 GB) on an M-series Mac.
Gateway: Kong OSS, 2 replicas, DB-less. Load: 50 req/s from a pod inside the cluster through the `kong-proxy` Service.

- **Recovery** is from the control plane's own events: time from the service leaving Ready until it is Ready again.
- **Wall** is from the moment the fault was injected until `fleetctl` reports Ready with all replicas (includes polling).

| Fault | Recovery | Wall | Requests | Failed | p99 |
| --- | --- | --- | --- | --- | --- |
| Kill 1 of 2 gateway pods | 3.2s | 4.0s | 1249 | 0 | 38 ms |
| Kill all gateway pods | 2.9s | 3.3s | 1479 | 0 | 83 ms |
| Delete the Deployment by hand | 1.4s ¹ | 2.1s | 1454 | 36 | 101 ms |
| Scale Deployment to 0 by hand | 3.8s | 4.2s | 1465 | 0 | 123 ms |
| Delete the proxy Service by hand | 0.1s | 1.2s | 999 | 0 | 10 ms |
| Tamper with the ConfigMap | 0.1s | 1.3s | 748 | 0 | 61 ms |
| Config change (add a route), live | planned, no outage | 7.4s | 1495 | 0 | 38 ms |
| Version upgrade 3.8 -> 3.9, live | planned, no outage | 20.8s | 2079 | 0 | 518 ms |

## Notes

¹ **Deleting the Deployment is the one fault users felt (36 of 1,454 requests, 2.5%).**
The control plane re-created the Deployment 1.4 s after it vanished, but Kubernetes'
garbage collector was already deleting the old ReplicaSet and its pods (they were owned
by the deleted object's UID). For a moment, no pod was serving. A PodDisruptionBudget
doesn't help, because garbage collection doesn't go through the eviction API. The fix in
a real platform is to stop this fault happening at all: RBAC that keeps humans from
deleting managed workloads, or an admission policy that rejects the delete.

**Killing all gateway pods dropped nothing** because each pod's `preStop` hook sleeps 3 s
before `kong quit`. kube-proxy keeps routing to *terminating* endpoints while no ready
ones exist, so the old pods kept serving until their replacements were ready.

**The ConfigMap tamper and Service delete never reached users.** Kong reads its
declarative config only at start, and the control plane put the original ConfigMap back
in about 100 ms, long before any pod restarted.

**Upgrade p99 (518 ms)** is the cost of new Kong workers warming up while carrying full
load on a 4-vCPU laptop VM. No request failed.

Reproduce with `make chaos` (needs `scripts/up.sh` first and a port-forward to fleetd).
