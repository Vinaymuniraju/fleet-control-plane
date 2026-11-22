#!/usr/bin/env bash
# End-to-end test on kind: the whole control plane in-cluster, one Kong
# gateway and one Postgres cluster, real traffic, real faults, real failover.
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_MONITORING=1 ./scripts/up.sh >/dev/null
kubectl -n fleet-system port-forward svc/fleetd 18081:8080 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
export FLEET_API=http://localhost:18081
for i in $(seq 1 30); do curl -sf "$FLEET_API/healthz" >/dev/null && break; sleep 1; done

CTL=./bin/fleetctl
pass() { echo "  PASS $*"; }
fail() { echo "  FAIL $*"; exit 1; }
field() { # field <svc> <python expr on s>
  curl -sf "$FLEET_API/v1/services" | python3 -c "import json,sys; s=next(x for x in json.load(sys.stdin)['items'] if x['name']=='$1'); print($2)"
}
wait_phase() { # wait_phase <svc> <phase> <timeout>
  for i in $(seq 1 "$3"); do [ "$(field "$1" "s['status']['phase']")" = "$2" ] && return 0; sleep 1; done
  $CTL events "$1" | tail -20; fail "$1 did not reach $2 in $3s"
}
cleanup_svc() { # delete a leftover from an earlier run and wait until it is gone
  $CTL delete "$1" >/dev/null 2>&1 || return 0
  for i in $(seq 1 120); do $CTL get "$1" >/dev/null 2>&1 || return 0; sleep 1; done
  fail "leftover $1 was not deleted"
}

echo "== Kong gateway"
cleanup_svc e2e-gw
$CTL create kong e2e-gw --replicas 2 --route echo=/echo=http://echo.default.svc:8080 >/dev/null
wait_phase e2e-gw Ready 180 && pass "gateway Ready"

body=$(kubectl run curl-$RANDOM --rm -i --restart=Never --image=fleetd:dev --image-pull-policy=IfNotPresent \
  --command -- /fleet-load -url http://kong-proxy.fleet-e2e-gw.svc/echo/hostname -rps 20 -for 5s -json 2>/dev/null | grep "^{" | tail -1)
python3 -c "import json,sys; r=json.loads('$body'); sys.exit(0 if r['ok']>0 and r['non2xx']+r['errors']==0 else 1)" \
  && pass "traffic through Kong: $body" || fail "traffic: $body"

kubectl -n fleet-e2e-gw delete deploy kong --wait=false >/dev/null
# (capture first: grep -q closing the pipe early would SIGPIPE fleetctl under pipefail)
for i in $(seq 1 120); do ev=$($CTL events e2e-gw); [[ $ev == *"recovered in"* ]] && break; sleep 1; done
[[ $ev == *DriftDetected* && $ev == *"recovered in"* ]] && pass "deleted Deployment was re-created: $(grep -o 'recovered in [0-9.]*s' <<<"$ev" | tail -1)" || fail "no drift recovery"
[[ $ev == *ApplyFailed* ]] && fail "apply failed during heal" || true

echo "== Postgres cluster"
cleanup_svc e2e-db
$CTL create postgres e2e-db --replicas 2 --set work_mem=8MB --ext pgcrypto >/dev/null
wait_phase e2e-db Ready 300 && pass "postgres Ready (primary + streaming replica)"
NS=fleet-e2e-db
P0=$(field e2e-db "s['status']['primary']")
kubectl -n $NS exec "$P0" -- psql -U postgres -d app -Atc "CREATE TABLE orders(id serial primary key, item text); INSERT INTO orders(item) VALUES ('before-failover');" >/dev/null
[ "$(kubectl -n $NS exec "$P0" -- psql -U postgres -d app -Atc 'SHOW work_mem')" = "8MB" ] && pass "work_mem applied" || fail "work_mem"
[[ $(kubectl -n $NS exec "$P0" -- psql -U postgres -d app -Atc "SELECT extname FROM pg_extension") == *pgcrypto* ]] && pass "pgcrypto created" || fail "extension"

REPLICA=$(kubectl -n $NS get pod -l fleet.dev/role=replica -o jsonpath='{.items[0].metadata.name}')
for i in $(seq 1 20); do
  n=$(kubectl -n $NS exec "$REPLICA" -- psql -U postgres -d app -Atc "SELECT count(*) FROM orders" 2>/dev/null || echo 0)
  [ "$n" = 1 ] && break; sleep 1
done
[ "$n" = 1 ] && pass "row replicated to $REPLICA" || fail "replication"

echo "== Planned switchover"
$CTL failover e2e-db >/dev/null
for i in $(seq 1 60); do P1=$(field e2e-db "s['status']['primary']"); [ "$P1" != "$P0" ] && break; sleep 1; done
[ "$P1" != "$P0" ] && pass "primary moved $P0 -> $P1" || fail "switchover"
wait_phase e2e-db Ready 240
kubectl -n $NS exec "$P1" -- psql -U postgres -d app -Atc "INSERT INTO orders(item) VALUES ('after-failover')" >/dev/null
rows=$(kubectl -n $NS exec "$P1" -- psql -U postgres -d app -Atc "SELECT string_agg(item, ',' ORDER BY id) FROM orders")
[ "$rows" = "before-failover,after-failover" ] && pass "no data lost, new primary writable" || fail "rows: $rows"
for i in $(seq 1 60); do
  n=$(kubectl -n $NS exec "$P0" -- psql -U postgres -d app -Atc "SELECT count(*) FROM orders" 2>/dev/null || echo 0)
  [ "$n" = 2 ] && break; sleep 2
done
[ "$n" = 2 ] && pass "old primary $P0 rejoined as a replica" || fail "rejoin"

echo "== Automatic failover (primary hung, not dead)"
# SIGSTOP the primary's postmaster from the node (inside the container it is
# PID 1, which ignores SIGSTOP from its own namespace). The pod stays "Running"
# but stops answering: the case where a naive failover causes split brain.
CID=$(kubectl -n $NS get pod "$P1" -o jsonpath='{.status.containerStatuses[0].containerID}' | sed 's|containerd://||')
HPID=$(docker exec fleet-control-plane crictl inspect --output go-template --template '{{.info.pid}}' "$CID")
docker exec fleet-control-plane kill -STOP "$HPID"
SECONDS=0
for i in $(seq 1 120); do P2=$(field e2e-db "s['status']['primary']"); [ "$P2" != "$P1" ] && break; sleep 1; done
[ "$P2" != "$P1" ] && pass "hung primary $P1 replaced by $P2 after ${SECONDS}s" || fail "no automatic failover"
kubectl -n $NS exec "$P2" -- psql -U postgres -d app -Atc "INSERT INTO orders(item) VALUES ('after-auto-failover')" >/dev/null
rows=$(kubectl -n $NS exec "$P2" -- psql -U postgres -d app -Atc "SELECT count(*) FROM orders")
[ "$rows" = 3 ] && pass "all committed rows present on $P2, writes accepted" || fail "rows after failover: $rows"
wait_phase e2e-db Ready 240
pass "old primary fenced and re-cloned; cluster Ready"

echo "== Teardown"
$CTL delete e2e-gw >/dev/null; $CTL delete e2e-db >/dev/null
for i in $(seq 1 120); do
  kubectl get ns fleet-e2e-gw fleet-e2e-db >/dev/null 2>&1 || break; sleep 1
done
kubectl get ns fleet-e2e-gw fleet-e2e-db >/dev/null 2>&1 && fail "namespaces still present" || pass "namespaces removed"
echo "ALL PASSED"
