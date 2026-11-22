#!/usr/bin/env bash
# Break a managed Kong gateway on purpose, under live traffic, and record how
# long the control plane takes to put it back and how many requests failed.
#
# Needs: fleetd reachable at $FLEET_API (default http://localhost:8080, e.g.
# via `kubectl -n fleet-system port-forward svc/fleetd 8080:8080`), the echo
# upstream from deploy/echo.yaml, and the fleetd:dev image (it carries the
# /fleet-load traffic generator, run as a pod so traffic goes through the
# cluster Service exactly as a client's would).
#
# Writes a Markdown table to docs/chaos-results.md.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${FLEET_API:-http://localhost:8080}
NAME=${NAME:-chaos}
NS=fleet-$NAME
RPS=${RPS:-50}
FROM_VERSION=${FROM_VERSION:-3.8}
TO_VERSION=${TO_VERSION:-3.9}
OUT=${OUT:-docs/chaos-results.md}
NODE=${NODE:-fleet-control-plane}
URL="http://kong-proxy.$NS.svc/echo/hostname"

now() { python3 -c 'import time; print(f"{time.time():.3f}")'; }
api() { curl -sf -X "$1" -H 'Content-Type: application/json' ${3:+-d "$3"} "$API$2"; }
svc_id() { api GET /v1/services | python3 -c "import json,sys; print(next((s['id'] for s in json.load(sys.stdin)['items'] if s['name']=='$NAME'), ''))"; }

# status prints "<phase> <ready> <desired> <runningVersion>"
status() {
  api GET "/v1/services/$ID" | python3 -c '
import json,sys; s=json.load(sys.stdin)
print(s["status"]["phase"], s["status"]["readyReplicas"], s["spec"]["replicas"], s["status"].get("runningVersion",""))'
}

wait_ready() { # wait_ready <timeout-seconds> [version]
  local deadline=$(( $(date +%s) + $1 )) want=${2:-}
  while [ "$(date +%s)" -lt "$deadline" ]; do
    read -r phase ready desired ver <<<"$(status)"
    if [ "$phase" = Ready ] && [ "$ready" -ge "$desired" ] && { [ -z "$want" ] || [ "$ver" = "$want" ]; }; then return 0; fi
    sleep 0.25
  done
  echo "timed out waiting for Ready ($phase $ready/$desired $ver)" >&2; return 1
}

# recovery_since <epoch>: seconds until the service was whole again, read
# from the control plane's own events (the reconciler timestamps them).
recovery_since() {
  api GET "/v1/services/$ID/events" | python3 -c '
import json,sys,re,datetime
t0=float(sys.argv[1]); best=None; drift=None
for e in json.load(sys.stdin)["items"]:
    ts=e["time"]; ts=re.sub(r"(\.\d{6})\d*", r"\1", ts).replace("Z","+00:00")
    t=datetime.datetime.fromisoformat(ts).timestamp()
    if t < t0 - 0.5: continue
    m=re.search(r"recovered in ([0-9.]+)s", e["message"])
    if m: best=float(m.group(1))
    if e["reason"]=="DriftCorrected" and drift is None: drift=max(t-t0, 0.0)
if best is not None: print(f"{best:.1f}s")
elif drift is not None: print(f"{drift:.1f}s (drift reverted)")
else: print("planned, no outage")' "$1"
}

start_load() { # start_load <seconds> -> pod name
  local pod="load-$RANDOM"
  kubectl run "$pod" --image=fleetd:dev --image-pull-policy=IfNotPresent --restart=Never \
    --overrides='{"spec":{"containers":[{"name":"l","image":"fleetd:dev","imagePullPolicy":"IfNotPresent","command":["/fleet-load","-url","'"$URL"'","-rps","'"$RPS"'","-for","'"$1"'s","-json"]}]}}' >/dev/null
  kubectl wait --for=condition=Ready "pod/$pod" --timeout=60s >/dev/null 2>&1 || true
  echo "$pod"
}

finish_load() { # finish_load <pod> -> "ok/total failed p99"
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$1" --timeout=180s >/dev/null
  kubectl logs "$1" | python3 -c 'import json,sys; r=json.loads(sys.stdin.read().strip().splitlines()[-1]); print("%d|%d|%.0f ms" % (r["requests"], r["non2xx"]+r["errors"], r["p99_ms"]))'
  kubectl delete pod "$1" --wait=false >/dev/null
}

ROWS=()
run() { # run <title> <load-seconds> <timeout> <inject...>
  local title=$1 load=$2 timeout=$3; shift 3
  echo "==> $title"
  local pod; pod=$(start_load "$load")
  sleep 3
  local t0; t0=$(now)
  "$@"
  sleep 1
  wait_ready "$timeout" "${WANT_VERSION:-}"
  local t1; t1=$(now)
  local rec; rec=$(recovery_since "$t0")
  local res; res=$(finish_load "$pod")
  IFS='|' read -r total failed p99 <<<"$res"
  local wall; wall=$(python3 -c "print(f'{$t1-$t0:.1f}s')")
  echo "    recovery=$rec  wall=$wall  requests=$total failed=$failed p99=$p99"
  ROWS+=("| $title | $rec | $wall | $total | $failed | $p99 |")
}

# --- faults ----------------------------------------------------------------
kill_one_pod()   { kubectl -n "$NS" delete pod "$(kubectl -n "$NS" get pod -l app=kong -o name | head -1 | cut -d/ -f2)" --wait=false >/dev/null; }
kill_all_pods()  { kubectl -n "$NS" delete pod -l app=kong --wait=false >/dev/null; }
delete_deploy()  { kubectl -n "$NS" delete deploy kong --wait=false >/dev/null; }
tamper_config()  { kubectl -n "$NS" patch cm kong-config --type merge -p '{"data":{"kong.yml":"{\"_format_version\":\"3.0\"}"}}' >/dev/null; }
scale_by_hand()  { kubectl -n "$NS" scale deploy kong --replicas 0 >/dev/null; }
delete_proxy()   { kubectl -n "$NS" delete svc kong-proxy --wait=false >/dev/null; }
add_route()      { api PATCH "/v1/services/$ID" '{"kong":{"routes":[{"name":"echo","path":"/echo","upstream":"http://echo.default.svc:8080"},{"name":"extra","path":"/extra","upstream":"http://echo.default.svc:8080"}],"plugins":[{"name":"rate-limiting","config":{"minute":100000,"policy":"local"}}]}}' >/dev/null; }
upgrade()        { api PATCH "/v1/services/$ID" "{\"version\":\"$TO_VERSION\"}" >/dev/null; }

# --- setup -----------------------------------------------------------------
echo "==> pre-pulling kong:$FROM_VERSION and kong:$TO_VERSION on the node (so we time rollouts, not downloads)"
docker exec "$NODE" crictl pull "kong:$FROM_VERSION" >/dev/null
docker exec "$NODE" crictl pull "kong:$TO_VERSION" >/dev/null

ID=$(svc_id)
if [ -n "$ID" ]; then api DELETE "/v1/services/$ID" >/dev/null; while [ -n "$(svc_id)" ]; do sleep 1; done; fi
echo "==> creating gateway $NAME (2 replicas, kong:$FROM_VERSION)"
api POST /v1/services "{\"name\":\"$NAME\",\"engine\":\"kong\",\"version\":\"$FROM_VERSION\",\"replicas\":2,\"kong\":{\"routes\":[{\"name\":\"echo\",\"path\":\"/echo\",\"upstream\":\"http://echo.default.svc:8080\"}],\"plugins\":[{\"name\":\"rate-limiting\",\"config\":{\"minute\":100000,\"policy\":\"local\"}}]}}" >/dev/null
ID=$(svc_id)
T=$(now); wait_ready 180; echo "    Ready in $(python3 -c "print(f'{$(now)-$T:.1f}s')")"

run "Kill 1 of 2 gateway pods"                  25 120 kill_one_pod
run "Kill all gateway pods"                     30 120 kill_all_pods
run "Delete the Deployment by hand"             30 120 delete_deploy
run "Scale Deployment to 0 by hand"             30 120 scale_by_hand
run "Delete the proxy Service by hand"          20 120 delete_proxy
run "Tamper with the ConfigMap"                 15 120 tamper_config
run "Config change (add a route), live"         30 120 add_route
WANT_VERSION=$TO_VERSION run "Version upgrade $FROM_VERSION -> $TO_VERSION, live" 45 240 upgrade

mkdir -p "$(dirname "$OUT")"
{
  echo "# Chaos results"
  echo
  echo "Measured by \`scripts/chaos.sh\` on $(date -u +%Y-%m-%d) against a single-node kind cluster (colima, 4 vCPU / 3 GB) on an M-series Mac."
  echo "Gateway: Kong OSS, 2 replicas, DB-less. Load: ${RPS} req/s from a pod inside the cluster through the \`kong-proxy\` Service."
  echo
  echo "- **Recovery** is from the control plane's own events: time from the service leaving Ready until it is Ready again."
  echo "- **Wall** is from the moment the fault was injected until \`fleetctl\` reports Ready with all replicas (includes polling)."
  echo
  echo "| Fault | Recovery | Wall | Requests | Failed | p99 |"
  echo "| --- | --- | --- | --- | --- | --- |"
  printf '%s\n' "${ROWS[@]}"
} > "$OUT"
echo "==> wrote $OUT"
