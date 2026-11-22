#!/usr/bin/env bash
# Bring up the whole demo on a kind cluster: fleetd in-cluster, the echo
# upstream, Prometheus and Grafana. Then port-forward the API and Grafana.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${CLUSTER:-fleet}

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s

echo "==> building fleetd image"
go build -o bin/fleetctl ./cmd/fleetctl
make -s image
kind load docker-image fleetd:dev --name "$CLUSTER" >/dev/null

echo "==> deploying fleetd + state DB, echo upstream${SKIP_MONITORING:+ (monitoring skipped)}"
kubectl apply -f deploy/fleetd.yaml -f deploy/echo.yaml >/dev/null
WAIT="fleet-system/fleetd default/echo"
if [ -z "${SKIP_MONITORING:-}" ]; then
  kubectl apply -f deploy/monitoring.yaml >/dev/null
  kubectl -n monitoring create configmap grafana-dashboard \
    --from-file=fleet.json=deploy/grafana-dashboard.json --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  WAIT="$WAIT monitoring/prometheus monitoring/grafana"
fi
kubectl -n fleet-system rollout status statefulset/fleetdb --timeout=300s >/dev/null
kubectl -n fleet-system rollout restart deploy/fleetd >/dev/null
for d in $WAIT; do
  kubectl -n "${d%%/*}" rollout status "deploy/${d##*/}" --timeout=300s >/dev/null
  echo "    ${d} ready"
done

cat <<EOF

Next, in separate terminals:
  kubectl -n fleet-system port-forward svc/fleetd 8080:8080
  kubectl -n monitoring port-forward svc/grafana 3000:3000     # http://localhost:3000
Then:
  ./bin/fleetctl create kong edge --replicas 2 --route echo=/echo=http://echo.default.svc:8080
EOF
