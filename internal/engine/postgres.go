package engine

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

// Postgres runs a primary with streaming replicas as one StatefulSet.
//
// Which pod is primary is runtime state, not spec: it lives in the
// pg-topology ConfigMap and in each pod's fleet.dev/role label. The pg-rw
// Service selects role=primary and pg-ro selects role=replica, so a failover
// is "promote, rewrite topology, relabel" with no change to the pod template.
//
// A pod reads the topology when it starts. A pod that finds primary data on
// disk but is not the recorded primary was demoted while it was away: it wipes
// its data and re-clones from pg-rw, so an old primary can never come back as
// a second writer. (pg_rewind would make that rejoin cheaper; see README.)
type Postgres struct {
	Kube Kube
	// SQL runs statements with psql inside a pod (each statement separately,
	// so ALTER SYSTEM is never inside a transaction). Tests stub it.
	SQL func(ctx context.Context, ns, pod, db string, stmts ...string) (string, error)
	// FailoverAfter is how long the primary may be down before a replica is promoted.
	FailoverAfter time.Duration
	Now           func() time.Time

	mu        sync.Mutex
	locks     map[string]*sync.Mutex // per service: Maintain and Failover never overlap
	downSince map[string]time.Time   // per service: when the primary was first seen down
}

// Maintainer is implemented by engines that manage runtime state every pass
// (role labels, settings, automatic failover). It returns events to record.
type Maintainer interface {
	Maintain(ctx context.Context, svc *model.Service) ([]model.Event, error)
}

// Failoverer is implemented by engines that support promoting a replica.
type Failoverer interface {
	Failover(ctx context.Context, svc *model.Service, planned bool) ([]model.Event, error)
}

const (
	pgName       = "pg"
	pgRW         = "pg-rw"
	pgRO         = "pg-ro"
	pgTopology   = "pg-topology"
	pgScripts    = "pg-scripts"
	pgSecret     = "pg-credentials"
	pgDB         = "app"
	pgContainer  = "postgres"
	RoleLabel    = "fleet.dev/role"
	rolePrimary  = "primary"
	roleReplica  = "replica"
	roleFenced   = "fenced"
	annoSettings = "fleet.dev/settings-hash"
)

// NewPostgres returns an engine that runs SQL through `kubectl exec`-style calls.
func NewPostgres(k Kube) *Postgres {
	p := &Postgres{Kube: k, FailoverAfter: 10 * time.Second}
	p.SQL = p.execSQL
	return p
}

func (p *Postgres) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Postgres) lock(id string) func() {
	p.mu.Lock()
	if p.locks == nil {
		p.locks = map[string]*sync.Mutex{}
		p.downSince = map[string]time.Time{}
	}
	l, ok := p.locks[id]
	if !ok {
		l = &sync.Mutex{}
		p.locks[id] = l
	}
	p.mu.Unlock()
	l.Lock()
	return l.Unlock
}

func (p *Postgres) execSQL(ctx context.Context, ns, pod, db string, stmts ...string) (string, error) {
	cmd := []string{"psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-AtXq"}
	for _, s := range stmts {
		cmd = append(cmd, "-c", s)
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return p.Kube.Exec(ctx, ns, pod, pgContainer, cmd)
}

// ---------------------------------------------------------------------------
// Desired objects

// entrypoint decides, on every start, whether this pod bootstraps the cluster,
// starts on its existing data, or (re-)clones from the current primary.
const entrypoint = `#!/bin/sh
set -eu
ME=$(hostname)
PRIMARY=$(cat /etc/fleet/topology/primary 2>/dev/null || echo pg-0)
BOOT=$(cat /etc/fleet/topology/bootstrapped 2>/dev/null || echo false)
mkdir -p "$PGDATA"
chown -R postgres:postgres "$(dirname "$PGDATA")"
chmod 700 "$PGDATA"

if [ -s "$PGDATA/PG_VERSION" ]; then
  if [ "$ME" != "$PRIMARY" ] && [ ! -f "$PGDATA/standby.signal" ]; then
    echo "fleet: $ME has primary data but $PRIMARY is the primary now; discarding and re-cloning"
    find "$PGDATA" -mindepth 1 -delete
  else
    echo "fleet: starting on existing data (primary is $PRIMARY)"
    exec docker-entrypoint.sh "$@"
  fi
fi

if [ "$ME" = "$PRIMARY" ]; then
  if [ "$BOOT" = "true" ]; then
    echo "fleet: primary data is missing after bootstrap; refusing to initdb an empty primary"
    exit 1
  fi
  echo "fleet: bootstrapping a new cluster on $ME"
  exec docker-entrypoint.sh "$@"
fi

echo "fleet: waiting for pg-rw to clone from"
until pg_isready -h pg-rw -U postgres -t 2 >/dev/null 2>&1; do sleep 1; done
echo "fleet: cloning from pg-rw"
find "$PGDATA" -mindepth 1 -delete # leftovers of an interrupted clone
AS_PG=$(command -v gosu || command -v su-exec)
"$AS_PG" postgres env PGPASSWORD="$REPLICATION_PASSWORD" \
  pg_basebackup -h pg-rw -U replicator -D "$PGDATA" -R -X stream -c fast
exec docker-entrypoint.sh "$@"
`

const initReplication = `#!/bin/sh
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '$REPLICATION_PASSWORD'"
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
`

// restartSettings returns the -c flags for settings that need a restart.
// They go on the command line of every pod so standbys always match the
// primary (hot standby refuses to start with a lower max_connections).
func restartSettings(spec *model.PostgresSpec) []string {
	vals := map[string]string{"shared_buffers": "32MB", "max_connections": "50"}
	if spec != nil {
		for k, v := range spec.Settings {
			if model.AllowedPostgresSettings[k].NeedsRestart {
				vals[k] = v
			}
		}
	}
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		out = append(out, "-c", k+"="+vals[k])
	}
	return out
}

// reloadSettings returns the user settings applied with ALTER SYSTEM, plus their hash.
func reloadSettings(spec *model.PostgresSpec) (map[string]string, string) {
	out := map[string]string{}
	if spec != nil {
		for k, v := range spec.Settings {
			if !model.AllowedPostgresSettings[k].NeedsRestart {
				out[k] = v
			}
		}
	}
	b, _ := json.Marshal(out) // map keys are sorted by encoding/json
	return out, Hash(b)
}

func randomPassword() string {
	const chars = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	b := make([]byte, 24)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		b[i] = chars[n.Int64()]
	}
	return string(b)
}

func (p *Postgres) statefulSet(svc *model.Service) *appsv1.StatefulSet {
	ns, labels := Namespace(svc), Labels(svc)
	podLabels := map[string]string{"app": pgName}
	for k, v := range labels {
		podLabels[k] = v
	}
	args := append([]string{"postgres",
		"-c", "listen_addresses=*",
		"-c", "wal_level=replica",
		"-c", "hot_standby=on",
		"-c", "max_wal_senders=10",
		"-c", "wal_keep_size=256MB",
		"-c", "shared_preload_libraries=pg_stat_statements",
	}, restartSettings(svc.Spec.Postgres)...)
	tmplHash := Hash([]byte(entrypoint + initReplication + strings.Join(args, " ")))
	secretEnv := func(name, key string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: pgSecret}, Key: key}}}
	}
	isReady := corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"pg_isready", "-U", "postgres", "-h", "127.0.0.1"}}}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: pgName, Namespace: ns, Labels: labels,
			Annotations: map[string]string{AnnoGeneration: fmt.Sprint(svc.Generation)}},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         pgName,
			Replicas:            int32p(svc.Spec.Replicas),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: map[string]string{"app": pgName}},
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels, Annotations: map[string]string{AnnoConfigHash: tmplHash}},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr64(10),
					Containers: []corev1.Container{{
						Name:            pgContainer,
						Image:           "postgres:" + svc.Spec.Version,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"/bin/sh", "/scripts/entrypoint.sh"},
						Args:            args,
						Env: []corev1.EnvVar{
							secretEnv("POSTGRES_PASSWORD", "password"),
							secretEnv("REPLICATION_PASSWORD", "replication-password"),
							{Name: "POSTGRES_DB", Value: pgDB},
							{Name: "PGDATA", Value: "/var/lib/postgresql/data/pgdata"},
						},
						Ports:          []corev1.ContainerPort{{Name: "postgres", ContainerPort: 5432}},
						StartupProbe:   &corev1.Probe{ProbeHandler: isReady, PeriodSeconds: 2, FailureThreshold: 150},
						ReadinessProbe: &corev1.Probe{ProbeHandler: isReady, PeriodSeconds: 2, FailureThreshold: 2},
						LivenessProbe:  &corev1.Probe{ProbeHandler: isReady, PeriodSeconds: 10, FailureThreshold: 3},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/var/lib/postgresql/data"},
							{Name: "scripts", MountPath: "/scripts"},
							{Name: "scripts", MountPath: "/docker-entrypoint-initdb.d/10-replication.sh", SubPath: "init-replication.sh"},
							{Name: "topology", MountPath: "/etc/fleet/topology"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "scripts", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: pgScripts}}}},
						{Name: "topology", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: pgTopology}}}},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
				},
			}},
		},
	}
}

func (p *Postgres) services(svc *model.Service) []*corev1.Service {
	ns, labels := Namespace(svc), Labels(svc)
	port := []corev1.ServicePort{{Name: "postgres", Port: 5432, TargetPort: intstr.FromString("postgres")}}
	return []*corev1.Service{
		{ObjectMeta: metav1.ObjectMeta{Name: pgName, Namespace: ns, Labels: labels},
			Spec: corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, Selector: map[string]string{"app": pgName}, Ports: port, PublishNotReadyAddresses: true}},
		{ObjectMeta: metav1.ObjectMeta{Name: pgRW, Namespace: ns, Labels: labels},
			Spec: corev1.ServiceSpec{Selector: map[string]string{"app": pgName, RoleLabel: rolePrimary}, Ports: port}},
		{ObjectMeta: metav1.ObjectMeta{Name: pgRO, Namespace: ns, Labels: labels},
			Spec: corev1.ServiceSpec{Selector: map[string]string{"app": pgName, RoleLabel: roleReplica}, Ports: port}},
	}
}

// ---------------------------------------------------------------------------
// Apply / Observe / Delete

// Apply converges the cluster's shape (objects, replicas, version, restart
// settings). Runtime work (roles, reloadable settings, failover) is Maintain's.
func (p *Postgres) Apply(ctx context.Context, svc *model.Service) error {
	ns, labels := Namespace(svc), Labels(svc)
	if err := p.Kube.EnsureNamespace(ctx, ns, labels); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}
	if _, err := p.Kube.EnsureSecret(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: pgSecret, Namespace: ns, Labels: labels},
		StringData: map[string]string{"password": randomPassword(), "replication-password": randomPassword()},
	}); err != nil {
		return fmt.Errorf("secret: %w", err)
	}
	if err := p.Kube.EnsureConfigMap(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: pgScripts, Namespace: ns, Labels: labels},
		Data:       map[string]string{"entrypoint.sh": entrypoint, "init-replication.sh": initReplication},
	}); err != nil {
		return fmt.Errorf("scripts: %w", err)
	}
	// Topology is created once and then owned by failover, never overwritten here.
	topo, err := p.topology(ctx, ns)
	if apierrors.IsNotFound(err) {
		_, err = p.Kube.Client.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: pgTopology, Namespace: ns, Labels: labels},
			Data:       map[string]string{"primary": pgName + "-0", "bootstrapped": "false"},
		}, metav1.CreateOptions{})
		topo = topology{Primary: pgName + "-0"}
	}
	if err != nil {
		return fmt.Errorf("topology: %w", err)
	}
	if ord := ordinal(topo.Primary); ord >= svc.Spec.Replicas {
		return fmt.Errorf("scaling to %d would remove the primary %s; fail over to a lower ordinal first", svc.Spec.Replicas, topo.Primary)
	}
	for _, s := range p.services(svc) {
		if err := p.Kube.EnsureService(ctx, s); err != nil {
			return fmt.Errorf("service %s: %w", s.Name, err)
		}
	}
	if err := p.Kube.EnsureStatefulSet(ctx, p.statefulSet(svc)); err != nil {
		return fmt.Errorf("statefulset: %w", err)
	}
	return nil
}

type topology struct {
	Primary      string
	Bootstrapped bool
}

func (p *Postgres) topology(ctx context.Context, ns string) (topology, error) {
	cm, err := p.Kube.Client.CoreV1().ConfigMaps(ns).Get(ctx, pgTopology, metav1.GetOptions{})
	if err != nil {
		return topology{}, err
	}
	return topology{Primary: cm.Data["primary"], Bootstrapped: cm.Data["bootstrapped"] == "true"}, nil
}

func (p *Postgres) setTopology(ctx context.Context, ns string, t topology) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm, err := p.Kube.Client.CoreV1().ConfigMaps(ns).Get(ctx, pgTopology, metav1.GetOptions{})
		if err != nil {
			return err
		}
		cm.Data = map[string]string{"primary": t.Primary, "bootstrapped": strconv.FormatBool(t.Bootstrapped)}
		_, err = p.Kube.Client.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}

func (p *Postgres) pods(ctx context.Context, ns string) (map[string]*corev1.Pod, error) {
	list, err := p.Kube.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=" + pgName})
	if err != nil {
		return nil, err
	}
	out := map[string]*corev1.Pod{}
	for i := range list.Items {
		out[list.Items[i].Name] = &list.Items[i]
	}
	return out, nil
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func ordinal(pod string) int {
	i := strings.LastIndex(pod, "-")
	n, err := strconv.Atoi(pod[i+1:])
	if i < 0 || err != nil {
		return -1
	}
	return n
}

// Observe reports shape drift and replication health. It never writes.
func (p *Postgres) Observe(ctx context.Context, svc *model.Service) (Observed, error) {
	ns := Namespace(svc)
	want := p.statefulSet(svc)
	obs := Observed{Endpoint: fmt.Sprintf("postgres://postgres@%s.%s.svc:5432/%s", pgRW, ns, pgDB)}

	sts, err := p.Kube.Client.AppsV1().StatefulSets(ns).Get(ctx, pgName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		obs.Drift = append(obs.Drift, "statefulset missing")
		return obs, nil
	}
	if err != nil {
		return obs, err
	}
	obs.Exists = true
	obs.ReadyReplicas = int(sts.Status.ReadyReplicas)
	obs.ConfigHash = sts.Spec.Template.Annotations[AnnoConfigHash]
	obs.RunningVersion = tagOf(sts.Spec.Template.Spec.Containers[0].Image)
	if got := int(*sts.Spec.Replicas); got != svc.Spec.Replicas {
		obs.Drift = append(obs.Drift, fmt.Sprintf("replicas %d, want %d", got, svc.Spec.Replicas))
	}
	if obs.RunningVersion != svc.Spec.Version {
		obs.Drift = append(obs.Drift, fmt.Sprintf("image tag %s, want %s", obs.RunningVersion, svc.Spec.Version))
	}
	if obs.ConfigHash != want.Spec.Template.Annotations[AnnoConfigHash] {
		obs.Drift = append(obs.Drift, "pod template differs from spec")
	}
	if cm, err := p.Kube.Client.CoreV1().ConfigMaps(ns).Get(ctx, pgScripts, metav1.GetOptions{}); err != nil || cm.Data["entrypoint.sh"] != entrypoint {
		obs.Drift = append(obs.Drift, "scripts configmap missing or edited")
	}
	for _, s := range p.services(svc) {
		if _, err := p.Kube.Client.CoreV1().Services(ns).Get(ctx, s.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			obs.Drift = append(obs.Drift, "service "+s.Name+" missing")
		}
	}
	if len(obs.Drift) > 0 {
		return obs, nil
	}
	if sts.Status.UpdatedReplicas < *sts.Spec.Replicas || sts.Status.ObservedGeneration < sts.Generation ||
		(sts.Status.UpdateRevision != "" && sts.Status.CurrentRevision != sts.Status.UpdateRevision) {
		obs.Message = fmt.Sprintf("rollout in progress: %d/%d updated", sts.Status.UpdatedReplicas, *sts.Spec.Replicas)
	}

	topo, err := p.topology(ctx, ns)
	if err != nil {
		obs.Message = "topology unavailable: " + err.Error()
		return obs, nil
	}
	obs.Primary = topo.Primary
	pods, err := p.pods(ctx, ns)
	if err != nil {
		return obs, err
	}
	if !podReady(pods[topo.Primary]) {
		obs.Message = "primary " + topo.Primary + " is not ready"
		return obs, nil
	}

	out, err := p.SQL(ctx, ns, topo.Primary, "postgres",
		"SELECT pg_is_in_recovery()::text || ',' || count(*) || ',' || coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)), 0)::bigint FROM pg_stat_replication WHERE state = 'streaming'")
	if err != nil {
		obs.Message = "health query failed: " + err.Error()
		return obs, nil
	}
	parts := strings.Split(out, ",")
	if len(parts) != 3 {
		obs.Message = "unexpected health output: " + out
		return obs, nil
	}
	_, wantSettings := reloadSettings(svc.Spec.Postgres)
	streaming, _ := strconv.Atoi(parts[1])
	obs.LagBytes, _ = strconv.ParseInt(parts[2], 10, 64)
	switch {
	case parts[0] != "false":
		obs.Message = "primary " + topo.Primary + " is still in recovery"
	case streaming < svc.Spec.Replicas-1 && obs.Message == "":
		obs.Message = fmt.Sprintf("%d/%d replicas streaming", streaming, svc.Spec.Replicas-1)
	case obs.Message == "" && sts.Annotations[annoSettings] != wantSettings:
		obs.Message = "settings not yet applied"
	}
	if obs.Message == "" && svc.Spec.Postgres != nil && len(svc.Spec.Postgres.Extensions) > 0 {
		missing, err := p.missingExtensions(ctx, ns, topo.Primary, svc.Spec.Postgres.Extensions)
		if err != nil {
			obs.Message = "extension query failed: " + err.Error()
		} else if len(missing) > 0 {
			obs.Message = "extensions not yet created: " + strings.Join(missing, ", ")
		}
	}
	return obs, nil
}

func (p *Postgres) missingExtensions(ctx context.Context, ns, pod string, want []string) ([]string, error) {
	out, err := p.SQL(ctx, ns, pod, pgDB, "SELECT string_agg(extname, ',') FROM pg_extension")
	if err != nil {
		return nil, err
	}
	have := map[string]bool{}
	for _, e := range strings.Split(out, ",") {
		have[e] = true
	}
	var missing []string
	for _, e := range want {
		if !have[e] {
			missing = append(missing, e)
		}
	}
	return missing, nil
}

// Delete removes the namespace (and with it the PVCs).
func (p *Postgres) Delete(ctx context.Context, svc *model.Service) (bool, error) {
	done, err := p.Kube.DeleteNamespace(ctx, Namespace(svc))
	if done {
		p.mu.Lock()
		delete(p.locks, svc.ID)
		delete(p.downSince, svc.ID)
		p.mu.Unlock()
	}
	return done, err
}

// ---------------------------------------------------------------------------
// Maintain: roles, bootstrap, settings, extensions, automatic failover

func (p *Postgres) Maintain(ctx context.Context, svc *model.Service) ([]model.Event, error) {
	unlock := p.lock(svc.ID)
	defer unlock()
	ns := Namespace(svc)
	topo, err := p.topology(ctx, ns)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	pods, err := p.pods(ctx, ns)
	if err != nil {
		return nil, err
	}
	var events []model.Event
	ev := func(typ model.EventType, reason, f string, a ...any) {
		events = append(events, model.Event{Type: typ, Reason: reason, Message: fmt.Sprintf(f, a...)})
	}

	// Role labels follow the topology.
	for name, pod := range pods {
		want := roleReplica
		if name == topo.Primary {
			want = rolePrimary
		}
		if pod.Labels[RoleLabel] != want && pod.DeletionTimestamp == nil {
			if err := p.Kube.LabelPod(ctx, ns, name, RoleLabel, want); err != nil {
				return events, fmt.Errorf("label %s: %w", name, err)
			}
		}
	}

	primary := pods[topo.Primary]
	if !podReady(primary) {
		if !topo.Bootstrapped {
			return events, nil // still initialising; nothing to fail over to
		}
		p.mu.Lock()
		since, ok := p.downSince[svc.ID]
		if !ok {
			since = p.now()
			p.downSince[svc.ID] = since
		}
		p.mu.Unlock()
		down := p.now().Sub(since)
		if svc.Spec.Replicas < 2 || down < p.FailoverAfter {
			return events, nil
		}
		ev(model.EventWarning, "PrimaryDown", "primary %s not ready for %.0fs; promoting a replica", topo.Primary, down.Seconds())
		more, err := p.failover(ctx, svc, topo, pods, false)
		return append(events, more...), err
	}
	p.mu.Lock()
	delete(p.downSince, svc.ID)
	p.mu.Unlock()

	if !topo.Bootstrapped {
		topo.Bootstrapped = true
		if err := p.setTopology(ctx, ns, topo); err != nil {
			return events, err
		}
		ev(model.EventNormal, "Bootstrapped", "primary %s initialised", topo.Primary)
	}

	// Reloadable settings: ALTER SYSTEM on every ready pod, then reload.
	sts, err := p.Kube.Client.AppsV1().StatefulSets(ns).Get(ctx, pgName, metav1.GetOptions{})
	if err != nil {
		return events, err
	}
	settings, hash := reloadSettings(svc.Spec.Postgres)
	if sts.Annotations[annoSettings] != hash {
		stmts := settingStatements(settings)
		var applied []string
		for name, pod := range pods {
			if !podReady(pod) {
				continue
			}
			if _, err := p.SQL(ctx, ns, name, "postgres", stmts...); err != nil {
				return events, fmt.Errorf("apply settings on %s: %w", name, err)
			}
			applied = append(applied, name)
		}
		patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, annoSettings, hash)
		if _, err := p.Kube.Client.AppsV1().StatefulSets(ns).Patch(ctx, pgName, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			return events, err
		}
		sort.Strings(applied)
		ev(model.EventNormal, "SettingsApplied", "%d settings reloaded on %s", len(settings), strings.Join(applied, ", "))
	}

	if svc.Spec.Postgres != nil && len(svc.Spec.Postgres.Extensions) > 0 {
		missing, err := p.missingExtensions(ctx, ns, topo.Primary, svc.Spec.Postgres.Extensions)
		if err != nil {
			return events, err
		}
		for _, e := range missing {
			// e is from the allow-list, so quoting it as an identifier is enough.
			if _, err := p.SQL(ctx, ns, topo.Primary, pgDB, fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS %q`, e)); err != nil {
				return events, fmt.Errorf("create extension %s: %w", e, err)
			}
			ev(model.EventNormal, "ExtensionCreated", "%s created in database %s", e, pgDB)
		}
	}
	return events, nil
}

// settingStatements turns the desired reloadable settings into ALTER SYSTEM
// statements: SET for those in the spec, RESET for every other tunable, so a
// removed setting goes back to its default.
func settingStatements(settings map[string]string) []string {
	keys := make([]string, 0, len(model.AllowedPostgresSettings))
	for k, s := range model.AllowedPostgresSettings {
		if !s.NeedsRestart {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var stmts []string
	for _, k := range keys {
		if v, ok := settings[k]; ok {
			// Keys come from the allow-list and values are validated by
			// model.Validate (no quotes possible), so this cannot inject.
			stmts = append(stmts, fmt.Sprintf("ALTER SYSTEM SET %s = '%s'", k, v))
		} else {
			stmts = append(stmts, "ALTER SYSTEM RESET "+k)
		}
	}
	return append(stmts, "SELECT pg_reload_conf()")
}

// ---------------------------------------------------------------------------
// Failover

// Failover promotes the most caught-up ready replica. When planned is true and
// the primary is healthy, it is a switchover: the primary is fenced (no new
// connections, read-only, clients disconnected) and the candidate must replay
// up to the primary's last WAL position before it is promoted, so no
// acknowledged write is lost.
func (p *Postgres) Failover(ctx context.Context, svc *model.Service, planned bool) ([]model.Event, error) {
	unlock := p.lock(svc.ID)
	defer unlock()
	ns := Namespace(svc)
	topo, err := p.topology(ctx, ns)
	if err != nil {
		return nil, err
	}
	pods, err := p.pods(ctx, ns)
	if err != nil {
		return nil, err
	}
	return p.failover(ctx, svc, topo, pods, planned)
}

func (p *Postgres) failover(ctx context.Context, svc *model.Service, topo topology, pods map[string]*corev1.Pod, planned bool) ([]model.Event, error) {
	ns, old := Namespace(svc), topo.Primary
	var events []model.Event

	var target uint64
	if planned && podReady(pods[old]) {
		// Fence: out of pg-rw first, then make it read-only and drop sessions.
		if err := p.Kube.LabelPod(ctx, ns, old, RoleLabel, roleFenced); err != nil {
			return nil, err
		}
		if _, err := p.SQL(ctx, ns, old, "postgres",
			"ALTER SYSTEM SET default_transaction_read_only = on",
			"SELECT pg_reload_conf()",
			"SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()",
			"CHECKPOINT"); err != nil {
			return nil, fmt.Errorf("fence %s: %w", old, err)
		}
		lsn, err := p.SQL(ctx, ns, old, "postgres", "SELECT pg_current_wal_lsn()")
		if err != nil {
			return nil, fmt.Errorf("read primary LSN: %w", err)
		}
		target, _ = parseLSN(lsn)
	}

	// Pick the ready replica that has replayed the most WAL.
	best, bestLSN := "", uint64(0)
	deadline := p.now().Add(15 * time.Second)
	for {
		best, bestLSN = "", 0
		for name, pod := range pods {
			if name == old || !podReady(pod) {
				continue
			}
			out, err := p.SQL(ctx, ns, name, "postgres", "SELECT pg_last_wal_replay_lsn()")
			if err != nil {
				continue
			}
			lsn, err := parseLSN(out)
			if err != nil {
				continue
			}
			if best == "" || lsn > bestLSN || (lsn == bestLSN && name < best) {
				best, bestLSN = name, lsn
			}
		}
		if best == "" || target == 0 || bestLSN >= target || p.now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if best == "" {
		if planned {
			_ = p.Kube.LabelPod(ctx, ns, old, RoleLabel, rolePrimary)
			_, _ = p.SQL(ctx, ns, old, "postgres", "ALTER SYSTEM RESET default_transaction_read_only", "SELECT pg_reload_conf()")
		}
		return nil, fmt.Errorf("no ready replica to promote")
	}
	if target != 0 && bestLSN < target {
		_ = p.Kube.LabelPod(ctx, ns, old, RoleLabel, rolePrimary)
		_, _ = p.SQL(ctx, ns, old, "postgres", "ALTER SYSTEM RESET default_transaction_read_only", "SELECT pg_reload_conf()")
		return nil, fmt.Errorf("switchover aborted: %s did not catch up (%d bytes behind)", best, target-bestLSN)
	}

	// Record the new primary before promoting, so a restarting old primary
	// already sees it has been demoted.
	if err := p.setTopology(ctx, ns, topology{Primary: best, Bootstrapped: true}); err != nil {
		return nil, err
	}
	if _, err := p.SQL(ctx, ns, best, "postgres", "SELECT pg_promote(true, 60)"); err != nil {
		return nil, fmt.Errorf("promote %s: %w", best, err)
	}
	if err := p.Kube.LabelPod(ctx, ns, best, RoleLabel, rolePrimary); err != nil {
		return nil, err
	}
	if _, ok := pods[old]; ok {
		_ = p.Kube.LabelPod(ctx, ns, old, RoleLabel, roleFenced)
		// Recreate the old primary so it remounts the topology and rejoins as a replica.
		zero := int64(0)
		_ = p.Kube.Client.CoreV1().Pods(ns).Delete(ctx, old, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	}
	p.mu.Lock()
	delete(p.downSince, svc.ID)
	p.mu.Unlock()

	kind := "Failover"
	if planned {
		kind = "Switchover"
	}
	lag := "unknown"
	if target != 0 {
		lag = fmt.Sprintf("%d bytes", target-min(target, bestLSN))
	}
	events = append(events, model.Event{Type: model.EventNormal, Reason: kind,
		Message: fmt.Sprintf("promoted %s (replayed to %s, data loss %s); %s fenced and re-cloning", best, formatLSN(bestLSN), lag, old)})
	return events, nil
}

func parseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok {
		return 0, fmt.Errorf("bad LSN %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, err
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, err
	}
	return h<<32 | l, nil
}

func formatLSN(v uint64) string { return fmt.Sprintf("%X/%X", v>>32, v&0xffffffff) }
