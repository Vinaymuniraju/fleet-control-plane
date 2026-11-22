package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

// Kong runs Kong Gateway OSS in DB-less mode: the whole gateway config is one
// declarative file in a ConfigMap, mounted into every data-plane pod. A config
// change rolls the pods (maxUnavailable=0) so traffic never drops.
type Kong struct {
	Kube Kube
	// AdminProbe, when set, asks a running gateway how many routes it serves.
	// It is a field so unit tests can stub it; in-cluster it goes through the
	// API server's service proxy.
	AdminProbe func(ctx context.Context, ns string) (int, error)
}

const (
	kongName      = "kong"
	kongProxySvc  = "kong-proxy"
	kongAdminSvc  = "kong-admin"
	kongConfigKey = "kong.yml"
)

// NewKong returns a Kong engine that probes the Admin API through the API server.
func NewKong(k Kube) *Kong {
	e := &Kong{Kube: k}
	e.AdminProbe = e.probeAdmin
	return e
}

// DeclarativeConfig renders the Kong declarative config (JSON is valid YAML)
// and its hash. Output is deterministic so the hash only moves when config does.
func DeclarativeConfig(spec *model.KongSpec) ([]byte, string, error) {
	type route struct {
		Name      string   `json:"name"`
		Paths     []string `json:"paths"`
		StripPath bool     `json:"strip_path"`
	}
	type service struct {
		Name   string  `json:"name"`
		URL    string  `json:"url"`
		Routes []route `json:"routes"`
	}
	type plugin struct {
		Name   string         `json:"name"`
		Config map[string]any `json:"config,omitempty"`
	}
	cfg := struct {
		Format   string    `json:"_format_version"`
		Services []service `json:"services"`
		Plugins  []plugin  `json:"plugins"`
	}{Format: "3.0", Services: []service{}, Plugins: []plugin{}}

	if spec != nil {
		routes := append([]model.Route(nil), spec.Routes...)
		sort.Slice(routes, func(i, j int) bool { return routes[i].Name < routes[j].Name })
		for _, r := range routes {
			cfg.Services = append(cfg.Services, service{
				Name:   r.Name,
				URL:    r.Upstream,
				Routes: []route{{Name: r.Name, Paths: []string{r.Path}, StripPath: true}},
			})
		}
		for _, p := range spec.Plugins {
			cfg.Plugins = append(cfg.Plugins, plugin{Name: p.Name, Config: pluginDefaults(p)})
		}
	}
	// Observability is platform-owned: every gateway exports Prometheus metrics
	// on its status listener whether or not the user asked for the plugin.
	hasProm := false
	for _, p := range cfg.Plugins {
		hasProm = hasProm || p.Name == "prometheus"
	}
	if !hasProm {
		cfg.Plugins = append(cfg.Plugins, plugin{Name: "prometheus", Config: pluginDefaults(model.Plugin{Name: "prometheus"})})
	}
	sort.Slice(cfg.Plugins, func(i, j int) bool { return cfg.Plugins[i].Name < cfg.Plugins[j].Name })
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, "", err
	}
	return b, Hash(b), nil
}

// pluginDefaults fills config Kong would otherwise reject at startup.
func pluginDefaults(p model.Plugin) map[string]any {
	if len(p.Config) > 0 {
		return p.Config
	}
	switch p.Name {
	case "rate-limiting":
		return map[string]any{"minute": 600, "policy": "local"}
	case "prometheus":
		return map[string]any{"status_code_metrics": true, "latency_metrics": true, "bandwidth_metrics": true, "upstream_health_metrics": true}
	case "key-auth":
		return map[string]any{"key_names": []string{"apikey"}}
	}
	return nil
}

func (k *Kong) objects(svc *model.Service) (*corev1.ConfigMap, *appsv1.Deployment, []*corev1.Service, *policyv1.PodDisruptionBudget, error) {
	ns, labels := Namespace(svc), Labels(svc)
	cfg, hash, err := DeclarativeConfig(svc.Spec.Kong)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	sel := map[string]string{"app": kongName}
	podLabels := map[string]string{"app": kongName}
	for k, v := range labels {
		podLabels[k] = v
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "kong-config", Namespace: ns, Labels: labels},
		Data:       map[string]string{kongConfigKey: string(cfg)},
	}
	env := []corev1.EnvVar{
		{Name: "KONG_DATABASE", Value: "off"},
		{Name: "KONG_DECLARATIVE_CONFIG", Value: "/kong/" + kongConfigKey},
		{Name: "KONG_PROXY_LISTEN", Value: "0.0.0.0:8000"},
		{Name: "KONG_ADMIN_LISTEN", Value: "0.0.0.0:8001"},
		{Name: "KONG_STATUS_LISTEN", Value: "0.0.0.0:8100"},
		{Name: "KONG_NGINX_WORKER_PROCESSES", Value: "1"},
		{Name: "KONG_MEM_CACHE_SIZE", Value: "32m"},
		{Name: "KONG_PROXY_ACCESS_LOG", Value: "/dev/stdout"},
		{Name: "KONG_PROXY_ERROR_LOG", Value: "/dev/stderr"},
		{Name: "KONG_ADMIN_ERROR_LOG", Value: "/dev/stderr"},
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: kongName, Namespace: ns, Labels: labels,
			Annotations: map[string]string{AnnoGeneration: fmt.Sprint(svc.Generation)},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32p(svc.Spec.Replicas),
			Selector: &metav1.LabelSelector{MatchLabels: sel},
			Strategy: *maxUnavailableZero(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
					Annotations: map[string]string{
						AnnoConfigHash:         hash,
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "8100",
					},
				},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr64(15),
					Containers: []corev1.Container{{
						Name:            kongName,
						Image:           "kong:" + svc.Spec.Version,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env:             env,
						Ports: []corev1.ContainerPort{
							{Name: "proxy", ContainerPort: 8000},
							{Name: "admin", ContainerPort: 8001},
							{Name: "status", ContainerPort: 8100},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/status/ready", Port: intstr.FromString("status")}},
							PeriodSeconds:    2,
							FailureThreshold: 3,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/status", Port: intstr.FromString("status")}},
							InitialDelaySeconds: 10,
							PeriodSeconds:       10,
						},
						Lifecycle: &corev1.Lifecycle{
							// Let endpoints drop the pod before nginx stops accepting.
							PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", "sleep 3 && kong quit"}}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("384Mi")},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/kong", ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{{
						Name:         "config",
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cm.Name}}},
					}},
				},
			},
		},
	}
	proxy := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: kongProxySvc, Namespace: ns, Labels: labels},
		Spec:       corev1.ServiceSpec{Selector: sel, Ports: []corev1.ServicePort{{Name: "proxy", Port: 80, TargetPort: intstr.FromString("proxy")}}},
	}
	admin := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: kongAdminSvc, Namespace: ns, Labels: labels},
		Spec:       corev1.ServiceSpec{Selector: sel, Ports: []corev1.ServicePort{{Name: "admin", Port: 8001, TargetPort: intstr.FromString("admin")}}},
	}
	one := intstr.FromInt32(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: kongName, Namespace: ns, Labels: labels},
		Spec:       policyv1.PodDisruptionBudgetSpec{MinAvailable: &one, Selector: &metav1.LabelSelector{MatchLabels: sel}},
	}
	return cm, dep, []*corev1.Service{proxy, admin}, pdb, nil
}

// Apply converges the gateway to svc.Spec.
func (k *Kong) Apply(ctx context.Context, svc *model.Service) error {
	cm, dep, svcs, pdb, err := k.objects(svc)
	if err != nil {
		return err
	}
	if err := k.Kube.EnsureNamespace(ctx, cm.Namespace, Labels(svc)); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}
	// ConfigMap before Deployment: the new pods must find the new config.
	if err := k.Kube.EnsureConfigMap(ctx, cm); err != nil {
		return fmt.Errorf("configmap: %w", err)
	}
	if err := k.Kube.EnsureDeployment(ctx, dep); err != nil {
		return fmt.Errorf("deployment: %w", err)
	}
	for _, s := range svcs {
		if err := k.Kube.EnsureService(ctx, s); err != nil {
			return fmt.Errorf("service %s: %w", s.Name, err)
		}
	}
	if err := k.Kube.EnsurePDB(ctx, pdb, svc.Spec.Replicas > 1); err != nil {
		return fmt.Errorf("pdb: %w", err)
	}
	return nil
}

// Observe reads the live Deployment and ConfigMap and compares them to the spec.
func (k *Kong) Observe(ctx context.Context, svc *model.Service) (Observed, error) {
	ns := Namespace(svc)
	wantCM, wantDep, _, _, err := k.objects(svc)
	if err != nil {
		return Observed{}, err
	}
	obs := Observed{Endpoint: fmt.Sprintf("http://%s.%s.svc:80", kongProxySvc, ns)}

	dep, err := k.Kube.Client.AppsV1().Deployments(ns).Get(ctx, kongName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		obs.Drift = append(obs.Drift, "deployment missing")
		return obs, nil
	}
	if err != nil {
		return obs, err
	}
	obs.Exists = true
	obs.ReadyReplicas = int(dep.Status.ReadyReplicas)
	obs.ConfigHash = dep.Spec.Template.Annotations[AnnoConfigHash]
	if len(dep.Spec.Template.Spec.Containers) > 0 {
		obs.RunningVersion = tagOf(dep.Spec.Template.Spec.Containers[0].Image)
	}
	// Only report the version as running once the rollout has finished.
	if dep.Status.UpdatedReplicas != *dep.Spec.Replicas || dep.Status.Replicas != *dep.Spec.Replicas || dep.Status.ObservedGeneration < dep.Generation {
		obs.Message = fmt.Sprintf("rollout in progress: %d/%d updated", dep.Status.UpdatedReplicas, *dep.Spec.Replicas)
	}

	if got := int(*dep.Spec.Replicas); got != svc.Spec.Replicas {
		obs.Drift = append(obs.Drift, fmt.Sprintf("replicas %d, want %d", got, svc.Spec.Replicas))
	}
	if obs.RunningVersion != svc.Spec.Version {
		obs.Drift = append(obs.Drift, fmt.Sprintf("image tag %s, want %s", obs.RunningVersion, svc.Spec.Version))
	}
	if want := wantDep.Spec.Template.Annotations[AnnoConfigHash]; obs.ConfigHash != want {
		obs.Drift = append(obs.Drift, fmt.Sprintf("pod config hash %s, want %s", obs.ConfigHash, want))
	}
	cm, err := k.Kube.Client.CoreV1().ConfigMaps(ns).Get(ctx, wantCM.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		obs.Drift = append(obs.Drift, "configmap missing")
	case err != nil:
		return obs, err
	case cm.Data[kongConfigKey] != wantCM.Data[kongConfigKey]:
		obs.Drift = append(obs.Drift, "configmap edited outside the control plane")
	}
	for _, name := range []string{kongProxySvc, kongAdminSvc} {
		if _, err := k.Kube.Client.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			obs.Drift = append(obs.Drift, "service "+name+" missing")
		}
	}

	// Ask the data plane itself what it serves, not just what we told it.
	if k.AdminProbe != nil && obs.ReadyReplicas > 0 && len(obs.Drift) == 0 && obs.Message == "" {
		n, err := k.AdminProbe(ctx, ns)
		want := 0
		if svc.Spec.Kong != nil {
			want = len(svc.Spec.Kong.Routes)
		}
		switch {
		case err != nil:
			obs.Message = "admin API probe failed: " + err.Error()
		case n != want:
			obs.Message = fmt.Sprintf("gateway serves %d routes, want %d", n, want)
		}
	}
	return obs, nil
}

// Delete removes the namespace, which takes every object with it.
func (k *Kong) Delete(ctx context.Context, svc *model.Service) (bool, error) {
	return k.Kube.DeleteNamespace(ctx, Namespace(svc))
}

// probeAdmin calls GET /routes on the Admin API via the API server's service proxy.
func (k *Kong) probeAdmin(ctx context.Context, ns string) (int, error) {
	raw, err := k.Kube.Client.CoreV1().Services(ns).ProxyGet("http", kongAdminSvc, "8001", "/routes", nil).DoRaw(ctx)
	if err != nil {
		return 0, err
	}
	var out struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, err
	}
	return len(out.Data), nil
}

func tagOf(image string) string {
	if i := strings.LastIndex(image, ":"); i >= 0 && !strings.Contains(image[i:], "/") {
		return image[i+1:]
	}
	return "latest"
}

func ptr64(i int64) *int64 { return &i }
