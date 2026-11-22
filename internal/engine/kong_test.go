package engine

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/Vinaymuniraju/fleet-control-plane/internal/model"
)

func kongSvc() *model.Service {
	return &model.Service{
		ID: "svc-1", Name: "edge", Engine: model.EngineKong, Generation: 1,
		Spec: model.Spec{Version: "3.9", Replicas: 2, Kong: &model.KongSpec{
			Routes:  []model.Route{{Name: "echo", Path: "/echo", Upstream: "http://echo.default.svc:8080"}},
			Plugins: []model.Plugin{{Name: "rate-limiting"}},
		}},
	}
}

func newKong() (*Kong, *fake.Clientset) {
	c := fake.NewClientset()
	k := &Kong{Kube: Kube{Client: c}}
	k.AdminProbe = func(context.Context, string) (int, error) { return 1, nil }
	return k, c
}

// markReady fakes what the Deployment controller would report.
func markReady(t *testing.T, c *fake.Clientset, ns string, n int32) {
	t.Helper()
	d, err := c.AppsV1().Deployments(ns).Get(t.Context(), kongName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d.Status.ReadyReplicas, d.Status.UpdatedReplicas, d.Status.Replicas = n, n, n
	if _, err := c.AppsV1().Deployments(ns).UpdateStatus(t.Context(), d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestDeclarativeConfigIsDeterministic(t *testing.T) {
	a := &model.KongSpec{Routes: []model.Route{
		{Name: "b", Path: "/b", Upstream: "http://b"},
		{Name: "a", Path: "/a", Upstream: "http://a"},
	}}
	b := &model.KongSpec{Routes: []model.Route{a.Routes[1], a.Routes[0]}}
	_, h1, _ := DeclarativeConfig(a)
	_, h2, _ := DeclarativeConfig(b)
	if h1 != h2 {
		t.Fatal("route order changed the config hash; pods would roll for nothing")
	}
	cfg, _, _ := DeclarativeConfig(&model.KongSpec{Plugins: []model.Plugin{{Name: "rate-limiting"}}})
	if !strings.Contains(string(cfg), `"policy": "local"`) {
		t.Fatalf("rate-limiting defaults missing: %s", cfg)
	}
	if !strings.Contains(string(cfg), `"_format_version": "3.0"`) {
		t.Fatal("missing _format_version")
	}
}

func TestKongApplyCreatesObjects(t *testing.T) {
	k, c := newKong()
	svc := kongSvc()
	if err := k.Apply(t.Context(), svc); err != nil {
		t.Fatal(err)
	}
	ns := Namespace(svc)
	d, err := c.AppsV1().Deployments(ns).Get(t.Context(), kongName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if *d.Spec.Replicas != 2 || d.Spec.Template.Spec.Containers[0].Image != "kong:3.9" {
		t.Fatalf("unexpected deployment: replicas=%d image=%s", *d.Spec.Replicas, d.Spec.Template.Spec.Containers[0].Image)
	}
	if d.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue() != 0 {
		t.Fatal("rolling update must keep maxUnavailable=0")
	}
	if d.Labels["fleet.dev/service-id"] != "svc-1" {
		t.Fatal("missing ownership labels")
	}
	for _, name := range []string{kongProxySvc, kongAdminSvc} {
		if _, err := c.CoreV1().Services(ns).Get(t.Context(), name, metav1.GetOptions{}); err != nil {
			t.Fatalf("service %s: %v", name, err)
		}
	}
	if _, err := c.PolicyV1().PodDisruptionBudgets(ns).Get(t.Context(), kongName, metav1.GetOptions{}); err != nil {
		t.Fatalf("pdb should exist for 2 replicas: %v", err)
	}
	// Idempotent.
	if err := k.Apply(t.Context(), svc); err != nil {
		t.Fatal(err)
	}
}

func TestKongPDBRemovedWhenScaledToOne(t *testing.T) {
	k, c := newKong()
	svc := kongSvc()
	_ = k.Apply(t.Context(), svc)
	svc.Spec.Replicas = 1
	if err := k.Apply(t.Context(), svc); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PolicyV1().PodDisruptionBudgets(Namespace(svc)).Get(t.Context(), kongName, metav1.GetOptions{}); err == nil {
		t.Fatal("a 1-replica gateway must not keep a minAvailable=1 PDB (it would block node drains)")
	}
}

func TestKongConfigChangeRollsPods(t *testing.T) {
	k, c := newKong()
	svc := kongSvc()
	_ = k.Apply(t.Context(), svc)
	ns := Namespace(svc)
	d1, _ := c.AppsV1().Deployments(ns).Get(t.Context(), kongName, metav1.GetOptions{})

	svc.Spec.Kong.Routes = append(svc.Spec.Kong.Routes, model.Route{Name: "two", Path: "/two", Upstream: "http://two"})
	_ = k.Apply(t.Context(), svc)
	d2, _ := c.AppsV1().Deployments(ns).Get(t.Context(), kongName, metav1.GetOptions{})
	if d1.Spec.Template.Annotations[AnnoConfigHash] == d2.Spec.Template.Annotations[AnnoConfigHash] {
		t.Fatal("config change did not change the pod template hash")
	}
}

func TestKongObserveHealthy(t *testing.T) {
	k, c := newKong()
	svc := kongSvc()
	_ = k.Apply(t.Context(), svc)
	markReady(t, c, Namespace(svc), 2)
	obs, err := k.Observe(t.Context(), svc)
	if err != nil {
		t.Fatal(err)
	}
	if !obs.Exists || obs.ReadyReplicas != 2 || len(obs.Drift) != 0 || obs.RunningVersion != "3.9" || obs.Message != "" {
		t.Fatalf("unexpected observation: %+v", obs)
	}
	if obs.Endpoint != "http://kong-proxy.fleet-edge.svc:80" {
		t.Fatalf("endpoint: %s", obs.Endpoint)
	}
}

func TestKongObserveDetectsDrift(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T, c *fake.Clientset, ns string)
		want   string
	}{
		{"scaled by hand", func(t *testing.T, c *fake.Clientset, ns string) {
			d, _ := c.AppsV1().Deployments(ns).Get(t.Context(), kongName, metav1.GetOptions{})
			d.Spec.Replicas = int32p(5)
			_, _ = c.AppsV1().Deployments(ns).Update(t.Context(), d, metav1.UpdateOptions{})
		}, "replicas 5, want 2"},
		{"image changed", func(t *testing.T, c *fake.Clientset, ns string) {
			d, _ := c.AppsV1().Deployments(ns).Get(t.Context(), kongName, metav1.GetOptions{})
			d.Spec.Template.Spec.Containers[0].Image = "kong:3.4"
			_, _ = c.AppsV1().Deployments(ns).Update(t.Context(), d, metav1.UpdateOptions{})
		}, "image tag 3.4, want 3.9"},
		{"configmap edited", func(t *testing.T, c *fake.Clientset, ns string) {
			cm, _ := c.CoreV1().ConfigMaps(ns).Get(t.Context(), "kong-config", metav1.GetOptions{})
			cm.Data[kongConfigKey] = "{}"
			_, _ = c.CoreV1().ConfigMaps(ns).Update(t.Context(), cm, metav1.UpdateOptions{})
		}, "configmap edited outside the control plane"},
		{"deployment deleted", func(t *testing.T, c *fake.Clientset, ns string) {
			_ = c.AppsV1().Deployments(ns).Delete(t.Context(), kongName, metav1.DeleteOptions{})
		}, "deployment missing"},
		{"service deleted", func(t *testing.T, c *fake.Clientset, ns string) {
			_ = c.CoreV1().Services(ns).Delete(t.Context(), kongProxySvc, metav1.DeleteOptions{})
		}, "service kong-proxy missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k, c := newKong()
			svc := kongSvc()
			_ = k.Apply(t.Context(), svc)
			tc.mutate(t, c, Namespace(svc))
			obs, err := k.Observe(t.Context(), svc)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(obs.Drift, "|"), tc.want) {
				t.Fatalf("want drift %q, got %v", tc.want, obs.Drift)
			}
			// Apply heals it.
			if err := k.Apply(t.Context(), svc); err != nil {
				t.Fatal(err)
			}
			obs, _ = k.Observe(t.Context(), svc)
			if len(obs.Drift) != 0 {
				t.Fatalf("drift not healed by Apply: %v", obs.Drift)
			}
		})
	}
}

func TestKongObserveRouteCountMismatch(t *testing.T) {
	k, c := newKong()
	k.AdminProbe = func(context.Context, string) (int, error) { return 0, nil }
	svc := kongSvc()
	_ = k.Apply(t.Context(), svc)
	markReady(t, c, Namespace(svc), 2)
	obs, _ := k.Observe(t.Context(), svc)
	if obs.Message != "gateway serves 0 routes, want 1" {
		t.Fatalf("want route mismatch message, got %q", obs.Message)
	}
}

func TestKongDelete(t *testing.T) {
	k, c := newKong()
	svc := kongSvc()
	_ = k.Apply(t.Context(), svc)
	done, err := k.Delete(t.Context(), svc)
	if err != nil || done {
		t.Fatalf("first delete should start teardown: done=%v err=%v", done, err)
	}
	if _, err := c.CoreV1().Namespaces().Get(t.Context(), Namespace(svc), metav1.GetOptions{}); err == nil {
		t.Fatal("namespace still present in fake after delete")
	}
	done, err = k.Delete(t.Context(), svc)
	if err != nil || !done {
		t.Fatalf("second delete should report done: done=%v err=%v", done, err)
	}
}

func TestTagOf(t *testing.T) {
	for in, want := range map[string]string{"kong:3.9": "3.9", "kong": "latest", "registry:5000/kong": "latest", "registry:5000/kong:3.10": "3.10"} {
		if got := tagOf(in); got != want {
			t.Errorf("tagOf(%q)=%q want %q", in, got, want)
		}
	}
}
