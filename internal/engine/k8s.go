package engine

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Kube wraps the typed client with create-or-update helpers. Each helper
// overwrites only the fields the control plane owns and returns whether it
// changed anything, so callers can tell a no-op apply from a real one.
type Kube struct {
	Client kubernetes.Interface
	Config *rest.Config // needed only for Exec
}

func (k Kube) EnsureNamespace(ctx context.Context, name string, labels map[string]string) error {
	ns, err := k.Client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = k.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if ns.Status.Phase == corev1.NamespaceTerminating {
		return fmt.Errorf("namespace %s is still terminating from an earlier delete", name)
	}
	return nil
}

func (k Kube) EnsureConfigMap(ctx context.Context, want *corev1.ConfigMap) error {
	c := k.Client.CoreV1().ConfigMaps(want.Namespace)
	cur, err := c.Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = c.Create(ctx, want, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	cur.Labels, cur.Data = want.Labels, want.Data
	_, err = c.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

func (k Kube) EnsureSecret(ctx context.Context, want *corev1.Secret) (*corev1.Secret, error) {
	c := k.Client.CoreV1().Secrets(want.Namespace)
	cur, err := c.Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, want, metav1.CreateOptions{})
	}
	// Secrets are create-once: never rotate a password behind a running database.
	return cur, err
}

func (k Kube) EnsureService(ctx context.Context, want *corev1.Service) error {
	c := k.Client.CoreV1().Services(want.Namespace)
	cur, err := c.Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = c.Create(ctx, want, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	cur.Labels = want.Labels
	cur.Spec.Selector = want.Spec.Selector
	cur.Spec.Ports = want.Spec.Ports
	_, err = c.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

func (k Kube) EnsureDeployment(ctx context.Context, want *appsv1.Deployment) error {
	c := k.Client.AppsV1().Deployments(want.Namespace)
	cur, err := c.Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = c.Create(ctx, want, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	cur.Labels = want.Labels
	cur.Annotations = want.Annotations
	cur.Spec.Replicas = want.Spec.Replicas
	cur.Spec.Strategy = want.Spec.Strategy
	cur.Spec.Template = want.Spec.Template
	_, err = c.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

func (k Kube) EnsurePDB(ctx context.Context, want *policyv1.PodDisruptionBudget, enabled bool) error {
	c := k.Client.PolicyV1().PodDisruptionBudgets(want.Namespace)
	cur, err := c.Get(ctx, want.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err) && !enabled:
		return nil
	case apierrors.IsNotFound(err):
		_, err = c.Create(ctx, want, metav1.CreateOptions{})
		return err
	case err != nil:
		return err
	case !enabled:
		return c.Delete(ctx, want.Name, metav1.DeleteOptions{})
	}
	cur.Spec = want.Spec
	_, err = c.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

// DeleteNamespace deletes ns and reports true once it is fully gone.
func (k Kube) DeleteNamespace(ctx context.Context, ns string) (bool, error) {
	_, err := k.Client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	err = k.Client.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	return false, nil
}

func int32p(i int) *int32 { v := int32(i); return &v }

func maxUnavailableZero() *appsv1.DeploymentStrategy {
	zero, one := intstr.FromInt32(0), intstr.FromInt32(1)
	return &appsv1.DeploymentStrategy{
		Type:          appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: &zero, MaxSurge: &one},
	}
}

func (k Kube) EnsureStatefulSet(ctx context.Context, want *appsv1.StatefulSet) error {
	c := k.Client.AppsV1().StatefulSets(want.Namespace)
	cur, err := c.Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = c.Create(ctx, want, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	// Selector, serviceName and volumeClaimTemplates are immutable; only the
	// mutable fields are carried over.
	cur.Labels = want.Labels
	for k, v := range want.Annotations {
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[k] = v
	}
	cur.Spec.Replicas = want.Spec.Replicas
	cur.Spec.Template = want.Spec.Template
	cur.Spec.UpdateStrategy = want.Spec.UpdateStrategy
	_, err = c.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

// LabelPod sets one label on a pod.
func (k Kube) LabelPod(ctx context.Context, ns, pod, key, value string) error {
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, key, value)
	_, err := k.Client.CoreV1().Pods(ns).Patch(ctx, pod, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}
