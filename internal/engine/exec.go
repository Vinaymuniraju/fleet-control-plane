package engine

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// Exec runs cmd in a pod's container through the API server, the same path
// `kubectl exec` uses, so it works whether fleetd runs in-cluster or on a laptop.
func (k Kube) Exec(ctx context.Context, ns, pod, container string, cmd []string) (string, error) {
	if k.Config == nil {
		return "", fmt.Errorf("exec needs a rest config")
	}
	req := k.Client.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(ns).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	ex, err := remotecommand.NewSPDYExecutor(k.Config, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	if err := ex.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("exec in %s/%s: %s", ns, pod, msg)
	}
	return strings.TrimSpace(stdout.String()), nil
}
