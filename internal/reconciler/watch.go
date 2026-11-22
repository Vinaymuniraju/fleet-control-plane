package reconciler

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Watch turns changes to managed objects into reconciles, so a deleted pod or
// a hand-edited Deployment is noticed in milliseconds instead of at the next
// resync. Only objects labelled as ours are watched.
func (r *Reconciler) Watch(ctx context.Context, client kubernetes.Interface) error {
	factory := informers.NewSharedInformerFactoryWithOptions(client, 10*time.Minute,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = "app.kubernetes.io/managed-by=fleet-control-plane"
		}))

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.enqueueObject(obj) },
		UpdateFunc: func(_, obj any) { r.enqueueObject(obj) },
		DeleteFunc: func(obj any) { r.enqueueObject(obj) },
	}
	for _, inf := range []cache.SharedIndexInformer{
		factory.Apps().V1().Deployments().Informer(),
		factory.Apps().V1().StatefulSets().Informer(),
		factory.Core().V1().ConfigMaps().Informer(),
		factory.Core().V1().Services().Informer(),
		factory.Core().V1().Pods().Informer(),
	} {
		if _, err := inf.AddEventHandler(handler); err != nil {
			return err
		}
	}
	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			r.opts.Log.Warn("informer cache did not sync", "type", typ.String())
		}
	}
	return nil
}

func (r *Reconciler) enqueueObject(obj any) {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = t.Obj
	}
	m, ok := obj.(metav1.Object)
	if !ok {
		return
	}
	r.Enqueue(m.GetLabels()["fleet.dev/service-id"])
}
