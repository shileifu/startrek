package client

import (
	"context"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func CreateClientObject(ctx context.Context, k8sclient client.Client, object client.Object) error {
	klog.V(4).Info("Creating resource service ", "namespace ", object.GetNamespace(), " name ", object.GetName(), " kind ", object.GetObjectKind().GroupVersionKind().Kind)
	if err := k8sclient.Create(ctx, object); err != nil {
		return err
	}
	return nil
}

func UpdateClientObject(ctx context.Context, k8sclient client.Client, object client.Object) error {
	klog.V(4).Info("Updating resource service ", "namespace ", object.GetNamespace(), " name ", object.GetName(), " kind ", object.GetObjectKind())
	if err := k8sclient.Update(ctx, object); err != nil {
		return err
	}
	return nil
}

func ListClientObject(ctx context.Context, k8sclient client.Client, objectList client.ObjectList, opts ...client.ListOption) error {
	klog.V(4).Info("list resource type ", objectList.GetObjectKind().GroupVersionKind().Kind)
	if err := k8sclient.List(ctx, objectList, opts...); err != nil {
		return err
	}
	return nil
}

func DeleteClientObject(ctx context.Context, k8sclient client.Client, namespace, name string) (bool, error) {
	var ob client.Object
	err := k8sclient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, ob)
	if err != nil && apierrors.IsNotFound(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}

	if err := k8sclient.Delete(ctx, ob); err != nil {
		return true, nil
	}
	return true, nil
}
