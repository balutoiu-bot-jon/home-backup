package kube

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	SnapshotAPIGroup    = "snapshot.storage.k8s.io"
	VolumeSnapshotKind  = "VolumeSnapshot"
	TempVolumeName      = "home-backup-snapshot"
	RunLabel            = "home-backup.balutoiu.com/run"
	ManagedByLabel      = "app.kubernetes.io/managed-by"
	ManagedByLabelValue = "home-backup"
)

var VolumeSnapshotGVR = schema.GroupVersionResource{
	Group: SnapshotAPIGroup, Version: "v1", Resource: "volumesnapshots",
}
var VolumeSnapshotContentGVR = schema.GroupVersionResource{
	Group: SnapshotAPIGroup, Version: "v1", Resource: "volumesnapshotcontents",
}

func BuildVolumeSnapshot(name, namespace, pvcName, snapshotClass, runID string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1",
		"kind":       VolumeSnapshotKind,
		"metadata": map[string]any{
			"name": name, "namespace": namespace,
			"labels": map[string]any{ManagedByLabel: ManagedByLabelValue, RunLabel: runID},
		},
		"spec": map[string]any{
			"volumeSnapshotClassName": snapshotClass,
			"source":                  map[string]any{"persistentVolumeClaimName": pvcName},
		},
	}}
}

func BuildVolumeSnapshotAliasContent(contentName, snapshotName, namespace string, sourceContent *unstructured.Unstructured, runID string) (*unstructured.Unstructured, error) {
	if sourceContent == nil {
		return nil, fmt.Errorf("source VolumeSnapshotContent is nil")
	}
	driver, found, err := unstructured.NestedString(sourceContent.Object, "spec", "driver")
	if err != nil {
		return nil, fmt.Errorf("reading source VolumeSnapshotContent driver: %w", err)
	}
	if !found || driver == "" {
		return nil, fmt.Errorf("source VolumeSnapshotContent has no driver")
	}
	snapshotHandle, found, err := unstructured.NestedString(sourceContent.Object, "status", "snapshotHandle")
	if err != nil {
		return nil, fmt.Errorf("reading source VolumeSnapshotContent snapshot handle: %w", err)
	}
	if !found || snapshotHandle == "" {
		snapshotHandle, found, err = unstructured.NestedString(sourceContent.Object, "spec", "source", "snapshotHandle")
		if err != nil {
			return nil, fmt.Errorf("reading pre-provisioned source VolumeSnapshotContent snapshot handle: %w", err)
		}
		if !found || snapshotHandle == "" {
			return nil, fmt.Errorf("source VolumeSnapshotContent has no snapshot handle")
		}
	}

	contentSpec := map[string]any{
		"deletionPolicy": "Retain",
		"driver":         driver,
		"source":         map[string]any{"snapshotHandle": snapshotHandle},
		"volumeSnapshotRef": map[string]any{
			"name": snapshotName, "namespace": namespace,
		},
	}
	for _, field := range []string{"sourceVolumeMode", "volumeSnapshotClassName"} {
		if value, found, err := unstructured.NestedString(sourceContent.Object, "spec", field); err != nil {
			return nil, fmt.Errorf("reading source VolumeSnapshotContent %s: %w", field, err)
		} else if found && value != "" {
			contentSpec[field] = value
		}
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1",
		"kind":       "VolumeSnapshotContent",
		"metadata": map[string]any{
			"name":   contentName,
			"labels": map[string]any{ManagedByLabel: ManagedByLabelValue, RunLabel: runID},
		},
		"spec": contentSpec,
	}}, nil
}

func BuildPreprovisionedVolumeSnapshot(name, namespace, contentName, runID string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1",
		"kind":       VolumeSnapshotKind,
		"metadata": map[string]any{
			"name": name, "namespace": namespace,
			"labels": map[string]any{ManagedByLabel: ManagedByLabelValue, RunLabel: runID},
		},
		"spec": map[string]any{"source": map[string]any{"volumeSnapshotContentName": contentName}},
	}}
}

func BuildRestorePVC(name, namespace string, sourcePVC *corev1.PersistentVolumeClaim, snapshotName, storageClassOverride, runID string) (*corev1.PersistentVolumeClaim, error) {
	if sourcePVC == nil {
		return nil, fmt.Errorf("source PVC is nil")
	}
	if sourcePVC.Spec.VolumeMode != nil && *sourcePVC.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		return nil, fmt.Errorf("block-mode PVCs are not supported by longhorn_pvc backups")
	}
	storageRequest, ok := sourcePVC.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || storageRequest.IsZero() {
		return nil, fmt.Errorf("source PVC %s/%s has no storage request", sourcePVC.Namespace, sourcePVC.Name)
	}
	var storageClassName *string
	if storageClassOverride != "" {
		storageClassName = &storageClassOverride
	} else if sourcePVC.Spec.StorageClassName != nil {
		storageClass := *sourcePVC.Spec.StorageClassName
		storageClassName = &storageClass
	}
	apiGroup := SnapshotAPIGroup
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: runID},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: sourcePVC.Spec.AccessModes, StorageClassName: storageClassName,
			Resources:  corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storageRequest}},
			DataSource: &corev1.TypedLocalObjectReference{APIGroup: &apiGroup, Kind: VolumeSnapshotKind, Name: snapshotName},
		},
	}
	if sourcePVC.Spec.VolumeMode != nil {
		mode := *sourcePVC.Spec.VolumeMode
		pvc.Spec.VolumeMode = &mode
	}
	return pvc, nil
}

func CreateVolumeSnapshot(ctx context.Context, clients *Clients, snapshot *unstructured.Unstructured) error {
	_, err := clients.Dynamic.Resource(VolumeSnapshotGVR).Namespace(snapshot.GetNamespace()).Create(ctx, snapshot, metav1.CreateOptions{})
	return err
}

func CreateVolumeSnapshotContent(ctx context.Context, clients *Clients, content *unstructured.Unstructured) error {
	_, err := clients.Dynamic.Resource(VolumeSnapshotContentGVR).Create(ctx, content, metav1.CreateOptions{})
	return err
}
