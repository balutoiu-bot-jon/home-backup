package kube

import (
	"context"
	"crypto/sha256"
	"fmt"
	"regexp"
	"strings"

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
	RunnerScopeLabel    = "home-backup.balutoiu.com/runner-scope"
	ManagedByLabel      = "app.kubernetes.io/managed-by"
	ManagedByLabelValue = "home-backup"
)

var invalidLabelValueChars = regexp.MustCompile(`[^a-z0-9-]+`)

// RunnerScope returns a stable, collision-resistant DNS label value for one runner installation.
func RunnerScope(runnerNamespace string) string {
	sum := sha256.Sum256([]byte(runnerNamespace))
	prefix := strings.Trim(invalidLabelValueChars.ReplaceAllString(strings.ToLower(runnerNamespace), "-"), "-")
	if prefix == "" {
		prefix = "runner"
	}
	const hashLength = 16
	maxPrefixLength := 63 - 1 - hashLength
	if len(prefix) > maxPrefixLength {
		prefix = strings.TrimRight(prefix[:maxPrefixLength], "-")
	}
	return fmt.Sprintf("%s-%x", prefix, sum[:hashLength/2])
}

func managedLabels(runID, runnerScope string) map[string]string {
	return map[string]string{
		ManagedByLabel:   ManagedByLabelValue,
		RunLabel:         runID,
		RunnerScopeLabel: runnerScope,
	}
}

func unstructuredManagedLabels(runID, runnerScope string) map[string]any {
	return map[string]any{
		ManagedByLabel:   ManagedByLabelValue,
		RunLabel:         runID,
		RunnerScopeLabel: runnerScope,
	}
}

var VolumeSnapshotGVR = schema.GroupVersionResource{
	Group: SnapshotAPIGroup, Version: "v1", Resource: "volumesnapshots",
}
var VolumeSnapshotContentGVR = schema.GroupVersionResource{
	Group: SnapshotAPIGroup, Version: "v1", Resource: "volumesnapshotcontents",
}

func BuildChildConfigSecret(name, namespace, configBase64, runID, runnerScope string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			Labels: managedLabels(runID, runnerScope),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{ChildConfigSecretKey: []byte(configBase64)},
	}
}

func BuildVolumeSnapshot(name, namespace, pvcName, snapshotClass, runID, runnerScope string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1",
		"kind":       VolumeSnapshotKind,
		"metadata": map[string]any{
			"name": name, "namespace": namespace,
			"labels": unstructuredManagedLabels(runID, runnerScope),
		},
		"spec": map[string]any{
			"volumeSnapshotClassName": snapshotClass,
			"source":                  map[string]any{"persistentVolumeClaimName": pvcName},
		},
	}}
}

func BuildVolumeSnapshotAliasContent(contentName, snapshotName, namespace string, sourceContent *unstructured.Unstructured, runID, runnerScope string) (*unstructured.Unstructured, error) {
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
			"labels": unstructuredManagedLabels(runID, runnerScope),
		},
		"spec": contentSpec,
	}}, nil
}

func BuildPreprovisionedVolumeSnapshot(name, namespace, contentName, runID, runnerScope string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1",
		"kind":       VolumeSnapshotKind,
		"metadata": map[string]any{
			"name": name, "namespace": namespace,
			"labels": unstructuredManagedLabels(runID, runnerScope),
		},
		"spec": map[string]any{"source": map[string]any{"volumeSnapshotContentName": contentName}},
	}}
}

type RestorePVCOptions struct {
	Name                 string
	Namespace            string
	SourcePVC            *corev1.PersistentVolumeClaim
	SnapshotName         string
	StorageClassOverride string
	RunID                string
	RunnerScope          string
}

func BuildRestorePVC(opts RestorePVCOptions) (*corev1.PersistentVolumeClaim, error) {
	if opts.SourcePVC == nil {
		return nil, fmt.Errorf("source PVC is nil")
	}
	if opts.SourcePVC.Spec.VolumeMode != nil && *opts.SourcePVC.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		return nil, fmt.Errorf("block-mode PVCs are not supported by longhorn_pvc backups")
	}
	storageRequest, ok := opts.SourcePVC.Spec.Resources.Requests[corev1.ResourceStorage]
	if !ok || storageRequest.IsZero() {
		return nil, fmt.Errorf("source PVC %s/%s has no storage request", opts.SourcePVC.Namespace, opts.SourcePVC.Name)
	}
	var storageClassName *string
	if opts.StorageClassOverride != "" {
		storageClassName = &opts.StorageClassOverride
	} else if opts.SourcePVC.Spec.StorageClassName != nil {
		storageClass := *opts.SourcePVC.Spec.StorageClassName
		storageClassName = &storageClass
	}
	apiGroup := SnapshotAPIGroup
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name: opts.Name, Namespace: opts.Namespace,
			Labels: managedLabels(opts.RunID, opts.RunnerScope),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: opts.SourcePVC.Spec.AccessModes, StorageClassName: storageClassName,
			Resources:  corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storageRequest}},
			DataSource: &corev1.TypedLocalObjectReference{APIGroup: &apiGroup, Kind: VolumeSnapshotKind, Name: opts.SnapshotName},
		},
	}
	if opts.SourcePVC.Spec.VolumeMode != nil {
		mode := *opts.SourcePVC.Spec.VolumeMode
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
