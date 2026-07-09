package kube

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildVolumeSnapshot(t *testing.T) {
	snapshot := BuildVolumeSnapshot("snapshot", "media", "data", "longhorn", "run-1")
	if snapshot.GetName() != "snapshot" || snapshot.GetNamespace() != "media" || snapshot.GetLabels()[RunLabel] != "run-1" {
		t.Fatalf("snapshot metadata = %#v", snapshot.Object["metadata"])
	}
	pvcName, _, err := unstructured.NestedString(snapshot.Object, "spec", "source", "persistentVolumeClaimName")
	if err != nil || pvcName != "data" {
		t.Fatalf("snapshot source = %q, %v", pvcName, err)
	}
}

func TestBuildVolumeSnapshotAliasContentRetainsSourceHandle(t *testing.T) {
	source := &unstructured.Unstructured{Object: map[string]any{
		"spec":   map[string]any{"driver": "driver.longhorn.io", "volumeSnapshotClassName": "longhorn"},
		"status": map[string]any{"snapshotHandle": "snapshot-handle"},
	}}
	content, err := BuildVolumeSnapshotAliasContent("alias-content", "alias", "backup", source, "run-1")
	if err != nil {
		t.Fatalf("BuildVolumeSnapshotAliasContent() error = %v", err)
	}
	policy, _, _ := unstructured.NestedString(content.Object, "spec", "deletionPolicy")
	handle, _, _ := unstructured.NestedString(content.Object, "spec", "source", "snapshotHandle")
	if policy != "Retain" || handle != "snapshot-handle" || content.GetLabels()[RunLabel] != "run-1" {
		t.Fatalf("alias content = %#v", content.Object)
	}
}

func TestBuildRestorePVCFromSnapshot(t *testing.T) {
	storageClass := "longhorn"
	mode := corev1.PersistentVolumeFilesystem
	source := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "media"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			VolumeMode:       &mode,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			}},
		},
	}

	pvc, err := BuildRestorePVC("restored", "backup", source, "snapshot", "", "run-1")
	if err != nil {
		t.Fatalf("BuildRestorePVC() error = %v", err)
	}
	if pvc.Namespace != "backup" || pvc.Spec.DataSource == nil || pvc.Spec.DataSource.Name != "snapshot" || pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != storageClass {
		t.Fatalf("restored PVC = %#v", pvc)
	}
	if pvc.Labels[RunLabel] != "run-1" || pvc.Spec.Resources.Requests.Storage().Cmp(resource.MustParse("10Gi")) != 0 {
		t.Fatalf("restored PVC labels/resources = %#v / %#v", pvc.Labels, pvc.Spec.Resources)
	}
}
