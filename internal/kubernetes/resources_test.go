package kube

import (
	"regexp"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestManagedResourceBuildersAddDurableRunnerScope(t *testing.T) {
	scope := RunnerScope("backup.runner")
	if repeated := RunnerScope("backup.runner"); repeated != scope {
		t.Fatalf("RunnerScope() = %q, repeated = %q", scope, repeated)
	}
	if other := RunnerScope("backup-runner"); other == scope {
		t.Fatalf("scope collision for distinct runner namespaces: %q", scope)
	}
	if len(scope) > 63 || !regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`).MatchString(scope) {
		t.Fatalf("scope is not a DNS-valid label value: %q", scope)
	}

	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "media"},
		Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse("1Gi"),
		}}},
	}
	sourceContent := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"driver": "driver.longhorn.io", "source": map[string]any{"snapshotHandle": "handle"}},
	}}
	aliasContent, err := BuildVolumeSnapshotAliasContent("content", "alias", "backup.runner", sourceContent, "run-1", scope)
	if err != nil {
		t.Fatalf("BuildVolumeSnapshotAliasContent() error = %v", err)
	}
	restoredPVC, err := BuildRestorePVC("restored", "backup.runner", sourcePVC, "alias", "", "run-1", scope)
	if err != nil {
		t.Fatalf("BuildRestorePVC() error = %v", err)
	}
	objects := []metav1.Object{
		BuildVolumeSnapshot("source", "media", "data", "longhorn", "run-1", scope),
		aliasContent,
		BuildPreprovisionedVolumeSnapshot("alias", "backup.runner", "content", "run-1", scope),
		BuildChildConfigSecret("config", "backup.runner", "encoded", "run-1", scope),
		restoredPVC,
	}
	for _, object := range objects {
		if got := object.GetLabels()[RunnerScopeLabel]; got != scope {
			t.Errorf("%T runner scope label = %q, want %q", object, got, scope)
		}
	}
}

func TestBuildVolumeSnapshot(t *testing.T) {
	snapshot := BuildVolumeSnapshot("snapshot", "media", "data", "longhorn", "run-1", RunnerScope("backup"))
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
	content, err := BuildVolumeSnapshotAliasContent("alias-content", "alias", "backup", source, "run-1", RunnerScope("backup"))
	if err != nil {
		t.Fatalf("BuildVolumeSnapshotAliasContent() error = %v", err)
	}
	policy, _, _ := unstructured.NestedString(content.Object, "spec", "deletionPolicy")
	handle, _, _ := unstructured.NestedString(content.Object, "spec", "source", "snapshotHandle")
	if policy != "Retain" || handle != "snapshot-handle" || content.GetLabels()[RunLabel] != "run-1" {
		t.Fatalf("alias content = %#v", content.Object)
	}
}

func TestBuildChildConfigSecret(t *testing.T) {
	secret := BuildChildConfigSecret("child-config", "backup", "encoded-config", "run-1", RunnerScope("backup"))
	if secret.Name != "child-config" || secret.Namespace != "backup" || secret.Labels[ManagedByLabel] != ManagedByLabelValue || secret.Labels[RunLabel] != "run-1" {
		t.Fatalf("Secret metadata = %#v", secret.ObjectMeta)
	}
	if string(secret.Data[ChildConfigSecretKey]) != "encoded-config" || len(secret.Data) != 1 {
		t.Fatalf("Secret data = %#v", secret.Data)
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

	pvc, err := BuildRestorePVC("restored", "backup", source, "snapshot", "", "run-1", RunnerScope("backup"))
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
