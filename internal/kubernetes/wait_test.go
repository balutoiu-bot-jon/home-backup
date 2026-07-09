package kube

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestWaitVolumeSnapshotReadyReturnsSnapshotError(t *testing.T) {
	snapshot := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": VolumeSnapshotKind,
		"metadata": map[string]any{"name": "snap-1", "namespace": "backup"},
		"status":   map[string]any{"error": map[string]any{"message": "volume is detached"}},
	}}
	clients := &Clients{Dynamic: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), snapshot)}

	err := WaitVolumeSnapshotReady(context.Background(), clients, "backup", "snap-1", time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "volume is detached") {
		t.Fatalf("WaitVolumeSnapshotReady() error = %v", err)
	}
}

func TestGetBoundVolumeSnapshotContent(t *testing.T) {
	snapshot := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": VolumeSnapshotKind,
		"metadata": map[string]any{"name": "snap-1", "namespace": "source", "uid": "snapshot-uid"},
		"status":   map[string]any{"boundVolumeSnapshotContentName": "snapcontent-1"},
	}}
	content := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": "VolumeSnapshotContent",
		"metadata": map[string]any{"name": "snapcontent-1"},
		"spec": map[string]any{"volumeSnapshotRef": map[string]any{
			"name": "snap-1", "namespace": "source", "uid": "snapshot-uid",
		}},
	}}
	clients := &Clients{Dynamic: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), snapshot, content)}

	got, err := GetBoundVolumeSnapshotContent(context.Background(), clients, "source", "snap-1")
	if err != nil || got.GetName() != "snapcontent-1" {
		t.Fatalf("GetBoundVolumeSnapshotContent() = %#v, %v", got, err)
	}
}

func TestWaitJobFinishedReturnsSuccessForCompletedJob(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "backup"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		}}},
	}
	clients := &Clients{Core: kubernetesfake.NewSimpleClientset(job)}

	terminal, err := WaitJobFinished(context.Background(), clients, "backup", "child", time.Millisecond)
	if err != nil || !terminal {
		t.Fatalf("WaitJobFinished() error = %v", err)
	}
}

func TestWaitJobFinishedReturnsFailureForFailedJob(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "backup"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
			Reason: "BackoffLimitExceeded", Message: "container failed",
		}}},
	}
	clients := &Clients{Core: kubernetesfake.NewSimpleClientset(job)}

	terminal, err := WaitJobFinished(context.Background(), clients, "backup", "child", time.Millisecond)
	if err == nil || !terminal || !strings.Contains(err.Error(), "BackoffLimitExceeded container failed") {
		t.Fatalf("WaitJobFinished() = terminal %v, error %v", terminal, err)
	}
}

func TestWaitJobFinishedReportsNonterminalTimeout(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "backup"}}
	clients := &Clients{Core: kubernetesfake.NewSimpleClientset(job)}

	terminal, err := WaitJobFinished(context.Background(), clients, "backup", "child", time.Millisecond)
	if err == nil || terminal {
		t.Fatalf("WaitJobFinished() = terminal %v, error %v", terminal, err)
	}
}

func TestDeleteJobIfOwned(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "child", Namespace: "backup", UID: "job-uid",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1"},
	}}
	clients := &Clients{Core: kubernetesfake.NewSimpleClientset(job)}

	if err := DeleteJobIfOwned(context.Background(), clients, "backup", "child", "run-1"); err != nil {
		t.Fatalf("DeleteJobIfOwned() error = %v", err)
	}
	if _, err := clients.Core.BatchV1().Jobs("backup").Get(context.Background(), "child", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted Job Get() error = %v", err)
	}
}

func TestDeletePVCIfOwnedRejectsMismatchedRun(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "restored", Namespace: "backup",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "another-run"},
	}}
	clients := &Clients{Core: kubernetesfake.NewSimpleClientset(pvc)}

	err := DeletePVCIfOwned(context.Background(), clients, "backup", "restored", "run-1")
	if err == nil || !strings.Contains(err.Error(), "not owned by run") {
		t.Fatalf("DeletePVCIfOwned() error = %v", err)
	}
}
