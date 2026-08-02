package kube

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestReconcileStaleRunsDeletesAbandonedResourcesInDependencyOrder(t *testing.T) {
	created := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	labels := map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "stale-run", RunnerScopeLabel: RunnerScope("backup")}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "run-job", Namespace: "backup", UID: "job-uid", CreationTimestamp: created, Labels: labels}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "run-config", Namespace: "backup", UID: "secret-uid", CreationTimestamp: created, Labels: labels}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "run-pvc", Namespace: "backup", UID: "pvc-uid", CreationTimestamp: created, Labels: labels}}
	unstructuredResource := func(name, namespace, kind string) *unstructured.Unstructured {
		metadata := map[string]any{
			"name": name, "uid": name + "-uid", "creationTimestamp": created.Format(time.RFC3339),
			"labels": map[string]any{ManagedByLabel: ManagedByLabelValue, RunLabel: "stale-run", RunnerScopeLabel: RunnerScope("backup")},
		}
		if namespace != "" {
			metadata["namespace"] = namespace
		}
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": SnapshotAPIGroup + "/v1", "kind": kind, "metadata": metadata,
		}}
	}
	sourceSnapshot := unstructuredResource("run-source-snap", "source", VolumeSnapshotKind)
	aliasSnapshot := unstructuredResource("run-alias-snap", "backup", VolumeSnapshotKind)
	aliasContent := unstructuredResource("run-alias-content", "", "VolumeSnapshotContent")
	aliasContent.Object["spec"] = map[string]any{"volumeSnapshotRef": map[string]any{"name": "run-alias-snap", "namespace": "backup"}}
	coreClient := kubernetesfake.NewSimpleClientset(job, secret, pvc)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	}, sourceSnapshot, aliasSnapshot, aliasContent)
	var deletes []string
	recordDelete := func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		deletes = append(deletes, action.GetResource().Resource+"/"+deleteAction.GetName())
		return false, nil, nil
	}
	coreClient.PrependReactor("delete", "*", recordDelete)
	dynamicClient.PrependReactor("delete", "*", recordDelete)

	if err := ReconcileStaleRuns(context.Background(), &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup", []string{"source"}, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
	want := []string{
		"jobs/run-job", "secrets/run-config", "persistentvolumeclaims/run-pvc",
		"volumesnapshots/run-alias-snap", "volumesnapshotcontents/run-alias-content", "volumesnapshots/run-source-snap",
	}
	if len(deletes) != len(want) {
		t.Fatalf("deletes = %#v, want %#v", deletes, want)
	}
	for i := range want {
		if deletes[i] != want[i] {
			t.Fatalf("deletes = %#v, want %#v", deletes, want)
		}
	}
}

func TestReconcileStaleRunsCleansOldActiveAndSkipsYoungRuns(t *testing.T) {
	now := time.Now()
	managed := func(run string, created time.Time) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Namespace: "backup", CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: run, RunnerScopeLabel: RunnerScope("backup")},
		}
	}
	activeJob := &batchv1.Job{ObjectMeta: managed("active-run", now.Add(-48*time.Hour)), Status: batchv1.JobStatus{Active: 1}}
	activeJob.Name = "active-job"
	activeSecret := &corev1.Secret{ObjectMeta: managed("active-run", now.Add(-48*time.Hour))}
	activeSecret.Name = "active-secret"
	youngPVC := &corev1.PersistentVolumeClaim{ObjectMeta: managed("young-run", now.Add(-time.Hour))}
	youngPVC.Name = "young-pvc"
	snapshot := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": VolumeSnapshotKind,
		"metadata": map[string]any{
			"name": "active-snapshot", "namespace": "backup",
			"creationTimestamp": now.Add(-48 * time.Hour).Format(time.RFC3339),
			"labels":            map[string]any{ManagedByLabel: ManagedByLabelValue, RunLabel: "active-run", RunnerScopeLabel: RunnerScope("backup")},
		},
	}}
	coreClient := kubernetesfake.NewSimpleClientset(activeJob, activeSecret, youngPVC)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	}, snapshot)

	if err := ReconcileStaleRuns(context.Background(), &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup", nil, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
	if _, err := coreClient.BatchV1().Jobs("backup").Get(context.Background(), "active-job", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old active Job Get() error = %v, want NotFound", err)
	}
	if _, err := coreClient.CoreV1().Secrets("backup").Get(context.Background(), "active-secret", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old active Secret Get() error = %v, want NotFound", err)
	}
	if _, err := dynamicClient.Resource(VolumeSnapshotGVR).Namespace("backup").Get(context.Background(), "active-snapshot", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old active VolumeSnapshot Get() error = %v, want NotFound", err)
	}
	if _, err := coreClient.CoreV1().PersistentVolumeClaims("backup").Get(context.Background(), "young-pvc", metav1.GetOptions{}); err != nil {
		t.Fatalf("young PVC Get() error = %v, want resource preserved", err)
	}
}

func TestReconcileStaleRunsCleansAllocationsWithoutChildJob(t *testing.T) {
	created := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	labels := map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "orphan-run", RunnerScopeLabel: RunnerScope("backup")}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "orphan-config", Namespace: "backup", UID: "secret-uid", CreationTimestamp: created, Labels: labels}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "orphan-pvc", Namespace: "backup", UID: "pvc-uid", CreationTimestamp: created, Labels: labels}}
	resource := func(name, namespace, kind string) *unstructured.Unstructured {
		metadata := map[string]any{
			"name": name, "uid": name + "-uid", "creationTimestamp": created.Format(time.RFC3339),
			"labels": map[string]any{ManagedByLabel: ManagedByLabelValue, RunLabel: "orphan-run", RunnerScopeLabel: RunnerScope("backup")},
		}
		if namespace != "" {
			metadata["namespace"] = namespace
		}
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": SnapshotAPIGroup + "/v1", "kind": kind, "metadata": metadata}}
	}
	sourceSnapshot := resource("orphan-source-snap", "source", VolumeSnapshotKind)
	aliasSnapshot := resource("orphan-alias-snap", "backup", VolumeSnapshotKind)
	aliasContent := resource("orphan-alias-content", "", "VolumeSnapshotContent")
	aliasContent.Object["spec"] = map[string]any{"volumeSnapshotRef": map[string]any{"name": "orphan-alias-snap", "namespace": "backup"}}
	coreClient := kubernetesfake.NewSimpleClientset(secret, pvc)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	}, sourceSnapshot, aliasSnapshot, aliasContent)
	var deletes []string
	record := func(action clienttesting.Action) (bool, runtime.Object, error) {
		deletes = append(deletes, action.GetResource().Resource+"/"+action.(clienttesting.DeleteAction).GetName())
		return false, nil, nil
	}
	coreClient.PrependReactor("delete", "*", record)
	dynamicClient.PrependReactor("delete", "*", record)

	if err := ReconcileStaleRuns(context.Background(), &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup", []string{"source"}, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
	want := []string{
		"secrets/orphan-config", "persistentvolumeclaims/orphan-pvc", "volumesnapshots/orphan-alias-snap",
		"volumesnapshotcontents/orphan-alias-content", "volumesnapshots/orphan-source-snap",
	}
	if !reflect.DeepEqual(deletes, want) {
		t.Fatalf("deletes = %#v, want %#v", deletes, want)
	}
}

func TestReconcileStaleRunsDeletesTerminalAndNonterminalJobsBeforeAllocations(t *testing.T) {
	created := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	labels := map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "stale-run", RunnerScopeLabel: RunnerScope("backup")}
	controller := true
	terminal := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "a-terminal", Namespace: "backup", UID: "terminal-uid", CreationTimestamp: created, Labels: labels},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: created,
		}}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "terminal-pod", Namespace: "backup", UID: "pod-uid", Labels: map[string]string{
			batchv1.JobNameLabel: terminal.Name, ManagedByLabel: ManagedByLabelValue, RunLabel: "stale-run", RunnerScopeLabel: RunnerScope("backup"),
		}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: terminal.Name, UID: terminal.UID, Controller: &controller}}},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
	nonterminal := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "b-running", Namespace: "backup", UID: "running-uid", CreationTimestamp: created, Labels: labels}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: "backup", UID: "secret-uid", CreationTimestamp: created, Labels: labels}}
	coreClient := kubernetesfake.NewSimpleClientset(terminal, pod, nonterminal, secret)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	})
	var deletes []string
	coreClient.PrependReactor("delete", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deletes = append(deletes, action.GetResource().Resource+"/"+action.(clienttesting.DeleteAction).GetName())
		return false, nil, nil
	})

	if err := ReconcileStaleRuns(context.Background(), &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup", nil, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
	want := []string{"jobs/a-terminal", "jobs/b-running", "secrets/config"}
	if !reflect.DeepEqual(deletes, want) {
		t.Fatalf("deletes = %#v, want %#v", deletes, want)
	}
	if _, err := coreClient.BatchV1().Jobs("backup").Get(context.Background(), terminal.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("terminal Job Get() error = %v, want NotFound", err)
	}
}

func TestReconcileStaleRunsPreservesRecentlyTerminalRun(t *testing.T) {
	created := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	recent := metav1.Now()
	labels := map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "still-finishing", RunnerScopeLabel: RunnerScope("backup")}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "recently-terminal", Namespace: "backup", UID: "job-uid", CreationTimestamp: created, Labels: labels},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: recent,
		}}},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: "backup", UID: "secret-uid", CreationTimestamp: created, Labels: labels}}
	coreClient := kubernetesfake.NewSimpleClientset(job, secret)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	})

	if err := ReconcileStaleRuns(context.Background(), &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup", nil, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
	if _, err := coreClient.BatchV1().Jobs("backup").Get(context.Background(), job.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("recently terminal Job was deleted while its parent could still be collecting logs: %v", err)
	}
	if _, err := coreClient.CoreV1().Secrets("backup").Get(context.Background(), secret.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("recently terminal run dependency was deleted: %v", err)
	}
}

func TestReconcileAbandonedRunBoundsForegroundJobDeletion(t *testing.T) {
	labels := map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "stale", RunnerScopeLabel: RunnerScope("backup")}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: "backup", UID: "job-uid", Labels: labels}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: "backup", UID: "secret-uid", Labels: labels}}
	coreClient := kubernetesfake.NewSimpleClientset(job, secret)
	coreClient.PrependReactor("delete", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil // Simulate a Job held by a Pod or finalizer.
	})
	run := &reconciledRun{jobs: []*batchv1.Job{job}, secrets: []*corev1.Secret{secret}}

	started := time.Now()
	err := reconcileAbandonedRunWithTimeouts(context.Background(), &Clients{Core: coreClient}, "backup", "stale", run, 20*time.Millisecond, 20*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reconcileAbandonedRunWithTimeouts() error = %v, want bounded deletion timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked stale Job cleanup took %s", elapsed)
	}
	if _, getErr := coreClient.CoreV1().Secrets("backup").Get(context.Background(), secret.Name, metav1.GetOptions{}); getErr != nil {
		t.Fatalf("dependent Secret was deleted after blocked Job deletion: %v", getErr)
	}
}

func TestReconcileStaleRunsStopsOnCancellationBeforeLowerDependencies(t *testing.T) {
	created := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	labels := map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "stale-run", RunnerScopeLabel: RunnerScope("backup")}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "backup", UID: "job-uid", CreationTimestamp: created, Labels: labels}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: "backup", UID: "secret-uid", CreationTimestamp: created, Labels: labels}}
	coreClient := kubernetesfake.NewSimpleClientset(job, secret)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	})
	ctx, cancel := context.WithCancel(context.Background())
	var deletes []string
	coreClient.PrependReactor("delete", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deletes = append(deletes, action.GetResource().Resource)
		cancel()
		return false, nil, nil
	})

	err := ReconcileStaleRuns(ctx, &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup", nil, 24*time.Hour)
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
	if !reflect.DeepEqual(deletes, []string{"jobs"}) {
		t.Fatalf("deletes after cancellation = %#v", deletes)
	}
}

func TestReconcileStaleRunsListsRunnerResourcesAndOnlySourceSnapshots(t *testing.T) {
	coreClient := kubernetesfake.NewSimpleClientset()
	coreClient.PrependReactor("list", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetNamespace() != "backup" {
			t.Fatalf("listed core resource %s in source namespace %q", action.GetResource().Resource, action.GetNamespace())
		}
		selector := action.(clienttesting.ListAction).GetListRestrictions().Labels.String()
		if !strings.Contains(selector, RunnerScopeLabel+"="+RunnerScope("backup")) {
			t.Fatalf("list selector = %q, missing runner scope", selector)
		}
		return false, nil, nil
	})
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	})
	dynamicClient.PrependReactor("list", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		selector := action.(clienttesting.ListAction).GetListRestrictions().Labels.String()
		if !strings.Contains(selector, RunnerScopeLabel+"="+RunnerScope("backup")) {
			t.Fatalf("dynamic list selector = %q, missing runner scope", selector)
		}
		return false, nil, nil
	})

	if err := ReconcileStaleRuns(context.Background(), &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup", []string{"source"}, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
}

func TestReconcileStaleRunsDoesNotDeleteUnrelatedResources(t *testing.T) {
	created := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	unmanaged := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "unmanaged", Namespace: "backup", UID: "unmanaged-uid", CreationTimestamp: created}}
	missingRun := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "missing-run", Namespace: "backup", UID: "missing-run-uid", CreationTimestamp: created, Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunnerScopeLabel: RunnerScope("backup")}}}
	coreClient := kubernetesfake.NewSimpleClientset(unmanaged, missingRun)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	})
	coreClient.PrependReactor("delete", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		t.Fatalf("unexpected delete of %s", action.GetResource().Resource)
		return true, nil, nil
	})

	if err := ReconcileStaleRuns(context.Background(), &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup", nil, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
}

func TestReconcileStaleRunsPreservesActiveForeignRunnerContent(t *testing.T) {
	created := time.Now().Add(-48 * time.Hour)
	foreignScope := RunnerScope("backup-b")
	foreignJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "active-job", Namespace: "backup-b", CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "shared-run", RunnerScopeLabel: foreignScope},
		},
		Status: batchv1.JobStatus{Active: 1},
	}
	foreignContent := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": "VolumeSnapshotContent",
		"metadata": map[string]any{
			"name": "foreign-alias-content", "uid": "foreign-content-uid",
			"creationTimestamp": created.Format(time.RFC3339),
			"labels":            map[string]any{ManagedByLabel: ManagedByLabelValue, RunLabel: "shared-run", RunnerScopeLabel: foreignScope},
		},
		"spec": map[string]any{"volumeSnapshotRef": map[string]any{"name": "alias", "namespace": "backup-b"}},
	}}
	coreClient := kubernetesfake.NewSimpleClientset(foreignJob)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	}, foreignContent)
	dynamicClient.PrependReactor("delete", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		t.Fatalf("foreign runner resource was deleted: %s/%s", action.GetResource().Resource, action.(clienttesting.DeleteAction).GetName())
		return true, nil, nil
	})

	if err := ReconcileStaleRuns(context.Background(), &Clients{Core: coreClient, Dynamic: dynamicClient}, "backup-a", nil, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
	if _, err := dynamicClient.Resource(VolumeSnapshotContentGVR).Get(context.Background(), foreignContent.GetName(), metav1.GetOptions{}); err != nil {
		t.Fatalf("foreign VolumeSnapshotContent did not survive: %v", err)
	}
}

func TestReconcileStaleRunsPreservesEntireRunWithOwnScopeContentTargetingAnotherNamespace(t *testing.T) {
	created := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	runID := "old-run"
	labels := map[string]string{
		ManagedByLabel: ManagedByLabelValue, RunLabel: runID, RunnerScopeLabel: RunnerScope("backup-a"),
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "old-job", Namespace: "backup-a", UID: "job-uid", CreationTimestamp: created, Labels: labels,
	}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "old-pvc", Namespace: "backup-a", UID: "pvc-uid", CreationTimestamp: created, Labels: labels,
	}}
	snapshot := func(name, namespace string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": SnapshotAPIGroup + "/v1", "kind": VolumeSnapshotKind,
			"metadata": map[string]any{
				"name": name, "namespace": namespace, "uid": name + "-uid",
				"creationTimestamp": created.Format(time.RFC3339),
				"labels": map[string]any{
					ManagedByLabel: ManagedByLabelValue, RunLabel: runID, RunnerScopeLabel: RunnerScope("backup-a"),
				},
			},
		}}
	}
	sourceSnapshot := snapshot("old-source-snap", "source")
	aliasSnapshot := snapshot("old-alias-snap", "backup-a")
	content := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": "VolumeSnapshotContent",
		"metadata": map[string]any{
			"name": "misdirected-alias-content", "uid": "content-uid",
			"creationTimestamp": created.Format(time.RFC3339),
			"labels": map[string]any{
				ManagedByLabel: ManagedByLabelValue, RunLabel: runID, RunnerScopeLabel: RunnerScope("backup-a"),
			},
		},
		"spec": map[string]any{"volumeSnapshotRef": map[string]any{"name": aliasSnapshot.GetName(), "namespace": "backup-b"}},
	}}
	coreClient := kubernetesfake.NewSimpleClientset(job, pvc)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		VolumeSnapshotGVR: "VolumeSnapshotList", VolumeSnapshotContentGVR: "VolumeSnapshotContentList",
	}, sourceSnapshot, aliasSnapshot, content)
	failDelete := func(action clienttesting.Action) (bool, runtime.Object, error) {
		t.Fatalf("unsafe dependency group resource was deleted: %s/%s", action.GetResource().Resource, action.(clienttesting.DeleteAction).GetName())
		return true, nil, nil
	}
	coreClient.PrependReactor("delete", "*", failDelete)
	dynamicClient.PrependReactor("delete", "*", failDelete)
	clients := &Clients{Core: coreClient, Dynamic: dynamicClient}

	if err := ReconcileStaleRuns(context.Background(), clients, "backup-a", []string{"source"}, 24*time.Hour); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
	if _, err := coreClient.BatchV1().Jobs("backup-a").Get(context.Background(), job.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("Job did not survive unsafe run: %v", err)
	}
	if _, err := coreClient.CoreV1().PersistentVolumeClaims("backup-a").Get(context.Background(), pvc.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("PVC did not survive unsafe run: %v", err)
	}
	for _, resource := range []struct {
		gvr       schema.GroupVersionResource
		namespace string
		name      string
	}{{VolumeSnapshotGVR, "source", sourceSnapshot.GetName()}, {VolumeSnapshotGVR, "backup-a", aliasSnapshot.GetName()}, {VolumeSnapshotContentGVR, "", content.GetName()}} {
		var err error
		if resource.namespace == "" {
			_, err = dynamicClient.Resource(resource.gvr).Get(context.Background(), resource.name, metav1.GetOptions{})
		} else {
			_, err = dynamicClient.Resource(resource.gvr).Namespace(resource.namespace).Get(context.Background(), resource.name, metav1.GetOptions{})
		}
		if err != nil {
			t.Fatalf("%s %s did not survive unsafe run: %v", resource.gvr.Resource, resource.name, err)
		}
	}
}
