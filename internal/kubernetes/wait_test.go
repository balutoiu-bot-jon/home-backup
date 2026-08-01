package kube

import (
	"context"
	"errors"
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
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
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

func TestWaitVolumeSnapshotReadyRetriesTransientRead(t *testing.T) {
	snapshot := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": VolumeSnapshotKind,
		"metadata": map[string]any{"name": "snap-1", "namespace": "backup"},
		"status":   map[string]any{"readyToUse": true},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), snapshot)
	attempts := 0
	dynamicClient.PrependReactor("get", "volumesnapshots", func(clienttesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts == 1 {
			return true, nil, apierrors.NewInternalError(errors.New("temporary apiserver failure"))
		}
		return false, nil, nil
	})
	clients := &Clients{Dynamic: dynamicClient}

	if err := WaitVolumeSnapshotReady(context.Background(), clients, "backup", "snap-1", 3*time.Second); err != nil {
		t.Fatalf("WaitVolumeSnapshotReady() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("snapshot Get attempts = %d", attempts)
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

func TestWaitJobFinishedRetriesTransientRead(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "backup"},
		Status:     batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}},
	}
	coreClient := kubernetesfake.NewSimpleClientset(job)
	attempts := 0
	coreClient.PrependReactor("get", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts == 1 {
			return true, nil, apierrors.NewTooManyRequests("temporary throttle", 0)
		}
		return false, nil, nil
	})
	clients := &Clients{Core: coreClient}

	terminal, err := WaitJobFinished(context.Background(), clients, "backup", "child", 6*time.Second)
	if err != nil || !terminal {
		t.Fatalf("WaitJobFinished() = terminal %v, error %v", terminal, err)
	}
	if attempts != 2 {
		t.Fatalf("Job Get attempts = %d", attempts)
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

func TestWaitForDeletionRetriesTransientObservation(t *testing.T) {
	attempts := 0
	err := waitForDeletion(context.Background(), "PVC backup/restored", func(context.Context) error {
		attempts++
		if attempts == 1 {
			return apierrors.NewInternalError(errors.New("temporary watch failure"))
		}
		return apierrors.NewNotFound(corev1.Resource("persistentvolumeclaims"), "restored")
	})
	if err != nil {
		t.Fatalf("waitForDeletion() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("deletion Get attempts = %d", attempts)
	}
}

func TestDeleteJobIfOwned(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "child", Namespace: "backup", UID: "job-uid",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")},
	}}
	coreClient := kubernetesfake.NewSimpleClientset(job)
	coreClient.PrependReactor("delete", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		options := deleteAction.GetDeleteOptions()
		if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
			t.Fatalf("Job propagation policy = %v, want Foreground", options.PropagationPolicy)
		}
		preconditions := options.Preconditions
		if preconditions == nil || preconditions.UID == nil || *preconditions.UID != "job-uid" {
			t.Fatalf("Job delete preconditions = %#v, want UID job-uid", preconditions)
		}
		return false, nil, nil
	})
	clients := &Clients{Core: coreClient}

	if err := DeleteJobIfOwned(context.Background(), clients, "backup", "child", "run-1", RunnerScope("backup")); err != nil {
		t.Fatalf("DeleteJobIfOwned() error = %v", err)
	}
	if _, err := clients.Core.BatchV1().Jobs("backup").Get(context.Background(), "child", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted Job Get() error = %v", err)
	}
}

func TestDeleteJobIfOwnedWaitsForForegroundDeletion(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "child", Namespace: "backup", UID: "job-uid",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")},
	}}
	coreClient := kubernetesfake.NewSimpleClientset(job)
	coreClient.PrependReactor("delete", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil // Simulate a Job held in foreground deletion by an owned Pod/finalizer.
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := DeleteJobIfOwned(ctx, &Clients{Core: coreClient}, "backup", "child", "run-1", RunnerScope("backup"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DeleteJobIfOwned() error = %v, want foreground deletion timeout", err)
	}
	if _, getErr := coreClient.BatchV1().Jobs("backup").Get(context.Background(), "child", metav1.GetOptions{}); getErr != nil {
		t.Fatalf("Job did not remain while foreground deletion was blocked: %v", getErr)
	}
}

func TestJobPodLogsIfOwnedCollectsAllContainersBeforeDeletion(t *testing.T) {
	controller := true
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "child", Namespace: "backup", UID: "job-uid",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")},
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "child-abc", Namespace: "backup", UID: "pod-uid",
			Labels: map[string]string{batchv1.JobNameLabel: "child", ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "Job", Name: "child", UID: "job-uid", Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "prepare"}},
			Containers:     []corev1.Container{{Name: "home-backup"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
	clients := &Clients{
		Core: kubernetesfake.NewSimpleClientset(job, pod),
		PodLogs: func(_ context.Context, namespace, podName, containerName string, options corev1.PodLogOptions, maxBytes int64) (string, bool, error) {
			if options.Previous {
				t.Fatal("restartPolicy Never child requested previous logs")
			}
			if options.LimitBytes == nil || *options.LimitBytes <= 0 || maxBytes <= 0 {
				t.Fatalf("unbounded log request: options=%#v maxBytes=%d", options, maxBytes)
			}
			return namespace + "/" + podName + "/" + containerName, false, nil
		},
	}

	logs, err := JobPodLogsIfOwned(context.Background(), clients, "backup", "child", "run-1")
	if err != nil {
		t.Fatalf("JobPodLogsIfOwned() error = %v", err)
	}
	for _, want := range []string{"container prepare", "backup/child-abc/prepare", "container home-backup", "backup/child-abc/home-backup"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("JobPodLogsIfOwned() logs = %q, missing %q", logs, want)
		}
	}
}

func TestJobPodLogsIfOwnedPreservesPartialLogsAndContainerErrors(t *testing.T) {
	clients := ownedJobPodLogClients(t, func(_ context.Context, _, _, container string, _ corev1.PodLogOptions, _ int64) (string, bool, error) {
		if container == "prepare" {
			return "root cause from init", false, nil
		}
		return "", false, errors.New("container never started")
	})

	logs, err := JobPodLogsIfOwned(context.Background(), clients, "backup", "child", "run-1")
	if !strings.Contains(logs, "root cause from init") {
		t.Fatalf("partial logs = %q", logs)
	}
	if err == nil || !strings.Contains(err.Error(), "home-backup") || !strings.Contains(err.Error(), "never started") {
		t.Fatalf("log read error = %v", err)
	}
}

func TestJobPodLogsIfOwnedCapsAggregateOutputAndReportsTruncation(t *testing.T) {
	clients := ownedJobPodLogClients(t, func(_ context.Context, _, _, _ string, _ corev1.PodLogOptions, maxBytes int64) (string, bool, error) {
		return strings.Repeat("x", int(maxBytes)+1), true, nil
	})

	logs, err := JobPodLogsIfOwned(context.Background(), clients, "backup", "child", "run-1")
	if len(logs) > ChildLogAggregateLimitBytes {
		t.Fatalf("aggregate logs length = %d, cap = %d", len(logs), ChildLogAggregateLimitBytes)
	}
	if err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("truncation error = %v", err)
	}
}

func TestJobPodLogsIfOwnedTimesOutStalledLogRequest(t *testing.T) {
	clients := ownedJobPodLogClients(t, func(ctx context.Context, _, _, _ string, _ corev1.PodLogOptions, _ int64) (string, bool, error) {
		<-ctx.Done()
		return "partial before timeout", false, ctx.Err()
	})
	started := time.Now()
	logs, err := jobPodLogsIfOwned(context.Background(), clients, "backup", "child", "run-1", 20*time.Millisecond, ChildLogAggregateLimitBytes, ChildLogContainerLimitBytes)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stalled log request took %s", elapsed)
	}
	if !strings.Contains(logs, "partial before timeout") || err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("logs = %q, error = %v", logs, err)
	}
}

func TestJobPodLogsIfOwnedHonorsCallerCancellation(t *testing.T) {
	clients := ownedJobPodLogClients(t, func(ctx context.Context, _, _, _ string, _ corev1.PodLogOptions, _ int64) (string, bool, error) {
		if ctx.Err() == nil {
			return "", false, errors.New("caller cancellation was not propagated")
		}
		return "", false, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := jobPodLogsIfOwned(ctx, clients, "backup", "child", "run-1", ChildLogCollectionTimeout, ChildLogAggregateLimitBytes, ChildLogContainerLimitBytes)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("jobPodLogsIfOwned() error = %v, want caller cancellation", err)
	}
}

func TestJobPodLogsIfOwnedReportsMidCollectionCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reads := 0
	clients := ownedJobPodLogClients(t, func(context.Context, string, string, string, corev1.PodLogOptions, int64) (string, bool, error) {
		reads++
		cancel()
		return "first container output", false, nil
	})

	logs, err := jobPodLogsIfOwned(ctx, clients, "backup", "child", "run-1", ChildLogCollectionTimeout, ChildLogAggregateLimitBytes, ChildLogContainerLimitBytes)
	if !strings.Contains(logs, "first container output") || !errors.Is(err, context.Canceled) {
		t.Fatalf("logs = %q, error = %v, want partial logs plus cancellation", logs, err)
	}
	if reads != 1 {
		t.Fatalf("container log reads = %d, want 1 after cancellation", reads)
	}
}

func ownedJobPodLogClients(t *testing.T, reader PodLogReader) *Clients {
	t.Helper()
	controller := true
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "child", Namespace: "backup", UID: "job-uid",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")},
	}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "child-abc", Namespace: "backup", UID: "pod-uid",
			Labels: map[string]string{batchv1.JobNameLabel: "child", ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1", Kind: "Job", Name: "child", UID: "job-uid", Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:  corev1.RestartPolicyNever,
			InitContainers: []corev1.Container{{Name: "prepare"}},
			Containers:     []corev1.Container{{Name: "home-backup"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}
	return &Clients{Core: kubernetesfake.NewSimpleClientset(job, pod), PodLogs: reader}
}

func TestDeleteSecretIfOwned(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "child-config", Namespace: "backup", UID: "secret-uid",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")},
	}}
	coreClient := kubernetesfake.NewSimpleClientset(secret)
	coreClient.PrependReactor("delete", "secrets", assertDeleteUIDPrecondition(t, "secret-uid"))
	clients := &Clients{Core: coreClient}

	if err := DeleteSecretIfOwned(context.Background(), clients, "backup", "child-config", "run-1", RunnerScope("backup")); err != nil {
		t.Fatalf("DeleteSecretIfOwned() error = %v", err)
	}
	if _, err := clients.Core.CoreV1().Secrets("backup").Get(context.Background(), secret.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted Secret Get() error = %v", err)
	}
}

func TestDeletePVCIfOwnedRejectsMismatchedRun(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "restored", Namespace: "backup",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "another-run"},
	}}
	clients := &Clients{Core: kubernetesfake.NewSimpleClientset(pvc)}

	err := DeletePVCIfOwned(context.Background(), clients, "backup", "restored", "run-1", RunnerScope("backup"))
	if err == nil || !strings.Contains(err.Error(), "not owned by run") {
		t.Fatalf("DeletePVCIfOwned() error = %v", err)
	}
}

func TestOwnedStorageDeletesUseObservedUIDPreconditions(t *testing.T) {
	labels := map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")}
	t.Run("PVC", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "restored", Namespace: "backup", UID: "pvc-uid", Labels: labels}}
		coreClient := kubernetesfake.NewSimpleClientset(pvc)
		coreClient.PrependReactor("delete", "persistentvolumeclaims", assertDeleteUIDPrecondition(t, "pvc-uid"))
		if err := DeletePVCIfOwned(context.Background(), &Clients{Core: coreClient}, "backup", "restored", "run-1", RunnerScope("backup")); err != nil {
			t.Fatalf("DeletePVCIfOwned() error = %v", err)
		}
	})

	for _, test := range []struct {
		name      string
		gvr       schema.GroupVersionResource
		kind      string
		namespace string
		delete    func(context.Context, *Clients) error
	}{
		{
			name: "VolumeSnapshot", gvr: VolumeSnapshotGVR, kind: VolumeSnapshotKind, namespace: "backup",
			delete: func(ctx context.Context, clients *Clients) error {
				return DeleteVolumeSnapshotIfOwned(ctx, clients, "backup", "snapshot", "run-1", RunnerScope("backup"))
			},
		},
		{
			name: "VolumeSnapshotContent", gvr: VolumeSnapshotContentGVR, kind: "VolumeSnapshotContent",
			delete: func(ctx context.Context, clients *Clients) error {
				return DeleteVolumeSnapshotContentIfOwned(ctx, clients, "snapshot-content", "run-1", RunnerScope("backup"), "backup")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			name := "snapshot"
			if test.namespace == "" {
				name = "snapshot-content"
			}
			object := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": SnapshotAPIGroup + "/v1", "kind": test.kind,
				"metadata": map[string]any{"name": name, "namespace": test.namespace, "uid": "storage-uid", "labels": map[string]any{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")}},
			}}
			if test.namespace == "" {
				object.Object["spec"] = map[string]any{"volumeSnapshotRef": map[string]any{"namespace": "backup"}}
			}
			dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
			dynamicClient.PrependReactor("delete", test.gvr.Resource, assertDeleteUIDPrecondition(t, "storage-uid"))
			if err := test.delete(context.Background(), &Clients{Dynamic: dynamicClient}); err != nil {
				t.Fatalf("delete error = %v", err)
			}
		})
	}
}

func TestUIDPreconditionPreservesSameNamePVCReplacement(t *testing.T) {
	labels := map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup")}
	oldPVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "restored", Namespace: "backup", UID: "old-uid", Labels: labels}}
	replacement := oldPVC.DeepCopy()
	replacement.UID = "new-uid"
	coreClient := kubernetesfake.NewSimpleClientset(oldPVC)
	coreClient.PrependReactor("delete", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if err := coreClient.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), replacement, "backup"); err != nil {
			t.Fatalf("replace PVC in tracker: %v", err)
		}
		preconditions := action.(clienttesting.DeleteAction).GetDeleteOptions().Preconditions
		if preconditions != nil && preconditions.UID != nil && *preconditions.UID == oldPVC.UID {
			return true, nil, apierrors.NewConflict(corev1.Resource("persistentvolumeclaims"), oldPVC.Name, errors.New("UID precondition failed"))
		}
		return false, nil, nil
	})

	err := DeletePVCIfOwned(context.Background(), &Clients{Core: coreClient}, "backup", oldPVC.Name, "run-1", RunnerScope("backup"))
	if !apierrors.IsConflict(err) {
		t.Fatalf("DeletePVCIfOwned() error = %v", err)
	}
	got, getErr := coreClient.CoreV1().PersistentVolumeClaims("backup").Get(context.Background(), replacement.Name, metav1.GetOptions{})
	if getErr != nil || got.UID != replacement.UID {
		t.Fatalf("replacement PVC = %#v, error = %v", got, getErr)
	}
}

func TestDeleteHelpersRejectCrossRunnerResourcesWithSameRunID(t *testing.T) {
	ctx := context.Background()
	foreignScope := RunnerScope("backup-b")
	expectedScope := RunnerScope("backup-a")
	labels := map[string]string{
		ManagedByLabel: ManagedByLabelValue, RunLabel: "shared-run", RunnerScopeLabel: foreignScope,
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "shared-job", Namespace: "backup-a", UID: "job-uid", Labels: labels}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "shared-secret", Namespace: "backup-a", UID: "secret-uid", Labels: labels}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "shared-pvc", Namespace: "backup-a", UID: "pvc-uid", Labels: labels}}
	coreClient := kubernetesfake.NewSimpleClientset(job, secret, pvc)
	dynamicLabels := map[string]any{
		ManagedByLabel: ManagedByLabelValue, RunLabel: "shared-run", RunnerScopeLabel: foreignScope,
	}
	snapshot := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": VolumeSnapshotKind,
		"metadata": map[string]any{"name": "shared-snapshot", "namespace": "backup-a", "uid": "snapshot-uid", "labels": dynamicLabels},
	}}
	content := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": "VolumeSnapshotContent",
		"metadata": map[string]any{"name": "shared-content", "uid": "content-uid", "labels": dynamicLabels},
		"spec":     map[string]any{"volumeSnapshotRef": map[string]any{"name": "shared-alias", "namespace": "backup-b"}},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), snapshot, content)
	clients := &Clients{Core: coreClient, Dynamic: dynamicClient}

	for name, deleteResource := range map[string]func() error{
		"Job": func() error {
			return DeleteJobIfOwned(ctx, clients, "backup-a", job.Name, "shared-run", expectedScope)
		},
		"Secret": func() error {
			return DeleteSecretIfOwned(ctx, clients, "backup-a", secret.Name, "shared-run", expectedScope)
		},
		"PVC": func() error {
			return DeletePVCIfOwned(ctx, clients, "backup-a", pvc.Name, "shared-run", expectedScope)
		},
		"VolumeSnapshot": func() error {
			return DeleteVolumeSnapshotIfOwned(ctx, clients, "backup-a", snapshot.GetName(), "shared-run", expectedScope)
		},
		"VolumeSnapshotContent": func() error {
			return DeleteVolumeSnapshotContentIfOwned(ctx, clients, content.GetName(), "shared-run", expectedScope, "backup-a")
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := deleteResource()
			if err == nil || !strings.Contains(err.Error(), "runner scope") {
				t.Fatalf("delete error = %v, want runner-scope refusal", err)
			}
		})
	}
	for _, action := range append(coreClient.Actions(), dynamicClient.Actions()...) {
		if action.GetVerb() == "delete" {
			t.Fatalf("foreign resource was deleted: %s/%s", action.GetResource().Resource, action.GetNamespace())
		}
	}
}

func TestAliasContentAlreadyExistsCollisionNeverDeletesForeignRunnerContent(t *testing.T) {
	ctx := context.Background()
	foreign := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": "VolumeSnapshotContent",
		"metadata": map[string]any{
			"name": "same-base-alias-content", "uid": "foreign-uid",
			"labels": map[string]any{
				ManagedByLabel: ManagedByLabelValue, RunLabel: "same-base", RunnerScopeLabel: RunnerScope("runner-b"),
			},
		},
		"spec": map[string]any{"volumeSnapshotRef": map[string]any{"name": "same-base-alias-snap", "namespace": "runner-b"}},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), foreign)
	clients := &Clients{Dynamic: dynamicClient}
	own := foreign.DeepCopy()
	own.SetUID("")
	own.SetLabels(map[string]string{
		ManagedByLabel: ManagedByLabelValue, RunLabel: "same-base", RunnerScopeLabel: RunnerScope("runner-a"),
	})
	if err := unstructured.SetNestedField(own.Object, "runner-a", "spec", "volumeSnapshotRef", "namespace"); err != nil {
		t.Fatalf("set desired alias namespace: %v", err)
	}
	if err := CreateVolumeSnapshotContent(ctx, clients, own); !apierrors.IsAlreadyExists(err) {
		t.Fatalf("CreateVolumeSnapshotContent() error = %v, want AlreadyExists", err)
	}
	if err := DeleteVolumeSnapshotContentIfOwned(ctx, clients, foreign.GetName(), "same-base", RunnerScope("runner-a"), "runner-a"); err == nil {
		t.Fatal("collision cleanup error = nil")
	}
	got, err := dynamicClient.Resource(VolumeSnapshotContentGVR).Get(ctx, foreign.GetName(), metav1.GetOptions{})
	if err != nil || got.GetUID() != foreign.GetUID() || got.GetLabels()[RunnerScopeLabel] != RunnerScope("runner-b") {
		t.Fatalf("foreign collision object = %#v, error = %v", got, err)
	}
}

func TestDeleteAliasContentRejectsMisdirectedNamespaceInFinalGet(t *testing.T) {
	content := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": SnapshotAPIGroup + "/v1", "kind": "VolumeSnapshotContent",
		"metadata": map[string]any{
			"name": "misdirected", "uid": "content-uid",
			"labels": map[string]any{
				ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: RunnerScope("backup-a"),
			},
		},
		"spec": map[string]any{"volumeSnapshotRef": map[string]any{"namespace": "backup-b"}},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), content)
	err := DeleteVolumeSnapshotContentIfOwned(context.Background(), &Clients{Dynamic: dynamicClient}, content.GetName(), "run-1", RunnerScope("backup-a"), "backup-a")
	if err == nil || !strings.Contains(err.Error(), "volumeSnapshotRef.namespace") {
		t.Fatalf("DeleteVolumeSnapshotContentIfOwned() error = %v", err)
	}
	if _, getErr := dynamicClient.Resource(VolumeSnapshotContentGVR).Get(context.Background(), content.GetName(), metav1.GetOptions{}); getErr != nil {
		t.Fatalf("misdirected content did not survive: %v", getErr)
	}
}

func assertDeleteUIDPrecondition(t *testing.T, want types.UID) clienttesting.ReactionFunc {
	t.Helper()
	return func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		preconditions := deleteAction.GetDeleteOptions().Preconditions
		if preconditions == nil || preconditions.UID == nil || *preconditions.UID != want {
			t.Fatalf("delete preconditions = %#v, want UID %q", preconditions, want)
		}
		return false, nil, nil
	}
}
