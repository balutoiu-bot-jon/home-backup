package kube

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestValidateChildJobRejectsAPIServerThatDropsFailedReplacementPolicy(t *testing.T) {
	policy := batchv1.Failed
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "backup"}, Spec: batchv1.JobSpec{PodReplacementPolicy: &policy}}
	coreClient := kubernetesfake.NewSimpleClientset()
	createCalls := 0
	coreClient.PrependReactor("create", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		createCalls++
		options := action.(interface{ GetCreateOptions() metav1.CreateOptions }).GetCreateOptions()
		if len(options.DryRun) != 1 || options.DryRun[0] != metav1.DryRunAll {
			t.Fatalf("compatibility probe DryRun = %#v, want All", options.DryRun)
		}
		dropped := job.DeepCopy()
		dropped.Spec.PodReplacementPolicy = nil
		return true, dropped, nil
	})

	err := (&LonghornCluster{clients: &Clients{Core: coreClient}}).ValidateChildJob(context.Background(), job)
	if err == nil || !strings.Contains(err.Error(), "podReplacementPolicy") || !strings.Contains(err.Error(), "Kubernetes 1.34") {
		t.Fatalf("ValidateChildJob() error = %v, want enforceable compatibility error", err)
	}
	if createCalls != 1 {
		t.Fatalf("create calls = %d, want only the server-side compatibility probe", createCalls)
	}
}

func TestValidateThenCreateChildJobUsesDryRunOnlyForProbe(t *testing.T) {
	policy := batchv1.Failed
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "backup"}, Spec: batchv1.JobSpec{PodReplacementPolicy: &policy}}
	coreClient := kubernetesfake.NewSimpleClientset()
	createCalls := 0
	coreClient.PrependReactor("create", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		createCalls++
		options := action.(interface{ GetCreateOptions() metav1.CreateOptions }).GetCreateOptions()
		if createCalls == 1 {
			if len(options.DryRun) != 1 || options.DryRun[0] != metav1.DryRunAll {
				t.Fatalf("probe DryRun = %#v, want All", options.DryRun)
			}
			return true, job.DeepCopy(), nil
		}
		if len(options.DryRun) != 0 {
			t.Fatalf("persisted create unexpectedly used DryRun: %#v", options.DryRun)
		}
		return false, nil, nil
	})

	cluster := &LonghornCluster{clients: &Clients{Core: coreClient}}
	if err := cluster.ValidateChildJob(context.Background(), job); err != nil {
		t.Fatalf("ValidateChildJob() error = %v", err)
	}
	if err := cluster.CreateChildJob(context.Background(), job); err != nil {
		t.Fatalf("CreateChildJob() error = %v", err)
	}
	if createCalls != 2 {
		t.Fatalf("create calls = %d, want dry-run probe plus persisted create", createCalls)
	}
}

func TestCreateChildJobDeletesPersistedJobIfActualResponseDropsReplacementPolicy(t *testing.T) {
	policy := batchv1.Failed
	runnerScope := RunnerScope("backup")
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "child", Namespace: "backup",
		Labels: map[string]string{ManagedByLabel: ManagedByLabelValue, RunLabel: "run-1", RunnerScopeLabel: runnerScope},
	}, Spec: batchv1.JobSpec{PodReplacementPolicy: &policy}}
	coreClient := kubernetesfake.NewSimpleClientset()
	coreClient.PrependReactor("create", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) {
		dropped := job.DeepCopy()
		dropped.UID = "job-uid"
		dropped.Spec.PodReplacementPolicy = nil
		if err := coreClient.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), dropped, dropped.Namespace); err != nil {
			t.Fatalf("tracker Create() error = %v", err)
		}
		return true, dropped, nil
	})

	err := (&LonghornCluster{clients: &Clients{Core: coreClient}}).CreateChildJob(context.Background(), job)
	if err == nil || !strings.Contains(err.Error(), "persisted child Job lost podReplacementPolicy") {
		t.Fatalf("CreateChildJob() error = %v, want persisted compatibility error", err)
	}
	if _, getErr := coreClient.BatchV1().Jobs(job.Namespace).Get(context.Background(), job.Name, metav1.GetOptions{}); !apierrors.IsNotFound(getErr) {
		t.Fatalf("incompatible persisted Job Get() error = %v, want NotFound after foreground deletion", getErr)
	}
}
