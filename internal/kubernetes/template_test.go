package kube

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
)

func TestResolveCronJobUsesConfiguredName(t *testing.T) {
	t.Setenv(CronJobNameEnv, "home-backup")
	cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "backup"}}
	clients := &Clients{Core: kubernetesfake.NewSimpleClientset(cronJob)}

	got, err := ResolveCronJob(context.Background(), clients, "backup")
	if err != nil {
		t.Fatalf("ResolveCronJob() error = %v", err)
	}
	if got.Name != cronJob.Name || got.Namespace != cronJob.Namespace {
		t.Fatalf("ResolveCronJob() = %s/%s", got.Namespace, got.Name)
	}
}

func TestResolveCronJobDetectsOwnerOfCurrentPodJob(t *testing.T) {
	t.Setenv(CronJobNameEnv, "")
	t.Setenv(PodNameOverrideEnv, "home-backup-123-abc")
	controller := true
	cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "backup"}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      "home-backup-123",
		Namespace: "backup",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "batch/v1", Kind: "CronJob", Name: cronJob.Name, Controller: &controller,
		}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "home-backup-123-abc",
		Namespace: "backup",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "batch/v1", Kind: "Job", Name: job.Name, Controller: &controller,
		}},
	}}
	clients := &Clients{Core: kubernetesfake.NewSimpleClientset(cronJob, job, pod)}

	got, err := ResolveCronJob(context.Background(), clients, "backup")
	if err != nil {
		t.Fatalf("ResolveCronJob() error = %v", err)
	}
	if got.Name != cronJob.Name || got.Namespace != cronJob.Namespace {
		t.Fatalf("ResolveCronJob() = %s/%s", got.Namespace, got.Name)
	}
}
