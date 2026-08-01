package kube

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestLoadRESTConfigFallsBackOnlyWhenNotInCluster(t *testing.T) {
	localCalled := false
	want := &rest.Config{Host: "https://local.example"}
	got, err := loadRESTConfig(
		func() (*rest.Config, error) { return nil, fmt.Errorf("wrapped: %w", rest.ErrNotInCluster) },
		func() (*rest.Config, error) {
			localCalled = true
			return want, nil
		},
	)
	if err != nil {
		t.Fatalf("loadRESTConfig() error = %v", err)
	}
	if !localCalled || got != want {
		t.Fatalf("local fallback called=%v config=%#v", localCalled, got)
	}
}

func TestLoadRESTConfigPreservesInClusterFailure(t *testing.T) {
	localCalled := false
	_, err := loadRESTConfig(
		func() (*rest.Config, error) { return nil, errors.New("service account CA is unreadable") },
		func() (*rest.Config, error) {
			localCalled = true
			return &rest.Config{}, nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "service account CA is unreadable") {
		t.Fatalf("loadRESTConfig() error = %v", err)
	}
	if localCalled {
		t.Fatal("local kubeconfig fallback was called for an in-cluster configuration failure")
	}
}

func TestResolveCronJobRejectsStaleOwnerUIDAtEachHop(t *testing.T) {
	controller := true
	for _, test := range []struct {
		name       string
		podJobUID  string
		jobUID     string
		jobCronUID string
		cronUID    string
	}{
		{name: "recreated Job", podJobUID: "old-job", jobUID: "new-job", jobCronUID: "cron-uid", cronUID: "cron-uid"},
		{name: "recreated CronJob", podJobUID: "job-uid", jobUID: "job-uid", jobCronUID: "old-cron", cronUID: "new-cron"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(PodNameOverrideEnv, "runner-pod")
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "runner-pod", Namespace: "backup",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: "runner-job", UID: types.UID(test.podJobUID), Controller: &controller,
				}},
			}}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "runner-job", Namespace: "backup", UID: types.UID(test.jobUID),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "CronJob", Name: "runner-cron", UID: types.UID(test.jobCronUID), Controller: &controller,
				}},
			}}
			cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "runner-cron", Namespace: "backup", UID: types.UID(test.cronUID)}}
			clients := &Clients{Core: kubernetesfake.NewSimpleClientset(pod, job, cronJob)}

			_, err := ResolveCronJob(context.Background(), clients, "backup")
			if err == nil || !strings.Contains(err.Error(), "UID") {
				t.Fatalf("ResolveCronJob() error = %v", err)
			}
		})
	}
}

func TestResolveCronJobRejectsWrongOwnerAPIVersionAtEachHop(t *testing.T) {
	controller := true
	for _, test := range []struct {
		name       string
		jobAPI     string
		cronJobAPI string
	}{
		{name: "wrong Job API", jobAPI: "example.com/v1", cronJobAPI: "batch/v1"},
		{name: "wrong CronJob API", jobAPI: "batch/v1", cronJobAPI: "example.com/v1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(PodNameOverrideEnv, "runner-pod")
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "runner-pod", Namespace: "backup",
				OwnerReferences: []metav1.OwnerReference{{APIVersion: test.jobAPI, Kind: "Job", Name: "runner-job", UID: "job-uid", Controller: &controller}},
			}}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "runner-job", Namespace: "backup", UID: "job-uid",
				OwnerReferences: []metav1.OwnerReference{{APIVersion: test.cronJobAPI, Kind: "CronJob", Name: "runner-cron", UID: "cron-uid", Controller: &controller}},
			}}
			cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "runner-cron", Namespace: "backup", UID: "cron-uid"}}
			clients := &Clients{Core: kubernetesfake.NewSimpleClientset(pod, job, cronJob)}

			_, err := ResolveCronJob(context.Background(), clients, "backup")
			if err == nil || !strings.Contains(err.Error(), "batch/v1") {
				t.Fatalf("ResolveCronJob() error = %v", err)
			}
		})
	}
}
