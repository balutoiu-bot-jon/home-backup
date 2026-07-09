package kube

import (
	"errors"
	"fmt"
	"strings"
	"testing"

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
