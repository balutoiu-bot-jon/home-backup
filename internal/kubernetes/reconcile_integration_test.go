package kube

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIntegrationReconcileStaleRuns(t *testing.T) {
	if os.Getenv("HOME_BACKUP_KUBERNETES_INTEGRATION") != "1" {
		t.Skip("set HOME_BACKUP_KUBERNETES_INTEGRATION=1 to run against a real staging cluster")
	}
	runnerNamespace := os.Getenv("HOME_BACKUP_INTEGRATION_RUNNER_NAMESPACE")
	if runnerNamespace == "" {
		t.Fatal("HOME_BACKUP_INTEGRATION_RUNNER_NAMESPACE is required")
	}
	sourceNamespaces := strings.Split(os.Getenv("HOME_BACKUP_INTEGRATION_SOURCE_NAMESPACES"), ",")
	if len(sourceNamespaces) == 0 || sourceNamespaces[0] == "" {
		t.Fatal("HOME_BACKUP_INTEGRATION_SOURCE_NAMESPACES is required")
	}
	staleAfter, err := time.ParseDuration(os.Getenv("HOME_BACKUP_INTEGRATION_STALE_AFTER"))
	if err != nil || staleAfter <= 0 {
		t.Fatalf("HOME_BACKUP_INTEGRATION_STALE_AFTER must be a positive duration: %v", err)
	}
	clients, err := NewClients()
	if err != nil {
		t.Fatalf("NewClients() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	if err := ReconcileStaleRuns(ctx, clients, runnerNamespace, sourceNamespaces, staleAfter); err != nil {
		t.Fatalf("ReconcileStaleRuns() error = %v", err)
	}
}
