package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SnapshotSpec struct {
	Name          string
	Namespace     string
	PVCName       string
	SnapshotClass string
	RunID         string
	RunnerScope   string
}

type SnapshotAliasSpec struct {
	SourceNamespace    string
	SourceSnapshotName string
	TargetNamespace    string
	TargetSnapshotName string
	AliasContentName   string
	RunID              string
	RunnerScope        string
}

type RestorePVCOptions struct {
	Name                 string
	Namespace            string
	SourcePVC            *corev1.PersistentVolumeClaim
	SnapshotName         string
	StorageClassOverride string
	RunID                string
	RunnerScope          string
}

type LonghornCluster struct {
	clients *Clients
}

func NewLonghornCluster() (*LonghornCluster, error) {
	clients, err := NewClients()
	if err != nil {
		return nil, err
	}
	return &LonghornCluster{clients: clients}, nil
}

func (c *LonghornCluster) ReconcileStaleRuns(ctx context.Context, runnerNamespace string, sourceNamespaces []string, staleAfter time.Duration) error {
	return ReconcileStaleRuns(ctx, c.clients, runnerNamespace, sourceNamespaces, staleAfter)
}

func (c *LonghornCluster) ResolveCronJob(ctx context.Context, namespace string) (*batchv1.CronJob, error) {
	return ResolveCronJob(ctx, c.clients, namespace)
}

func (c *LonghornCluster) GetPVC(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	return c.clients.Core.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (c *LonghornCluster) CreateSnapshot(ctx context.Context, spec SnapshotSpec) error {
	return CreateVolumeSnapshot(ctx, c.clients, BuildVolumeSnapshot(spec.Name, spec.Namespace, spec.PVCName, spec.SnapshotClass, spec.RunID, spec.RunnerScope))
}

func (c *LonghornCluster) WaitSnapshotReady(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return WaitVolumeSnapshotReady(ctx, c.clients, namespace, name, timeout)
}

func (c *LonghornCluster) CreateSnapshotAliasContent(ctx context.Context, spec SnapshotAliasSpec) error {
	sourceContent, err := GetBoundVolumeSnapshotContent(ctx, c.clients, spec.SourceNamespace, spec.SourceSnapshotName)
	if err != nil {
		return err
	}
	aliasContent, err := BuildVolumeSnapshotAliasContent(spec.AliasContentName, spec.TargetSnapshotName, spec.TargetNamespace, sourceContent, spec.RunID, spec.RunnerScope)
	if err != nil {
		return err
	}
	return CreateVolumeSnapshotContent(ctx, c.clients, aliasContent)
}

func (c *LonghornCluster) CreateSnapshotAlias(ctx context.Context, spec SnapshotAliasSpec) error {
	return CreateVolumeSnapshot(ctx, c.clients, BuildPreprovisionedVolumeSnapshot(spec.TargetSnapshotName, spec.TargetNamespace, spec.AliasContentName, spec.RunID, spec.RunnerScope))
}

func (c *LonghornCluster) CreateRestoredPVC(ctx context.Context, opts RestorePVCOptions) error {
	pvc, err := BuildRestorePVC(opts.Name, opts.Namespace, opts.SourcePVC, opts.SnapshotName, opts.StorageClassOverride, opts.RunID, opts.RunnerScope)
	if err != nil {
		return err
	}
	_, err = c.clients.Core.CoreV1().PersistentVolumeClaims(opts.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
	return err
}

func (c *LonghornCluster) CreateChildConfigSecret(ctx context.Context, secret *corev1.Secret) error {
	_, err := c.clients.Core.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	return err
}

func (c *LonghornCluster) ValidateChildJob(ctx context.Context, job *batchv1.Job) error {
	probe, err := c.clients.Core.BatchV1().Jobs(job.Namespace).Create(ctx, job.DeepCopy(), metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		return fmt.Errorf("dry-run child Job compatibility probe: %w", err)
	}
	if probe.Spec.PodReplacementPolicy == nil || *probe.Spec.PodReplacementPolicy != batchv1.Failed {
		return errors.New("Kubernetes API did not preserve podReplacementPolicy=Failed; Longhorn child Jobs require Kubernetes 1.34 or a cluster with the JobPodReplacementPolicy feature enabled")
	}
	return nil
}

func (c *LonghornCluster) CreateChildJob(ctx context.Context, job *batchv1.Job) error {
	created, err := c.clients.Core.BatchV1().Jobs(job.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	if created.Spec.PodReplacementPolicy == nil || *created.Spec.PodReplacementPolicy != batchv1.Failed {
		compatErr := errors.New("persisted child Job lost podReplacementPolicy=Failed; Kubernetes 1.34 or JobPodReplacementPolicy support is required")
		deleteErr := DeleteJobIfOwned(ctx, c.clients, created.Namespace, created.Name, created.Labels[RunLabel], created.Labels[RunnerScopeLabel])
		return errors.Join(compatErr, deleteErr)
	}
	return nil
}

func (c *LonghornCluster) WaitJobFinished(ctx context.Context, namespace, name string, timeout time.Duration) (bool, error) {
	return WaitJobFinished(ctx, c.clients, namespace, name, timeout)
}

func (c *LonghornCluster) JobPodLogs(ctx context.Context, namespace, name, runID string) (string, error) {
	return JobPodLogsIfOwned(ctx, c.clients, namespace, name, runID)
}

func (c *LonghornCluster) DeleteJob(ctx context.Context, namespace, name, runID, runnerScope string) error {
	return DeleteJobIfOwned(ctx, c.clients, namespace, name, runID, runnerScope)
}

func (c *LonghornCluster) DeleteSecret(ctx context.Context, namespace, name, runID, runnerScope string) error {
	return DeleteSecretIfOwned(ctx, c.clients, namespace, name, runID, runnerScope)
}

func (c *LonghornCluster) DeletePVC(ctx context.Context, namespace, name, runID, runnerScope string) error {
	return DeletePVCIfOwned(ctx, c.clients, namespace, name, runID, runnerScope)
}

func (c *LonghornCluster) DeleteSnapshot(ctx context.Context, namespace, name, runID, runnerScope string) error {
	return DeleteVolumeSnapshotIfOwned(ctx, c.clients, namespace, name, runID, runnerScope)
}

func (c *LonghornCluster) DeleteSnapshotContent(ctx context.Context, name, runID, runnerScope, targetNamespace string) error {
	return DeleteVolumeSnapshotContentIfOwned(ctx, c.clients, name, runID, runnerScope, targetNamespace)
}
