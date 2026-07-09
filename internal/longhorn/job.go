// Package longhorn implements Kubernetes orchestration for Longhorn-backed PVC backups.
package longhorn

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ionutbalutoiu/home-backup/internal/backup"
	homekube "github.com/ionutbalutoiu/home-backup/internal/kubernetes"
	"gopkg.in/yaml.v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

const cleanupTimeout = 2 * time.Minute

// Config describes the source PVC and child Job settings.
type Config struct {
	PVCName       string
	Namespace     string
	SnapshotClass string
	StorageClass  string
	MountPath     string
	ContainerName string
	Timeout       time.Duration
}

// ResticDestination is serialized into the child home-backup configuration.
type ResticDestination struct {
	Repo     string
	KeepLast int
	GroupBy  string
}

// Cluster is the semantic Kubernetes boundary used by Job.
type Cluster interface {
	ResolveCronJob(context.Context, string) (*batchv1.CronJob, error)
	GetPVC(context.Context, string, string) (*corev1.PersistentVolumeClaim, error)
	CreateSnapshot(context.Context, homekube.SnapshotSpec) error
	WaitSnapshotReady(context.Context, string, string, time.Duration) error
	CreateSnapshotAliasContent(context.Context, homekube.SnapshotAliasSpec) error
	CreateSnapshotAlias(context.Context, homekube.SnapshotAliasSpec) error
	CreateRestoredPVC(context.Context, homekube.RestorePVCOptions) error
	CreateChildJob(context.Context, *batchv1.Job) error
	WaitJobFinished(context.Context, string, string, time.Duration) (bool, error)
	DeleteJob(context.Context, string, string, string) error
	DeletePVC(context.Context, string, string, string) error
	DeleteSnapshot(context.Context, string, string, string) error
	DeleteSnapshotContent(context.Context, string, string) error
}

// Job snapshots a PVC, restores it beside the orchestrator, and starts a copy of the orchestrator CronJob.
type Job struct {
	config          Config
	destination     ResticDestination
	cluster         Cluster
	runnerNamespace string
	resourceName    func(string) (string, error)
	cleanupTimeout  time.Duration
}

// NewJob constructs a Longhorn PVC backup job.
func NewJob(config Config, destination ResticDestination, cluster Cluster, runnerNamespace string) (*Job, error) {
	if cluster == nil {
		return nil, errors.New("Longhorn cluster is required")
	}
	if runnerNamespace == "" {
		return nil, errors.New("runner namespace is required")
	}
	if config.PVCName == "" || config.SnapshotClass == "" || config.MountPath == "" || config.ContainerName == "" || config.Timeout <= 0 {
		return nil, errors.New("PVC name, snapshot class, mount path, container name, and positive timeout are required")
	}
	if destination.Repo == "" {
		return nil, errors.New("Restic repository is required")
	}
	return &Job{
		config:          config,
		destination:     destination,
		cluster:         cluster,
		runnerNamespace: runnerNamespace,
		resourceName:    temporaryResourceName,
		cleanupTimeout:  cleanupTimeout,
	}, nil
}

// Run executes one complete snapshot, restore, child backup, and cleanup lifecycle.
func (j *Job) Run(ctx context.Context) (retErr error) {
	sourceNamespace := j.config.Namespace
	if sourceNamespace == "" {
		sourceNamespace = j.runnerNamespace
	}

	cronJob, err := j.cluster.ResolveCronJob(ctx, j.runnerNamespace)
	if err != nil {
		return fmt.Errorf("resolve parent CronJob: %w", err)
	}
	sourcePVC, err := j.cluster.GetPVC(ctx, sourceNamespace, j.config.PVCName)
	if err != nil {
		return fmt.Errorf("get source PVC %s/%s: %w", sourceNamespace, j.config.PVCName, err)
	}
	baseName, err := j.resourceName(j.config.PVCName)
	if err != nil {
		return err
	}
	sourceSnapshotName := resourceNameWithSuffix(baseName, "source-snap")
	aliasContentName := resourceNameWithSuffix(baseName, "alias-content")
	aliasSnapshotName := resourceNameWithSuffix(baseName, "alias-snap")
	tempPVCName := resourceNameWithSuffix(baseName, "pvc")
	childJobName := resourceNameWithSuffix(baseName, "job")

	restoreSnapshotName := sourceSnapshotName
	if sourceNamespace != j.runnerNamespace {
		restoreSnapshotName = aliasSnapshotName
	}
	childConfig, err := buildChildConfigBase64(j.config.MountPath, j.destination)
	if err != nil {
		return err
	}
	restorePVCOptions := homekube.RestorePVCOptions{
		Name: tempPVCName, Namespace: j.runnerNamespace, SourcePVC: sourcePVC,
		SnapshotName: restoreSnapshotName, StorageClassOverride: j.config.StorageClass, RunID: baseName,
	}
	if _, err := homekube.BuildRestorePVC(
		restorePVCOptions.Name, restorePVCOptions.Namespace, restorePVCOptions.SourcePVC,
		restorePVCOptions.SnapshotName, restorePVCOptions.StorageClassOverride, restorePVCOptions.RunID,
	); err != nil {
		return fmt.Errorf("validate restored PVC: %w", err)
	}
	childJobOptions := homekube.ChildJobOptions{
		Name: childJobName, RunID: baseName, CronJob: cronJob, ContainerName: j.config.ContainerName,
		TempPVCName: tempPVCName, MountPath: j.config.MountPath, ChildConfigBase64: childConfig,
	}
	childJob, err := homekube.BuildChildJob(childJobOptions)
	if err != nil {
		return fmt.Errorf("validate child Job: %w", err)
	}

	boundedCleanup := j.cleanupTimeout
	if boundedCleanup <= 0 {
		boundedCleanup = cleanupTimeout
	}
	var cleanup []cleanupFunc
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), boundedCleanup)
		defer cancel()
		retErr = errors.Join(retErr, runCleanup(cleanupCtx, cleanup))
	}()

	cleanup = append(cleanup, func(ctx context.Context) error {
		return j.cluster.DeleteSnapshot(ctx, sourceNamespace, sourceSnapshotName, baseName)
	})
	if err := j.cluster.CreateSnapshot(ctx, homekube.SnapshotSpec{
		Name: sourceSnapshotName, Namespace: sourceNamespace,
		PVCName: j.config.PVCName, SnapshotClass: j.config.SnapshotClass, RunID: baseName,
	}); err != nil {
		return fmt.Errorf("create source VolumeSnapshot %s/%s: %w", sourceNamespace, sourceSnapshotName, err)
	}
	if err := j.cluster.WaitSnapshotReady(ctx, sourceNamespace, sourceSnapshotName, j.config.Timeout); err != nil {
		return fmt.Errorf("wait for source VolumeSnapshot %s/%s: %w", sourceNamespace, sourceSnapshotName, err)
	}

	if sourceNamespace != j.runnerNamespace {
		aliasSpec := homekube.SnapshotAliasSpec{
			SourceNamespace: sourceNamespace, SourceSnapshotName: sourceSnapshotName,
			TargetNamespace: j.runnerNamespace, TargetSnapshotName: aliasSnapshotName,
			AliasContentName: aliasContentName, RunID: baseName,
		}
		cleanup = append(cleanup, func(ctx context.Context) error {
			return j.cluster.DeleteSnapshotContent(ctx, aliasContentName, baseName)
		})
		if err := j.cluster.CreateSnapshotAliasContent(ctx, aliasSpec); err != nil {
			return fmt.Errorf("create VolumeSnapshotContent alias %s: %w", aliasContentName, err)
		}
		cleanup = append(cleanup, func(ctx context.Context) error {
			return j.cluster.DeleteSnapshot(ctx, j.runnerNamespace, aliasSnapshotName, baseName)
		})
		if err := j.cluster.CreateSnapshotAlias(ctx, aliasSpec); err != nil {
			return fmt.Errorf("create target VolumeSnapshot alias %s/%s: %w", j.runnerNamespace, aliasSnapshotName, err)
		}
		if err := j.cluster.WaitSnapshotReady(ctx, j.runnerNamespace, aliasSnapshotName, j.config.Timeout); err != nil {
			return fmt.Errorf("wait for target VolumeSnapshot alias %s/%s: %w", j.runnerNamespace, aliasSnapshotName, err)
		}
	}

	cleanup = append(cleanup, func(ctx context.Context) error {
		return j.cluster.DeletePVC(ctx, j.runnerNamespace, tempPVCName, baseName)
	})
	if err := j.cluster.CreateRestoredPVC(ctx, restorePVCOptions); err != nil {
		return fmt.Errorf("create temporary PVC %s/%s: %w", j.runnerNamespace, tempPVCName, err)
	}
	jobTerminal := false
	cleanup = append(cleanup, func(ctx context.Context) error {
		if jobTerminal {
			return nil
		}
		return j.cluster.DeleteJob(ctx, j.runnerNamespace, childJobName, baseName)
	})
	if err := j.cluster.CreateChildJob(ctx, childJob); err != nil {
		return fmt.Errorf("create child backup Job %s/%s: %w", j.runnerNamespace, childJobName, err)
	}
	jobTerminal, err = j.cluster.WaitJobFinished(ctx, j.runnerNamespace, childJobName, j.config.Timeout)
	if err != nil {
		return fmt.Errorf("wait for child backup Job %s/%s: %w", j.runnerNamespace, childJobName, err)
	}
	return nil
}

type cleanupFunc func(context.Context) error

func runCleanup(ctx context.Context, cleanup []cleanupFunc) error {
	for i := len(cleanup) - 1; i >= 0; i-- {
		if err := cleanup[i](ctx); err != nil {
			return fmt.Errorf("cleanup stopped to preserve dependent resources: %w", err)
		}
	}
	return nil
}

type childConfig struct {
	Backups []childBackup `yaml:"backups"`
}
type childBackup struct {
	Source      childDirectorySource   `yaml:"source"`
	Destination childResticDestination `yaml:"destination"`
}
type childDirectorySource struct {
	Type string `yaml:"type"`
	Path string `yaml:"path"`
}
type childResticDestination struct {
	Type     string `yaml:"type"`
	Repo     string `yaml:"repo"`
	KeepLast int    `yaml:"keep_last"`
	GroupBy  string `yaml:"group_by"`
}

func buildChildConfigBase64(mountPath string, destination ResticDestination) (string, error) {
	cfg := childConfig{Backups: []childBackup{{
		Source: childDirectorySource{Type: "directory", Path: mountPath},
		Destination: childResticDestination{
			Type: "restic", Repo: destination.Repo, KeepLast: destination.KeepLast, GroupBy: destination.GroupBy,
		},
	}}}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal child backup config: %w", err)
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

var invalidDNS1123Chars = regexp.MustCompile(`[^a-z0-9-]+`)

func temporaryResourceName(pvcName string) (string, error) {
	suffix := make([]byte, 12)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("generate random resource suffix: %w", err)
	}
	safePVCName := strings.Trim(invalidDNS1123Chars.ReplaceAllString(strings.ToLower(pvcName), "-"), "-")
	if safePVCName == "" {
		safePVCName = "pvc"
	}
	encodedSuffix := hex.EncodeToString(suffix)
	const maxBaseLength = 63 - 1 - len("alias-content")
	maxPrefixLength := maxBaseLength - 1 - len(encodedSuffix)
	prefix := "home-backup-" + safePVCName
	if len(prefix) > maxPrefixLength {
		prefix = strings.TrimRight(prefix[:maxPrefixLength], "-")
	}
	return fmt.Sprintf("%s-%s", prefix, encodedSuffix), nil
}

func resourceNameWithSuffix(base, suffix string) string {
	maxBaseLength := 63 - len(suffix) - 1
	if len(base) > maxBaseLength {
		base = strings.TrimRight(base[:maxBaseLength], "-")
	}
	return base + "-" + suffix
}

var _ backup.Job = (*Job)(nil)
