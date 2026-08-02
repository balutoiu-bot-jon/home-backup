// Package longhorn implements Kubernetes orchestration for Longhorn-backed PVC backups.
package longhorn

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/ionutbalutoiu/home-backup/internal/backup"
	appconfig "github.com/ionutbalutoiu/home-backup/internal/config"
	homekube "github.com/ionutbalutoiu/home-backup/internal/kubernetes"
	"gopkg.in/yaml.v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	cleanupTimeout         = 2 * time.Minute
	childShutdownTimeout   = homekube.ChildJobDeletionTimeout
	staleRunAge            = 24 * time.Hour
	maxLonghornWaitTimeout = appconfig.MaxLonghornPVCTimeout
	maxLiveRunDuration     = 20 * time.Hour
)

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
	ReconcileStaleRuns(context.Context, string, []string, time.Duration) error
	ResolveCronJob(context.Context, string) (*batchv1.CronJob, error)
	GetPVC(context.Context, string, string) (*corev1.PersistentVolumeClaim, error)
	CreateSnapshot(context.Context, homekube.SnapshotSpec) error
	WaitSnapshotReady(context.Context, string, string, time.Duration) error
	CreateSnapshotAliasContent(context.Context, homekube.SnapshotAliasSpec) (dependencyCleanupSafe bool, err error)
	CreateSnapshotAlias(context.Context, homekube.SnapshotAliasSpec) error
	CreateRestoredPVC(context.Context, *corev1.PersistentVolumeClaim) error
	CreateChildConfigSecret(context.Context, *corev1.Secret) error
	ValidateChildJob(context.Context, *batchv1.Job) error
	CreateChildJob(context.Context, *batchv1.Job) (dependencyCleanupSafe bool, err error)
	WaitJobFinished(context.Context, string, string, time.Duration) (bool, error)
	JobPodLogs(context.Context, string, string, string) (string, error)
	DeleteJob(context.Context, string, string, string, string) error
	DeleteSecret(context.Context, string, string, string, string) error
	DeletePVC(context.Context, string, string, string, string) error
	DeleteSnapshot(context.Context, string, string, string, string) error
	DeleteSnapshotContent(context.Context, string, string, string, string) error
}

// Job snapshots a PVC, restores it beside the orchestrator, and starts a copy of the orchestrator CronJob.
type Job struct {
	config          Config
	destination     ResticDestination
	cluster         Cluster
	runnerNamespace string
	resourceName    func(string) (string, error)
	cleanupTimeout  time.Duration
	logger          *slog.Logger
}

// Option configures optional Longhorn Job behavior.
type Option func(*Job)

// WithLogger routes lifecycle diagnostics through the application's configured logger.
func WithLogger(logger *slog.Logger) Option {
	return func(job *Job) {
		if logger != nil {
			job.logger = logger
		}
	}
}

// NewJob constructs a Longhorn PVC backup job.
func NewJob(config Config, destination ResticDestination, cluster Cluster, runnerNamespace string, options ...Option) (*Job, error) {
	if cluster == nil {
		return nil, errors.New("longhorn cluster is required")
	}
	if runnerNamespace == "" {
		return nil, errors.New("runner namespace is required")
	}
	if config.PVCName == "" || config.SnapshotClass == "" || config.MountPath == "" || config.ContainerName == "" || config.Timeout <= 0 {
		return nil, errors.New("PVC name, snapshot class, mount path, container name, and positive timeout are required")
	}
	if config.Timeout > maxLonghornWaitTimeout {
		return nil, fmt.Errorf("longhorn PVC timeout must not exceed %s so a live run cannot outlast the %s stale-reconciliation safety window", maxLonghornWaitTimeout, staleRunAge)
	}
	if destination.Repo == "" {
		return nil, errors.New("restic repository is required")
	}
	if destination.KeepLast < 0 {
		return nil, errors.New("restic keep_last cannot be negative")
	}
	if err := ValidateResticGroupBy(destination.GroupBy); err != nil {
		return nil, err
	}
	job := &Job{
		config:          config,
		destination:     destination,
		cluster:         cluster,
		runnerNamespace: runnerNamespace,
		resourceName:    temporaryResourceName,
		cleanupTimeout:  cleanupTimeout,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, option := range options {
		if option != nil {
			option(job)
		}
	}
	return job, nil
}

// ValidateResticGroupBy ensures Longhorn retention remains isolated by the stable source host.
func ValidateResticGroupBy(groupBy string) error {
	for _, group := range strings.Split(groupBy, ",") {
		if strings.TrimSpace(group) == "host" {
			return nil
		}
	}
	return fmt.Errorf("longhorn_pvc Restic group_by must include %q (for example %q or %q) to isolate retention by source; got %q", "host", "host", "host,paths", groupBy)
}

// Run executes one complete snapshot, restore, child backup, and cleanup lifecycle.
func (j *Job) Run(ctx context.Context) (retErr error) {
	runCtx, cancelRun := context.WithTimeout(ctx, maxLiveRunDuration)
	defer cancelRun()
	ctx = runCtx
	sourceNamespace := j.config.Namespace
	if sourceNamespace == "" {
		sourceNamespace = j.runnerNamespace
	}
	runnerScope := homekube.RunnerScope(j.runnerNamespace)
	if err := j.cluster.ReconcileStaleRuns(ctx, j.runnerNamespace, []string{sourceNamespace}, staleRunAge); err != nil {
		return fmt.Errorf("reconcile stale Longhorn backup runs: %w", err)
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
	childConfigSecretName := resourceNameWithSuffix(baseName, "config")
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
		SnapshotName: restoreSnapshotName, StorageClassOverride: j.config.StorageClass, RunID: baseName, RunnerScope: runnerScope,
	}
	restoredPVC, err := homekube.BuildRestorePVC(restorePVCOptions)
	if err != nil {
		return fmt.Errorf("validate restored PVC: %w", err)
	}
	childJobOptions := homekube.ChildJobOptions{
		Name: childJobName, RunID: baseName, CronJob: cronJob, ContainerName: j.config.ContainerName,
		TempPVCName: tempPVCName, MountPath: j.config.MountPath, ChildConfigSecretName: childConfigSecretName,
		ResticHost: resticHostIdentity(sourceNamespace, j.config.PVCName),
	}
	childJob, err := homekube.BuildChildJob(childJobOptions)
	if err != nil {
		return fmt.Errorf("validate child Job: %w", err)
	}
	if err := j.cluster.ValidateChildJob(ctx, childJob); err != nil {
		return fmt.Errorf("validate child Job API compatibility: %w", err)
	}
	childConfigSecret := homekube.BuildChildConfigSecret(childConfigSecretName, j.runnerNamespace, childConfig, baseName, runnerScope)

	lifecycle := runLifecycle{dependencyCleanupTimeout: j.cleanupTimeout}
	defer func() {
		retErr = errors.Join(retErr, lifecycle.cleanup(ctx))
	}()

	sourceSnapshotSpec := homekube.SnapshotSpec{
		Name: sourceSnapshotName, Namespace: sourceNamespace,
		PVCName: j.config.PVCName, SnapshotClass: j.config.SnapshotClass, RunID: baseName, RunnerScope: runnerScope,
	}
	if err := lifecycle.create(ctx, func(ctx context.Context) error {
		return j.cluster.DeleteSnapshot(ctx, sourceNamespace, sourceSnapshotName, baseName, runnerScope)
	}, func(ctx context.Context) error {
		return j.cluster.CreateSnapshot(ctx, sourceSnapshotSpec)
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
			RunnerScope: runnerScope,
		}
		if err := lifecycle.createWithDisposition(ctx, func(ctx context.Context) error {
			return j.cluster.DeleteSnapshotContent(ctx, aliasContentName, baseName, runnerScope, j.runnerNamespace)
		}, func(ctx context.Context) (bool, error) {
			return j.cluster.CreateSnapshotAliasContent(ctx, aliasSpec)
		}); err != nil {
			return fmt.Errorf("create VolumeSnapshotContent alias %s: %w", aliasContentName, err)
		}
		if err := lifecycle.create(ctx, func(ctx context.Context) error {
			return j.cluster.DeleteSnapshot(ctx, j.runnerNamespace, aliasSnapshotName, baseName, runnerScope)
		}, func(ctx context.Context) error {
			return j.cluster.CreateSnapshotAlias(ctx, aliasSpec)
		}); err != nil {
			return fmt.Errorf("create target VolumeSnapshot alias %s/%s: %w", j.runnerNamespace, aliasSnapshotName, err)
		}
		if err := j.cluster.WaitSnapshotReady(ctx, j.runnerNamespace, aliasSnapshotName, j.config.Timeout); err != nil {
			return fmt.Errorf("wait for target VolumeSnapshot alias %s/%s: %w", j.runnerNamespace, aliasSnapshotName, err)
		}
	}

	if err := lifecycle.create(ctx, func(ctx context.Context) error {
		return j.cluster.DeletePVC(ctx, j.runnerNamespace, tempPVCName, baseName, runnerScope)
	}, func(ctx context.Context) error {
		return j.cluster.CreateRestoredPVC(ctx, restoredPVC)
	}); err != nil {
		return fmt.Errorf("create temporary PVC %s/%s: %w", j.runnerNamespace, tempPVCName, err)
	}
	if err := lifecycle.create(ctx, func(ctx context.Context) error {
		return j.cluster.DeleteSecret(ctx, j.runnerNamespace, childConfigSecretName, baseName, runnerScope)
	}, func(ctx context.Context) error {
		return j.cluster.CreateChildConfigSecret(ctx, childConfigSecret)
	}); err != nil {
		return fmt.Errorf("create child config Secret %s/%s: %w", j.runnerNamespace, childConfigSecretName, err)
	}
	if err := lifecycle.createChildJob(ctx, func(ctx context.Context) error {
		return j.cluster.DeleteJob(ctx, j.runnerNamespace, childJobName, baseName, runnerScope)
	}, func(ctx context.Context) (bool, error) {
		return j.cluster.CreateChildJob(ctx, childJob)
	}); err != nil {
		return fmt.Errorf("create child backup Job %s/%s: %w", j.runnerNamespace, childJobName, err)
	}
	jobTerminal, err := j.cluster.WaitJobFinished(ctx, j.runnerNamespace, childJobName, j.config.Timeout)
	if jobTerminal {
		logs, logsErr := j.cluster.JobPodLogs(ctx, j.runnerNamespace, childJobName, baseName)
		if err != nil {
			waitErr := fmt.Errorf("wait for child backup Job %s/%s: %w", j.runnerNamespace, childJobName, err)
			if logsErr != nil {
				combinedErr := errors.Join(waitErr, fmt.Errorf("collect child backup Job logs: %w", logsErr))
				if logs != "" {
					return fmt.Errorf("%w\nchild backup Job logs:\n%s", combinedErr, logs)
				}
				return combinedErr
			}
			return fmt.Errorf("%w\nchild backup Job logs:\n%s", waitErr, logs)
		}
		if logs != "" {
			j.logger.Debug("child backup Job logs", "namespace", j.runnerNamespace, "job", childJobName, "logs", logs)
		}
		if logsErr != nil {
			j.logger.Warn("failed to collect all completed child backup Job logs", "namespace", j.runnerNamespace, "job", childJobName, "error", logsErr)
		}
	}
	if err != nil {
		return fmt.Errorf("wait for child backup Job %s/%s: %w", j.runnerNamespace, childJobName, err)
	}
	return nil
}

type cleanupFunc func(context.Context) error

type runLifecycle struct {
	cleanups                 []cleanupFunc
	preserveReason           error
	childJobCleanup          cleanupFunc
	dependencyCleanupTimeout time.Duration
}

func (l *runLifecycle) create(ctx context.Context, cleanup cleanupFunc, create func(context.Context) error) error {
	l.cleanups = append(l.cleanups, cleanup)
	l.preserveReason = errors.New("cleanup stopped to preserve dependent resources: Kubernetes create outcome is unresolved; stale reconciliation will recover the run after its safety window")
	if err := create(ctx); err != nil {
		return err
	}
	l.preserveReason = nil
	return nil
}

func (l *runLifecycle) createWithDisposition(ctx context.Context, cleanup cleanupFunc, create func(context.Context) (dependencyCleanupSafe bool, err error)) error {
	l.cleanups = append(l.cleanups, cleanup)
	l.preserveReason = errors.New("cleanup stopped to preserve dependent resources: Kubernetes create outcome is unresolved; stale reconciliation will recover the run after its safety window")
	dependencyCleanupSafe, err := create(ctx)
	if dependencyCleanupSafe {
		l.preserveReason = nil
	}
	if err == nil && !dependencyCleanupSafe {
		return errors.New("kubernetes create returned an unresolved outcome without an error")
	}
	return err
}

func (l *runLifecycle) createChildJob(ctx context.Context, cleanup cleanupFunc, create func(context.Context) (dependencyCleanupSafe bool, err error)) error {
	l.childJobCleanup = cleanup
	l.preserveReason = errors.New("cleanup stopped to preserve dependent resources: child Job create or rollback outcome is unresolved; stale reconciliation will recover the run after its safety window")
	dependencyCleanupSafe, err := create(ctx)
	if dependencyCleanupSafe {
		l.preserveReason = nil
	}
	if err == nil && !dependencyCleanupSafe {
		return errors.New("child Job create returned an unresolved outcome without an error")
	}
	return err
}

func (l *runLifecycle) cleanup(ctx context.Context) error {
	if l.preserveReason != nil {
		return l.preserveReason
	}
	if l.childJobCleanup != nil {
		childCtx, cancelChild := context.WithTimeout(context.WithoutCancel(ctx), childShutdownTimeout)
		childErr := l.childJobCleanup(childCtx)
		cancelChild()
		if childErr != nil {
			return fmt.Errorf("cleanup stopped to preserve dependent resources: delete child Job: %w", childErr)
		}
	}
	cleanupDuration := l.dependencyCleanupTimeout
	if cleanupDuration <= 0 {
		cleanupDuration = cleanupTimeout
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupDuration)
	defer cancel()
	return runCleanup(cleanupCtx, l.cleanups)
}

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

func resticHostIdentity(namespace, pvcName string) string {
	sum := sha256.Sum256([]byte(namespace + "\x00" + pvcName))
	prefix := strings.Trim(invalidDNS1123Chars.ReplaceAllString(strings.ToLower(namespace+"-"+pvcName), "-"), "-")
	if prefix == "" {
		prefix = "pvc"
	}
	const hashLength = 16
	const base = "home-backup-"
	maxPrefixLength := 63 - len(base) - 1 - hashLength
	if len(prefix) > maxPrefixLength {
		prefix = strings.TrimRight(prefix[:maxPrefixLength], "-")
	}
	return fmt.Sprintf("%s%s-%x", base, prefix, sum[:hashLength/2])
}

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
