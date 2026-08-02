package longhorn

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ionutbalutoiu/home-backup/internal/config"
	homekube "github.com/ionutbalutoiu/home-backup/internal/kubernetes"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

type recordingCluster struct {
	calls                 []string
	failAt                map[string]error
	cronJobNamespace      string
	pvcNamespace          string
	pvcName               string
	snapshotSpec          homekube.SnapshotSpec
	aliasContentSpec      homekube.SnapshotAliasSpec
	aliasSnapshotSpec     homekube.SnapshotAliasSpec
	restoredPVC           *corev1.PersistentVolumeClaim
	childJob              *batchv1.Job
	childConfigSecret     *corev1.Secret
	cronJob               *batchv1.CronJob
	nonterminalWait       bool
	jobLogs               string
	deleteJobDelay        time.Duration
	dependencyCleanupSafe bool
	aliasCleanupSafe      bool
	cleanupCanceled       bool
	cleanupScopes         []string
	aliasTargetNS         string
	runDeadline           time.Time
	hasRunDeadline        bool
	onCall                func(string)
}

func (f *recordingCluster) record(call string) error {
	f.calls = append(f.calls, call)
	if f.onCall != nil {
		f.onCall(call)
	}
	return f.failAt[call]
}

func (f *recordingCluster) recordCleanup(ctx context.Context, call string) error {
	if ctx.Err() != nil {
		f.cleanupCanceled = true
	}
	return f.record(call)
}

func (f *recordingCluster) ReconcileStaleRuns(ctx context.Context, _ string, _ []string, _ time.Duration) error {
	f.runDeadline, f.hasRunDeadline = ctx.Deadline()
	return f.record("reconcile-stale-runs")
}

func (f *recordingCluster) ResolveCronJob(_ context.Context, namespace string) (*batchv1.CronJob, error) {
	f.cronJobNamespace = namespace
	if err := f.record("resolve-cronjob"); err != nil {
		return nil, err
	}
	if f.cronJob != nil {
		return f.cronJob.DeepCopy(), nil
	}
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: namespace},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: ptr.To(homekube.ParentMinimumGraceSeconds),
			Containers:                    []corev1.Container{{Name: "home-backup"}},
		}}}}},
	}, nil
}

func (f *recordingCluster) GetPVC(_ context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	f.pvcNamespace = namespace
	f.pvcName = name
	if err := f.record("get-pvc"); err != nil {
		return nil, err
	}
	storageClass := "longhorn"
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
	}, nil
}

func (f *recordingCluster) CreateSnapshot(_ context.Context, spec homekube.SnapshotSpec) error {
	f.snapshotSpec = spec
	return f.record("create-source-snapshot")
}
func (f *recordingCluster) WaitSnapshotReady(_ context.Context, _ string, name string, _ time.Duration) error {
	if strings.Contains(name, "alias-snap") {
		return f.record("wait-alias-snapshot")
	}
	return f.record("wait-source-snapshot")
}
func (f *recordingCluster) CreateSnapshotAliasContent(_ context.Context, spec homekube.SnapshotAliasSpec) (bool, error) {
	f.aliasContentSpec = spec
	err := f.record("create-alias-content")
	return err == nil || f.aliasCleanupSafe, err
}
func (f *recordingCluster) CreateSnapshotAlias(_ context.Context, spec homekube.SnapshotAliasSpec) error {
	f.aliasSnapshotSpec = spec
	return f.record("create-alias-snapshot")
}
func (f *recordingCluster) CreateRestoredPVC(_ context.Context, pvc *corev1.PersistentVolumeClaim) error {
	f.restoredPVC = pvc.DeepCopy()
	return f.record("create-pvc")
}
func (f *recordingCluster) CreateChildConfigSecret(_ context.Context, secret *corev1.Secret) error {
	f.childConfigSecret = secret.DeepCopy()
	return f.record("create-secret")
}
func (f *recordingCluster) ValidateChildJob(_ context.Context, _ *batchv1.Job) error {
	return f.failAt["validate-job"]
}
func (f *recordingCluster) CreateChildJob(_ context.Context, job *batchv1.Job) (bool, error) {
	f.childJob = job.DeepCopy()
	err := f.record("create-job")
	return err == nil || f.dependencyCleanupSafe, err
}
func (f *recordingCluster) WaitJobFinished(context.Context, string, string, time.Duration) (bool, error) {
	return !f.nonterminalWait, f.record("wait-job")
}
func (f *recordingCluster) JobPodLogs(context.Context, string, string, string) (string, error) {
	err := f.record("job-pod-logs")
	logs := f.jobLogs
	if logs == "" {
		logs = "restic child output"
	}
	return logs, err
}
func (f *recordingCluster) DeleteJob(ctx context.Context, _, _, _, runnerScope string) error {
	f.cleanupScopes = append(f.cleanupScopes, runnerScope)
	if err := f.recordCleanup(ctx, "delete-job"); err != nil {
		return err
	}
	if f.deleteJobDelay <= 0 {
		return nil
	}
	select {
	case <-time.After(f.deleteJobDelay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (f *recordingCluster) DeleteSecret(ctx context.Context, _, _, _, runnerScope string) error {
	f.cleanupScopes = append(f.cleanupScopes, runnerScope)
	return f.recordCleanup(ctx, "delete-secret")
}
func (f *recordingCluster) DeletePVC(ctx context.Context, _, _, _, runnerScope string) error {
	f.cleanupScopes = append(f.cleanupScopes, runnerScope)
	return f.recordCleanup(ctx, "delete-pvc")
}
func (f *recordingCluster) DeleteSnapshot(ctx context.Context, _, name, _, runnerScope string) error {
	f.cleanupScopes = append(f.cleanupScopes, runnerScope)
	if strings.Contains(name, "alias-snap") {
		return f.recordCleanup(ctx, "delete-alias-snapshot")
	}
	return f.recordCleanup(ctx, "delete-source-snapshot")
}
func (f *recordingCluster) DeleteSnapshotContent(ctx context.Context, _, _, runnerScope, targetNamespace string) error {
	f.cleanupScopes = append(f.cleanupScopes, runnerScope)
	f.aliasTargetNS = targetNamespace
	return f.recordCleanup(ctx, "delete-alias-content")
}

func TestCleanupBudgetsFitParentGraceAndAllowForegroundDeletionOverhead(t *testing.T) {
	childGrace := time.Duration(homekube.ChildJobTerminationGraceSeconds) * time.Second
	parentGrace := time.Duration(homekube.ParentMinimumGraceSeconds) * time.Second
	if childShutdownTimeout <= childGrace {
		t.Fatalf("child shutdown timeout = %s, must exceed Pod grace %s for foreground deletion overhead", childShutdownTimeout, childGrace)
	}
	if total := childShutdownTimeout + cleanupTimeout; total >= parentGrace {
		t.Fatalf("sequential cleanup budgets = %s, must leave process margin under parent grace %s", total, parentGrace)
	}
}

func TestJobRunHasEndToEndDeadlineBelowStaleWindow(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{}}
	started := time.Now()
	if err := newTestJob(cluster, "source").Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !cluster.hasRunDeadline {
		t.Fatal("Run() did not apply an end-to-end deadline")
	}
	duration := cluster.runDeadline.Sub(started)
	if duration > maxLiveRunDuration+time.Second || duration+childShutdownTimeout+cleanupTimeout >= staleRunAge {
		t.Fatalf("run deadline duration = %s, does not leave safe cleanup margin below stale age %s", duration, staleRunAge)
	}
}

func TestNewJobRejectsTimeoutThatCanOutliveStaleSafetyWindow(t *testing.T) {
	_, err := NewJob(Config{
		PVCName: "data", SnapshotClass: "longhorn", MountPath: "/backup-source",
		ContainerName: "home-backup", Timeout: maxLonghornWaitTimeout + time.Second,
	}, ResticDestination{Repo: "s3:test", GroupBy: "host"}, &recordingCluster{}, "backup")
	if err == nil || !strings.Contains(err.Error(), maxLonghornWaitTimeout.String()) {
		t.Fatalf("NewJob() error = %v, want stale-window timeout rejection", err)
	}
}

func TestNewJobRejectsNegativeRetention(t *testing.T) {
	_, err := NewJob(Config{
		PVCName: "data", SnapshotClass: "longhorn", MountPath: "/backup-source",
		ContainerName: "home-backup", Timeout: time.Minute,
	}, ResticDestination{Repo: "/repo", KeepLast: -1, GroupBy: "host"}, &recordingCluster{}, "runner")
	if err == nil || !strings.Contains(err.Error(), "keep_last") {
		t.Fatalf("NewJob() error = %v, want negative retention rejection", err)
	}
}

func TestNewJobRejectsResticGroupingWithoutHost(t *testing.T) {
	_, err := NewJob(Config{
		PVCName: "data", SnapshotClass: "longhorn", MountPath: "/backup-source",
		ContainerName: "home-backup", Timeout: time.Minute,
	}, ResticDestination{Repo: "/repo", KeepLast: 1, GroupBy: "paths"}, &recordingCluster{}, "runner")
	if err == nil || !strings.Contains(err.Error(), "group_by") || !strings.Contains(err.Error(), "host") || !strings.Contains(err.Error(), "paths") {
		t.Fatalf("NewJob() error = %v", err)
	}
}

func newTestJob(cluster Cluster, sourceNamespace string) *Job {
	return &Job{
		config: Config{
			PVCName: "data", Namespace: sourceNamespace, SnapshotClass: "longhorn-snapshot-vsc",
			MountPath: "/backup-source", ContainerName: "home-backup", Timeout: time.Minute,
		},
		destination:     ResticDestination{Repo: "/repo", KeepLast: 4, GroupBy: "host"},
		cluster:         cluster,
		runnerNamespace: "runner",
		resourceName:    func(string) (string, error) { return "home-backup-data-fixed", nil },
		cleanupTimeout:  time.Second,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func newTestJobWithLogger(t *testing.T, cluster Cluster, sourceNamespace string, logger *slog.Logger) *Job {
	t.Helper()
	job, err := NewJob(Config{
		PVCName: "data", Namespace: sourceNamespace, SnapshotClass: "longhorn-snapshot-vsc",
		MountPath: "/backup-source", ContainerName: "home-backup", Timeout: time.Minute,
	}, ResticDestination{Repo: "/repo", KeepLast: 4, GroupBy: "host"}, cluster, "runner", WithLogger(logger))
	if err != nil {
		t.Fatalf("NewJob() error = %v", err)
	}
	job.resourceName = func(string) (string, error) { return "home-backup-data-fixed", nil }
	job.cleanupTimeout = time.Second
	return job
}

func TestJobReconcilesStaleRunsBeforeAllocatingResources(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{}}
	if err := newTestJob(cluster, "source").Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(cluster.calls) == 0 || cluster.calls[0] != "reconcile-stale-runs" {
		t.Fatalf("first call = %#v", cluster.calls)
	}
}

func TestSuccessfulChildLogsRespectConfiguredLoggerLevelAndWriter(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{}, jobLogs: "sensitive-success-marker"}
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelError}))
	job := newTestJobWithLogger(t, cluster, "source", logger)

	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if strings.Contains(output.String(), "sensitive-success-marker") {
		t.Fatalf("successful child output bypassed error log level: %q", output.String())
	}
}

func TestSuccessfulChildLogsUseInjectedDebugWriter(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{}, jobLogs: "debug-success-marker"}
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	job := newTestJobWithLogger(t, cluster, "source", logger)

	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !strings.Contains(output.String(), "debug-success-marker") {
		t.Fatalf("injected debug writer did not receive child output: %q", output.String())
	}
}

func TestJobCopiesCronJobSpecIntoChildJob(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{}}
	job := newTestJob(cluster, "source")

	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []string{
		"reconcile-stale-runs", "resolve-cronjob", "get-pvc", "create-source-snapshot", "wait-source-snapshot",
		"create-alias-content", "create-alias-snapshot", "wait-alias-snapshot",
		"create-pvc", "create-secret", "create-job", "wait-job", "job-pod-logs", "delete-job",
		"delete-secret", "delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot",
	}
	if !reflect.DeepEqual(cluster.calls, want) {
		t.Fatalf("calls = %#v\nwant  = %#v", cluster.calls, want)
	}
	if cluster.cronJobNamespace != "runner" || cluster.pvcNamespace != "source" || cluster.pvcName != "data" {
		t.Fatalf("CronJob namespace=%q source PVC=%s/%s", cluster.cronJobNamespace, cluster.pvcNamespace, cluster.pvcName)
	}
	if cluster.snapshotSpec.Namespace != "source" || cluster.snapshotSpec.PVCName != "data" || cluster.snapshotSpec.Name != "home-backup-data-fixed-source-snap" {
		t.Fatalf("source snapshot = %#v", cluster.snapshotSpec)
	}
	if cluster.aliasContentSpec.TargetNamespace != "runner" || cluster.aliasContentSpec.SourceNamespace != "source" || cluster.aliasContentSpec.TargetSnapshotName != "home-backup-data-fixed-alias-snap" || cluster.aliasSnapshotSpec != cluster.aliasContentSpec {
		t.Fatalf("snapshot aliases = %#v / %#v", cluster.aliasContentSpec, cluster.aliasSnapshotSpec)
	}
	if cluster.restoredPVC == nil || cluster.restoredPVC.Namespace != "runner" || cluster.restoredPVC.Spec.DataSource == nil || cluster.restoredPVC.Spec.DataSource.Name != "home-backup-data-fixed-alias-snap" {
		t.Fatalf("restored PVC = %#v", cluster.restoredPVC)
	}
	if cluster.childJob == nil || cluster.childJob.Name != "home-backup-data-fixed-job" || cluster.childJob.Namespace != "runner" {
		t.Fatalf("child Job = %#v", cluster.childJob)
	}
	if cluster.childConfigSecret == nil || cluster.childConfigSecret.Name != "home-backup-data-fixed-config" || len(cluster.childConfigSecret.Data[homekube.ChildConfigSecretKey]) == 0 {
		t.Fatalf("child config Secret = %#v", cluster.childConfigSecret)
	}
	if cluster.childJob.Labels[homekube.RunLabel] != "home-backup-data-fixed" || cluster.childJob.Spec.TTLSecondsAfterFinished != nil {
		t.Fatalf("child Job ownership/TTL = %#v / %v", cluster.childJob.Labels, cluster.childJob.Spec.TTLSecondsAfterFinished)
	}
	wantScope := homekube.RunnerScope("runner")
	for _, scope := range cluster.cleanupScopes {
		if scope != wantScope {
			t.Fatalf("cleanup runner scope = %q, want %q", scope, wantScope)
		}
	}
	if cluster.aliasTargetNS != "runner" {
		t.Fatalf("alias cleanup target namespace = %q, want runner", cluster.aliasTargetNS)
	}
}

func TestJobSameNamespaceSkipsSnapshotAlias(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{}}
	if err := newTestJob(cluster, "runner").Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for _, call := range cluster.calls {
		if strings.Contains(call, "alias") {
			t.Fatalf("same-namespace run used alias operation %q", call)
		}
	}
	if cluster.restoredPVC == nil || cluster.restoredPVC.Spec.DataSource == nil || cluster.restoredPVC.Spec.DataSource.Name != "home-backup-data-fixed-source-snap" {
		t.Fatalf("same-namespace restored PVC = %#v", cluster.restoredPVC)
	}
}

func TestJobFailureDeletesChildJobBeforeStorage(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{"wait-job": errors.New("backup failed")}}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "backup failed") {
		t.Fatalf("Run() error = %v", err)
	}
	if !strings.Contains(err.Error(), "restic child output") {
		t.Fatalf("Run() error does not include child logs: %v", err)
	}
	wantTail := []string{"job-pod-logs", "delete-job", "delete-secret", "delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot"}
	if got := cluster.calls[len(cluster.calls)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("cleanup calls = %#v, want %#v", got, wantTail)
	}
}

func TestJobDeleteFailureStopsDependentCleanup(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{
		"wait-job":   errors.New("backup failed"),
		"delete-job": errors.New("foreground deletion blocked"),
	}}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "backup failed") || !strings.Contains(err.Error(), "foreground deletion blocked") {
		t.Fatalf("Run() error = %v", err)
	}
	wantTail := []string{"job-pod-logs", "delete-job"}
	if got := cluster.calls[len(cluster.calls)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("cleanup calls = %#v, want tail %#v with no dependent cleanup", got, wantTail)
	}
}

func TestJobFailurePreservesPartialLogsAlongsideCollectionError(t *testing.T) {
	cluster := &recordingCluster{
		failAt:  map[string]error{"wait-job": errors.New("backup failed"), "job-pod-logs": errors.New("application container never started")},
		jobLogs: "init root cause",
	}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "backup failed") || !strings.Contains(err.Error(), "init root cause") || !strings.Contains(err.Error(), "never started") {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestJobNonterminalFailureDeletesChildBeforeStorage(t *testing.T) {
	cluster := &recordingCluster{
		failAt:          map[string]error{"wait-job": errors.New("wait canceled")},
		nonterminalWait: true,
	}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "wait canceled") {
		t.Fatalf("Run() error = %v", err)
	}
	wantTail := []string{"delete-job", "delete-secret", "delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot"}
	if got := cluster.calls[len(cluster.calls)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("cleanup calls = %#v, want %#v", got, wantTail)
	}
}

func TestJobNonterminalChildShutdownGetsDedicatedBudgetBeforeResourceCleanup(t *testing.T) {
	cluster := &recordingCluster{
		failAt:          map[string]error{"wait-job": errors.New("wait canceled")},
		nonterminalWait: true,
		deleteJobDelay:  40 * time.Millisecond,
	}
	job := newTestJob(cluster, "source")
	job.cleanupTimeout = 30 * time.Millisecond

	err := job.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "wait canceled") {
		t.Fatalf("Run() error = %v", err)
	}
	wantTail := []string{"delete-job", "delete-secret", "delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot"}
	if len(cluster.calls) < len(wantTail) {
		t.Fatalf("cleanup calls = %#v, want tail %#v", cluster.calls, wantTail)
	}
	if got := cluster.calls[len(cluster.calls)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("cleanup calls = %#v, want tail %#v", got, wantTail)
	}
	if cluster.cleanupCanceled {
		t.Fatal("storage cleanup context expired during child Job deletion instead of starting afterward")
	}
}

func TestJobCompatibilityProbeFailsBeforeAllocations(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{"validate-job": errors.New("unsupported podReplacementPolicy")}}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsupported podReplacementPolicy") {
		t.Fatalf("Run() error = %v", err)
	}
	for _, call := range cluster.calls {
		if strings.HasPrefix(call, "create-") {
			t.Fatalf("compatibility probe allocated resources: %#v", cluster.calls)
		}
	}
}

func TestJobAmbiguousCreatePreservesDependencies(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{"create-job": errors.New("transport error")}}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "transport error") {
		t.Fatalf("Run() error = %v", err)
	}
	if got := cluster.calls[len(cluster.calls)-1:]; !reflect.DeepEqual(got, []string{"create-job"}) {
		t.Fatalf("calls = %#v, want unresolved create to preserve every dependency", got)
	}
}

func TestJobCleanupSafeCreateFailureCleansDependencies(t *testing.T) {
	cluster := &recordingCluster{
		failAt:                map[string]error{"create-job": errors.New("incompatible persisted Job was removed")},
		dependencyCleanupSafe: true,
	}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "incompatible persisted Job was removed") {
		t.Fatalf("Run() error = %v", err)
	}
	wantTail := []string{"delete-job", "delete-secret", "delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot"}
	if got := cluster.calls[len(cluster.calls)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("cleanup calls = %#v, want %#v", got, wantTail)
	}
}

func TestJobAliasPreparationFailureCleansSourceSnapshot(t *testing.T) {
	cluster := &recordingCluster{
		failAt:           map[string]error{"create-alias-content": errors.New("read bound snapshot content")},
		aliasCleanupSafe: true,
	}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "read bound snapshot content") {
		t.Fatalf("Run() error = %v", err)
	}
	if got := cluster.calls[len(cluster.calls)-1]; got != "delete-source-snapshot" {
		t.Fatalf("calls = %#v, want source snapshot cleanup after pre-create failure", cluster.calls)
	}
}

func TestJobCancellationAtEachAllocationStageUsesFreshDependencyOrderedCleanup(t *testing.T) {
	for _, test := range []struct {
		stage   string
		cleanup []string
	}{
		{stage: "create-source-snapshot"},
		{stage: "wait-source-snapshot", cleanup: []string{"delete-source-snapshot"}},
		{stage: "create-alias-content"},
		{stage: "create-alias-snapshot"},
		{stage: "wait-alias-snapshot", cleanup: []string{"delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot"}},
		{stage: "create-pvc"},
		{stage: "create-secret"},
		{stage: "create-job"},
	} {
		t.Run(test.stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cluster := &recordingCluster{failAt: map[string]error{test.stage: context.Canceled}}
			cluster.onCall = func(call string) {
				if call == test.stage {
					cancel()
				}
			}
			err := newTestJob(cluster, "source").Run(ctx)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run() error = %v", err)
			}
			if test.cleanup == nil {
				if cluster.calls[len(cluster.calls)-1] != test.stage {
					t.Fatalf("calls = %#v, want unresolved create to preserve every dependency", cluster.calls)
				}
				return
			}
			if len(cluster.calls) < len(test.cleanup) {
				t.Fatalf("calls = %#v", cluster.calls)
			}
			if got := cluster.calls[len(cluster.calls)-len(test.cleanup):]; !reflect.DeepEqual(got, test.cleanup) {
				t.Fatalf("cleanup calls = %#v, want %#v", got, test.cleanup)
			}
			if cluster.cleanupCanceled {
				t.Fatal("cleanup reused a canceled operation context")
			}
		})
	}
}

func TestResticHostIdentityIsStableAndCollisionResistantPerSource(t *testing.T) {
	first := resticHostIdentity("media", "photos.data")
	if repeated := resticHostIdentity("media", "photos.data"); repeated != first {
		t.Fatalf("repeated identity = %q, want %q", repeated, first)
	}
	for _, source := range [][2]string{{"media", "photos-data"}, {"media-photos", "data"}, {"other", "photos.data"}} {
		if got := resticHostIdentity(source[0], source[1]); got == first {
			t.Fatalf("identity collision for %s/%s: %q", source[0], source[1], got)
		}
	}
	if len(first) > 63 || first == "" {
		t.Fatalf("identity is not a valid bounded hostname: %q", first)
	}
	for _, char := range first {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
			t.Fatalf("identity %q contains invalid character %q", first, char)
		}
	}
}

func TestJobOverridesSelectedContainerResticHostForSource(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{}, cronJob: &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "runner"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: ptr.To(homekube.ParentMinimumGraceSeconds),
			Containers: []corev1.Container{
				{Name: "home-backup", Env: []corev1.EnvVar{{Name: "RESTIC_HOST", Value: "shared-host"}}},
				{Name: "sidecar", Env: []corev1.EnvVar{{Name: "RESTIC_HOST", Value: "sidecar-host"}}},
			},
		}}}}},
	}}
	if err := newTestJob(cluster, "source").Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	selected := cluster.childJob.Spec.Template.Spec.Containers[0]
	if got := envValue(selected.Env, "RESTIC_HOST"); got != resticHostIdentity("source", "data") {
		t.Fatalf("selected RESTIC_HOST = %q", got)
	}
	if got := envValue(cluster.childJob.Spec.Template.Spec.Containers[1].Env, "RESTIC_HOST"); got != "sidecar-host" {
		t.Fatalf("sidecar RESTIC_HOST = %q", got)
	}
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, item := range env {
		if item.Name == name {
			return item.Value
		}
	}
	return ""
}

func TestNonDefaultRetentionGroupingIsStablePerSourceAndIsolatesTwoSources(t *testing.T) {
	run := func(sourceNamespace string) *recordingCluster {
		t.Helper()
		cluster := &recordingCluster{failAt: map[string]error{}}
		job := newTestJob(cluster, sourceNamespace)
		job.destination.GroupBy = "host,paths"
		if err := job.Run(context.Background()); err != nil {
			t.Fatalf("Run(%q) error = %v", sourceNamespace, err)
		}
		data, err := base64.StdEncoding.DecodeString(string(cluster.childConfigSecret.Data[homekube.ChildConfigSecretKey]))
		if err != nil {
			t.Fatalf("decode child config for %q: %v", sourceNamespace, err)
		}
		childCfg, err := config.Decode(bytes.NewReader(data), "child config")
		if err != nil {
			t.Fatalf("parse child config for %q: %v", sourceNamespace, err)
		}
		if got := childCfg.Backups[0].Destination.Restic.GroupBy; got != "host,paths" {
			t.Fatalf("child group_by for %q = %q", sourceNamespace, got)
		}
		return cluster
	}

	first := run("media")
	repeated := run("media")
	other := run("documents")
	firstHost := envValue(first.childJob.Spec.Template.Spec.Containers[0].Env, "RESTIC_HOST")
	if repeatedHost := envValue(repeated.childJob.Spec.Template.Spec.Containers[0].Env, "RESTIC_HOST"); repeatedHost != firstHost {
		t.Fatalf("repeated source host = %q, want %q", repeatedHost, firstHost)
	}
	if otherHost := envValue(other.childJob.Spec.Template.Spec.Containers[0].Env, "RESTIC_HOST"); otherHost == firstHost {
		t.Fatalf("two sources share retention host %q", firstHost)
	}
}

func TestBuildChildConfigBase64(t *testing.T) {
	encoded, err := buildChildConfigBase64("/backup-source", ResticDestination{Repo: "/repo", KeepLast: 4, GroupBy: "host"})
	if err != nil {
		t.Fatalf("buildChildConfigBase64() error = %v", err)
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("DecodeString() error = %v", err)
	}
	cfg, err := config.Decode(bytes.NewReader(data), "child config")
	if err != nil {
		t.Fatalf("config.Decode() error = %v", err)
	}
	if len(cfg.Backups) != 1 {
		t.Fatalf("child backups = %#v", cfg.Backups)
	}
	backup := cfg.Backups[0]
	if backup.Source.Kind != config.SourceDirectory || backup.Source.Directory.Path != "/backup-source" || backup.Destination.Restic.Repo != "/repo" || backup.Destination.Restic.KeepLast != 4 || backup.Destination.Restic.GroupBy != "host" {
		t.Fatalf("child backup = %#v", backup)
	}
}
