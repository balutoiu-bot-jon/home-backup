package longhorn

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
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
)

type recordingCluster struct {
	calls              []string
	failAt             map[string]error
	cronJobNamespace   string
	pvcNamespace       string
	pvcName            string
	snapshotSpec       homekube.SnapshotSpec
	aliasContentSpec   homekube.SnapshotAliasSpec
	aliasSnapshotSpec  homekube.SnapshotAliasSpec
	restoredPVCOptions homekube.RestorePVCOptions
	childJob           *batchv1.Job
	cronJob            *batchv1.CronJob
	nonterminalWait    bool
}

func (f *recordingCluster) record(call string) error {
	f.calls = append(f.calls, call)
	return f.failAt[call]
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
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "home-backup"}},
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
func (f *recordingCluster) CreateSnapshotAliasContent(_ context.Context, spec homekube.SnapshotAliasSpec) error {
	f.aliasContentSpec = spec
	return f.record("create-alias-content")
}
func (f *recordingCluster) CreateSnapshotAlias(_ context.Context, spec homekube.SnapshotAliasSpec) error {
	f.aliasSnapshotSpec = spec
	return f.record("create-alias-snapshot")
}
func (f *recordingCluster) CreateRestoredPVC(_ context.Context, opts homekube.RestorePVCOptions) error {
	f.restoredPVCOptions = opts
	return f.record("create-pvc")
}
func (f *recordingCluster) CreateChildJob(_ context.Context, job *batchv1.Job) error {
	f.childJob = job.DeepCopy()
	return f.record("create-job")
}
func (f *recordingCluster) WaitJobFinished(context.Context, string, string, time.Duration) (bool, error) {
	return !f.nonterminalWait, f.record("wait-job")
}
func (f *recordingCluster) DeleteJob(context.Context, string, string, string) error {
	return f.record("delete-job")
}
func (f *recordingCluster) DeletePVC(context.Context, string, string, string) error {
	return f.record("delete-pvc")
}
func (f *recordingCluster) DeleteSnapshot(_ context.Context, _, name, _ string) error {
	if strings.Contains(name, "alias-snap") {
		return f.record("delete-alias-snapshot")
	}
	return f.record("delete-source-snapshot")
}
func (f *recordingCluster) DeleteSnapshotContent(context.Context, string, string) error {
	return f.record("delete-alias-content")
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
	}
}

func TestJobCopiesCronJobSpecIntoChildJob(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{}}
	job := newTestJob(cluster, "source")

	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []string{
		"resolve-cronjob", "get-pvc", "create-source-snapshot", "wait-source-snapshot",
		"create-alias-content", "create-alias-snapshot", "wait-alias-snapshot",
		"create-pvc", "create-job", "wait-job",
		"delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot",
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
	if cluster.restoredPVCOptions.Namespace != "runner" || cluster.restoredPVCOptions.SnapshotName != "home-backup-data-fixed-alias-snap" {
		t.Fatalf("restored PVC = %#v", cluster.restoredPVCOptions)
	}
	if cluster.childJob == nil || cluster.childJob.Name != "home-backup-data-fixed-job" || cluster.childJob.Namespace != "runner" {
		t.Fatalf("child Job = %#v", cluster.childJob)
	}
	if cluster.childJob.Labels[homekube.RunLabel] != "home-backup-data-fixed" || cluster.childJob.Spec.TTLSecondsAfterFinished == nil || *cluster.childJob.Spec.TTLSecondsAfterFinished != homekube.ChildJobTTLSeconds {
		t.Fatalf("child Job ownership/TTL = %#v / %v", cluster.childJob.Labels, cluster.childJob.Spec.TTLSecondsAfterFinished)
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
	if cluster.restoredPVCOptions.SnapshotName != "home-backup-data-fixed-source-snap" {
		t.Fatalf("same-namespace restored PVC = %#v", cluster.restoredPVCOptions)
	}
}

func TestJobFailureLeavesChildJobForTTLAndCleansStorage(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{"wait-job": errors.New("backup failed")}}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "backup failed") {
		t.Fatalf("Run() error = %v", err)
	}
	wantTail := []string{"delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot"}
	if got := cluster.calls[len(cluster.calls)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("cleanup calls = %#v, want %#v", got, wantTail)
	}
	for _, call := range cluster.calls {
		if call == "delete-job" {
			t.Fatalf("child Job was explicitly deleted: %#v", cluster.calls)
		}
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
	wantTail := []string{"delete-job", "delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot"}
	if got := cluster.calls[len(cluster.calls)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("cleanup calls = %#v, want %#v", got, wantTail)
	}
}

func TestJobAmbiguousCreateDeletesChildBeforeStorage(t *testing.T) {
	cluster := &recordingCluster{failAt: map[string]error{"create-job": errors.New("transport error")}}
	err := newTestJob(cluster, "source").Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "transport error") {
		t.Fatalf("Run() error = %v", err)
	}
	wantTail := []string{"create-job", "delete-job", "delete-pvc", "delete-alias-snapshot", "delete-alias-content", "delete-source-snapshot"}
	if got := cluster.calls[len(cluster.calls)-len(wantTail):]; !reflect.DeepEqual(got, wantTail) {
		t.Fatalf("cleanup calls = %#v, want %#v", got, wantTail)
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
