package kube

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ionutbalutoiu/home-backup/internal/config"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestBuildChildJobCopiesCronJobSpecAndAddsSnapshotBackup(t *testing.T) {
	backoffLimit := int32(4)
	parallelism := int32(2)
	terminationGrace := ParentMinimumGraceSeconds
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "backup"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{"app": "home-backup"},
				Annotations: map[string]string{"example.com/template": "kept"},
			},
			Spec: batchv1.JobSpec{
				BackoffLimit: &backoffLimit,
				Parallelism:  &parallelism,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "home-backup"}},
					Spec: corev1.PodSpec{
						ServiceAccountName:            "home-backup",
						RestartPolicy:                 corev1.RestartPolicyNever,
						TerminationGracePeriodSeconds: &terminationGrace,
						InitContainers: []corev1.Container{{
							Name: "copy-rclone-conf", Image: "busybox:test",
						}},
						Containers: []corev1.Container{
							{
								Name:  "home-backup",
								Image: "home-backup:test",
								Env: []corev1.EnvVar{
									{Name: config.EnvConfigBase64, Value: "old"},
									{Name: "RESTIC_HOST", Value: "backup-cronjobs"},
								},
							},
							{Name: "sidecar", Image: "sidecar:test"},
						},
						Volumes: []corev1.Volume{{Name: "existing"}},
					},
				},
			},
		}},
	}
	original := cronJob.DeepCopy()

	job, err := BuildChildJob(ChildJobOptions{
		Name:                  "home-backup-data-fixed-job",
		RunID:                 "home-backup-data-fixed",
		CronJob:               cronJob,
		ContainerName:         "home-backup",
		TempPVCName:           "home-backup-data-fixed-pvc",
		MountPath:             "/backup-source",
		ChildConfigSecretName: "home-backup-data-fixed-config",
		ResticHost:            "home-backup-source-data-0123456789abcdef",
	})
	if err != nil {
		t.Fatalf("BuildChildJob() error = %v", err)
	}

	if job.Name != "home-backup-data-fixed-job" || job.Namespace != "backup" {
		t.Fatalf("job identity = %s/%s", job.Namespace, job.Name)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != ChildJobTTLSeconds {
		t.Fatalf("TTLSecondsAfterFinished = %v", job.Spec.TTLSecondsAfterFinished)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 || job.Spec.Completions == nil || *job.Spec.Completions != 1 {
		t.Fatalf("copied Job settings = %#v", job.Spec)
	}
	if job.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *job.Spec.Template.Spec.TerminationGracePeriodSeconds != ChildJobTerminationGraceSeconds {
		t.Fatalf("child termination grace = %v", job.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	if job.Labels["app"] != "home-backup" || job.Labels[ManagedByLabel] != ManagedByLabelValue || job.Labels[RunLabel] != "home-backup-data-fixed" || job.Annotations["example.com/template"] != "kept" {
		t.Fatalf("job template metadata = labels %#v annotations %#v", job.Labels, job.Annotations)
	}
	wantScope := RunnerScope("backup")
	if job.Spec.Template.Labels[ManagedByLabel] != ManagedByLabelValue || job.Spec.Template.Labels[RunLabel] != "home-backup-data-fixed" || job.Spec.Template.Labels[RunnerScopeLabel] != wantScope {
		t.Fatalf("child Pod template ownership labels = %#v", job.Spec.Template.Labels)
	}
	if job.Labels[RunnerScopeLabel] != wantScope {
		t.Fatalf("child Job runner scope label = %q, want %q", job.Labels[RunnerScopeLabel], wantScope)
	}
	if got := job.Spec.Template.Spec; got.ServiceAccountName != "home-backup" || got.AutomountServiceAccountToken == nil || *got.AutomountServiceAccountToken || len(got.InitContainers) != 1 || len(got.Containers) != 2 || len(got.Volumes) != 2 {
		t.Fatalf("copied pod spec = %#v", got)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Env[0].Name != config.EnvConfigBase64 || container.Env[0].Value != "" || container.Env[0].ValueFrom == nil || container.Env[0].ValueFrom.SecretKeyRef == nil || container.Env[0].ValueFrom.SecretKeyRef.Name != "home-backup-data-fixed-config" || container.Env[0].ValueFrom.SecretKeyRef.Key != ChildConfigSecretKey {
		t.Fatalf("config env = %#v", container.Env)
	}
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].Name != TempVolumeName || container.VolumeMounts[0].MountPath != "/backup-source" || !container.VolumeMounts[0].ReadOnly {
		t.Fatalf("snapshot mount = %#v", container.VolumeMounts)
	}
	volume := job.Spec.Template.Spec.Volumes[1]
	if volume.Name != TempVolumeName || volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName != "home-backup-data-fixed-pvc" || !volume.PersistentVolumeClaim.ReadOnly {
		t.Fatalf("snapshot volume = %#v", volume)
	}
	expectedSpec := *original.Spec.JobTemplate.Spec.DeepCopy()
	expectedTTL := ChildJobTTLSeconds
	expectedSpec.TTLSecondsAfterFinished = &expectedTTL
	singleton := int32(1)
	expectedSpec.Parallelism = &singleton
	expectedSpec.Completions = &singleton
	zero := int32(0)
	expectedSpec.BackoffLimit = &zero
	falseValue := false
	expectedSpec.Suspend = &falseValue
	expectedSpec.Template.Labels[ManagedByLabel] = ManagedByLabelValue
	expectedSpec.Template.Labels[RunLabel] = "home-backup-data-fixed"
	expectedSpec.Template.Labels[RunnerScopeLabel] = RunnerScope("backup")
	expectedChildGrace := ChildJobTerminationGraceSeconds
	expectedSpec.Template.Spec.TerminationGracePeriodSeconds = &expectedChildGrace
	expectedSpec.Template.Spec.AutomountServiceAccountToken = &falseValue
	expectedSpec.Template.Spec.Volumes = append(expectedSpec.Template.Spec.Volumes, corev1.Volume{
		Name: TempVolumeName,
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: "home-backup-data-fixed-pvc", ReadOnly: true,
		}},
	})
	expectedContainer := &expectedSpec.Template.Spec.Containers[0]
	expectedContainer.Env = []corev1.EnvVar{
		{Name: config.EnvConfigBase64, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "home-backup-data-fixed-config"}, Key: ChildConfigSecretKey,
		}}},
		{Name: "RESTIC_HOST", Value: "home-backup-source-data-0123456789abcdef"},
	}
	expectedContainer.VolumeMounts = append(expectedContainer.VolumeMounts, corev1.VolumeMount{
		Name: TempVolumeName, MountPath: "/backup-source", ReadOnly: true,
	})
	if !reflect.DeepEqual(job.Spec, expectedSpec) {
		t.Fatalf("copied JobSpec differs from expected\ngot:  %#v\nwant: %#v", job.Spec, expectedSpec)
	}
	if !reflect.DeepEqual(cronJob, original) {
		t.Fatal("BuildChildJob mutated the source CronJob")
	}
}

func TestBuildChildJobRemovesServiceAccountTokensFromEveryContainerClass(t *testing.T) {
	tokenVolume := "kube-api-access-parent"
	unrelatedProjected := "projected-app-config"
	resticSecret := "restic-credentials"
	mounts := []corev1.VolumeMount{
		{Name: tokenVolume, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true},
		{Name: unrelatedProjected, MountPath: "/var/run/app-config", ReadOnly: true},
	}
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "backup"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				ServiceAccountName:            "home-backup",
				TerminationGracePeriodSeconds: ptr.To(ParentMinimumGraceSeconds),
				InitContainers:                []corev1.Container{{Name: "init", VolumeMounts: append([]corev1.VolumeMount(nil), mounts...)}},
				Containers: []corev1.Container{
					{
						Name: "home-backup", VolumeMounts: append(append([]corev1.VolumeMount(nil), mounts...), corev1.VolumeMount{Name: resticSecret, MountPath: "/restic"}),
						EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: resticSecret}}}},
					},
					{Name: "sidecar", VolumeMounts: append([]corev1.VolumeMount(nil), mounts...)},
				},
				EphemeralContainers: []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{
					Name: "debugger", VolumeMounts: append([]corev1.VolumeMount(nil), mounts...),
				}}},
				Volumes: []corev1.Volume{
					{Name: tokenVolume, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{
						ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"},
					}}}}},
					{Name: unrelatedProjected, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{
						ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "app-config"}},
					}}}}},
					{Name: resticSecret, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: resticSecret}}},
				},
			}},
		}}},
	}

	job, err := BuildChildJob(ChildJobOptions{
		Name: "child", RunID: "run", CronJob: cronJob, ContainerName: "home-backup",
		TempPVCName: "restored", MountPath: "/backup-source", ChildConfigSecretName: "child-config",
		ResticHost: "home-backup-source-data-0123456789abcdef",
	})
	if err != nil {
		t.Fatalf("BuildChildJob() error = %v", err)
	}
	podSpec := job.Spec.Template.Spec
	if podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken {
		t.Fatalf("automountServiceAccountToken = %v, want false", podSpec.AutomountServiceAccountToken)
	}
	if podSpec.ServiceAccountName != "home-backup" {
		t.Fatalf("serviceAccountName = %q, want preserved", podSpec.ServiceAccountName)
	}
	for _, volume := range podSpec.Volumes {
		if volume.Name == tokenVolume {
			t.Fatalf("service-account-token projected volume survived: %#v", volume)
		}
	}
	for _, name := range []string{unrelatedProjected, resticSecret, TempVolumeName} {
		if !hasVolume(podSpec.Volumes, name) {
			t.Fatalf("unrelated volume %q was removed: %#v", name, podSpec.Volumes)
		}
	}
	assertMounts := func(class, name string, volumeMounts []corev1.VolumeMount) {
		t.Helper()
		if hasVolumeMount(volumeMounts, tokenVolume) {
			t.Fatalf("%s container %q retained service-account-token mount: %#v", class, name, volumeMounts)
		}
		if !hasVolumeMount(volumeMounts, unrelatedProjected) {
			t.Fatalf("%s container %q lost unrelated projected mount: %#v", class, name, volumeMounts)
		}
	}
	for _, container := range podSpec.InitContainers {
		assertMounts("init", container.Name, container.VolumeMounts)
	}
	for _, container := range podSpec.Containers {
		assertMounts("regular/sidecar", container.Name, container.VolumeMounts)
	}
	for _, container := range podSpec.EphemeralContainers {
		assertMounts("ephemeral", container.Name, container.VolumeMounts)
	}
	selected := podSpec.Containers[0]
	if len(selected.EnvFrom) != 1 || selected.EnvFrom[0].SecretRef == nil || selected.EnvFrom[0].SecretRef.Name != resticSecret || !hasVolumeMount(selected.VolumeMounts, resticSecret) {
		t.Fatalf("source Restic Secret wiring was not preserved: %#v / %#v", selected.EnvFrom, selected.VolumeMounts)
	}
}

func hasVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func hasVolumeMount(mounts []corev1.VolumeMount, name string) bool {
	for _, mount := range mounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func TestBuildChildJobNormalizesEveryControllerFieldToAtMostOnePod(t *testing.T) {
	one := int32(1)
	four := int32(4)
	trueValue := true
	managedBy := "example.com/external-controller"
	indexed := batchv1.IndexedCompletion
	failedIndexes := int32(2)
	replaceFailed := batchv1.Failed
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "backup"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{
			Parallelism:          &four,
			Completions:          &four,
			BackoffLimit:         &four,
			BackoffLimitPerIndex: &one,
			MaxFailedIndexes:     &failedIndexes,
			CompletionMode:       &indexed,
			Suspend:              &trueValue,
			ManagedBy:            &managedBy,
			PodFailurePolicy: &batchv1.PodFailurePolicy{Rules: []batchv1.PodFailurePolicyRule{{
				Action: batchv1.PodFailurePolicyActionIgnore,
				OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{{
					Type: "DisruptionTarget", Status: corev1.ConditionTrue,
				}},
			}}},
			SuccessPolicy:        &batchv1.SuccessPolicy{Rules: []batchv1.SuccessPolicyRule{{SucceededCount: &one}}},
			PodReplacementPolicy: &replaceFailed,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy:                 corev1.RestartPolicyOnFailure,
				TerminationGracePeriodSeconds: ptr.To(ParentMinimumGraceSeconds),
				Containers:                    []corev1.Container{{Name: "home-backup"}},
			}},
		}}},
	}

	job, err := BuildChildJob(ChildJobOptions{
		Name: "child", RunID: "run", CronJob: cronJob, ContainerName: "home-backup",
		TempPVCName: "restored", MountPath: "/backup-source", ChildConfigSecretName: "child-config",
		ResticHost: "home-backup-source-data-0123456789abcdef",
	})
	if err != nil {
		t.Fatalf("BuildChildJob() error = %v", err)
	}
	if job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 || job.Spec.Completions == nil || *job.Spec.Completions != 1 {
		t.Fatalf("cardinality = parallelism %v completions %v", job.Spec.Parallelism, job.Spec.Completions)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.BackoffLimitPerIndex != nil || job.Spec.MaxFailedIndexes != nil {
		t.Fatalf("retry fields were not cleared: %#v", job.Spec)
	}
	if job.Spec.Suspend == nil || *job.Spec.Suspend || job.Spec.ManagedBy != nil || job.Spec.CompletionMode != nil || job.Spec.PodFailurePolicy != nil || job.Spec.SuccessPolicy != nil || job.Spec.PodReplacementPolicy != nil {
		t.Fatalf("controller fields were not normalized: %#v", job.Spec)
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restartPolicy = %q", job.Spec.Template.Spec.RestartPolicy)
	}
}

func TestBuildChildJobRejectsParentTerminationGraceBelowCleanupContract(t *testing.T) {
	tooShort := int64(179)
	for _, test := range []struct {
		name  string
		grace *int64
	}{
		{name: "default grace is too short"},
		{name: "explicit grace is too short", grace: &tooShort},
	} {
		t.Run(test.name, func(t *testing.T) {
			cronJob := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "backup"},
				Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						TerminationGracePeriodSeconds: test.grace,
						Containers:                    []corev1.Container{{Name: "home-backup"}},
					}},
				}}},
			}
			_, err := BuildChildJob(ChildJobOptions{
				Name: "child", RunID: "run", CronJob: cronJob, ContainerName: "home-backup",
				TempPVCName: "restored", MountPath: "/backup-source", ChildConfigSecretName: "child-config",
				ResticHost: "home-backup-source-data-0123456789abcdef",
			})
			if err == nil || !strings.Contains(err.Error(), "terminationGracePeriodSeconds") || !strings.Contains(err.Error(), "180") {
				t.Fatalf("BuildChildJob() error = %v", err)
			}
		})
	}
}

func TestBuildChildJobRejectsNativeRestartableInitContainers(t *testing.T) {
	restartAlways := corev1.ContainerRestartPolicyAlways
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "backup"},
		Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: ptr.To(ParentMinimumGraceSeconds),
				InitContainers:                []corev1.Container{{Name: "sidecar-init", RestartPolicy: &restartAlways}},
				Containers:                    []corev1.Container{{Name: "home-backup"}},
			}},
		}}},
	}

	_, err := BuildChildJob(ChildJobOptions{
		Name: "child", RunID: "run", CronJob: cronJob, ContainerName: "home-backup",
		TempPVCName: "restored", MountPath: "/backup-source", ChildConfigSecretName: "child-config",
		ResticHost: "home-backup-source-data-0123456789abcdef",
	})
	if err == nil || !strings.Contains(err.Error(), "restartable init container") {
		t.Fatalf("BuildChildJob() error = %v", err)
	}
}

func TestBuildChildJobRejectsSnapshotVolumeCollisions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*batchv1.CronJob)
	}{
		{
			name: "reserved volume name",
			mutate: func(cronJob *batchv1.CronJob) {
				cronJob.Spec.JobTemplate.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: TempVolumeName}}
			},
		},
		{
			name: "reserved mount path",
			mutate: func(cronJob *batchv1.CronJob) {
				cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "existing", MountPath: "/backup-source"}}
			},
		},
		{
			name: "reserved volume device name",
			mutate: func(cronJob *batchv1.CronJob) {
				cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{{Name: TempVolumeName, DevicePath: "/dev/snapshot"}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cronJob := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{Name: "home-backup", Namespace: "backup"},
				Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr.To(ParentMinimumGraceSeconds),
					Containers:                    []corev1.Container{{Name: "home-backup"}},
				}}}}},
			}
			test.mutate(cronJob)
			_, err := BuildChildJob(ChildJobOptions{
				Name: "child", RunID: "run", CronJob: cronJob, ContainerName: "home-backup",
				TempPVCName: "restored", MountPath: "/backup-source", ChildConfigSecretName: "child-config", ResticHost: "home-backup-source-data-0123456789abcdef",
			})
			if err == nil {
				t.Fatal("BuildChildJob() error = nil")
			}
		})
	}
}
