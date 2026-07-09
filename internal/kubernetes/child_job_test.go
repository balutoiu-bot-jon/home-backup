package kube

import (
	"reflect"
	"testing"

	"github.com/ionutbalutoiu/home-backup/internal/config"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildChildJobCopiesCronJobSpecAndAddsSnapshotBackup(t *testing.T) {
	backoffLimit := int32(4)
	parallelism := int32(2)
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
						ServiceAccountName: "home-backup",
						RestartPolicy:      corev1.RestartPolicyNever,
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
		Name:              "home-backup-data-fixed-job",
		RunID:             "home-backup-data-fixed",
		CronJob:           cronJob,
		ContainerName:     "home-backup",
		TempPVCName:       "home-backup-data-fixed-pvc",
		MountPath:         "/backup-source",
		ChildConfigBase64: "new-config",
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
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != backoffLimit || job.Spec.Parallelism == nil || *job.Spec.Parallelism != parallelism {
		t.Fatalf("copied Job settings = %#v", job.Spec)
	}
	if job.Labels["app"] != "home-backup" || job.Labels[ManagedByLabel] != ManagedByLabelValue || job.Labels[RunLabel] != "home-backup-data-fixed" || job.Annotations["example.com/template"] != "kept" {
		t.Fatalf("job template metadata = labels %#v annotations %#v", job.Labels, job.Annotations)
	}
	if got := job.Spec.Template.Spec; got.ServiceAccountName != "home-backup" || len(got.InitContainers) != 1 || len(got.Containers) != 2 || len(got.Volumes) != 2 {
		t.Fatalf("copied pod spec = %#v", got)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Env[0].Name != config.EnvConfigBase64 || container.Env[0].Value != "new-config" {
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
	expectedSpec.Template.Spec.Volumes = append(expectedSpec.Template.Spec.Volumes, corev1.Volume{
		Name: TempVolumeName,
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: "home-backup-data-fixed-pvc", ReadOnly: true,
		}},
	})
	expectedContainer := &expectedSpec.Template.Spec.Containers[0]
	expectedContainer.Env = []corev1.EnvVar{
		{Name: config.EnvConfigBase64, Value: "new-config"},
		{Name: "RESTIC_HOST", Value: "backup-cronjobs"},
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
					Containers: []corev1.Container{{Name: "home-backup"}},
				}}}}},
			}
			test.mutate(cronJob)
			_, err := BuildChildJob(ChildJobOptions{
				Name: "child", RunID: "run", CronJob: cronJob, ContainerName: "home-backup",
				TempPVCName: "restored", MountPath: "/backup-source", ChildConfigBase64: "config",
			})
			if err == nil {
				t.Fatal("BuildChildJob() error = nil")
			}
		})
	}
}
