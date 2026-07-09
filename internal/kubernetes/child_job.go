package kube

import (
	"fmt"
	"maps"
	"slices"

	"github.com/ionutbalutoiu/home-backup/internal/config"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const ChildJobTTLSeconds int32 = 3 * 24 * 60 * 60

type ChildJobOptions struct {
	Name              string
	RunID             string
	CronJob           *batchv1.CronJob
	ContainerName     string
	TempPVCName       string
	MountPath         string
	ChildConfigBase64 string
}

func BuildChildJob(opts ChildJobOptions) (*batchv1.Job, error) {
	if opts.CronJob == nil {
		return nil, fmt.Errorf("CronJob is required")
	}
	if opts.Name == "" || opts.RunID == "" || opts.CronJob.Namespace == "" || opts.TempPVCName == "" || opts.MountPath == "" || opts.ChildConfigBase64 == "" {
		return nil, fmt.Errorf("child Job name, run ID, CronJob namespace, temporary PVC name, mount path, and child config are required")
	}

	jobSpec := *opts.CronJob.Spec.JobTemplate.Spec.DeepCopy()
	containerIndex, err := selectContainer(jobSpec.Template.Spec.Containers, opts.ContainerName)
	if err != nil {
		return nil, err
	}
	if err := validateSnapshotMount(jobSpec.Template.Spec, containerIndex, opts.MountPath); err != nil {
		return nil, err
	}
	ttlSeconds := ChildJobTTLSeconds
	jobSpec.TTLSecondsAfterFinished = &ttlSeconds
	jobSpec.Template.Spec.Volumes = append(jobSpec.Template.Spec.Volumes, corev1.Volume{
		Name: TempVolumeName,
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: opts.TempPVCName,
			ReadOnly:  true,
		}},
	})
	container := &jobSpec.Template.Spec.Containers[containerIndex]
	container.Env = slices.DeleteFunc(container.Env, func(env corev1.EnvVar) bool {
		return env.Name == config.EnvConfigBase64
	})
	container.Env = append([]corev1.EnvVar{{Name: config.EnvConfigBase64, Value: opts.ChildConfigBase64}}, container.Env...)
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name: TempVolumeName, MountPath: opts.MountPath, ReadOnly: true,
	})

	labels := maps.Clone(opts.CronJob.Spec.JobTemplate.Labels)
	if labels == nil {
		labels = make(map[string]string, 2)
	}
	labels[ManagedByLabel] = ManagedByLabelValue
	labels[RunLabel] = opts.RunID
	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        opts.Name,
			Namespace:   opts.CronJob.Namespace,
			Labels:      labels,
			Annotations: maps.Clone(opts.CronJob.Spec.JobTemplate.Annotations),
		},
		Spec: jobSpec,
	}, nil
}

func validateSnapshotMount(podSpec corev1.PodSpec, containerIndex int, mountPath string) error {
	for _, volume := range podSpec.Volumes {
		if volume.Name == TempVolumeName {
			return fmt.Errorf("CronJob uses reserved volume name %q", TempVolumeName)
		}
	}
	container := podSpec.Containers[containerIndex]
	for _, mount := range container.VolumeMounts {
		if mount.Name == TempVolumeName {
			return fmt.Errorf("container %q uses reserved volume name %q", container.Name, TempVolumeName)
		}
		if mount.MountPath == mountPath {
			return fmt.Errorf("container %q already uses snapshot mount path %q", container.Name, mountPath)
		}
	}
	for _, device := range container.VolumeDevices {
		if device.Name == TempVolumeName {
			return fmt.Errorf("container %q uses reserved volume name %q", container.Name, TempVolumeName)
		}
	}
	return nil
}

func selectContainer(containers []corev1.Container, name string) (int, error) {
	if name == "" {
		if len(containers) == 1 {
			return 0, nil
		}
		return 0, fmt.Errorf("CronJob has %d containers; set longhorn_pvc source 'container_name'", len(containers))
	}
	for index, container := range containers {
		if container.Name == name {
			return index, nil
		}
	}
	return 0, fmt.Errorf("container %q not found in CronJob", name)
}
