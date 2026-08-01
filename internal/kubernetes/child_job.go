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

const (
	ChildJobTTLSeconds              int32 = 3 * 24 * 60 * 60
	ChildJobTerminationGraceSeconds int64 = 150
	ParentMinimumGraceSeconds       int64 = 300
	ChildConfigSecretKey                  = "config-b64"
)

type ChildJobOptions struct {
	Name                  string
	RunID                 string
	CronJob               *batchv1.CronJob
	ContainerName         string
	TempPVCName           string
	MountPath             string
	ChildConfigSecretName string
	ResticHost            string
	RunnerScope           string
}

func BuildChildJob(opts ChildJobOptions) (*batchv1.Job, error) {
	if opts.CronJob == nil {
		return nil, fmt.Errorf("CronJob is required")
	}
	if opts.Name == "" || opts.RunID == "" || opts.CronJob.Namespace == "" || opts.TempPVCName == "" || opts.MountPath == "" || opts.ChildConfigSecretName == "" || opts.ResticHost == "" {
		return nil, fmt.Errorf("child Job name, run ID, CronJob namespace, temporary PVC name, mount path, child config Secret name, and Restic host are required")
	}
	runnerScope := opts.RunnerScope
	if runnerScope == "" {
		runnerScope = RunnerScope(opts.CronJob.Namespace)
	}
	parentGrace := opts.CronJob.Spec.JobTemplate.Spec.Template.Spec.TerminationGracePeriodSeconds
	if parentGrace == nil || *parentGrace < ParentMinimumGraceSeconds {
		return nil, fmt.Errorf("parent CronJob Job template terminationGracePeriodSeconds must be at least %d seconds (120-second cleanup, 150-second child shutdown, and 30-second margin)", ParentMinimumGraceSeconds)
	}

	jobSpec := *opts.CronJob.Spec.JobTemplate.Spec.DeepCopy()
	disableServiceAccountTokens(&jobSpec.Template.Spec)
	for _, initContainer := range jobSpec.Template.Spec.InitContainers {
		if initContainer.RestartPolicy != nil && *initContainer.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			return nil, fmt.Errorf("CronJob uses native restartable init container %q; exact-once child Jobs require ordinary init containers", initContainer.Name)
		}
	}
	singleton := int32(1)
	zero := int32(0)
	falseValue := false
	jobSpec.Parallelism = &singleton
	jobSpec.Completions = &singleton
	jobSpec.BackoffLimit = &zero
	jobSpec.BackoffLimitPerIndex = nil
	jobSpec.MaxFailedIndexes = nil
	jobSpec.PodFailurePolicy = nil
	jobSpec.SuccessPolicy = nil
	jobSpec.CompletionMode = nil
	jobSpec.Suspend = &falseValue
	jobSpec.PodReplacementPolicy = nil
	jobSpec.ManagedBy = nil
	jobSpec.Template.Labels = maps.Clone(jobSpec.Template.Labels)
	if jobSpec.Template.Labels == nil {
		jobSpec.Template.Labels = make(map[string]string, 2)
	}
	jobSpec.Template.Labels[ManagedByLabel] = ManagedByLabelValue
	jobSpec.Template.Labels[RunLabel] = opts.RunID
	jobSpec.Template.Labels[RunnerScopeLabel] = runnerScope
	jobSpec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	childGrace := ChildJobTerminationGraceSeconds
	jobSpec.Template.Spec.TerminationGracePeriodSeconds = &childGrace
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
		return env.Name == config.EnvConfigBase64 || env.Name == "RESTIC_HOST"
	})
	container.Env = append([]corev1.EnvVar{{
		Name: config.EnvConfigBase64,
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: opts.ChildConfigSecretName},
			Key:                  ChildConfigSecretKey,
		}},
	}, {Name: "RESTIC_HOST", Value: opts.ResticHost}}, container.Env...)
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name: TempVolumeName, MountPath: opts.MountPath, ReadOnly: true,
	})

	labels := maps.Clone(opts.CronJob.Spec.JobTemplate.Labels)
	if labels == nil {
		labels = make(map[string]string, 2)
	}
	labels[ManagedByLabel] = ManagedByLabelValue
	labels[RunLabel] = opts.RunID
	labels[RunnerScopeLabel] = runnerScope
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

func disableServiceAccountTokens(podSpec *corev1.PodSpec) {
	falseValue := false
	podSpec.AutomountServiceAccountToken = &falseValue
	tokenVolumes := make(map[string]struct{})
	podSpec.Volumes = slices.DeleteFunc(podSpec.Volumes, func(volume corev1.Volume) bool {
		if volume.Projected == nil {
			return false
		}
		for _, source := range volume.Projected.Sources {
			if source.ServiceAccountToken != nil {
				tokenVolumes[volume.Name] = struct{}{}
				return true
			}
		}
		return false
	})
	removeTokenMounts := func(mounts []corev1.VolumeMount) []corev1.VolumeMount {
		return slices.DeleteFunc(mounts, func(mount corev1.VolumeMount) bool {
			_, remove := tokenVolumes[mount.Name]
			return remove
		})
	}
	for i := range podSpec.InitContainers {
		podSpec.InitContainers[i].VolumeMounts = removeTokenMounts(podSpec.InitContainers[i].VolumeMounts)
	}
	for i := range podSpec.Containers {
		podSpec.Containers[i].VolumeMounts = removeTokenMounts(podSpec.Containers[i].VolumeMounts)
	}
	for i := range podSpec.EphemeralContainers {
		podSpec.EphemeralContainers[i].VolumeMounts = removeTokenMounts(podSpec.EphemeralContainers[i].VolumeMounts)
	}
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
