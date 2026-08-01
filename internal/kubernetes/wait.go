package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	ChildLogCollectionTimeout   = 15 * time.Second
	ChildLogContainerLimitBytes = 256 * 1024
	ChildLogAggregateLimitBytes = 1024 * 1024
)

func WaitVolumeSnapshotReady(ctx context.Context, clients *Clients, namespace, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		snapshot, err := clients.Dynamic.Resource(VolumeSnapshotGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if isTransientKubernetesError(err) {
				return false, nil
			}
			return false, err
		}
		if message, found, err := unstructured.NestedString(snapshot.Object, "status", "error", "message"); err != nil {
			return false, fmt.Errorf("reading VolumeSnapshot %s/%s error status: %w", namespace, name, err)
		} else if found && message != "" {
			return false, fmt.Errorf("VolumeSnapshot %s/%s failed: %s", namespace, name, message)
		}
		ready, found, err := unstructured.NestedBool(snapshot.Object, "status", "readyToUse")
		if err != nil {
			return false, fmt.Errorf("reading VolumeSnapshot %s/%s ready status: %w", namespace, name, err)
		}
		return found && ready, nil
	})
}

func isTransientKubernetesError(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsInternalError(err) || apierrors.IsServiceUnavailable(err) {
		return true
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) && status.Status().Code >= 500 {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary()) {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func GetBoundVolumeSnapshotContent(ctx context.Context, clients *Clients, namespace, name string) (*unstructured.Unstructured, error) {
	snapshot, err := clients.Dynamic.Resource(VolumeSnapshotGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	contentName, found, err := unstructured.NestedString(snapshot.Object, "status", "boundVolumeSnapshotContentName")
	if err != nil {
		return nil, fmt.Errorf("reading VolumeSnapshot %s/%s bound content name: %w", namespace, name, err)
	}
	if !found || contentName == "" {
		return nil, fmt.Errorf("VolumeSnapshot %s/%s has no bound VolumeSnapshotContent", namespace, name)
	}
	content, err := clients.Dynamic.Resource(VolumeSnapshotContentGVR).Get(ctx, contentName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting VolumeSnapshotContent %s: %w", contentName, err)
	}
	refName, _, err := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "name")
	if err != nil {
		return nil, fmt.Errorf("reading VolumeSnapshotContent %s snapshot reference name: %w", contentName, err)
	}
	refNamespace, _, err := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
	if err != nil {
		return nil, fmt.Errorf("reading VolumeSnapshotContent %s snapshot reference namespace: %w", contentName, err)
	}
	refUID, _, err := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "uid")
	if err != nil {
		return nil, fmt.Errorf("reading VolumeSnapshotContent %s snapshot reference UID: %w", contentName, err)
	}
	snapshotUID := string(snapshot.GetUID())
	if snapshotUID == "" {
		return nil, fmt.Errorf("VolumeSnapshot %s/%s has no UID", namespace, name)
	}
	if refName != snapshot.GetName() || refNamespace != snapshot.GetNamespace() || refUID != snapshotUID {
		return nil, fmt.Errorf("VolumeSnapshotContent %s does not reference VolumeSnapshot %s/%s UID %s", contentName, namespace, name, snapshotUID)
	}
	return content, nil
}

func WaitJobFinished(ctx context.Context, clients *Clients, namespace, name string, timeout time.Duration) (bool, error) {
	terminal := false
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		job, err := clients.Core.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if isTransientKubernetesError(err) {
				return false, nil
			}
			return false, err
		}
		for _, condition := range job.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			switch condition.Type {
			case batchv1.JobComplete:
				terminal = true
				return true, nil
			case batchv1.JobFailed:
				terminal = true
				return false, fmt.Errorf("child backup Job %s/%s failed: %s %s", namespace, name, condition.Reason, condition.Message)
			}
		}
		return false, nil
	})
	return terminal, err
}

func DeleteJobIfOwned(ctx context.Context, clients *Clients, namespace, name, runID, runnerScope string) error {
	job, err := clients.Core.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("Job %s/%s", namespace, name)
	if err := verifyRunnerOwnership(resource, job.Labels, runID, runnerScope); err != nil {
		return err
	}
	uid := job.UID
	propagation := metav1.DeletePropagationForeground
	if err := clients.Core.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
		Preconditions:     &metav1.Preconditions{UID: &uid},
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return waitForDeletion(ctx, resource, func(ctx context.Context) error {
		_, err := clients.Core.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

func JobPodLogsIfOwned(ctx context.Context, clients *Clients, namespace, name, runID string) (string, error) {
	return jobPodLogsIfOwned(ctx, clients, namespace, name, runID, ChildLogCollectionTimeout, ChildLogAggregateLimitBytes, ChildLogContainerLimitBytes)
}

func jobPodLogsIfOwned(ctx context.Context, clients *Clients, namespace, name, runID string, timeout time.Duration, aggregateLimit, containerLimit int64) (string, error) {
	if clients.PodLogs == nil {
		return "", errors.New("Kubernetes Pod log reader is not configured")
	}
	if timeout <= 0 || aggregateLimit <= 0 || containerLimit <= 0 {
		return "", errors.New("positive child log timeout and byte limits are required")
	}
	logCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	job, err := clients.Core.BatchV1().Jobs(namespace).Get(logCtx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	resource := fmt.Sprintf("Job %s/%s", namespace, name)
	if err := verifyRunOwnership(resource, job.Labels, runID); err != nil {
		return "", err
	}
	pods, err := clients.Core.CoreV1().Pods(namespace).List(logCtx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", batchv1.JobNameLabel, name),
	})
	if err != nil {
		return "", err
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	var output strings.Builder
	var readErrs []error
	writeBounded := func(text string) bool {
		remaining := aggregateLimit - int64(output.Len())
		if remaining <= 0 {
			return false
		}
		if int64(len(text)) > remaining {
			output.WriteString(text[:remaining])
			return false
		}
		output.WriteString(text)
		return true
	}
	collectionFull := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		podResource := fmt.Sprintf("Pod %s/%s", namespace, pod.Name)
		owner, err := controllerOwner(pod.OwnerReferences, "Job")
		if err != nil || owner.Name != job.Name || owner.UID != job.UID {
			return output.String(), fmt.Errorf("refusing to read logs from %s: Pod is not controlled by %s", podResource, resource)
		}
		containers := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
		for _, container := range containers {
			header := fmt.Sprintf("== pod %s container %s ==\n", pod.Name, container.Name)
			if !writeBounded(header) {
				readErrs = append(readErrs, fmt.Errorf("child backup Job logs truncated at aggregate limit of %d bytes", aggregateLimit))
				collectionFull = true
				break
			}
			remaining := aggregateLimit - int64(output.Len())
			maxBytes := min(containerLimit, remaining)
			requestLimit := maxBytes + 1
			options := corev1.PodLogOptions{Container: container.Name, LimitBytes: &requestLimit}
			logs, truncated, err := clients.PodLogs(logCtx, namespace, pod.Name, container.Name, options, maxBytes)
			if !writeBounded(logs) {
				truncated = true
			}
			if truncated {
				readErrs = append(readErrs, fmt.Errorf("logs for %s container %s truncated at %d bytes", podResource, container.Name, maxBytes))
			}
			if err != nil {
				readErrs = append(readErrs, fmt.Errorf("read logs for %s container %s: %w", podResource, container.Name, err))
			}
			if logs != "" && !strings.HasSuffix(logs, "\n") && !writeBounded("\n") {
				readErrs = append(readErrs, fmt.Errorf("child backup Job logs truncated at aggregate limit of %d bytes", aggregateLimit))
				collectionFull = true
			}
			if contextErr := logCtx.Err(); contextErr != nil {
				readErrs = append(readErrs, fmt.Errorf("collect child backup Job logs: %w", contextErr))
				collectionFull = true
				break
			}
		}
		if collectionFull {
			break
		}
	}
	return output.String(), errors.Join(readErrs...)
}

func DeleteSecretIfOwned(ctx context.Context, clients *Clients, namespace, name, runID, runnerScope string) error {
	secret, err := clients.Core.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("Secret %s/%s", namespace, name)
	if err := verifyRunnerOwnership(resource, secret.Labels, runID, runnerScope); err != nil {
		return err
	}
	uid := secret.UID
	if err := clients.Core.CoreV1().Secrets(namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return waitForDeletion(ctx, resource, func(ctx context.Context) error {
		_, err := clients.Core.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

func DeletePVCIfOwned(ctx context.Context, clients *Clients, namespace, name, runID, runnerScope string) error {
	pvc, err := clients.Core.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("PVC %s/%s", namespace, name)
	if err := verifyRunnerOwnership(resource, pvc.Labels, runID, runnerScope); err != nil {
		return err
	}
	uid := pvc.UID
	if err := clients.Core.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return waitForDeletion(ctx, resource, func(ctx context.Context) error {
		_, err := clients.Core.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

func DeleteVolumeSnapshotIfOwned(ctx context.Context, clients *Clients, namespace, name, runID, runnerScope string) error {
	resourceClient := clients.Dynamic.Resource(VolumeSnapshotGVR).Namespace(namespace)
	snapshot, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("VolumeSnapshot %s/%s", namespace, name)
	if err := verifyRunnerOwnership(resource, snapshot.GetLabels(), runID, runnerScope); err != nil {
		return err
	}
	uid := snapshot.GetUID()
	if err := resourceClient.Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return waitForDeletion(ctx, resource, func(ctx context.Context) error {
		_, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

func DeleteVolumeSnapshotContentIfOwned(ctx context.Context, clients *Clients, name, runID, runnerScope, targetNamespace string) error {
	resourceClient := clients.Dynamic.Resource(VolumeSnapshotContentGVR)
	content, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("VolumeSnapshotContent %s", name)
	if err := verifyRunnerOwnership(resource, content.GetLabels(), runID, runnerScope); err != nil {
		return err
	}
	if targetNamespace == "" {
		return fmt.Errorf("refusing to delete %s without a target namespace", resource)
	}
	refNamespace, found, err := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
	if err != nil {
		return fmt.Errorf("refusing to delete %s: reading volumeSnapshotRef.namespace: %w", resource, err)
	}
	if !found || refNamespace != targetNamespace {
		return fmt.Errorf("refusing to delete %s: volumeSnapshotRef.namespace %q does not match expected target namespace %q", resource, refNamespace, targetNamespace)
	}
	uid := content.GetUID()
	if err := resourceClient.Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return waitForDeletion(ctx, resource, func(ctx context.Context) error {
		_, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

func verifyRunOwnership(resource string, labels map[string]string, runID string) error {
	if runID == "" {
		return fmt.Errorf("refusing to delete %s without a run ID", resource)
	}
	if labels[ManagedByLabel] != ManagedByLabelValue || labels[RunLabel] != runID {
		return fmt.Errorf("refusing to delete %s: resource is not owned by run %q", resource, runID)
	}
	return nil
}

func verifyRunnerOwnership(resource string, labels map[string]string, runID, runnerScope string) error {
	if err := verifyRunOwnership(resource, labels, runID); err != nil {
		return err
	}
	if runnerScope == "" {
		return fmt.Errorf("refusing to delete %s without a runner scope", resource)
	}
	if labels[RunnerScopeLabel] != runnerScope {
		return fmt.Errorf("refusing to delete %s: resource runner scope %q does not match expected runner scope %q", resource, labels[RunnerScopeLabel], runnerScope)
	}
	return nil
}

func waitForDeletion(ctx context.Context, resource string, get func(context.Context) error) error {
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		err := get(ctx)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			if isTransientKubernetesError(err) {
				return false, nil
			}
			return false, err
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("waiting for %s deletion: %w", resource, err)
	}
	return nil
}
