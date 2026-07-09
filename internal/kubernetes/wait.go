package kube

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
)

func WaitVolumeSnapshotReady(ctx context.Context, clients *Clients, namespace, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		snapshot, err := clients.Dynamic.Resource(VolumeSnapshotGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
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

func DeleteJobIfOwned(ctx context.Context, clients *Clients, namespace, name, runID string) error {
	job, err := clients.Core.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("Job %s/%s", namespace, name)
	if err := verifyRunOwnership(resource, job.Labels, runID); err != nil {
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

func DeletePVCIfOwned(ctx context.Context, clients *Clients, namespace, name, runID string) error {
	pvc, err := clients.Core.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("PVC %s/%s", namespace, name)
	if err := verifyRunOwnership(resource, pvc.Labels, runID); err != nil {
		return err
	}
	if err := clients.Core.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return waitForDeletion(ctx, resource, func(ctx context.Context) error {
		_, err := clients.Core.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

func DeleteVolumeSnapshotIfOwned(ctx context.Context, clients *Clients, namespace, name, runID string) error {
	resourceClient := clients.Dynamic.Resource(VolumeSnapshotGVR).Namespace(namespace)
	snapshot, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("VolumeSnapshot %s/%s", namespace, name)
	if err := verifyRunOwnership(resource, snapshot.GetLabels(), runID); err != nil {
		return err
	}
	if err := resourceClient.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return waitForDeletion(ctx, resource, func(ctx context.Context) error {
		_, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

func DeleteVolumeSnapshotContentIfOwned(ctx context.Context, clients *Clients, name, runID string) error {
	resourceClient := clients.Dynamic.Resource(VolumeSnapshotContentGVR)
	content, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	resource := fmt.Sprintf("VolumeSnapshotContent %s", name)
	if err := verifyRunOwnership(resource, content.GetLabels(), runID); err != nil {
		return err
	}
	if err := resourceClient.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
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

func waitForDeletion(ctx context.Context, resource string, get func(context.Context) error) error {
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		err := get(ctx)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("waiting for %s deletion: %w", resource, err)
	}
	return nil
}
