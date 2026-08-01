package kube

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type reconciledRun struct {
	active         bool
	unsafe         bool
	latestActivity time.Time
	resources      []metav1.Object
	jobs           []*batchv1.Job
	secrets        []*corev1.Secret
	pvcs           []*corev1.PersistentVolumeClaim
	snapshots      []*unstructured.Unstructured
	contents       []*unstructured.Unstructured
}

const (
	staleJobCleanupTimeout      = time.Duration(ChildJobTerminationGraceSeconds)*time.Second + 15*time.Second
	staleResourceCleanupTimeout = 2 * time.Minute
)

func ReconcileStaleRuns(ctx context.Context, clients *Clients, runnerNamespace string, sourceNamespaces []string, staleAfter time.Duration) error {
	if clients == nil || clients.Core == nil || clients.Dynamic == nil {
		return fmt.Errorf("Kubernetes clients are required for stale-run reconciliation")
	}
	if staleAfter <= 0 {
		return fmt.Errorf("positive stale-run age is required")
	}
	runnerScope := RunnerScope(runnerNamespace)
	selector := ManagedByLabel + "=" + ManagedByLabelValue + "," + RunnerScopeLabel + "=" + runnerScope
	runs := make(map[string]*reconciledRun)
	add := func(object metav1.Object) *reconciledRun {
		labels := object.GetLabels()
		if labels[ManagedByLabel] != ManagedByLabelValue || labels[RunnerScopeLabel] != runnerScope {
			return nil
		}
		runID := labels[RunLabel]
		if runID == "" {
			return nil
		}
		run := runs[runID]
		if run == nil {
			run = &reconciledRun{}
			runs[runID] = run
		}
		run.resources = append(run.resources, object)
		return run
	}

	if runnerNamespace == "" {
		return fmt.Errorf("stale-run reconciliation runner namespace is empty")
	}
	listOptions := metav1.ListOptions{LabelSelector: selector}
	jobs, err := clients.Core.BatchV1().Jobs(runnerNamespace).List(ctx, listOptions)
	if err != nil {
		return fmt.Errorf("list managed Jobs in %s: %w", runnerNamespace, err)
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if run := add(job); run != nil {
			run.jobs = append(run.jobs, job)
			run.active = run.active || job.Status.Active > 0
			for _, condition := range job.Status.Conditions {
				if condition.Status != corev1.ConditionTrue || (condition.Type != batchv1.JobComplete && condition.Type != batchv1.JobFailed) {
					continue
				}
				if condition.LastTransitionTime.IsZero() {
					run.unsafe = true
					continue
				}
				if condition.LastTransitionTime.Time.After(run.latestActivity) {
					run.latestActivity = condition.LastTransitionTime.Time
				}
			}
		}
	}
	secrets, err := clients.Core.CoreV1().Secrets(runnerNamespace).List(ctx, listOptions)
	if err != nil {
		return fmt.Errorf("list managed Secrets in %s: %w", runnerNamespace, err)
	}
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if run := add(secret); run != nil {
			run.secrets = append(run.secrets, secret)
		}
	}
	pvcs, err := clients.Core.CoreV1().PersistentVolumeClaims(runnerNamespace).List(ctx, listOptions)
	if err != nil {
		return fmt.Errorf("list managed PVCs in %s: %w", runnerNamespace, err)
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if run := add(pvc); run != nil {
			run.pvcs = append(run.pvcs, pvc)
		}
	}

	snapshotNamespaces := make([]string, 0, len(sourceNamespaces)+1)
	snapshotNamespaces = append(snapshotNamespaces, runnerNamespace)
	snapshotNamespaces = append(snapshotNamespaces, sourceNamespaces...)
	seenNamespaces := make(map[string]struct{}, len(snapshotNamespaces))
	for _, namespace := range snapshotNamespaces {
		if namespace == "" {
			return fmt.Errorf("stale-run reconciliation source namespace is empty")
		}
		if _, seen := seenNamespaces[namespace]; seen {
			continue
		}
		seenNamespaces[namespace] = struct{}{}
		snapshots, err := clients.Dynamic.Resource(VolumeSnapshotGVR).Namespace(namespace).List(ctx, listOptions)
		if err != nil {
			return fmt.Errorf("list managed VolumeSnapshots in %s: %w", namespace, err)
		}
		for i := range snapshots.Items {
			snapshot := &snapshots.Items[i]
			if run := add(snapshot); run != nil {
				run.snapshots = append(run.snapshots, snapshot)
			}
		}
	}
	contents, err := clients.Dynamic.Resource(VolumeSnapshotContentGVR).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list managed VolumeSnapshotContents: %w", err)
	}
	for i := range contents.Items {
		content := &contents.Items[i]
		run := add(content)
		if run == nil {
			continue
		}
		if !aliasContentTargetsNamespace(content, runnerNamespace) {
			run.unsafe = true
			continue
		}
		run.contents = append(run.contents, content)
	}

	cutoff := time.Now().Add(-staleAfter)
	runIDs := make([]string, 0, len(runs))
	for runID := range runs {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		run := runs[runID]
		if run.active || run.unsafe || !runIsOlderThan(run, cutoff) {
			continue
		}
		if err := reconcileAbandonedRun(ctx, clients, runnerNamespace, runID, run); err != nil {
			return fmt.Errorf("reconcile abandoned run %q: %w", runID, err)
		}
	}
	return nil
}

func reconcileAbandonedRun(ctx context.Context, clients *Clients, runnerNamespace, runID string, run *reconciledRun) error {
	return reconcileAbandonedRunWithTimeouts(ctx, clients, runnerNamespace, runID, run, staleJobCleanupTimeout, staleResourceCleanupTimeout)
}

func reconcileAbandonedRunWithTimeouts(ctx context.Context, clients *Clients, runnerNamespace, runID string, run *reconciledRun, jobTimeout, resourceTimeout time.Duration) error {
	if jobTimeout <= 0 || resourceTimeout <= 0 {
		return fmt.Errorf("positive stale Job and resource cleanup timeouts are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	parentCtx := ctx
	jobCtx, cancelJobs := context.WithTimeout(parentCtx, jobTimeout)
	defer cancelJobs()
	ctx = jobCtx
	runnerScope := RunnerScope(runnerNamespace)
	sort.Slice(run.jobs, func(i, j int) bool {
		return run.jobs[i].Namespace+"/"+run.jobs[i].Name < run.jobs[j].Namespace+"/"+run.jobs[j].Name
	})
	for _, job := range run.jobs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := DeleteJobIfOwned(ctx, clients, job.Namespace, job.Name, runID, runnerScope); err != nil {
			return err
		}
	}
	cancelJobs()
	resourceCtx, cancelResources := context.WithTimeout(parentCtx, resourceTimeout)
	defer cancelResources()
	ctx = resourceCtx
	sort.Slice(run.secrets, func(i, j int) bool {
		return run.secrets[i].Namespace+"/"+run.secrets[i].Name < run.secrets[j].Namespace+"/"+run.secrets[j].Name
	})
	for _, secret := range run.secrets {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := DeleteSecretIfOwned(ctx, clients, secret.Namespace, secret.Name, runID, runnerScope); err != nil {
			return err
		}
	}
	sort.Slice(run.pvcs, func(i, j int) bool {
		return run.pvcs[i].Namespace+"/"+run.pvcs[i].Name < run.pvcs[j].Namespace+"/"+run.pvcs[j].Name
	})
	for _, pvc := range run.pvcs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := DeletePVCIfOwned(ctx, clients, pvc.Namespace, pvc.Name, runID, runnerScope); err != nil {
			return err
		}
	}
	aliasSnapshots := make([]*unstructured.Unstructured, 0, len(run.snapshots))
	sourceSnapshots := make([]*unstructured.Unstructured, 0, len(run.snapshots))
	for _, snapshot := range run.snapshots {
		if strings.HasSuffix(snapshot.GetName(), "-alias-snap") {
			aliasSnapshots = append(aliasSnapshots, snapshot)
		} else {
			sourceSnapshots = append(sourceSnapshots, snapshot)
		}
	}
	sort.Slice(aliasSnapshots, func(i, j int) bool {
		return aliasSnapshots[i].GetNamespace()+"/"+aliasSnapshots[i].GetName() < aliasSnapshots[j].GetNamespace()+"/"+aliasSnapshots[j].GetName()
	})
	for _, snapshot := range aliasSnapshots {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := DeleteVolumeSnapshotIfOwned(ctx, clients, snapshot.GetNamespace(), snapshot.GetName(), runID, runnerScope); err != nil {
			return err
		}
	}
	sort.Slice(run.contents, func(i, j int) bool { return run.contents[i].GetName() < run.contents[j].GetName() })
	for _, content := range run.contents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !aliasContentTargetsNamespace(content, runnerNamespace) {
			continue
		}
		if err := DeleteVolumeSnapshotContentIfOwned(ctx, clients, content.GetName(), runID, runnerScope, runnerNamespace); err != nil {
			return err
		}
	}
	sort.Slice(sourceSnapshots, func(i, j int) bool {
		return sourceSnapshots[i].GetNamespace()+"/"+sourceSnapshots[i].GetName() < sourceSnapshots[j].GetNamespace()+"/"+sourceSnapshots[j].GetName()
	})
	for _, snapshot := range sourceSnapshots {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := DeleteVolumeSnapshotIfOwned(ctx, clients, snapshot.GetNamespace(), snapshot.GetName(), runID, runnerScope); err != nil {
			return err
		}
	}
	return nil
}

func aliasContentTargetsNamespace(content *unstructured.Unstructured, namespace string) bool {
	refNamespace, found, err := unstructured.NestedString(content.Object, "spec", "volumeSnapshotRef", "namespace")
	return err == nil && found && refNamespace == namespace
}

func runIsOlderThan(run *reconciledRun, cutoff time.Time) bool {
	if len(run.resources) == 0 {
		return false
	}
	if !run.latestActivity.IsZero() && !run.latestActivity.Before(cutoff) {
		return false
	}
	for _, resource := range run.resources {
		created := resource.GetCreationTimestamp()
		if created.IsZero() || !created.Time.Before(cutoff) {
			return false
		}
	}
	return true
}
