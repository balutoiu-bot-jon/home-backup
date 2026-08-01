# home-backup

`home-backup` is a Linux CLI for sequential directory, LVM snapshot, and Longhorn PVC backups to Restic.

## Usage

```sh
make build
RESTIC_PASSWORD='...' ./build/home-backup -config ./config.yaml
```

Directory backups can run as an ordinary user. LVM backups require root and the Linux LVM tools. Longhorn PVC backups require Kubernetes access and the supplied RBAC resources.

## Configuration

See [`examples/sample-config.yaml`](examples/sample-config.yaml).

Provide the Restic password and backend credentials through Restic's standard environment variables or configuration. Configuration decoding is strict; unknown fields and invalid values are rejected before any backup starts.

## Longhorn PVC backups

For a `longhorn_pvc` source, the running process:

1. Creates a CSI `VolumeSnapshot` beside the source PVC.
2. If the source is in another namespace, exposes the snapshot in the CronJob namespace through a temporary retained `VolumeSnapshotContent` and `VolumeSnapshot` alias.
3. Restores a temporary PVC in the CronJob namespace.
4. Copies the parent CronJob's `JobSpec`, normalizes it to one child Pod, adds the restored PVC as a read-only mount, and overrides the selected home-backup container's `HOME_BACKUP_CONFIG_B64` through an ephemeral Secret.
5. Creates the copied Job and waits for it to complete or fail, then captures current logs for every init and application container. Log collection has a dedicated 15-second timeout, a 256 KiB per-container cap, and a 1 MiB aggregate cap; partial logs and per-container errors are retained.
6. Deletes the child Job with foreground propagation after log capture and waits for Kubernetes to remove the Job and its Pod before removing the ephemeral Secret, temporary PVC, and snapshot resources. If any Kubernetes `CREATE` response is ambiguous or fails, cleanup fails closed without deleting that resource or any dependency beneath it; the labeled run is recovered later by the age-gated stale reconciler. This avoids racing a delayed API-server write. Normal acknowledged runs leave no child Job/Pod metadata behind and cannot be blocked by Kubernetes PVC protection.

The child Job preserves the CronJob's containers, ordinary init containers, unrelated volumes, service-account name, and scheduling settings. Preserving the name keeps non-API behavior such as ServiceAccount image-pull secrets, but the child explicitly sets `automountServiceAccountToken: false` and removes inherited service-account-token projection sources from regular, init, sidecar, and ephemeral containers. Mixed projected volumes retain their unrelated Secret, ConfigMap, and downwardAPI sources and mounts. Unrelated application secrets, including Restic credentials, remain intact. The child deliberately forces one completion, one Pod, no Job or container-level retries, `podReplacementPolicy: Failed`, built-in Job management, and a 150-second child termination grace. It clears inherited container restart policies/rules, manual selectors, and standard controller ownership/index labels so the child controller cannot rerun a container or adopt/count another controller's Pods. Native restartable init containers are rejected because their sidecar behavior conflicts with the single-execution child contract. The configured `container_name` selects the container that receives the PVC mount and Secret-backed configuration override; it defaults to `home-backup`.

Longhorn PVC orchestration requires Kubernetes 1.34 or an API server with `JobPodReplacementPolicy` enabled. Before allocating snapshots or other temporary resources, home-backup performs a server-side child-Job dry-run and requires the API response to retain `podReplacementPolicy: Failed`; unsupported clusters fail without leaving temporary resources.

Longhorn destinations require `group_by` to include `host` (for example, `host` or `host,paths`); configurations such as `group_by: paths` are rejected before Kubernetes resources are created. Home-backup overrides `RESTIC_HOST` in the selected child container with a stable, collision-resistant identity derived from the original source namespace/PVC tuple. This keeps repeated runs in one retention group without allowing one PVC's retention policy to prune another PVC's backups.

The parent CronJob Job template **must** set `terminationGracePeriodSeconds: 300` or greater. Home-backup fails before creating snapshots when it is absent or smaller. After bounded log capture when available, every child Job deletion (successful, failed, or interrupted) receives its own 165-second context: the child's 150-second Pod shutdown grace plus 15 seconds for foreground-deletion control-plane overhead. Log collection observes parent cancellation, so shutdown immediately advances to Job deletion rather than consuming its normal 15-second read timeout. Only after that Job and its owned Pod are gone does a fresh 120-second storage-cleanup context begin, leaving another 15 seconds of parent-process margin. The 300-second minimum therefore covers 120 seconds of storage cleanup, 150 seconds of child shutdown, and 30 seconds of total margin:

```yaml
spec:
  jobTemplate:
    spec:
      template:
        spec:
          terminationGracePeriodSeconds: 300
```

Every run labels its temporary Job, child Pod, Secret, PVC, snapshots, and snapshot content with both a run ID and a durable runner scope derived from the runner namespace plus a collision-resistant hash. Before allocating a new run, home-backup lists and validates only resources in that runner scope and removes abandoned runs older than 24 hours in dependency order while preserving active, young, recently terminal, malformed, or foreign-runner resources. A terminal Job's condition transition time resets the stale-age gate so a concurrent parent can finish log capture and deletion safely. Each configured Kubernetes readiness wait is capped at six hours, and the entire live lifecycle—including every Kubernetes request—is bounded to 20 hours. This leaves nearly four hours before the 24-hour stale threshold, in addition to the bounded final cleanup. Stale Job deletion has its own 165-second bound followed by a fresh 120-second dependency-cleanup bound. Cluster-scoped alias content is deleted only when its `volumeSnapshotRef.namespace` exactly matches the runner namespace. This startup sweep resumes cleanup after process death; it does not replace correct Pod termination grace.

Set `HOME_BACKUP_POD_TEMPLATE_CRONJOB` to use a specific CronJob. Otherwise, home-backup detects it by following the current `Pod -> Job -> CronJob` owner chain. `HOME_BACKUP_POD_NAME` overrides current Pod detection, and `HOME_BACKUP_NAMESPACE` overrides the runner namespace for local execution.

The cluster must provide the CSI snapshot CRDs/controller and a Longhorn `VolumeSnapshotClass`. The source snapshot class should use `deletionPolicy: Delete`; the temporary cross-namespace alias content uses `Retain` because it references the same physical snapshot handle.

The unit suite uses client-go fakes plus list/delete action, foreground-propagation, wait, and UID-precondition race assertions. Those fakes do not run the Job, garbage-collection, PVC-protection, CSI snapshot, external-provisioner, admission, or RBAC controllers. A real Kubernetes cluster with the CSI snapshot controller and Longhorn installed remains the controller-level release gate for Pod cardinality, foreground Job-to-Pod cascading, PVC protection/deletion, ServiceAccount/RBAC, and physical snapshot/alias cleanup; the Docker integration script is only the existing directory-backup smoke test.
