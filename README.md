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
6. Deletes the terminal child Pod before removing the ephemeral Secret, temporary PVC, and snapshot resources. The completed or failed Job metadata remains available for three days through `ttlSecondsAfterFinished`; removing its Pod first prevents Kubernetes PVC protection from blocking cleanup. If execution stops before the Job becomes terminal, home-backup deletes the whole Job first so it cannot outlive its temporary storage.

The child Job preserves the CronJob's containers, ordinary init containers, unrelated volumes, service-account name, and scheduling settings. Preserving the name keeps non-API behavior such as ServiceAccount image-pull secrets, but the child explicitly sets `automountServiceAccountToken: false` and removes inherited projected service-account-token volumes and mounts from regular, init, sidecar, and ephemeral containers. Unrelated projected volumes and application secrets, including Restic credentials, remain intact. The child deliberately forces one completion, one Pod, no Job or container retries, built-in Job management, and a 150-second child termination grace. Native restartable init containers are rejected because their retry/history behavior conflicts with the exact-once child contract. The configured `container_name` selects the container that receives the PVC mount and Secret-backed configuration override; it defaults to `home-backup`.

Longhorn destinations require `group_by` to include `host` (for example, `host` or `host,paths`); configurations such as `group_by: paths` are rejected before Kubernetes resources are created. Home-backup overrides `RESTIC_HOST` in the selected child container with a stable, collision-resistant identity derived from the original source namespace/PVC tuple. This keeps repeated runs in one retention group without allowing one PVC's retention policy to prune another PVC's backups.

The parent CronJob Job template **must** set `terminationGracePeriodSeconds: 300` or greater. Home-backup fails before creating snapshots when it is absent or smaller. The minimum covers the 120-second cleanup budget, the child's 150-second shutdown grace, and a 30-second margin:

```yaml
spec:
  jobTemplate:
    spec:
      template:
        spec:
          terminationGracePeriodSeconds: 300
```

Every run labels its temporary Job, child Pod, Secret, PVC, snapshots, and snapshot content with both a run ID and a durable runner scope derived from the runner namespace plus a collision-resistant hash. Before allocating a new run, home-backup lists and validates only resources in that runner scope and removes abandoned runs older than 24 hours in dependency order while preserving active, young, or foreign-runner resources. Cluster-scoped alias content is deleted only when its `volumeSnapshotRef.namespace` exactly matches the runner namespace. This startup sweep resumes cleanup after process death; it does not replace correct Pod termination grace.

Set `HOME_BACKUP_POD_TEMPLATE_CRONJOB` to use a specific CronJob. Otherwise, home-backup detects it by following the current `Pod -> Job -> CronJob` owner chain. `HOME_BACKUP_POD_NAME` overrides current Pod detection, and `HOME_BACKUP_NAMESPACE` overrides the runner namespace for local execution.

The cluster must provide the CSI snapshot CRDs/controller and a Longhorn `VolumeSnapshotClass`. The source snapshot class should use `deletionPolicy: Delete`; the temporary cross-namespace alias content uses `Retain` because it references the same physical snapshot handle.

The unit suite uses client-go fakes plus list/delete action and UID-precondition race assertions. Those fakes do not run the Job, TTL, PVC-protection, CSI snapshot, external-provisioner, admission, or RBAC controllers. A real Kubernetes cluster with the CSI snapshot controller and Longhorn installed remains the controller-level release gate for Pod cardinality, PVC protection/deletion, TTL behavior, ServiceAccount/RBAC, and physical snapshot/alias cleanup; the Docker integration script is only the existing directory-backup smoke test.
