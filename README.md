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
4. Copies the parent CronJob's complete `JobSpec`, adds the restored PVC as a read-only mount, and overrides the selected home-backup container's configuration through `HOME_BACKUP_CONFIG_B64`.
5. Creates the copied Job and waits for it to complete or fail.
6. Removes the temporary PVC and snapshot resources. Kubernetes automatically removes a completed or failed child Job after three days through `ttlSecondsAfterFinished`. If execution stops before the Job becomes terminal, home-backup deletes that Job first so it cannot outlive its temporary storage.

The child Job preserves the CronJob's containers, init containers, volumes, service account, scheduling settings, retry policy, and other Job settings. The configured `container_name` selects the container that receives the PVC mount and configuration override; it defaults to `home-backup`.

When Restic uses the default `group_by: host`, the CronJob must provide a stable host identity such as `RESTIC_HOST=backup-cronjobs`; otherwise Kubernetes' generated child Pod names create a new retention group on every run.

Set `HOME_BACKUP_POD_TEMPLATE_CRONJOB` to use a specific CronJob. Otherwise, home-backup detects it by following the current `Pod -> Job -> CronJob` owner chain. `HOME_BACKUP_POD_NAME` overrides current Pod detection, and `HOME_BACKUP_NAMESPACE` overrides the runner namespace for local execution.

The cluster must provide the CSI snapshot CRDs/controller and a Longhorn `VolumeSnapshotClass`. The source snapshot class should use `deletionPolicy: Delete`; the temporary cross-namespace alias content uses `Retain` because it references the same physical snapshot handle.
