// Package config decodes and validates home-backup configuration.
package config

import "time"

// SourceKind identifies a configured backup source.
type SourceKind string

const (
	// SourceDirectory selects a directory source.
	SourceDirectory SourceKind = "directory"
	// SourceLVM selects an LVM source.
	SourceLVM SourceKind = "lvm"
	// SourceLonghornPVC selects a Longhorn-backed Kubernetes PVC source.
	SourceLonghornPVC SourceKind = "longhorn_pvc"
)

// DestinationKind identifies a configured backup destination.
type DestinationKind string

const (
	// DestinationRestic selects a Restic destination.
	DestinationRestic DestinationKind = "restic"
)

const (
	// EnvConfigBase64 contains an inline base64-encoded YAML configuration.
	EnvConfigBase64 = "HOME_BACKUP_CONFIG_B64"
	// DefaultResticKeepLast is the default number of snapshots retained.
	DefaultResticKeepLast = 10
	// DefaultResticGroupBy is the default Restic snapshot grouping.
	DefaultResticGroupBy = "host"
	// DefaultLonghornPVCMountPath is where the restored snapshot is mounted in the child Job.
	DefaultLonghornPVCMountPath = "/backup-source"
	// DefaultLonghornPVCTimeout bounds each Kubernetes readiness wait.
	DefaultLonghornPVCTimeout = 30 * time.Minute
	// DefaultLonghornPVCContainerName selects the home-backup container in the copied CronJob spec.
	DefaultLonghornPVCContainerName = "home-backup"
)

// Config contains all configured backup jobs.
type Config struct {
	Backups []Backup
}

// Backup describes one source-to-destination backup.
type Backup struct {
	Source      Source
	Destination Destination
}

// Source is a typed source variant.
type Source struct {
	Kind        SourceKind
	Directory   *DirectorySource
	LVM         *LVMSource
	LonghornPVC *LonghornPVCSource
}

// DirectorySource configures a directory source.
type DirectorySource struct {
	Path string
}

// LVMSource configures an LVM source.
type LVMSource struct {
	VGName string
	LVName string
}

// LonghornPVCSource configures snapshot-and-restore orchestration for a Kubernetes PVC.
type LonghornPVCSource struct {
	PVCName       string
	Namespace     string
	SnapshotClass string
	StorageClass  string
	MountPath     string
	ContainerName string
	Timeout       time.Duration
}

// Destination is a typed destination variant.
type Destination struct {
	Kind   DestinationKind
	Restic *ResticDestination
}

// ResticDestination configures a Restic destination.
type ResticDestination struct {
	Repo     string
	KeepLast int
	GroupBy  string
}
