package app

import (
	"context"
	"fmt"

	"github.com/ionutbalutoiu/home-backup/internal/backup"
	"github.com/ionutbalutoiu/home-backup/internal/command"
	"github.com/ionutbalutoiu/home-backup/internal/config"
	"github.com/ionutbalutoiu/home-backup/internal/directory"
	homekube "github.com/ionutbalutoiu/home-backup/internal/kubernetes"
	"github.com/ionutbalutoiu/home-backup/internal/longhorn"
	"github.com/ionutbalutoiu/home-backup/internal/lvm"
	"github.com/ionutbalutoiu/home-backup/internal/restic"
)

type commandRunner interface {
	Run(context.Context, command.Spec) (command.Result, error)
}

type longhornJobBuilder func(config.LonghornPVCSource, config.ResticDestination) (backup.Job, error)

type wiringDependencies struct {
	runner      commandRunner
	euid        func() int
	longhornJob longhornJobBuilder
}

func newLonghornJobBuilder() longhornJobBuilder {
	var cluster longhorn.Cluster
	var runnerNamespace string
	return func(source config.LonghornPVCSource, destination config.ResticDestination) (backup.Job, error) {
		if cluster == nil {
			namespace, err := homekube.CurrentNamespace()
			if err != nil {
				return nil, err
			}
			loadedCluster, err := homekube.NewLonghornCluster()
			if err != nil {
				return nil, err
			}
			runnerNamespace = namespace
			cluster = loadedCluster
		}
		return longhorn.NewJob(longhorn.Config{
			PVCName: source.PVCName, Namespace: source.Namespace,
			SnapshotClass: source.SnapshotClass, StorageClass: source.StorageClass,
			MountPath: source.MountPath, ContainerName: source.ContainerName,
			Timeout: source.Timeout,
		}, longhorn.ResticDestination{
			Repo: destination.Repo, KeepLast: destination.KeepLast, GroupBy: destination.GroupBy,
		}, cluster, runnerNamespace)
	}
}

func buildJobs(cfg config.Config, deps wiringDependencies) ([]backup.Job, error) {
	jobs := make([]backup.Job, 0, len(cfg.Backups))
	for i, spec := range cfg.Backups {
		if spec.Source.Kind == config.SourceLonghornPVC {
			if spec.Destination.Kind != config.DestinationRestic {
				return nil, fmt.Errorf("build backup %d destination: unsupported destination kind %q", i+1, spec.Destination.Kind)
			}
			if deps.longhornJob == nil {
				return nil, fmt.Errorf("build backup %d source: Longhorn job builder is unavailable", i+1)
			}
			job, err := deps.longhornJob(*spec.Source.LonghornPVC, *spec.Destination.Restic)
			if err != nil {
				return nil, fmt.Errorf("build backup %d Longhorn job: %w", i+1, err)
			}
			jobs = append(jobs, job)
			continue
		}
		source, err := buildSource(spec.Source, deps)
		if err != nil {
			return nil, fmt.Errorf("build backup %d source: %w", i+1, err)
		}
		destination, err := buildDestination(spec.Destination, deps)
		if err != nil {
			return nil, fmt.Errorf("build backup %d destination: %w", i+1, err)
		}
		jobs = append(jobs, backup.NewLocalJob(source, destination))
	}
	return jobs, nil
}

func buildSource(spec config.Source, deps wiringDependencies) (backup.Source, error) {
	switch spec.Kind {
	case config.SourceDirectory:
		return directory.NewSource(spec.Directory.Path), nil
	case config.SourceLVM:
		mounter := lvm.NewSystemMounter(deps.runner)
		return lvm.NewSource(lvm.Config{
			VGName: spec.LVM.VGName,
			LVName: spec.LVM.LVName,
		}, lvm.Dependencies{
			Runner:  deps.runner,
			Mounter: mounter,
			EUID:    deps.euid,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported source kind %q", spec.Kind)
	}
}

func buildDestination(spec config.Destination, deps wiringDependencies) (backup.Destination, error) {
	switch spec.Kind {
	case config.DestinationRestic:
		return restic.NewDestination(restic.Config{
			Repo:     spec.Restic.Repo,
			KeepLast: spec.Restic.KeepLast,
			GroupBy:  spec.Restic.GroupBy,
		}, deps.runner), nil
	default:
		return nil, fmt.Errorf("unsupported destination kind %q", spec.Kind)
	}
}
