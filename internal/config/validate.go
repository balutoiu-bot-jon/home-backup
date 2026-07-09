package config

import (
	"errors"
	"fmt"
	"path"
	"strings"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

func (c Config) validate() error {
	if len(c.Backups) == 0 {
		return errors.New("configuration requires at least one backup")
	}
	for i, job := range c.Backups {
		if err := job.Source.validate(); err != nil {
			return fmt.Errorf("backup %d source: %w", i+1, err)
		}
		if err := job.Destination.validate(); err != nil {
			return fmt.Errorf("backup %d destination: %w", i+1, err)
		}
	}
	return nil
}

func (s Source) validate() error {
	switch s.Kind {
	case SourceDirectory:
		if s.Directory == nil || s.LVM != nil || s.LonghornPVC != nil {
			return errors.New("directory source variant is inconsistent")
		}
		if s.Directory.Path == "" {
			return errors.New("directory source path is required")
		}
	case SourceLVM:
		if s.LVM == nil || s.Directory != nil || s.LonghornPVC != nil {
			return errors.New("LVM source variant is inconsistent")
		}
		if s.LVM.VGName == "" {
			return errors.New("LVM source vg_name is required")
		}
		if s.LVM.LVName == "" {
			return errors.New("LVM source lv_name is required")
		}
	case SourceLonghornPVC:
		if s.LonghornPVC == nil || s.Directory != nil || s.LVM != nil {
			return errors.New("Longhorn PVC source variant is inconsistent")
		}
		if s.LonghornPVC.PVCName == "" {
			return errors.New("Longhorn PVC source pvc_name is required")
		}
		if s.LonghornPVC.SnapshotClass == "" {
			return errors.New("Longhorn PVC source snapshot_class is required")
		}
		if s.LonghornPVC.MountPath == "" {
			return errors.New("Longhorn PVC source mount_path is required")
		}
		if s.LonghornPVC.ContainerName == "" {
			return errors.New("Longhorn PVC source container_name is required")
		}
		if s.LonghornPVC.Namespace != "" {
			if err := validateDNS1123Label("namespace", s.LonghornPVC.Namespace); err != nil {
				return err
			}
		}
		for _, item := range []struct {
			field string
			value string
		}{
			{field: "pvc_name", value: s.LonghornPVC.PVCName},
			{field: "snapshot_class", value: s.LonghornPVC.SnapshotClass},
		} {
			if err := validateDNS1123Subdomain(item.field, item.value); err != nil {
				return err
			}
		}
		if s.LonghornPVC.StorageClass != "" {
			if err := validateDNS1123Subdomain("storage_class", s.LonghornPVC.StorageClass); err != nil {
				return err
			}
		}
		if err := validateDNS1123Label("container_name", s.LonghornPVC.ContainerName); err != nil {
			return err
		}
		if !path.IsAbs(s.LonghornPVC.MountPath) {
			return errors.New("Longhorn PVC source mount_path must be absolute")
		}
		if s.LonghornPVC.MountPath == "/" {
			return errors.New("Longhorn PVC source mount_path cannot be the filesystem root")
		}
		if path.Clean(s.LonghornPVC.MountPath) != s.LonghornPVC.MountPath {
			return errors.New("Longhorn PVC source mount_path must not contain backsteps, duplicate separators, or a trailing separator")
		}
		if s.LonghornPVC.Timeout <= 0 {
			return errors.New("Longhorn PVC source timeout must be greater than zero")
		}
	default:
		return fmt.Errorf("unsupported source type %q", s.Kind)
	}
	return nil
}

func validateDNS1123Subdomain(field, value string) error {
	if problems := k8svalidation.IsDNS1123Subdomain(value); len(problems) > 0 {
		return fmt.Errorf("Longhorn PVC source %s must be a valid DNS-1123 subdomain: %s", field, strings.Join(problems, "; "))
	}
	return nil
}

func validateDNS1123Label(field, value string) error {
	if problems := k8svalidation.IsDNS1123Label(value); len(problems) > 0 {
		return fmt.Errorf("Longhorn PVC source %s must be a valid DNS-1123 label: %s", field, strings.Join(problems, "; "))
	}
	return nil
}

func (d Destination) validate() error {
	if d.Kind != DestinationRestic {
		return fmt.Errorf("unsupported destination type %q", d.Kind)
	}
	if d.Restic == nil {
		return errors.New("restic destination variant is inconsistent")
	}
	if d.Restic.Repo == "" {
		return errors.New("restic destination repo is required")
	}
	if d.Restic.KeepLast < 0 {
		return errors.New("restic destination keep_last cannot be negative")
	}
	return nil
}
