package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

var sourceFields = map[SourceKind]map[string]struct{}{
	SourceDirectory: {"type": {}, "path": {}},
	SourceLVM:       {"type": {}, "vg_name": {}, "lv_name": {}},
	SourceLonghornPVC: {
		"type": {}, "pvc_name": {}, "namespace": {}, "snapshot_class": {},
		"storage_class": {}, "mount_path": {}, "container_name": {}, "timeout": {},
	},
}

var destinationFields = map[DestinationKind]map[string]struct{}{
	DestinationRestic: {"type": {}, "repo": {}, "keep_last": {}, "group_by": {}},
}

type rawConfig struct {
	Backups []rawBackup `yaml:"backups"`
}

type rawBackup struct {
	Source      yaml.Node `yaml:"source"`
	Destination yaml.Node `yaml:"destination"`
}

type rawType struct {
	Type string `yaml:"type"`
}

type rawDirectory struct {
	Type string `yaml:"type"`
	Path string `yaml:"path"`
}

type rawLVM struct {
	Type   string `yaml:"type"`
	VGName string `yaml:"vg_name"`
	LVName string `yaml:"lv_name"`
}

type rawLonghornPVC struct {
	Type          string `yaml:"type"`
	PVCName       string `yaml:"pvc_name"`
	Namespace     string `yaml:"namespace"`
	SnapshotClass string `yaml:"snapshot_class"`
	StorageClass  string `yaml:"storage_class"`
	MountPath     string `yaml:"mount_path"`
	ContainerName string `yaml:"container_name"`
	Timeout       string `yaml:"timeout"`
}

type rawRestic struct {
	Type     string      `yaml:"type"`
	Repo     string      `yaml:"repo"`
	KeepLast optionalInt `yaml:"keep_last"`
	GroupBy  string      `yaml:"group_by"`
}

type optionalInt struct {
	value int
	set   bool
}

func (v *optionalInt) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected integer scalar at line %d", node.Line)
	}
	value, err := strconv.Atoi(node.Value)
	if err != nil {
		return fmt.Errorf("parse integer %q at line %d: %w", node.Value, node.Line, err)
	}
	v.value = value
	v.set = true
	return nil
}

// Load reads, decodes, and validates a configuration file.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	return Decode(bytes.NewReader(data), path)
}

// Decode decodes and validates one YAML configuration document.
func Decode(r io.Reader, source string) (Config, error) {
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)

	var raw rawConfig
	if err := decoder.Decode(&raw); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", source, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("decode config %q: multiple YAML documents are not supported", source)
		}
		return Config{}, fmt.Errorf("decode config %q: %w", source, err)
	}

	cfg := Config{Backups: make([]Backup, 0, len(raw.Backups))}
	for i, rawBackup := range raw.Backups {
		sourceSpec, err := decodeSource(rawBackup.Source)
		if err != nil {
			return Config{}, fmt.Errorf("decode config %q backup %d source: %w", source, i+1, err)
		}
		destinationSpec, err := decodeDestination(rawBackup.Destination)
		if err != nil {
			return Config{}, fmt.Errorf("decode config %q backup %d destination: %w", source, i+1, err)
		}
		cfg.Backups = append(cfg.Backups, Backup{Source: sourceSpec, Destination: destinationSpec})
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", source, err)
	}
	return cfg, nil
}

func decodeSource(node yaml.Node) (Source, error) {
	var discriminator rawType
	if err := decodeMapping(node, nil, false, "source", &discriminator); err != nil {
		return Source{}, err
	}
	kind := SourceKind(discriminator.Type)
	allowed, ok := sourceFields[kind]
	if !ok {
		return Source{}, fmt.Errorf("unsupported source type %q", discriminator.Type)
	}

	switch kind {
	case SourceDirectory:
		var raw rawDirectory
		if err := decodeMapping(node, allowed, true, "source", &raw); err != nil {
			return Source{}, err
		}
		return Source{Kind: kind, Directory: &DirectorySource{Path: raw.Path}}, nil
	case SourceLVM:
		var raw rawLVM
		if err := decodeMapping(node, allowed, true, "source", &raw); err != nil {
			return Source{}, err
		}
		return Source{Kind: kind, LVM: &LVMSource{VGName: raw.VGName, LVName: raw.LVName}}, nil
	case SourceLonghornPVC:
		var raw rawLonghornPVC
		if err := decodeMapping(node, allowed, true, "source", &raw); err != nil {
			return Source{}, err
		}
		timeout := DefaultLonghornPVCTimeout
		if raw.Timeout != "" {
			parsed, err := time.ParseDuration(raw.Timeout)
			if err != nil {
				return Source{}, fmt.Errorf("parse timeout %q: %w", raw.Timeout, err)
			}
			timeout = parsed
		}
		mountPath := raw.MountPath
		if mountPath == "" {
			mountPath = DefaultLonghornPVCMountPath
		}
		containerName := raw.ContainerName
		if containerName == "" {
			containerName = DefaultLonghornPVCContainerName
		}
		return Source{Kind: kind, LonghornPVC: &LonghornPVCSource{
			PVCName:       raw.PVCName,
			Namespace:     raw.Namespace,
			SnapshotClass: raw.SnapshotClass,
			StorageClass:  raw.StorageClass,
			MountPath:     mountPath,
			ContainerName: containerName,
			Timeout:       timeout,
		}}, nil
	default:
		return Source{}, fmt.Errorf("unsupported source type %q", discriminator.Type)
	}
}

func decodeDestination(node yaml.Node) (Destination, error) {
	var discriminator rawType
	if err := decodeMapping(node, nil, false, "destination", &discriminator); err != nil {
		return Destination{}, err
	}
	kind := DestinationKind(discriminator.Type)
	allowed, ok := destinationFields[kind]
	if !ok {
		return Destination{}, fmt.Errorf("unsupported destination type %q", discriminator.Type)
	}

	var raw rawRestic
	if err := decodeMapping(node, allowed, true, "destination", &raw); err != nil {
		return Destination{}, err
	}
	keepLast := DefaultResticKeepLast
	if raw.KeepLast.set && raw.KeepLast.value != 0 {
		keepLast = raw.KeepLast.value
	}
	groupBy := raw.GroupBy
	if groupBy == "" {
		groupBy = DefaultResticGroupBy
	}
	return Destination{
		Kind:   kind,
		Restic: &ResticDestination{Repo: raw.Repo, KeepLast: keepLast, GroupBy: groupBy},
	}, nil
}

func decodeMapping(node yaml.Node, allowed map[string]struct{}, rejectUnknown bool, subject string, target any) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", subject)
	}
	if rejectUnknown {
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			if _, ok := allowed[key.Value]; !ok {
				return fmt.Errorf("unknown %s field %q at line %d", subject, key.Value, key.Line)
			}
		}
	}
	if err := node.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", subject, err)
	}
	return nil
}
