package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ionutbalutoiu/home-backup/internal/config"
)

func TestParseOptionsRequiresConfig(t *testing.T) {
	_, err := parseOptions(nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "-config") {
		t.Fatalf("parseOptions() error = %v", err)
	}
}

func TestParseOptionsLogLevels(t *testing.T) {
	tests := []struct {
		value string
		want  slog.Level
	}{
		{value: "debug", want: slog.LevelDebug},
		{value: "info", want: slog.LevelInfo},
		{value: "warn", want: slog.LevelWarn},
		{value: "error", want: slog.LevelError},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			opts, err := parseOptions([]string{"-config", "/tmp/config.yaml", "-log-level", tt.value}, io.Discard)
			if err != nil {
				t.Fatalf("parseOptions() error = %v", err)
			}
			if opts.configPath != "/tmp/config.yaml" || opts.logLevel != tt.want {
				t.Fatalf("parseOptions() = %#v", opts)
			}
		})
	}
}

func TestParseOptionsRejectsInvalidLogLevel(t *testing.T) {
	_, err := parseOptions([]string{"-config", "/tmp/config.yaml", "-log-level", "verbose"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "invalid log level") {
		t.Fatalf("parseOptions() error = %v", err)
	}
}

func TestRunPrefersBase64Config(t *testing.T) {
	yaml := "backups:\n" +
		"  - source: {type: directory, path: " + t.TempDir() + "}\n" +
		"    destination: {type: restic, repo: /repo}\n"
	encoded := base64.StdEncoding.EncodeToString([]byte(yaml))
	runner := &fakeRunner{}

	err := run(context.Background(), []string{"-config", "/does/not/exist"}, io.Discard, &bytes.Buffer{}, runtimeDependencies{
		newRunner: func(*slog.Logger) commandRunner { return runner },
		euid:      func() int { return 1000 },
		lookupEnv: func(name string) (string, bool) {
			if name == config.EnvConfigBase64 {
				return encoded, true
			}
			return "", false
		},
	})
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if len(runner.specs) != 3 {
		t.Fatalf("commands = %#v, want Restic check, backup, and retention", runner.specs)
	}
}

func TestRunLoadsBuildsAndExecutesJobs(t *testing.T) {
	sourcePath := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	contents := "backups:\n" +
		"  - source:\n" +
		"      type: directory\n" +
		"      path: " + sourcePath + "\n" +
		"    destination:\n" +
		"      type: restic\n" +
		"      repo: /backups/restic\n"
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	runner := &fakeRunner{}
	err := run(context.Background(), []string{"-config", configPath}, io.Discard, &bytes.Buffer{}, runtimeDependencies{
		newRunner: func(*slog.Logger) commandRunner { return runner },
		euid:      func() int { return 1000 },
		lookupEnv: func(string) (string, bool) { return "", false },
	})
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if len(runner.specs) != 3 {
		t.Fatalf("commands = %#v, want Restic check, backup, and retention", runner.specs)
	}
}
