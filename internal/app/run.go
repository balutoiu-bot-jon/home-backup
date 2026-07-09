// Package app composes and runs the home-backup application.
package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/ionutbalutoiu/home-backup/internal/backup"
	"github.com/ionutbalutoiu/home-backup/internal/command"
	"github.com/ionutbalutoiu/home-backup/internal/config"
)

type options struct {
	configPath string
	logLevel   slog.Level
}

type runtimeDependencies struct {
	newRunner func(*slog.Logger) commandRunner
	euid      func() int
	lookupEnv func(string) (string, bool)
}

// Run parses application arguments and executes all configured backups.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return run(ctx, args, stdout, stderr, runtimeDependencies{
		newRunner: func(logger *slog.Logger) commandRunner { return command.NewRunner(logger) },
		euid:      os.Geteuid,
		lookupEnv: os.LookupEnv,
	})
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, deps runtimeDependencies) error {
	opts, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(stdout, &slog.HandlerOptions{Level: opts.logLevel}))
	cfg, err := loadConfig(opts.configPath, deps.lookupEnv)
	if err != nil {
		return err
	}
	runner := deps.newRunner(logger)
	jobs, err := buildJobs(cfg, wiringDependencies{
		runner: runner, euid: deps.euid, longhornJob: newLonghornJobBuilder(),
	})
	if err != nil {
		return err
	}
	return backup.NewEngine(jobs...).Run(ctx)
}

type envLookup func(string) (string, bool)

func loadConfig(path string, lookupEnv envLookup) (config.Config, error) {
	if encoded, ok := lookupEnv(config.EnvConfigBase64); ok && strings.TrimSpace(encoded) != "" {
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return config.Config{}, fmt.Errorf("decode %s: %w", config.EnvConfigBase64, err)
		}
		return config.Decode(bytes.NewReader(data), config.EnvConfigBase64)
	}
	return config.Load(path)
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	flags := flag.NewFlagSet("home-backup", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var configPath string
	var levelText string
	flags.StringVar(&configPath, "config", "", "Path to the configuration file")
	flags.StringVar(&levelText, "log-level", "info", "Logging level")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if configPath == "" {
		return options{}, errors.New("usage: home-backup -config <config-path>")
	}
	level, err := parseLogLevel(levelText)
	if err != nil {
		return options{}, err
	}
	return options{configPath: configPath, logLevel: level}, nil
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q", value)
	}
}
