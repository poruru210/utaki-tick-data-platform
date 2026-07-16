package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"tick-data-platform/internal/ingest"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: tick-gateway <init|run|status|reconcile|verify-local|prune-local> --config <path>")
	}
	command := os.Args[1]
	configPath, err := flagValue(os.Args[2:], "--config")
	if err != nil {
		fatalf("%v", err)
	}
	config, err := ingest.LoadConfig(configPath)
	if err != nil {
		fatalf("load config: %v", err)
	}
	switch command {
	case "init":
		gateway, err := ingest.Open(config)
		if err != nil {
			fatalf("initialize gateway: %v", err)
		}
		if err := gateway.Close(); err != nil {
			fatalf("close initialized gateway: %v", err)
		}
		fmt.Printf("initialized gateway WAL=%s journal=%s\n", config.WALRoot, config.JournalPath)
	case "run":
		run(config)
	case "status", "reconcile", "verify-local":
		gateway, err := ingest.Open(config)
		if err != nil {
			fatalf("open gateway: %v", err)
		}
		status, statusErr := gateway.Status()
		closeErr := gateway.Close()
		if statusErr != nil {
			fatalf("read gateway status: %v", statusErr)
		}
		if closeErr != nil {
			fatalf("close gateway: %v", closeErr)
		}
		if err := json.NewEncoder(os.Stdout).Encode(status); err != nil {
			fatalf("write status: %v", err)
		}
	case "prune-local":
		if err := pruneLocal(config, os.Args[2:]); err != nil {
			fatalf("prune-local: %v", err)
		}
	default:
		fatalf("unknown command %q", command)
	}
}

func pruneLocal(config ingest.Config, args []string) error {
	return runPruneLocal(config, args)
}

func hasFlag(args []string, name string) bool {
	for _, arg := range args {
		if arg == name {
			return true
		}
	}
	return false
}

func run(config ingest.Config) {
	gateway, err := ingest.Open(config)
	if err != nil {
		fatalf("open gateway: %v", err)
	}
	defer func() { _ = gateway.Close() }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("tick-gateway listening on %s\n", config.ListenAddress)
	if err := gateway.ListenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fatalf("gateway stopped: %v", err)
	}
}

func flagValue(args []string, name string) (string, error) {
	for i, arg := range args {
		if arg == name && i+1 < len(args) && strings.TrimSpace(args[i+1]) != "" {
			return args[i+1], nil
		}
	}
	return "", fmt.Errorf("%s is required", name)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
