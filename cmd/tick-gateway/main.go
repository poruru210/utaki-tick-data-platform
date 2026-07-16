package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tick-data-platform/internal/app"
	appconfig "tick-data-platform/internal/config"
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
	switch command {
	case "run":
		configValue, err := appconfig.Load(configPath)
		if err != nil {
			fatalf("load config: %v", err)
		}
		run(configValue)
		return
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

func run(configValue appconfig.Config) {
	application, err := app.NewProductionApp(configValue)
	if err != nil {
		fatalf("create production application: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStart()
	if err := application.Start(startCtx); err != nil {
		fatalf("start production application: %v", err)
	}
	fmt.Printf("tick-gateway production application started\n")
	select {
	case <-ctx.Done():
	case <-application.Done():
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStop()
	if err := application.Stop(stopCtx); err != nil {
		fatalf("stop production application: %v", err)
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
