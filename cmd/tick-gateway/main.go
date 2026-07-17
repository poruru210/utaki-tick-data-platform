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
	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/publication"
	"tick-data-platform/internal/r2"
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
	configValue, err := appconfig.Load(configPath)
	if err != nil {
		fatalf("load config: %v", err)
	}
	config := ingest.ConfigFromGatewayConfig(configValue.Gateway())
	switch command {
	case "init":
		runtime, err := newLocalGatewayRuntime(configValue)
		if err != nil {
			fatalf("construct gateway runtime: %v", err)
		}
		if err := runtime.Start(context.Background()); err != nil {
			fatalf("start gateway runtime: %v", err)
		}
		if err := runtime.Stop(context.Background()); err != nil {
			fatalf("stop gateway runtime: %v", err)
		}
		fmt.Printf("initialized gateway WAL=%s journal=%s\n", config.WALRoot, config.JournalPath)
	case "status":
		status, err := readLocalStatus(configValue)
		if err != nil {
			fatalf("read application status: %v", err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(status); err != nil {
			fatalf("write status: %v", err)
		}
	case "reconcile", "verify-local":
		runtime, err := newLocalGatewayRuntime(configValue)
		if err != nil {
			fatalf("construct gateway runtime: %v", err)
		}
		if err := runtime.Start(context.Background()); err != nil {
			fatalf("start gateway runtime: %v", err)
		}
		status, statusErr := runtime.Gateway().Status()
		stopErr := runtime.Stop(context.Background())
		if statusErr != nil {
			fatalf("read gateway status: %v", statusErr)
		}
		if stopErr != nil {
			fatalf("stop gateway runtime: %v", stopErr)
		}
		if err := json.NewEncoder(os.Stdout).Encode(status); err != nil {
			fatalf("write status: %v", err)
		}
	case "prune-local":
		if err := pruneLocal(configValue, os.Args[2:]); err != nil {
			fatalf("prune-local: %v", err)
		}
	default:
		fatalf("unknown command %q", command)
	}
}

func readLocalStatus(configValue appconfig.Config) (operations.ApplicationStatus, error) {
	if configValue.Publication.CatalogPath == "" || configValue.Publication.RemoteJournalPath == "" {
		return operations.ApplicationStatus{}, fmt.Errorf("status requires publication catalog_path and remote_journal_path")
	}
	config := ingest.ConfigFromGatewayConfig(configValue.Gateway())
	runtime, err := newLocalGatewayRuntime(configValue)
	if err != nil {
		return operations.ApplicationStatus{}, fmt.Errorf("construct gateway runtime: %w", err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		return operations.ApplicationStatus{}, fmt.Errorf("start gateway runtime: %w", err)
	}
	catalog, err := publication.NewCatalog(configValue.Publication.CatalogPath)
	if err != nil {
		_ = runtime.Stop(context.Background())
		return operations.ApplicationStatus{}, err
	}
	if err := catalog.Start(context.Background()); err != nil {
		_ = runtime.Stop(context.Background())
		return operations.ApplicationStatus{}, err
	}
	remoteJournal, err := r2.NewPublicationJournal(configValue.Publication.RemoteJournalPath)
	if err != nil {
		_ = catalog.Stop(context.Background())
		_ = runtime.Stop(context.Background())
		return operations.ApplicationStatus{}, err
	}
	if err := remoteJournal.Start(context.Background()); err != nil {
		_ = catalog.Stop(context.Background())
		_ = runtime.Stop(context.Background())
		return operations.ApplicationStatus{}, err
	}
	disk, err := ingest.NewDiskStateMachine(config.WALRoot, ingest.DiskWatermarks{
		HighFreeBytes: config.DiskHighFreeBytes, CriticalFreeBytes: config.DiskCriticalFreeBytes,
		EmergencyFreeBytes: config.DiskEmergencyFreeBytes,
		MaxPendingSegments: configValue.Publication.MaxPendingSegments,
		MaxPendingBytes:    configValue.Publication.MaxPendingBytes,
	}, ingest.OSDiskUsageProvider{})
	if err != nil {
		_ = remoteJournal.Stop(context.Background())
		_ = catalog.Stop(context.Background())
		_ = runtime.Stop(context.Background())
		return operations.ApplicationStatus{}, err
	}
	service, err := app.NewLocalStatusService(runtime.Gateway(), catalog, remoteJournal, disk)
	if err != nil {
		_ = remoteJournal.Stop(context.Background())
		_ = catalog.Stop(context.Background())
		_ = runtime.Stop(context.Background())
		return operations.ApplicationStatus{}, err
	}
	status, statusErr := service.Snapshot(context.Background())
	remoteStopErr := remoteJournal.Stop(context.Background())
	catalogStopErr := catalog.Stop(context.Background())
	gatewayStopErr := runtime.Stop(context.Background())
	if statusErr != nil {
		return operations.ApplicationStatus{}, statusErr
	}
	if remoteStopErr != nil {
		return operations.ApplicationStatus{}, fmt.Errorf("stop remote journal: %w", remoteStopErr)
	}
	if catalogStopErr != nil {
		return operations.ApplicationStatus{}, fmt.Errorf("stop publication catalog: %w", catalogStopErr)
	}
	if gatewayStopErr != nil {
		return operations.ApplicationStatus{}, fmt.Errorf("stop gateway runtime: %w", gatewayStopErr)
	}
	return status, nil
}

func newLocalGatewayRuntime(configValue appconfig.Config) (*app.LocalGatewayRuntime, error) {
	config := ingest.ConfigFromGatewayConfig(configValue.Gateway())
	return app.NewLocalGatewayRuntime(config, ingest.DiskWatermarks{
		HighFreeBytes:      config.DiskHighFreeBytes,
		CriticalFreeBytes:  config.DiskCriticalFreeBytes,
		EmergencyFreeBytes: config.DiskEmergencyFreeBytes,
		MaxPendingSegments: configValue.Publication.MaxPendingSegments,
		MaxPendingBytes:    configValue.Publication.MaxPendingBytes,
	})
}

func pruneLocal(configValue appconfig.Config, args []string) error {
	return runPruneLocal(configValue, args)
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
