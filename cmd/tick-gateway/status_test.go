package main

import (
	"testing"

	appconfig "tick-data-platform/internal/config"
	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/protocol"
)

func TestReadLocalStatusUsesVersionedApplicationStatusWithoutR2Credentials(t *testing.T) {
	root := t.TempDir()
	status, err := readLocalStatus(appconfig.Config{
		ListenAddress:          "127.0.0.1:0",
		GatewayInstanceID:      "gateway-status-test",
		WALRoot:                root + "/wal",
		RawOutboxRoot:          root + "/raw-outbox",
		JournalPath:            root + "/journal.sqlite",
		MaxFrameBytes:          protocol.MaxFrameBytes,
		MaxRecords:             protocol.MaxRecords,
		InitialBatchCount:      1,
		MaximumBatchCount:      1,
		DenseBoundaryHardCap:   1,
		SessionLeaseTimeoutMS:  30000,
		HeartbeatIdleTimeoutMS: 60000,
		DiskHighFreeBytes:      512 << 20,
		DiskCriticalFreeBytes:  256 << 20,
		DiskEmergencyFreeBytes: 64 << 20,
		Publication: appconfig.PublicationConfig{
			CatalogPath:        root + "/catalog.sqlite",
			RemoteJournalPath:  root + "/remote.sqlite",
			MaxPendingSegments: 1,
			MaxPendingBytes:    1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.StatusVersion != operations.ApplicationStatusVersion || status.Overall != operations.OverallHealthy || status.Publication.RemoteAvailable != true {
		t.Fatalf("local application status = %+v", status)
	}
}
