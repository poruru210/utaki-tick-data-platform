package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"tick-data-platform/internal/ingest"
	"tick-data-platform/producers/fake"
	protocolv1 "tick-data-platform/protocol/v1/go"
)

// TestTenXIngestRate is intentionally opt-in. It produces a bounded
// verification record rather than turning the normal repository gate into a
// machine-dependent benchmark.
func TestTenXIngestRate(t *testing.T) {
	if os.Getenv("TICKDATA_ENABLE_LOAD") != "1" {
		t.Skip("set TICKDATA_ENABLE_LOAD=1 to run the explicit 10x load gate")
	}
	output := os.Getenv("TICKDATA_LOAD_OUTPUT")
	if output == "" {
		t.Fatal("TICKDATA_LOAD_OUTPUT is required when the load gate is enabled")
	}
	baselineRPS := loadUint(t, "TICKDATA_LOAD_BASELINE_RPS", 10, 10_000)
	durationMS := loadUint(t, "TICKDATA_LOAD_DURATION_MS", 2_000, 60_000)
	maxHeapBytes := loadUint(t, "TICKDATA_LOAD_MAX_HEAP_BYTES", 256<<20, 8<<30)
	maxWALBytes := loadUint(t, "TICKDATA_LOAD_MAX_WAL_BYTES", 64<<20, 64<<30)
	targetRPS := baselineRPS * 10
	targetRecords := targetRPS * durationMS / 1_000
	if targetRecords == 0 {
		t.Fatal("load duration is too short for one target record")
	}

	config := loadGateConfig(t)
	gatewayRuntime := newStartedGatewayRuntime(t, config)
	gateway := gatewayRuntime.Gateway()
	ctx, cancel := context.WithCancel(context.Background())
	server, clientConn := net.Pipe()
	handlerDone := make(chan error, 1)
	go func() { handlerDone <- gateway.HandleConn(ctx, server) }()
	client, err := fake.New(clientConn, loadHello(config))
	if err != nil {
		cancel()
		_ = server.Close()
		_ = clientConn.Close()
		_ = gatewayRuntime.Stop(context.Background())
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		cancel()
		select {
		case <-handlerDone:
		case <-time.After(time.Second):
			t.Error("load gateway handler did not stop")
		}
		if err := gatewayRuntime.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	}()

	latencies := make([]int64, 0, targetRecords)
	start := time.Now()
	requestedDuration := time.Duration(durationMS) * time.Millisecond
	maxHeap := uint64(0)
	maxGoroutines := runtime.NumGoroutine()
	var firstSend, lastAck time.Time
	for sequence := uint64(1); sequence <= targetRecords; sequence++ {
		scheduled := start.Add(time.Duration((sequence - 1) * uint64(time.Second) / targetRPS))
		if wait := time.Until(scheduled); wait > 0 {
			time.Sleep(wait)
		}
		batch := loadBatch(sequence)
		started := time.Now()
		if firstSend.IsZero() {
			firstSend = started
		}
		ack, err := client.SendBatch(batch)
		lastAck = time.Now()
		latencies = append(latencies, time.Since(started).Microseconds())
		if err != nil {
			t.Fatalf("load batch %d: %v", sequence, err)
		}
		if ack.Status != protocolv1.AckAcceptedAdvanced {
			t.Fatalf("load batch %d ack = %+v", sequence, ack)
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		if memory.HeapAlloc > maxHeap {
			maxHeap = memory.HeapAlloc
		}
		if goroutines := runtime.NumGoroutine(); goroutines > maxGoroutines {
			maxGoroutines = goroutines
		}
	}
	if wait := time.Until(start.Add(requestedDuration)); wait > 0 {
		time.Sleep(wait)
	}
	elapsed := time.Since(start)
	activeElapsed := lastAck.Sub(firstSend)
	if activeElapsed <= 0 {
		t.Fatal("load active duration is not positive")
	}
	status, err := gateway.Status()
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(len(latencies)*95+99)/100-1]
	actualRPS := float64(targetRecords) / activeElapsed.Seconds()
	record := map[string]any{
		"verification_version":        "m4-network-free-load-v1",
		"run_id":                      time.Now().UTC().Format(time.RFC3339Nano),
		"go_version":                  runtime.Version(),
		"goos":                        runtime.GOOS,
		"goarch":                      runtime.GOARCH,
		"baseline_records_per_second": baselineRPS,
		"target_records_per_second":   targetRPS,
		"target_records":              targetRecords,
		"requested_duration_ms":       durationMS,
		"actual_duration_ms":          elapsed.Milliseconds(),
		"active_duration_ms":          activeElapsed.Milliseconds(),
		"actual_records_per_second":   actualRPS,
		"p95_ack_latency_us":          p95,
		"max_heap_bytes":              maxHeap,
		"max_goroutines":              maxGoroutines,
		"wal_bytes":                   status.WALBytes,
		"wal_entries":                 status.WALEntries,
		"ack_ready":                   status.ReadyForACK,
		"pass_duration":               elapsed >= requestedDuration*9/10,
		"pass_rate":                   actualRPS >= float64(targetRPS),
		"pass_heap":                   maxHeap <= maxHeapBytes,
		"pass_wal":                    status.WALBytes <= int64(maxWALBytes),
	}
	record["pass"] = record["pass_duration"] == true && record["pass_rate"] == true && record["pass_heap"] == true && record["pass_wal"] == true && status.ReadyForACK
	if err := writeLoadRecord(output, record); err != nil {
		t.Fatal(err)
	}
	if !record["pass"].(bool) {
		t.Fatalf("10x load gate failed; verification record written to %s: %+v", output, record)
	}
}

func loadGateConfig(t *testing.T) ingest.Config {
	t.Helper()
	root := t.TempDir()
	config := ingest.DefaultConfig()
	config.ListenAddress = "127.0.0.1:0"
	config.GatewayInstanceID = "gateway-load-01"
	config.WALRoot = filepath.Join(root, "wal")
	config.JournalPath = filepath.Join(root, "journal", "gateway.sqlite")
	config.InitialFromMSC = 0
	config.InitialBatchCount = 1
	config.MaximumBatchCount = 1
	config.DenseBoundaryHardCap = 1
	config.ProducerInstanceID = "producer-load-01"
	config.ProducerBuildID = "producer-load-v1"
	config.ProviderID = "provider-load"
	config.StableFeedID = "feed-load"
	config.BrokerServerFingerprint = "broker-load"
	config.ExactSourceSymbol = "EURUSD.load"
	return config
}

func loadHello(config ingest.Config) protocolv1.HelloV1 {
	return protocolv1.HelloV1{
		ProducerInstanceID: config.ProducerInstanceID,
		ProducerSessionID:  "session-load-01",
		ProducerBuildID:    config.ProducerBuildID,
		MQLCompilerBuild:   "fake",
		TerminalBuild:      "fake",
		OSContract:         "linux-test",
		ClockAPIID:         "load-test-clock", ProviderID: config.ProviderID,
		StableFeedID:            config.StableFeedID,
		BrokerServerFingerprint: config.BrokerServerFingerprint,
		ExactSourceSymbol:       config.ExactSourceSymbol,
		SourceSchemaID:          protocolv1.SourceSchemaMT5,
		AcquisitionMode:         1,
		InitialFromMSC:          config.InitialFromMSC,
	}
}

func loadBatch(sequence uint64) protocolv1.BatchFrameV1 {
	timeMSC := int64(sequence * 1_000)
	return protocolv1.BatchFrameV1{
		ProducerSessionID:     "session-load-01",
		BatchSequence:         sequence,
		RequestedFromMSC:      timeMSC,
		RequestedCount:        1,
		FetchWallStartS:       1_710_000_000,
		FetchWallEndS:         1_710_000_001,
		FetchMonotonicStartUS: sequence * 1_000,
		FetchMonotonicEndUS:   sequence*1_000 + 500,
		ReturnedCount:         1,
		SourceSchemaID:        protocolv1.SourceSchemaMT5,
		Records: []protocolv1.RawMqlTickV1{{
			Time:            timeMSC / 1_000,
			BidBits:         sequence,
			AskBits:         sequence + 1,
			LastBits:        sequence,
			Volume:          1,
			TimeMSC:         timeMSC,
			Flags:           3,
			VolumeRealBits:  sequence,
			CaptureSequence: sequence,
		}},
	}
}

func loadUint(t *testing.T, name string, fallback, maximum uint64) uint64 {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 || parsed > maximum {
		t.Fatalf("%s must be a nonzero integer <= %d", name, maximum)
	}
	return parsed
}

func writeLoadRecord(path string, record map[string]any) error {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("encode load verification record: %w", err)
	}
	data = append(data, '\n')
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return fmt.Errorf("create load verification directory: %w", err)
		}
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write load verification record: %w", err)
	}
	return nil
}
