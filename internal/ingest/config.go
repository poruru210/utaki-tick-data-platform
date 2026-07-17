package ingest

import (
	"fmt"
	"time"

	appconfig "tick-data-platform/internal/config"
	"tick-data-platform/internal/protocol"
)

type Config struct {
	ListenAddress          string
	GatewayInstanceID      string
	WALRoot                string
	RawOutboxRoot          string
	JournalPath            string
	MaxFrameBytes          uint32
	MaxRecords             uint32
	InitialFromMSC         int64
	InitialBatchCount      uint32
	MaximumBatchCount      uint32
	DenseBoundaryHardCap   uint32
	SessionLeaseTimeout    time.Duration
	HeartbeatIdleTimeout   time.Duration
	DiskHighFreeBytes      uint64
	DiskCriticalFreeBytes  uint64
	DiskEmergencyFreeBytes uint64
	MaxPendingSegments     uint64
	MaxPendingBytes        uint64

	ProducerInstanceID      string
	ProducerBuildID         string
	DatasetID               string
	CampaignID              string
	ProviderID              string
	StableFeedID            string
	BrokerServerFingerprint string
	ExactSourceSymbol       string
	GatewayBuildIdentity    string
	DayDefinitionID         string
	SettlePolicy            string
	PublisherID             string
	PublisherEpoch          uint64
}

func DefaultConfig() Config {
	return Config{
		ListenAddress:          "127.0.0.1:17001",
		GatewayInstanceID:      "gateway-local-01",
		MaxFrameBytes:          protocol.MaxFrameBytes,
		MaxRecords:             protocol.MaxRecords,
		InitialBatchCount:      128,
		MaximumBatchCount:      protocol.MaxRecords,
		DenseBoundaryHardCap:   protocol.MaxRecords,
		SessionLeaseTimeout:    30 * time.Second,
		HeartbeatIdleTimeout:   60 * time.Second,
		DiskHighFreeBytes:      512 << 20,
		DiskCriticalFreeBytes:  256 << 20,
		DiskEmergencyFreeBytes: 64 << 20,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.ListenAddress == "" {
		c.ListenAddress = d.ListenAddress
	}
	if c.GatewayInstanceID == "" {
		c.GatewayInstanceID = d.GatewayInstanceID
	}
	if c.MaxFrameBytes == 0 {
		c.MaxFrameBytes = d.MaxFrameBytes
	}
	if c.MaxRecords == 0 {
		c.MaxRecords = d.MaxRecords
	}
	if c.InitialBatchCount == 0 {
		c.InitialBatchCount = d.InitialBatchCount
	}
	if c.MaximumBatchCount == 0 {
		c.MaximumBatchCount = c.MaxRecords
	}
	if c.DenseBoundaryHardCap == 0 {
		c.DenseBoundaryHardCap = c.MaximumBatchCount
	}
	if c.SessionLeaseTimeout == 0 {
		c.SessionLeaseTimeout = d.SessionLeaseTimeout
	}
	if c.HeartbeatIdleTimeout == 0 {
		c.HeartbeatIdleTimeout = d.HeartbeatIdleTimeout
	}
	if c.DiskHighFreeBytes == 0 {
		c.DiskHighFreeBytes = d.DiskHighFreeBytes
	}
	if c.DiskCriticalFreeBytes == 0 {
		c.DiskCriticalFreeBytes = d.DiskCriticalFreeBytes
	}
	if c.DiskEmergencyFreeBytes == 0 {
		c.DiskEmergencyFreeBytes = d.DiskEmergencyFreeBytes
	}
	return c
}

func (c Config) Validate() error {
	c = c.withDefaults()
	if c.ListenAddress == "" || c.WALRoot == "" || c.JournalPath == "" {
		return fmt.Errorf("listen address, WAL root, and journal path are required")
	}
	if c.GatewayInstanceID == "" || len(c.GatewayInstanceID) > 255 {
		return fmt.Errorf("gateway instance id must be 1..255 bytes")
	}
	if c.MaxFrameBytes < protocol.MinFrameBytes || c.MaxFrameBytes > protocol.MaxFrameBytes {
		return fmt.Errorf("max frame bytes must be between %d and %d", protocol.MinFrameBytes, protocol.MaxFrameBytes)
	}
	if c.MaxRecords == 0 || c.MaxRecords > protocol.MaxRecords {
		return fmt.Errorf("max records must be between 1 and %d", protocol.MaxRecords)
	}
	if c.InitialBatchCount == 0 || c.InitialBatchCount > c.MaxRecords {
		return fmt.Errorf("initial batch count is outside max records")
	}
	if c.MaximumBatchCount < c.InitialBatchCount || c.MaximumBatchCount > c.MaxRecords {
		return fmt.Errorf("maximum batch count is outside configured range")
	}
	if c.DenseBoundaryHardCap < c.InitialBatchCount || c.DenseBoundaryHardCap > c.MaximumBatchCount {
		return fmt.Errorf("dense boundary hard cap is outside configured range")
	}
	if c.SessionLeaseTimeout <= 0 || c.HeartbeatIdleTimeout <= 0 {
		return fmt.Errorf("timeouts must be positive")
	}
	if err := (DiskWatermarks{
		HighFreeBytes: c.DiskHighFreeBytes, CriticalFreeBytes: c.DiskCriticalFreeBytes, EmergencyFreeBytes: c.DiskEmergencyFreeBytes,
		MaxPendingSegments: c.MaxPendingSegments, MaxPendingBytes: c.MaxPendingBytes,
	}).Validate(); err != nil {
		return err
	}
	return nil
}

// ConfigFromGatewayConfig is the single boundary that converts the strict
// application config into the ingest-owned runtime config. Both one-shot
// commands and the Fx composition root use it so field mappings cannot drift.
func ConfigFromGatewayConfig(values appconfig.GatewayConfig) Config {
	config := Config{
		ListenAddress: values.ListenAddress, GatewayInstanceID: values.GatewayInstanceID,
		WALRoot: values.WALRoot, RawOutboxRoot: values.RawOutboxRoot, JournalPath: values.JournalPath,
		MaxFrameBytes: values.MaxFrameBytes, MaxRecords: values.MaxRecords,
		InitialFromMSC: values.InitialFromMSC, InitialBatchCount: values.InitialBatchCount,
		MaximumBatchCount: values.MaximumBatchCount, DenseBoundaryHardCap: values.DenseBoundaryHardCap,
		SessionLeaseTimeout: values.SessionLeaseTimeout, HeartbeatIdleTimeout: values.HeartbeatIdleTimeout,
		DiskHighFreeBytes: values.DiskHighFreeBytes, DiskCriticalFreeBytes: values.DiskCriticalFreeBytes,
		DiskEmergencyFreeBytes: values.DiskEmergencyFreeBytes,
		ProducerInstanceID:     values.ProducerInstanceID, ProducerBuildID: values.ProducerBuildID,
		DatasetID: values.DatasetID, CampaignID: values.CampaignID, ProviderID: values.ProviderID,
		StableFeedID: values.StableFeedID, BrokerServerFingerprint: values.BrokerServerFingerprint,
		ExactSourceSymbol: values.ExactSourceSymbol, GatewayBuildIdentity: values.GatewayBuildIdentity,
		DayDefinitionID: values.DayDefinitionID, SettlePolicy: values.SettlePolicy,
		PublisherID: values.PublisherID, PublisherEpoch: values.PublisherEpoch,
	}
	return config.withDefaults()
}
