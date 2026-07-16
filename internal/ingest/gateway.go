package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"tick-data-platform/internal/journal"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/retention"
	"tick-data-platform/internal/wal"
)

type Hooks struct {
	AfterWALSync       func() error
	AfterJournalCommit func() error
	BeforeACK          func() error
}

type Gateway struct {
	config  Config
	wal     *wal.Store
	journal *journal.Store
	started time.Time

	mu               sync.Mutex
	listener         net.Listener
	closed           bool
	activeLease      string
	activeUntil      time.Time
	activeGeneration uint64
	nextGeneration   uint64
	sessionMu        sync.Mutex
	connectionsMu    sync.Mutex
	connections      map[net.Conn]struct{}
	handlers         sync.WaitGroup
	reconcileMu      sync.Mutex
	hooksMu          sync.Mutex
	hooks            Hooks
	metricsMu        sync.Mutex
	metrics          Metrics
	disk             *DiskStateMachine
}

type Metrics struct {
	AcceptedBatches  uint64 `json:"accepted_batches"`
	AcceptedRecords  uint64 `json:"accepted_records"`
	DuplicateBatches uint64 `json:"duplicate_batches"`
	RejectedBatches  uint64 `json:"rejected_batches"`
	Connections      uint64 `json:"connections"`
	LastError        string `json:"last_error"`
}

type StatusSnapshot struct {
	GatewayInstanceID       string              `json:"gateway_instance_id"`
	ListenAddress           string              `json:"listen_address"`
	WALPath                 string              `json:"wal_path"`
	WALEntries              int                 `json:"wal_entries"`
	WALBytes                int64               `json:"wal_bytes"`
	JournalBatches          int                 `json:"journal_batches"`
	CommittedCursorMSC      int64               `json:"committed_cursor_msc"`
	CommittedBoundaryDigest string              `json:"committed_boundary_digest"`
	ChainRoot               string              `json:"chain_root"`
	NextFromMSC             int64               `json:"next_from_msc"`
	NextRequestedCount      uint32              `json:"next_requested_count"`
	ActiveSession           bool                `json:"active_session"`
	DiskClass               retention.DiskClass `json:"disk_class"`
	DiskFreeBytes           uint64              `json:"disk_free_bytes"`
	DiskTotalBytes          uint64              `json:"disk_total_bytes"`
	OldestRetainedSequence  uint64              `json:"oldest_retained_sequence"`
	PrunableBytes           uint64              `json:"prunable_bytes"`
	BlockedReason           string              `json:"blocked_reason"`
	ReadyForACK             bool                `json:"ready_for_ack"`
	Metrics                 Metrics             `json:"metrics"`
}

func Open(config Config) (*Gateway, error) {
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := retention.RecoverPrune(config.WALRoot, 1<<20); err != nil {
		return nil, err
	}
	var anchor *wal.PruneAnchor
	checkpoint, checkpointErr := retention.LoadLatestCheckpoint(config.WALRoot)
	if checkpointErr == nil {
		if _, err := retention.VerifyRetainedWAL(config.WALRoot, &checkpoint, 1<<20); err != nil {
			return nil, err
		}
		anchor = &wal.PruneAnchor{EndSequence: checkpoint.EndSequence, ChainRoot: checkpoint.RetainedChainRoot}
	} else if !errors.Is(checkpointErr, retention.ErrCheckpointAbsent) {
		return nil, checkpointErr
	}
	store, err := wal.OpenWithAnchor(config.WALRoot, config.GatewayInstanceID, anchor)
	if err != nil {
		return nil, err
	}
	if err := retention.PublishWallClock(config.WALRoot, uint64(time.Now().UnixMilli())); err != nil {
		_ = store.Close()
		return nil, err
	}
	journalStore, err := journal.Open(config.JournalPath, config.GatewayInstanceID, config.InitialFromMSC, config.InitialBatchCount)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	disk, err := NewDiskStateMachine(config.WALRoot, DiskWatermarks{HighFreeBytes: config.DiskHighFreeBytes, CriticalFreeBytes: config.DiskCriticalFreeBytes, EmergencyFreeBytes: config.DiskEmergencyFreeBytes}, OSDiskUsageProvider{})
	if err != nil {
		_ = store.Close()
		_ = journalStore.Close()
		return nil, err
	}
	gateway := &Gateway{
		config:      config,
		wal:         store,
		journal:     journalStore,
		started:     time.Now(),
		connections: make(map[net.Conn]struct{}),
		disk:        disk,
	}
	if err := gateway.reconcileJournal(); err != nil {
		_ = gateway.Close()
		return nil, err
	}
	return gateway, nil
}

func (g *Gateway) Config() Config { return g.config }

func (g *Gateway) WAL() *wal.Store { return g.wal }

func (g *Gateway) Journal() *journal.Store { return g.journal }

func (g *Gateway) SetHooks(hooks Hooks) {
	g.hooksMu.Lock()
	defer g.hooksMu.Unlock()
	g.hooks = hooks
}

func (g *Gateway) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", g.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", g.config.ListenAddress, err)
	}
	return g.Serve(ctx, listener)
}

func (g *Gateway) Serve(ctx context.Context, listener net.Listener) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		_ = listener.Close()
		return net.ErrClosed
	}
	g.listener = listener
	g.mu.Unlock()
	closeDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-closeDone:
		}
	}()
	defer func() {
		close(closeDone)
		_ = listener.Close()
		g.mu.Lock()
		if g.listener == listener {
			g.listener = nil
		}
		g.mu.Unlock()
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept gateway connection: %w", err)
		}
		g.connectionsMu.Lock()
		g.mu.Lock()
		closed := g.closed
		g.mu.Unlock()
		if closed {
			g.connectionsMu.Unlock()
			_ = connection.Close()
			continue
		}
		g.metricsMu.Lock()
		g.metrics.Connections++
		g.metricsMu.Unlock()
		g.connections[connection] = struct{}{}
		g.handlers.Add(1)
		g.connectionsMu.Unlock()
		go func() {
			defer g.handlers.Done()
			defer func() {
				g.connectionsMu.Lock()
				delete(g.connections, connection)
				g.connectionsMu.Unlock()
			}()
			if err := g.HandleConn(ctx, connection); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				g.setLastError(err)
			}
		}()
	}
}

func (g *Gateway) Close() error {
	g.mu.Lock()
	listener := g.listener
	g.listener = nil
	g.closed = true
	g.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	g.connectionsMu.Lock()
	for connection := range g.connections {
		_ = connection.Close()
	}
	g.connectionsMu.Unlock()
	g.sessionMu.Lock()
	g.sessionMu.Unlock()
	g.handlers.Wait()
	var first error
	if g.wal != nil {
		if err := g.wal.Close(); err != nil {
			first = err
		}
	}
	if g.journal != nil {
		if err := g.journal.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (g *Gateway) HandleConn(ctx context.Context, connection net.Conn) error {
	defer connection.Close()
	var sessionLease string
	var sessionGeneration uint64
	defer func() {
		if sessionLease != "" {
			g.releaseSession(sessionLease, sessionGeneration)
		}
	}()

	_ = connection.SetReadDeadline(time.Now().Add(g.config.HeartbeatIdleTimeout))
	raw, err := readFrame(connection, g.config.MaxFrameBytes)
	if err != nil {
		if raw != nil {
			_ = writeError(connection, 0, 0, err)
		}
		return err
	}
	frame, err := protocol.DecodeFrame(raw)
	if err != nil {
		_ = writeError(connection, 0, 0, err)
		return err
	}
	message, err := protocol.DecodeMessage(frame)
	if err != nil {
		_ = writeError(connection, frame.MessageType, 0, err)
		return err
	}
	hello, ok := message.(protocol.HelloV1)
	if !ok {
		err := protocolError(protocol.ErrInvalidField, "first message must be HelloV1")
		_ = writeError(connection, frame.MessageType, 0, err)
		return err
	}
	resume, lease, generation, err := g.startSession(hello)
	if err != nil {
		_ = writeError(connection, protocol.MessageHello, 0, err)
		return err
	}
	sessionLease = lease
	sessionGeneration = generation
	if err := writeMessage(connection, resume); err != nil {
		return err
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = connection.SetReadDeadline(time.Now().Add(g.config.HeartbeatIdleTimeout))
		raw, err = readFrame(connection, g.config.MaxFrameBytes)
		if err != nil {
			if raw != nil {
				_ = writeError(connection, 0, 0, err)
			}
			return err
		}
		frame, err = protocol.DecodeFrame(raw)
		if err != nil {
			_ = writeError(connection, 0, 0, err)
			return err
		}
		message, err = protocol.DecodeMessage(frame)
		if err != nil {
			_ = writeError(connection, frame.MessageType, 0, err)
			return err
		}
		batch, ok := message.(protocol.BatchFrameV1)
		if !ok {
			err := protocolError(protocol.ErrInvalidField, "producer message must be BatchFrameV1")
			_ = writeError(connection, frame.MessageType, 0, err)
			return err
		}
		ack, terminal, err := g.acceptBatch(raw, batch, hello, sessionLease, sessionGeneration)
		if err != nil {
			_ = writeError(connection, protocol.MessageBatch, batch.BatchSequence, err)
			return err
		}
		if hook := g.currentHooks().BeforeACK; hook != nil {
			if err := hook(); err != nil {
				g.setLastError(err)
				return err
			}
		}
		if err := writeMessage(connection, ack); err != nil {
			return err
		}
		g.touchSession(sessionLease, sessionGeneration)
		if terminal {
			return fmt.Errorf("terminal batch status %d", ack.Status)
		}
	}
}

func (g *Gateway) startSession(hello protocol.HelloV1) (protocol.ResumeV1, string, uint64, error) {
	if err := g.validateHello(hello); err != nil {
		return protocol.ResumeV1{}, "", 0, err
	}
	lease := protocol.DeriveSessionLeaseID(
		hello.ProducerInstanceID,
		hello.ProducerSessionID,
		hello.CampaignID,
		hello.ProviderID,
		hello.StableFeedID,
		hello.BrokerServerFingerprint,
		hello.ExactSourceSymbol,
	)
	g.sessionMu.Lock()
	defer g.sessionMu.Unlock()
	now := time.Now()
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return protocol.ResumeV1{}, "", 0, net.ErrClosed
	}
	if g.activeLease != "" && now.Before(g.activeUntil) {
		g.mu.Unlock()
		return protocol.ResumeV1{}, "", 0, protocolError(protocol.ErrSessionLeaseConflict, "another producer session is active")
	}
	g.nextGeneration++
	if g.nextGeneration == 0 {
		g.nextGeneration++
	}
	generation := g.nextGeneration
	g.activeLease = lease
	g.activeUntil = now.Add(g.config.SessionLeaseTimeout)
	g.activeGeneration = generation
	g.mu.Unlock()
	state, err := g.journal.State()
	if err != nil {
		g.releaseSession(lease, generation)
		return protocol.ResumeV1{}, "", 0, err
	}
	last, found, err := g.journal.LastForSession(hello.ProducerSessionID)
	if err != nil {
		g.releaseSession(lease, generation)
		return protocol.ResumeV1{}, "", 0, err
	}
	resume := protocol.ResumeV1{
		AcceptedProtocolVersion: protocol.ProtocolVersion,
		GatewayInstanceID:       g.config.GatewayInstanceID,
		SessionLeaseID:          lease,
		CommittedCursorMSC:      state.CommittedCursorMSC,
		CommittedBoundaryDigest: state.CommittedBoundaryDigest,
		NextFromMSC:             state.NextFromMSC,
		NextRequestedCount:      state.NextRequestedCount,
		MaximumFrameBytes:       g.config.MaxFrameBytes,
		MaximumRecords:          g.config.MaxRecords,
		HeartbeatIdleTimeoutMS:  uint32(g.config.HeartbeatIdleTimeout / time.Millisecond),
	}
	if found {
		resume.LastDurableBatchSequence = last.BatchSequence
		resume.LastDurableBatchHash = last.GatewayBatchSHA256
	}
	return resume, lease, generation, nil
}

func (g *Gateway) validateHello(hello protocol.HelloV1) error {
	if hello.ProducerInstanceID == "" || hello.ProducerSessionID == "" || hello.CampaignID == "" || hello.ExactSourceSymbol == "" {
		return protocolError(protocol.ErrInvalidField, "producer identity and exact source symbol are required")
	}
	if hello.SourceSchemaID != protocol.SourceSchemaMT5 {
		return protocolError(protocol.ErrInvalidField, "unsupported source schema %q", hello.SourceSchemaID)
	}
	checks := []struct {
		name string
		got  string
		want string
	}{
		{"producer_instance_id", hello.ProducerInstanceID, g.config.ProducerInstanceID},
		{"producer_build_id", hello.ProducerBuildID, g.config.ProducerBuildID},
		{"campaign_id", hello.CampaignID, g.config.CampaignID},
		{"provider_id", hello.ProviderID, g.config.ProviderID},
		{"stable_feed_id", hello.StableFeedID, g.config.StableFeedID},
		{"broker_server_fingerprint", hello.BrokerServerFingerprint, g.config.BrokerServerFingerprint},
		{"exact_source_symbol", hello.ExactSourceSymbol, g.config.ExactSourceSymbol},
	}
	for _, check := range checks {
		if check.want != "" && check.got != check.want {
			return protocolError(protocol.ErrInvalidField, "%s does not match configured dataset", check.name)
		}
	}
	return nil
}

func (g *Gateway) acceptBatch(raw []byte, batch protocol.BatchFrameV1, hello protocol.HelloV1, lease string, generation uint64) (protocol.AckV1, bool, error) {
	g.sessionMu.Lock()
	defer g.sessionMu.Unlock()
	incomingHash := protocol.GatewayBatchSHA256(raw)
	if !g.sessionActive(lease, generation) {
		return protocol.AckV1{
			ProducerSessionID:  batch.ProducerSessionID,
			BatchSequence:      batch.BatchSequence,
			GatewayBatchSHA256: incomingHash,
			Status:             protocol.AckSessionLeaseConflict,
			NextRequestedCount: g.config.InitialBatchCount,
		}, true, nil
	}
	if uint32(len(batch.Records)) > g.config.MaxRecords || batch.RequestedCount > g.config.MaximumBatchCount {
		return protocol.AckV1{}, false, protocolError(protocol.ErrOversizedFrame, "batch exceeds configured record or request limit")
	}
	if batch.ProducerSessionID != hello.ProducerSessionID || batch.SessionLeaseID != lease {
		return protocol.AckV1{
			ProducerSessionID:  batch.ProducerSessionID,
			BatchSequence:      batch.BatchSequence,
			GatewayBatchSHA256: incomingHash,
			Status:             protocol.AckSessionLeaseConflict,
			CommittedCursorMSC: 0,
			NextRequestedCount: g.config.InitialBatchCount,
		}, true, nil
	}
	stored, found, err := g.journal.Lookup(batch.ProducerSessionID, batch.BatchSequence)
	if err != nil {
		return protocol.AckV1{}, false, err
	}
	if !found {
		if foundInWAL, sameHash, err := g.findWALIdentity(batch.ProducerSessionID, batch.BatchSequence, incomingHash); err != nil {
			return protocol.AckV1{}, false, err
		} else if foundInWAL {
			if !sameHash {
				g.metricsMu.Lock()
				g.metrics.RejectedBatches++
				g.metricsMu.Unlock()
				return protocol.AckV1{
					ProducerSessionID:  batch.ProducerSessionID,
					BatchSequence:      batch.BatchSequence,
					GatewayBatchSHA256: incomingHash,
					Status:             protocol.AckSourceStateConflict,
					NextRequestedCount: g.config.InitialBatchCount,
				}, true, nil
			}
			if err := g.reconcileJournal(); err != nil {
				return protocol.AckV1{}, false, err
			}
			stored, found, err = g.journal.Lookup(batch.ProducerSessionID, batch.BatchSequence)
			if err != nil {
				return protocol.AckV1{}, false, err
			}
		}
	}
	if found {
		if stored.GatewayBatchSHA256 != incomingHash {
			g.metricsMu.Lock()
			g.metrics.RejectedBatches++
			g.metricsMu.Unlock()
			return protocol.AckV1{
				ProducerSessionID:       batch.ProducerSessionID,
				BatchSequence:           batch.BatchSequence,
				GatewayBatchSHA256:      incomingHash,
				GatewayIngestSequence:   stored.GatewayIngestSequence,
				Status:                  protocol.AckSourceStateConflict,
				CommittedCursorMSC:      stored.CommittedCursorMSC,
				CommittedBoundaryDigest: stored.CommittedBoundaryDigest,
				NextFromMSC:             stored.NextFromMSC,
				NextRequestedCount:      stored.NextRequestedCount,
			}, true, nil
		}
		g.metricsMu.Lock()
		g.metrics.DuplicateBatches++
		g.metricsMu.Unlock()
		return ackFromBatch(stored, protocol.AckDuplicate), false, nil
	}
	if g.disk != nil && !g.disk.ReadyForACK() {
		state := g.disk.State()
		g.setLastError(fmt.Errorf("disk is not ready for durable ACK: %s", state.BlockedReason))
		return protocol.AckV1{
			ProducerSessionID:  batch.ProducerSessionID,
			BatchSequence:      batch.BatchSequence,
			GatewayBatchSHA256: incomingHash,
			Status:             protocol.AckRetryableError,
			NextRequestedCount: g.config.InitialBatchCount,
		}, false, nil
	}

	state, err := g.journal.State()
	if err != nil {
		return protocol.AckV1{}, false, err
	}
	outcome := outcomeForBatch(state, batch, g.config)
	entry, err := g.wal.Append(raw, time.Now().Unix(), uint64(time.Since(g.started).Microseconds()))
	if err != nil {
		if g.disk != nil {
			g.disk.MarkPoisoned()
		}
		return protocol.AckV1{}, false, err
	}
	if hook := g.currentHooks().AfterWALSync; hook != nil {
		if err := hook(); err != nil {
			if g.disk != nil {
				g.disk.MarkPoisoned()
			}
			g.setLastError(err)
			return protocol.AckV1{}, false, err
		}
	}
	stored, err = g.journal.Apply(entry, batch, outcome)
	if err != nil {
		return protocol.AckV1{}, false, err
	}
	if hook := g.currentHooks().AfterJournalCommit; hook != nil {
		if err := hook(); err != nil {
			g.setLastError(err)
			return protocol.AckV1{}, false, err
		}
	}
	if g.disk != nil && !g.disk.ReadyForACK() {
		state := g.disk.State()
		g.setLastError(fmt.Errorf("disk became unavailable before durable ACK: %s", state.BlockedReason))
		return protocol.AckV1{
			ProducerSessionID:     batch.ProducerSessionID,
			BatchSequence:         batch.BatchSequence,
			GatewayBatchSHA256:    entry.BatchHash,
			GatewayIngestSequence: entry.Sequence,
			Status:                protocol.AckRetryableError,
			NextRequestedCount:    g.config.InitialBatchCount,
		}, false, nil
	}
	g.metricsMu.Lock()
	g.metrics.AcceptedBatches++
	g.metrics.AcceptedRecords += uint64(len(batch.Records))
	g.metricsMu.Unlock()
	return ackFromBatch(stored, stored.Status), stored.Status == protocol.AckSourceStateConflict || stored.Status == protocol.AckSessionLeaseConflict, nil
}

func ackFromBatch(batch journal.Batch, status uint8) protocol.AckV1 {
	return protocol.AckV1{
		ProducerSessionID:       batch.ProducerSessionID,
		BatchSequence:           batch.BatchSequence,
		GatewayBatchSHA256:      batch.GatewayBatchSHA256,
		GatewayIngestSequence:   batch.GatewayIngestSequence,
		Status:                  status,
		CommittedCursorMSC:      batch.CommittedCursorMSC,
		CommittedBoundaryDigest: batch.CommittedBoundaryDigest,
		NextFromMSC:             batch.NextFromMSC,
		NextRequestedCount:      batch.NextRequestedCount,
	}
}

func (g *Gateway) findWALIdentity(session string, sequence uint64, hash [32]byte) (bool, bool, error) {
	for _, entry := range g.wal.Entries() {
		frame, err := protocol.DecodeFrame(entry.Frame)
		if err != nil {
			return false, false, err
		}
		message, err := protocol.DecodeMessage(frame)
		if err != nil {
			return false, false, err
		}
		batch, ok := message.(protocol.BatchFrameV1)
		if ok && batch.ProducerSessionID == session && batch.BatchSequence == sequence {
			return true, entry.BatchHash == hash, nil
		}
	}
	return false, false, nil
}

func (g *Gateway) reconcileJournal() error {
	g.reconcileMu.Lock()
	defer g.reconcileMu.Unlock()
	entries := g.wal.Entries()
	count, err := g.journal.Count()
	if err != nil {
		return err
	}
	if prunedThrough := g.wal.PrunedThrough(); prunedThrough != 0 {
		if count < len(entries) {
			return fmt.Errorf("%w: journal is missing retained WAL entries after prune anchor", wal.ErrIntegrity)
		}
		matches, matchErr := g.journalMatchesRetainedWAL(entries, prunedThrough)
		if matchErr != nil {
			return matchErr
		}
		if !matches {
			return fmt.Errorf("%w: journal does not match retained WAL after prune anchor", wal.ErrIntegrity)
		}
		return nil
	}
	if count > len(entries) {
		return fmt.Errorf("%w: journal has %d batches but WAL has %d", wal.ErrIntegrity, count, len(entries))
	}
	if count == len(entries) {
		matches, err := g.journalMatchesWAL(entries)
		if err != nil {
			return err
		}
		if matches {
			return nil
		}
	}
	if err := g.journal.Reset(); err != nil {
		return err
	}
	for _, entry := range entries {
		batch, err := decodeWALEntry(entry)
		if err != nil {
			return err
		}
		state, err := g.journal.State()
		if err != nil {
			return err
		}
		outcome := outcomeForBatch(state, batch, g.config)
		if _, err := g.journal.Apply(entry, batch, outcome); err != nil {
			return fmt.Errorf("rebuild journal at WAL sequence %d: %w", entry.Sequence, err)
		}
	}
	return nil
}

func (g *Gateway) journalMatchesRetainedWAL(entries []wal.Entry, prunedThrough uint64) (bool, error) {
	actual, err := g.journal.State()
	if err != nil {
		return false, err
	}
	if actual.GatewayInstanceID != g.config.GatewayInstanceID {
		return false, nil
	}
	for _, entry := range entries {
		batch, err := decodeWALEntry(entry)
		if err != nil {
			return false, err
		}
		stored, found, err := g.journal.Lookup(batch.ProducerSessionID, batch.BatchSequence)
		if err != nil {
			return false, err
		}
		if !found || stored.GatewayIngestSequence != entry.Sequence || stored.GatewayBatchSHA256 != entry.BatchHash || !bytes.Equal(stored.Frame, entry.Frame) {
			return false, nil
		}
	}
	if len(entries) == 0 {
		return actual.LastIngestSequence == prunedThrough && actual.LastEntryHash == g.wal.ChainRoot(), nil
	}
	last := entries[len(entries)-1]
	return actual.LastIngestSequence == last.Sequence && actual.LastEntryHash == last.EntryHash, nil
}

func (g *Gateway) journalMatchesWAL(entries []wal.Entry) (bool, error) {
	expected := journal.State{
		GatewayInstanceID:  g.config.GatewayInstanceID,
		CommittedCursorMSC: g.config.InitialFromMSC,
		NextFromMSC:        g.config.InitialFromMSC,
		NextRequestedCount: g.config.InitialBatchCount,
	}
	for _, entry := range entries {
		batch, err := decodeWALEntry(entry)
		if err != nil {
			return false, err
		}
		outcome := outcomeForBatch(expected, batch, g.config)
		stored, found, err := g.journal.Lookup(batch.ProducerSessionID, batch.BatchSequence)
		if err != nil {
			return false, nil
		}
		if !found || !journalBatchMatches(stored, entry, batch, outcome) {
			return false, nil
		}
		expected.CommittedCursorMSC = outcome.CommittedCursorMSC
		expected.CommittedBoundaryDigest = outcome.CommittedBoundaryDigest
		expected.NextFromMSC = outcome.NextFromMSC
		expected.NextRequestedCount = outcome.NextRequestedCount
		expected.LastIngestSequence = entry.Sequence
		expected.LastEntryHash = entry.EntryHash
	}
	actual, err := g.journal.State()
	if err != nil {
		return false, nil
	}
	return journalStateMatches(actual, expected), nil
}

func decodeWALEntry(entry wal.Entry) (protocol.BatchFrameV1, error) {
	frame, err := protocol.DecodeFrame(entry.Frame)
	if err != nil {
		return protocol.BatchFrameV1{}, err
	}
	message, err := protocol.DecodeMessage(frame)
	if err != nil {
		return protocol.BatchFrameV1{}, err
	}
	batch, ok := message.(protocol.BatchFrameV1)
	if !ok {
		return protocol.BatchFrameV1{}, fmt.Errorf("WAL entry %d is not a batch", entry.Sequence)
	}
	return batch, nil
}

func journalBatchMatches(stored journal.Batch, entry wal.Entry, batch protocol.BatchFrameV1, outcome journal.Outcome) bool {
	return stored.ProducerSessionID == batch.ProducerSessionID &&
		stored.BatchSequence == batch.BatchSequence &&
		stored.GatewayBatchSHA256 == entry.BatchHash &&
		stored.GatewayIngestSequence == entry.Sequence &&
		stored.Status == outcome.Status &&
		stored.CommittedCursorMSC == outcome.CommittedCursorMSC &&
		stored.CommittedBoundaryDigest == outcome.CommittedBoundaryDigest &&
		stored.NextFromMSC == outcome.NextFromMSC &&
		stored.NextRequestedCount == outcome.NextRequestedCount &&
		bytes.Equal(stored.Frame, entry.Frame)
}

func journalStateMatches(actual, expected journal.State) bool {
	return actual.GatewayInstanceID == expected.GatewayInstanceID &&
		actual.CommittedCursorMSC == expected.CommittedCursorMSC &&
		actual.CommittedBoundaryDigest == expected.CommittedBoundaryDigest &&
		actual.NextFromMSC == expected.NextFromMSC &&
		actual.NextRequestedCount == expected.NextRequestedCount &&
		actual.LastIngestSequence == expected.LastIngestSequence &&
		actual.LastEntryHash == expected.LastEntryHash
}

func (g *Gateway) Status() (StatusSnapshot, error) {
	state, err := g.journal.State()
	if err != nil {
		return StatusSnapshot{}, err
	}
	count, err := g.journal.Count()
	if err != nil {
		return StatusSnapshot{}, err
	}
	g.mu.Lock()
	active := g.activeLease != "" && time.Now().Before(g.activeUntil)
	g.mu.Unlock()
	g.metricsMu.Lock()
	metrics := g.metrics
	g.metricsMu.Unlock()
	disk := DiskState{}
	if g.disk != nil {
		disk = g.disk.Refresh()
	}
	oldest := g.wal.PrunedThrough() + 1
	segments := g.wal.SealedSegments()
	if len(segments) > 0 {
		oldest = segments[0].StartSequence
	}
	return StatusSnapshot{
		GatewayInstanceID:       g.config.GatewayInstanceID,
		ListenAddress:           g.config.ListenAddress,
		WALPath:                 g.wal.Path(),
		WALEntries:              g.wal.Count(),
		WALBytes:                g.wal.FileBytes(),
		JournalBatches:          count,
		CommittedCursorMSC:      state.CommittedCursorMSC,
		CommittedBoundaryDigest: hex.EncodeToString(state.CommittedBoundaryDigest[:]),
		ChainRoot:               hex.EncodeToString(state.LastEntryHash[:]),
		NextFromMSC:             state.NextFromMSC,
		NextRequestedCount:      state.NextRequestedCount,
		ActiveSession:           active,
		DiskClass:               disk.Class,
		DiskFreeBytes:           disk.FreeBytes,
		DiskTotalBytes:          disk.TotalBytes,
		OldestRetainedSequence:  oldest,
		PrunableBytes:           0,
		BlockedReason:           disk.BlockedReason,
		ReadyForACK:             disk.ACKAllowed,
		Metrics:                 metrics,
	}, nil
}

func (g *Gateway) sessionActive(lease string, generation uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.closed && g.activeLease == lease && g.activeGeneration == generation && time.Now().Before(g.activeUntil)
}

func (g *Gateway) releaseSession(lease string, generation uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.activeLease == lease && g.activeGeneration == generation {
		g.activeLease = ""
		g.activeUntil = time.Time{}
		g.activeGeneration = 0
	}
}

func (g *Gateway) touchSession(lease string, generation uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed && g.activeLease == lease && g.activeGeneration == generation {
		g.activeUntil = time.Now().Add(g.config.SessionLeaseTimeout)
	}
}

func (g *Gateway) currentHooks() Hooks {
	g.hooksMu.Lock()
	defer g.hooksMu.Unlock()
	return g.hooks
}

func (g *Gateway) setLastError(err error) {
	g.metricsMu.Lock()
	g.metrics.LastError = err.Error()
	g.metricsMu.Unlock()
}

func protocolError(code protocol.ErrorCode, format string, args ...any) error {
	return &protocol.ProtocolError{Code: code, Detail: fmt.Sprintf(format, args...)}
}

func readFrame(reader io.Reader, maxFrameBytes uint32) ([]byte, error) {
	var header [16]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, &protocol.ProtocolError{Code: protocol.ErrTruncatedFrame, Detail: err.Error()}
	}
	frameLength := binary.LittleEndian.Uint32(header[8:12])
	if frameLength < protocol.MinFrameBytes {
		return nil, protocolError(protocol.ErrInvalidFrame, "frame length %d is below minimum", frameLength)
	}
	if frameLength > maxFrameBytes || frameLength > protocol.MaxFrameBytes {
		return nil, protocolError(protocol.ErrOversizedFrame, "frame length %d exceeds maximum", frameLength)
	}
	if string(header[0:4]) != protocol.Magic {
		return nil, protocolError(protocol.ErrInvalidFrame, "invalid magic")
	}
	if binary.LittleEndian.Uint32(header[12:16]) != protocol.HeaderLength {
		return nil, protocolError(protocol.ErrInvalidFrame, "invalid header length")
	}
	raw := make([]byte, frameLength)
	copy(raw[:16], header[:])
	if _, err := io.ReadFull(reader, raw[16:]); err != nil {
		return raw, &protocol.ProtocolError{Code: protocol.ErrTruncatedFrame, Detail: err.Error()}
	}
	if _, err := protocol.DecodeFrame(raw); err != nil {
		return raw, err
	}
	return raw, nil
}

func writeMessage(writer io.Writer, message protocol.Message) error {
	frame, err := protocol.EncodeMessage(message)
	if err != nil {
		return err
	}
	return writeFrame(writer, frame)
}

func writeFrame(writer io.Writer, frame []byte) error {
	for len(frame) > 0 {
		written, err := writer.Write(frame)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		frame = frame[written:]
	}
	return nil
}

func writeError(writer io.Writer, relatedType protocol.MessageType, batchSequence uint64, err error) error {
	code := protocol.ErrorCodeOf(err)
	if code == "" {
		code = protocol.ErrInternalRetryable
	}
	retryable := uint8(0)
	if code == protocol.ErrInternalRetryable || code == protocol.ErrSessionLeaseConflict {
		retryable = 1
	}
	message := err.Error()
	if len(message) > int(protocol.MaxStringBytes) {
		message = message[:protocol.MaxStringBytes]
	}
	return writeMessage(writer, protocol.ErrorV1{
		Code:                 protocol.ErrorCodeNumber(code),
		Retryable:            retryable,
		RelatedMessageType:   relatedType,
		RelatedBatchSequence: batchSequence,
		Message:              message,
	})
}
