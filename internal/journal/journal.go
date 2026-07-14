// Package journal owns the rebuildable SQLite index derived from the WAL.
package journal

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"

	_ "modernc.org/sqlite"
)

type State struct {
	GatewayInstanceID       string
	CommittedCursorMSC      int64
	CommittedBoundaryDigest [32]byte
	NextFromMSC             int64
	NextRequestedCount      uint32
	LastIngestSequence      uint64
	LastEntryHash           [32]byte
	UpdatedWallS            int64
}

type Outcome struct {
	Status                  uint8
	CommittedCursorMSC      int64
	CommittedBoundaryDigest [32]byte
	NextFromMSC             int64
	NextRequestedCount      uint32
}

type Batch struct {
	ProducerSessionID       string
	BatchSequence           uint64
	GatewayBatchSHA256      [32]byte
	GatewayIngestSequence   uint64
	Status                  uint8
	CommittedCursorMSC      int64
	CommittedBoundaryDigest [32]byte
	NextFromMSC             int64
	NextRequestedCount      uint32
	Frame                   []byte
}

type Store struct {
	db             *sql.DB
	path           string
	gatewayID      string
	initialFromMSC int64
	initialCount   uint32
}

func Open(path, gatewayID string, initialFromMSC int64, initialCount uint32) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("journal path is empty")
	}
	if parent := filepath.Dir(path); parent != "." {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return nil, fmt.Errorf("create journal directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite journal: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{
		db:             db,
		path:           path,
		gatewayID:      gatewayID,
		initialFromMSC: initialFromMSC,
		initialCount:   initialCount,
	}
	if err := store.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Path() string { return s.path }

func (s *Store) initialize() error {
	statements := []string{
		`PRAGMA journal_mode=DELETE`,
		`PRAGMA synchronous=FULL`,
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS gateway_state (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			gateway_instance_id TEXT NOT NULL,
			committed_cursor_msc INTEGER NOT NULL,
			committed_boundary_digest BLOB NOT NULL,
			next_from_msc INTEGER NOT NULL,
			next_requested_count INTEGER NOT NULL,
			last_ingest_sequence INTEGER NOT NULL,
			last_entry_hash BLOB NOT NULL,
			updated_wall_s INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS batches (
			producer_session_id TEXT NOT NULL,
			batch_sequence INTEGER NOT NULL,
			gateway_batch_sha256 BLOB NOT NULL,
			gateway_ingest_sequence INTEGER NOT NULL UNIQUE,
			status INTEGER NOT NULL,
			committed_cursor_msc INTEGER NOT NULL,
			committed_boundary_digest BLOB NOT NULL,
			next_from_msc INTEGER NOT NULL,
			next_requested_count INTEGER NOT NULL,
			frame BLOB NOT NULL,
			PRIMARY KEY (producer_session_id, batch_sequence)
		)`,
		`CREATE INDEX IF NOT EXISTS batches_session_sequence ON batches (producer_session_id, batch_sequence)`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("initialize journal: %w", err)
		}
	}
	zeroDigest := make([]byte, 32)
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO gateway_state
		(singleton, gateway_instance_id, committed_cursor_msc, committed_boundary_digest,
		 next_from_msc, next_requested_count, last_ingest_sequence, last_entry_hash, updated_wall_s)
		VALUES (1, ?, ?, ?, ?, ?, 0, ?, ?)`,
		s.gatewayID,
		s.initialFromMSC,
		zeroDigest,
		s.initialFromMSC,
		int64(s.initialCount),
		zeroDigest,
		time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("initialize journal state: %w", err)
	}
	return nil
}

func (s *Store) State() (State, error) {
	var state State
	var boundary, lastHash []byte
	var nextCount, lastSequence int64
	var updated int64
	err := s.db.QueryRow(
		`SELECT gateway_instance_id, committed_cursor_msc, committed_boundary_digest,
		 next_from_msc, next_requested_count, last_ingest_sequence, last_entry_hash, updated_wall_s
		 FROM gateway_state WHERE singleton=1`,
	).Scan(
		&state.GatewayInstanceID,
		&state.CommittedCursorMSC,
		&boundary,
		&state.NextFromMSC,
		&nextCount,
		&lastSequence,
		&lastHash,
		&updated,
	)
	if err != nil {
		return State{}, fmt.Errorf("read journal state: %w", err)
	}
	if len(boundary) != 32 || len(lastHash) != 32 || nextCount < 0 || nextCount > math.MaxUint32 || lastSequence < 0 {
		return State{}, fmt.Errorf("invalid journal state bytes")
	}
	copy(state.CommittedBoundaryDigest[:], boundary)
	copy(state.LastEntryHash[:], lastHash)
	state.NextRequestedCount = uint32(nextCount)
	state.LastIngestSequence = uint64(lastSequence)
	state.UpdatedWallS = updated
	return state, nil
}

func (s *Store) Lookup(producerSessionID string, batchSequence uint64) (Batch, bool, error) {
	sequence, err := sqliteInt(batchSequence)
	if err != nil {
		return Batch{}, false, err
	}
	var batch Batch
	var storedHash, boundary, frame []byte
	var storedSequence, ingestSequence, status, nextCount int64
	err = s.db.QueryRow(
		`SELECT producer_session_id, batch_sequence, gateway_batch_sha256,
		 gateway_ingest_sequence, status, committed_cursor_msc, committed_boundary_digest,
		 next_from_msc, next_requested_count, frame
		 FROM batches WHERE producer_session_id=? AND batch_sequence=?`,
		producerSessionID, sequence,
	).Scan(
		&batch.ProducerSessionID,
		&storedSequence,
		&storedHash,
		&ingestSequence,
		&status,
		&batch.CommittedCursorMSC,
		&boundary,
		&batch.NextFromMSC,
		&nextCount,
		&frame,
	)
	if err == sql.ErrNoRows {
		return Batch{}, false, nil
	}
	if err != nil {
		return Batch{}, false, fmt.Errorf("lookup journal batch: %w", err)
	}
	if len(storedHash) != 32 || len(boundary) != 32 || storedSequence < 0 || ingestSequence < 0 || status < 0 || status > math.MaxUint8 || nextCount < 0 || nextCount > math.MaxUint32 {
		return Batch{}, false, fmt.Errorf("invalid journal batch")
	}
	batch.BatchSequence = uint64(storedSequence)
	copy(batch.GatewayBatchSHA256[:], storedHash)
	batch.GatewayIngestSequence = uint64(ingestSequence)
	batch.Status = uint8(status)
	copy(batch.CommittedBoundaryDigest[:], boundary)
	batch.NextRequestedCount = uint32(nextCount)
	batch.Frame = append([]byte(nil), frame...)
	return batch, true, nil
}

func (s *Store) LastForSession(producerSessionID string) (Batch, bool, error) {
	var batchSequence int64
	err := s.db.QueryRow(
		`SELECT batch_sequence FROM batches WHERE producer_session_id=? ORDER BY batch_sequence DESC LIMIT 1`,
		producerSessionID,
	).Scan(&batchSequence)
	if err == sql.ErrNoRows {
		return Batch{}, false, nil
	}
	if err != nil {
		return Batch{}, false, fmt.Errorf("lookup last session batch: %w", err)
	}
	return s.Lookup(producerSessionID, uint64(batchSequence))
}

func (s *Store) Count() (int, error) {
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM batches`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count journal batches: %w", err)
	}
	return count, nil
}

func (s *Store) LastWALSequence() (uint64, [32]byte, error) {
	state, err := s.State()
	if err != nil {
		return 0, [32]byte{}, err
	}
	return state.LastIngestSequence, state.LastEntryHash, nil
}

func (s *Store) Apply(entry wal.Entry, batch protocol.BatchFrameV1, outcome Outcome) (Batch, error) {
	sequence, err := sqliteInt(batch.BatchSequence)
	if err != nil {
		return Batch{}, err
	}
	ingestSequence, err := sqliteInt(entry.Sequence)
	if err != nil {
		return Batch{}, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Batch{}, fmt.Errorf("begin journal transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	boundary := append([]byte(nil), outcome.CommittedBoundaryDigest[:]...)
	if _, err := tx.Exec(
		`INSERT INTO batches
		(producer_session_id, batch_sequence, gateway_batch_sha256, gateway_ingest_sequence,
		 status, committed_cursor_msc, committed_boundary_digest, next_from_msc,
		 next_requested_count, frame)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		batch.ProducerSessionID,
		sequence,
		entry.BatchHash[:],
		ingestSequence,
		int64(outcome.Status),
		outcome.CommittedCursorMSC,
		boundary,
		outcome.NextFromMSC,
		int64(outcome.NextRequestedCount),
		entry.Frame,
	); err != nil {
		return Batch{}, fmt.Errorf("insert journal batch: %w", err)
	}
	stateUpdate, err := tx.Exec(
		`UPDATE gateway_state SET
		 committed_cursor_msc=?, committed_boundary_digest=?, next_from_msc=?,
		 next_requested_count=?, last_ingest_sequence=?, last_entry_hash=?, updated_wall_s=?
		 WHERE singleton=1`,
		outcome.CommittedCursorMSC,
		boundary,
		outcome.NextFromMSC,
		int64(outcome.NextRequestedCount),
		ingestSequence,
		entry.EntryHash[:],
		time.Now().Unix(),
	)
	if err != nil {
		return Batch{}, fmt.Errorf("update journal state: %w", err)
	}
	rows, err := stateUpdate.RowsAffected()
	if err != nil {
		return Batch{}, fmt.Errorf("count updated journal state rows: %w", err)
	}
	if rows != 1 {
		return Batch{}, fmt.Errorf("update journal state affected %d rows", rows)
	}
	if err := tx.Commit(); err != nil {
		return Batch{}, fmt.Errorf("commit journal transaction: %w", err)
	}
	return Batch{
		ProducerSessionID:       batch.ProducerSessionID,
		BatchSequence:           batch.BatchSequence,
		GatewayBatchSHA256:      entry.BatchHash,
		GatewayIngestSequence:   entry.Sequence,
		Status:                  outcome.Status,
		CommittedCursorMSC:      outcome.CommittedCursorMSC,
		CommittedBoundaryDigest: outcome.CommittedBoundaryDigest,
		NextFromMSC:             outcome.NextFromMSC,
		NextRequestedCount:      outcome.NextRequestedCount,
		Frame:                   append([]byte(nil), entry.Frame...),
	}, nil
}

func (s *Store) Reset() error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin journal reset: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`DELETE FROM batches`); err != nil {
		return fmt.Errorf("clear journal batches: %w", err)
	}
	zero := make([]byte, 32)
	if _, err := tx.Exec(`DELETE FROM gateway_state WHERE singleton=1`); err != nil {
		return fmt.Errorf("clear journal state: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT INTO gateway_state
		(singleton, gateway_instance_id, committed_cursor_msc, committed_boundary_digest,
		 next_from_msc, next_requested_count, last_ingest_sequence, last_entry_hash, updated_wall_s)
		VALUES (1, ?, ?, ?, ?, ?, 0, ?, ?)`,
		s.gatewayID,
		s.initialFromMSC,
		zero,
		s.initialFromMSC,
		int64(s.initialCount),
		zero,
		time.Now().Unix(),
	); err != nil {
		return fmt.Errorf("reset journal state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit journal reset: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func sqliteInt(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("value %d does not fit SQLite INTEGER", value)
	}
	return int64(value), nil
}
