// Package publication owns the local, rebuildable coordination state for the
// WAL-to-R2 publication pipeline. It is intentionally separate from the R2
// journal, which records remote intent and remote verification.
package publication

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"tick-data-platform/internal/archive"

	_ "modernc.org/sqlite"
)

var ErrCatalogNotReady = errors.New("publication catalog is not ready")

const (
	SegmentStatePromoted   = "promoted"
	ManifestStateSpooled   = "manifest_spooled"
	ManifestStateRetryWait = "retry_wait"
	// ManifestStatePublished is only a local worker-completion marker. Remote
	// verification authority remains r2.PublicationJournal.StageReceiptSaved.
	ManifestStatePublished = "published"
)

// Clock is injected so retry and manifest timestamps are deterministic in
// tests. The production constructor uses time.Now.
type Clock func() time.Time

type SegmentRecord struct {
	Identity      string
	SealedPath    string
	RawKey        string
	RawPath       string
	SHA256        [32]byte
	Bytes         uint64
	StartSequence uint64
	EndSequence   uint64
	AffectedDates []string
	State         string
	Attempts      uint64
	NextRetryAt   time.Time
	ErrorClass    string
	UpdatedAt     time.Time
}

type ManifestRecord struct {
	Identity    string
	Date        string
	Revision    uint64
	Path        string
	SHA256      [32]byte
	Bytes       uint64
	State       string
	Attempts    uint64
	NextRetryAt time.Time
	ErrorClass  string
	UpdatedAt   time.Time
}

type Catalog struct {
	path    string
	clock   Clock
	mu      sync.RWMutex
	db      *sql.DB
	started bool
}

func NewCatalog(path string) (*Catalog, error) {
	return NewCatalogWithClock(path, time.Now)
}

func NewCatalogWithClock(path string, clock Clock) (*Catalog, error) {
	if path == "" {
		return nil, fmt.Errorf("publication catalog path is empty")
	}
	if clock == nil {
		return nil, fmt.Errorf("publication catalog clock is required")
	}
	return &Catalog{path: path, clock: clock}, nil
}

func (c *Catalog) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil {
		return ErrCatalogNotReady
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.db != nil {
		return fmt.Errorf("publication catalog is already started")
	}
	if parent := filepath.Dir(c.path); parent != "." {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return fmt.Errorf("create publication catalog directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", c.path)
	if err != nil {
		return fmt.Errorf("open publication catalog: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	c.db = db
	if err := initializeCatalog(db); err != nil {
		_ = db.Close()
		c.db = nil
		return err
	}
	c.started = true
	return nil
}

func (c *Catalog) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		c.started = false
		return nil
	}
	err := c.db.Close()
	c.db = nil
	c.started = false
	return err
}

func (c *Catalog) Path() string {
	if c == nil {
		return ""
	}
	return c.path
}

func (c *Catalog) UpsertSegment(ctx context.Context, record SegmentRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateSegmentRecord(record); err != nil {
		return err
	}
	if record.Identity == "" {
		record.Identity = SegmentIdentity(record.SHA256)
	}
	if record.State == "" {
		record.State = SegmentStatePromoted
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = c.clock().UTC()
	}
	dateBytes, err := json.Marshal(sortedDates(record.AffectedDates))
	if err != nil {
		return fmt.Errorf("encode segment affected dates: %w", err)
	}
	bytesValue, err := sqliteUint64(record.Bytes)
	if err != nil {
		return err
	}
	startValue, err := sqliteUint64(record.StartSequence)
	if err != nil {
		return err
	}
	endValue, err := sqliteUint64(record.EndSequence)
	if err != nil {
		return err
	}
	attempts, err := sqliteUint64(record.Attempts)
	if err != nil {
		return err
	}
	nextRetry := unixMilli(record.NextRetryAt)
	updated := record.UpdatedAt.UTC().UnixMilli()

	c.mu.Lock()
	defer c.mu.Unlock()
	db, err := c.dbLocked()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO publication_segments
		(identity, sealed_path, raw_key, raw_path, sha256, bytes, start_sequence, end_sequence,
		 affected_dates_json, state, attempts, next_retry_unix_ms, error_class, updated_unix_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(identity) DO UPDATE SET
		 sealed_path=excluded.sealed_path, raw_key=excluded.raw_key, raw_path=excluded.raw_path,
		 sha256=excluded.sha256, bytes=excluded.bytes, start_sequence=excluded.start_sequence,
		 end_sequence=excluded.end_sequence, affected_dates_json=excluded.affected_dates_json,
		 state=excluded.state, attempts=excluded.attempts, next_retry_unix_ms=excluded.next_retry_unix_ms,
		 error_class=excluded.error_class, updated_unix_ms=excluded.updated_unix_ms`,
		record.Identity, record.SealedPath, record.RawKey, record.RawPath, record.SHA256[:], bytesValue,
		startValue, endValue, string(dateBytes), record.State, attempts, nextRetry, record.ErrorClass, updated,
	)
	if err != nil {
		return fmt.Errorf("upsert publication segment: %w", err)
	}
	return nil
}

func (c *Catalog) ListSegments(ctx context.Context) ([]SegmentRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	db, err := c.dbLocked()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT identity, sealed_path, raw_key, raw_path, sha256, bytes, start_sequence, end_sequence,
		       affected_dates_json, state, attempts, next_retry_unix_ms, error_class, updated_unix_ms
		FROM publication_segments ORDER BY start_sequence, identity`)
	if err != nil {
		return nil, fmt.Errorf("list publication segments: %w", err)
	}
	defer rows.Close()
	result := make([]SegmentRecord, 0)
	for rows.Next() {
		var (
			record                           SegmentRecord
			shaBytes, datesBytes             []byte
			bytesValue, startValue, endValue int64
			attempts, nextRetry, updated     int64
		)
		if err := rows.Scan(&record.Identity, &record.SealedPath, &record.RawKey, &record.RawPath,
			&shaBytes, &bytesValue, &startValue, &endValue, &datesBytes, &record.State, &attempts,
			&nextRetry, &record.ErrorClass, &updated); err != nil {
			return nil, fmt.Errorf("scan publication segment: %w", err)
		}
		if err := decodeSegmentRecord(&record, shaBytes, datesBytes, bytesValue, startValue, endValue, attempts, nextRetry, updated); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate publication segments: %w", err)
	}
	return result, nil
}

func (c *Catalog) UpsertManifest(ctx context.Context, record ManifestRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateManifestRecord(record); err != nil {
		return err
	}
	if record.Identity == "" {
		record.Identity = ManifestIdentity(record.Date, record.Revision, record.SHA256)
	}
	if record.State == "" {
		record.State = ManifestStateSpooled
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = c.clock().UTC()
	}
	bytesValue, err := sqliteUint64(record.Bytes)
	if err != nil {
		return err
	}
	attempts, err := sqliteUint64(record.Attempts)
	if err != nil {
		return err
	}
	revision, err := sqliteUint64(record.Revision)
	if err != nil {
		return err
	}
	updated := record.UpdatedAt.UTC().UnixMilli()

	c.mu.Lock()
	defer c.mu.Unlock()
	db, err := c.dbLocked()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO publication_manifests
		(identity, date, revision, path, sha256, bytes, state, attempts, next_retry_unix_ms, error_class, updated_unix_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(identity) DO UPDATE SET
		 date=excluded.date, revision=excluded.revision, path=excluded.path, sha256=excluded.sha256,
		 bytes=excluded.bytes, state=excluded.state, attempts=excluded.attempts,
		 next_retry_unix_ms=excluded.next_retry_unix_ms, error_class=excluded.error_class,
		 updated_unix_ms=excluded.updated_unix_ms`,
		record.Identity, record.Date, revision, record.Path, record.SHA256[:], bytesValue, record.State,
		attempts, unixMilli(record.NextRetryAt), record.ErrorClass, updated,
	)
	if err != nil {
		return fmt.Errorf("upsert publication manifest: %w", err)
	}
	return nil
}

func (c *Catalog) LatestManifest(ctx context.Context, date string) (ManifestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ManifestRecord{}, false, err
	}
	if err := validateUTCDate(date); err != nil {
		return ManifestRecord{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	db, err := c.dbLocked()
	if err != nil {
		return ManifestRecord{}, false, err
	}
	var (
		record                         ManifestRecord
		shaBytes                       []byte
		revision, bytesValue, attempts int64
		nextRetry, updated             int64
	)
	err = db.QueryRowContext(ctx, `
		SELECT identity, date, revision, path, sha256, bytes, state, attempts, next_retry_unix_ms,
		       error_class, updated_unix_ms
		FROM publication_manifests WHERE date=? ORDER BY revision DESC LIMIT 1`, date).Scan(
		&record.Identity, &record.Date, &revision, &record.Path, &shaBytes, &bytesValue, &record.State,
		&attempts, &nextRetry, &record.ErrorClass, &updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ManifestRecord{}, false, nil
	}
	if err != nil {
		return ManifestRecord{}, false, fmt.Errorf("lookup latest publication manifest: %w", err)
	}
	if err := decodeManifestRecord(&record, shaBytes, revision, bytesValue, attempts, nextRetry, updated); err != nil {
		return ManifestRecord{}, false, err
	}
	return record, true, nil
}

// ManifestAt returns the durable record for one exact date/revision pair.
func (c *Catalog) ManifestAt(ctx context.Context, date string, revision uint64) (ManifestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ManifestRecord{}, false, err
	}
	if err := validateUTCDate(date); err != nil {
		return ManifestRecord{}, false, err
	}
	revisionValue, err := sqliteUint64(revision)
	if err != nil {
		return ManifestRecord{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	db, err := c.dbLocked()
	if err != nil {
		return ManifestRecord{}, false, err
	}
	var (
		record                       ManifestRecord
		shaBytes                     []byte
		revisionRow, bytesValue      int64
		attempts, nextRetry, updated int64
	)
	err = db.QueryRowContext(ctx, `
		SELECT identity, date, revision, path, sha256, bytes, state, attempts, next_retry_unix_ms,
		       error_class, updated_unix_ms
		FROM publication_manifests WHERE date=? AND revision=?`, date, revisionValue).Scan(
		&record.Identity, &record.Date, &revisionRow, &record.Path, &shaBytes, &bytesValue, &record.State,
		&attempts, &nextRetry, &record.ErrorClass, &updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ManifestRecord{}, false, nil
	}
	if err != nil {
		return ManifestRecord{}, false, fmt.Errorf("lookup publication manifest: %w", err)
	}
	if err := decodeManifestRecord(&record, shaBytes, revisionRow, bytesValue, attempts, nextRetry, updated); err != nil {
		return ManifestRecord{}, false, err
	}
	return record, true, nil
}

// EnsureManifest records a filesystem-discovered manifest without changing a
// state that a later remote publisher has already advanced.
func (c *Catalog) EnsureManifest(ctx context.Context, record ManifestRecord) error {
	existing, found, err := c.ManifestAt(ctx, record.Date, record.Revision)
	if err != nil {
		return err
	}
	if found {
		if existing.Identity != record.Identity || existing.Path != record.Path || existing.SHA256 != record.SHA256 || existing.Bytes != record.Bytes {
			return fmt.Errorf("manifest catalog conflicts with filesystem")
		}
		return nil
	}
	return c.UpsertManifest(ctx, record)
}

func (c *Catalog) ListDueManifests(ctx context.Context, now time.Time) ([]ManifestRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	db, err := c.dbLocked()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT identity, date, revision, path, sha256, bytes, state, attempts, next_retry_unix_ms,
		       error_class, updated_unix_ms
		FROM publication_manifests
		WHERE state IN (?, ?) AND (next_retry_unix_ms = 0 OR next_retry_unix_ms <= ?)
		ORDER BY date, revision`, ManifestStateSpooled, ManifestStateRetryWait, now.UTC().UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("list due publication manifests: %w", err)
	}
	defer rows.Close()
	result := make([]ManifestRecord, 0)
	for rows.Next() {
		var (
			record                         ManifestRecord
			shaBytes                       []byte
			revision, bytesValue, attempts int64
			nextRetry, updated             int64
		)
		if err := rows.Scan(&record.Identity, &record.Date, &revision, &record.Path, &shaBytes, &bytesValue, &record.State,
			&attempts, &nextRetry, &record.ErrorClass, &updated); err != nil {
			return nil, fmt.Errorf("scan due publication manifest: %w", err)
		}
		if err := decodeManifestRecord(&record, shaBytes, revision, bytesValue, attempts, nextRetry, updated); err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due publication manifests: %w", err)
	}
	return result, nil
}

func (c *Catalog) MarkManifestState(ctx context.Context, date string, revision uint64, state string, updatedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateUTCDate(date); err != nil {
		return err
	}
	if state == "" {
		return fmt.Errorf("publication manifest state is empty")
	}
	revisionValue, err := sqliteUint64(revision)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	db, err := c.dbLocked()
	if err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, `UPDATE publication_manifests SET state=?, next_retry_unix_ms=0, error_class='', updated_unix_ms=? WHERE date=? AND revision=?`, state, unixMilli(updatedAt), date, revisionValue)
	if err != nil {
		return fmt.Errorf("update publication manifest state: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return fmt.Errorf("publication manifest state target was not found")
	}
	return nil
}

func (c *Catalog) MarkManifestRetry(ctx context.Context, date string, revision uint64, errorClass string, nextRetryAt, updatedAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateUTCDate(date); err != nil {
		return err
	}
	if errorClass == "" {
		return fmt.Errorf("publication retry error class is empty")
	}
	revisionValue, err := sqliteUint64(revision)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	db, err := c.dbLocked()
	if err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, `UPDATE publication_manifests SET state=?, attempts=attempts+1, next_retry_unix_ms=?, error_class=?, updated_unix_ms=? WHERE date=? AND revision=?`, ManifestStateRetryWait, unixMilli(nextRetryAt), errorClass, unixMilli(updatedAt), date, revisionValue)
	if err != nil {
		return fmt.Errorf("record publication retry: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return fmt.Errorf("publication retry target was not found")
	}
	return nil
}

func SegmentIdentity(sha [32]byte) string {
	return hex.EncodeToString(sha[:])
}

func ManifestIdentity(date string, revision uint64, sha [32]byte) string {
	return fmt.Sprintf("%s/%d/%s", date, revision, hex.EncodeToString(sha[:]))
}

func initializeCatalog(db *sql.DB) error {
	statements := []string{
		`PRAGMA journal_mode=DELETE`,
		`PRAGMA synchronous=FULL`,
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS publication_segments (
			identity TEXT PRIMARY KEY,
			sealed_path TEXT NOT NULL,
			raw_key TEXT NOT NULL,
			raw_path TEXT NOT NULL,
			sha256 BLOB NOT NULL,
			bytes INTEGER NOT NULL,
			start_sequence INTEGER NOT NULL,
			end_sequence INTEGER NOT NULL,
			affected_dates_json TEXT NOT NULL,
			state TEXT NOT NULL,
			attempts INTEGER NOT NULL,
			next_retry_unix_ms INTEGER NOT NULL,
			error_class TEXT NOT NULL,
			updated_unix_ms INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS publication_segments_retry ON publication_segments (state, next_retry_unix_ms)`,
		`CREATE TABLE IF NOT EXISTS publication_manifests (
			identity TEXT PRIMARY KEY,
			date TEXT NOT NULL,
			revision INTEGER NOT NULL,
			path TEXT NOT NULL,
			sha256 BLOB NOT NULL,
			bytes INTEGER NOT NULL,
			state TEXT NOT NULL,
			attempts INTEGER NOT NULL,
			next_retry_unix_ms INTEGER NOT NULL,
			error_class TEXT NOT NULL,
			updated_unix_ms INTEGER NOT NULL,
			UNIQUE (date, revision)
		)`,
		`CREATE INDEX IF NOT EXISTS publication_manifests_date ON publication_manifests (date, revision DESC)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("initialize publication catalog: %w", err)
		}
	}
	return nil
}

func (c *Catalog) dbLocked() (*sql.DB, error) {
	if !c.started || c.db == nil {
		return nil, ErrCatalogNotReady
	}
	return c.db, nil
}

func validateSegmentRecord(record SegmentRecord) error {
	if record.SealedPath == "" || record.RawKey == "" || record.RawPath == "" {
		return fmt.Errorf("publication segment paths are incomplete")
	}
	if record.SHA256 == ([32]byte{}) || record.Bytes == 0 {
		return fmt.Errorf("publication segment digest and size are required")
	}
	if record.RawKey != archive.RawWALObjectKey(record.SHA256) {
		return fmt.Errorf("publication segment raw key does not match digest")
	}
	if record.EndSequence < record.StartSequence {
		return fmt.Errorf("publication segment sequence range is invalid")
	}
	for _, date := range record.AffectedDates {
		if err := validateUTCDate(date); err != nil {
			return err
		}
	}
	return nil
}

func validateManifestRecord(record ManifestRecord) error {
	if record.Date == "" || record.Path == "" || record.SHA256 == ([32]byte{}) || record.Bytes == 0 {
		return fmt.Errorf("publication manifest identity and file metadata are incomplete")
	}
	if record.Revision == 0 {
		return fmt.Errorf("publication manifest revision is invalid")
	}
	return validateUTCDate(record.Date)
}

func decodeSegmentRecord(record *SegmentRecord, shaBytes, datesBytes []byte, bytesValue, startValue, endValue, attempts, nextRetry, updated int64) error {
	if len(shaBytes) != 32 || bytesValue <= 0 || startValue < 0 || endValue < 0 || attempts < 0 {
		return fmt.Errorf("publication segment catalog row is invalid")
	}
	copy(record.SHA256[:], shaBytes)
	record.Bytes = uint64(bytesValue)
	record.StartSequence = uint64(startValue)
	record.EndSequence = uint64(endValue)
	record.Attempts = uint64(attempts)
	record.NextRetryAt = fromUnixMilli(nextRetry)
	record.UpdatedAt = fromUnixMilli(updated)
	if err := json.Unmarshal(datesBytes, &record.AffectedDates); err != nil {
		return fmt.Errorf("decode publication segment dates: %w", err)
	}
	if err := validateSegmentRecord(*record); err != nil {
		return err
	}
	return nil
}

func decodeManifestRecord(record *ManifestRecord, shaBytes []byte, revision, bytesValue, attempts, nextRetry, updated int64) error {
	if len(shaBytes) != 32 || revision <= 0 || bytesValue <= 0 || attempts < 0 {
		return fmt.Errorf("publication manifest catalog row is invalid")
	}
	record.Revision = uint64(revision)
	copy(record.SHA256[:], shaBytes)
	record.Bytes = uint64(bytesValue)
	record.Attempts = uint64(attempts)
	record.NextRetryAt = fromUnixMilli(nextRetry)
	record.UpdatedAt = fromUnixMilli(updated)
	return validateManifestRecord(*record)
}

func sortedDates(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func validateUTCDate(value string) error {
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil || parsed.Format("2006-01-02") != value {
		return fmt.Errorf("publication date is not UTC YYYY-MM-DD")
	}
	return nil
}

func sqliteUint64(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("publication value does not fit SQLite INTEGER")
	}
	return int64(value), nil
}

func unixMilli(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UTC().UnixMilli()
}

func fromUnixMilli(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.UnixMilli(value).UTC()
}
