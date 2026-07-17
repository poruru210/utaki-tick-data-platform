package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"

	_ "modernc.org/sqlite"
)

const (
	StageIntent             = "intent"
	StageClaimed            = "claimed"
	StageObjectsCopied      = "objects_copied"
	StageObjectsVerified    = "objects_verified"
	StageManifestCopied     = "manifest_copied"
	StageManifestVerified   = "manifest_verified"
	StageReceiptSaved       = "receipt_saved"
	publicationIntentDomain = "tick-data-platform/publication-intent/v1\x00"

	ObjectStateSealedLocal     = "sealed_local"
	ObjectStateUploading       = "uploading"
	ObjectStateRemoteCommitted = "remote_committed"
	ObjectStateRemoteVerified  = "remote_verified"
	UploadMethodS3PutObjectV1  = "aws-sdk-go-v2-s3-putobject-if-none-match-getobject-sha256-v1"
)

type PublicationObject struct {
	Key       string
	LocalPath string
	SHA256    [32]byte
	MD5       [16]byte
	Bytes     uint64
	RemoteKey string
}

type PublicationIntent struct {
	Scope                 archive.ScopeConfig
	Claim                 PublisherClaim
	ClaimKey              string
	ClaimHash             [32]byte
	ScopeDescriptorKey    string
	ScopeDescriptorPath   string
	ScopeDescriptorSHA256 [32]byte
	ScopeDescriptorBytes  uint64
	ManifestKey           string
	Manifest              archive.RawDayManifest
	ManifestBytes         []byte
	ManifestPath          string
	Objects               []PublicationObject
	ReceiptPath           string
}

type JournalRecord struct {
	Identity    string
	IntentHash  [32]byte
	IntentBytes []byte
	Stage       string
}

// UnfinishedPublication is the durable input needed to resume an intent
// after the local Catalog has not yet recorded completion. The remote journal
// is the source of truth for this recovery list; callers still revalidate all
// local bytes in Publisher.Publish.
type UnfinishedPublication struct {
	Identity string
	Stage    string
	Input    PublicationInput
}

type PublicationObjectStateRecord struct {
	Identity         string
	ObjectKey        string
	LocalPath        string
	Size             uint64
	SHA256           [32]byte
	MD5              [16]byte
	UploadMethod     string
	RemoteETag       string
	RemoteVerifiedAt time.Time
	State            string
}

type PublicationJournal struct {
	db      *sql.DB
	path    string
	started bool
}

// NewPublicationJournal validates the path without opening SQLite.
// Start performs schema recovery and durable initialization.
func NewPublicationJournal(path string) (*PublicationJournal, error) {
	if path == "" {
		return nil, fmt.Errorf("publication journal path is empty")
	}
	return &PublicationJournal{path: path}, nil
}

func (j *PublicationJournal) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if j == nil {
		return fmt.Errorf("publication journal is nil")
	}
	if j.started || j.db != nil {
		return fmt.Errorf("publication journal is already started")
	}
	if parent := filepath.Dir(j.path); parent != "." {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return fmt.Errorf("create publication journal directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", j.path)
	if err != nil {
		return fmt.Errorf("open publication journal")
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	j.db = db
	if err := j.initialize(); err != nil {
		_ = db.Close()
		j.db = nil
		return err
	}
	j.started = true
	return nil
}

func (j *PublicationJournal) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if j == nil {
		return nil
	}
	if j.db == nil {
		j.started = false
		return nil
	}
	err := j.db.Close()
	j.db = nil
	j.started = false
	return err
}

func (j *PublicationJournal) Path() string {
	if j == nil {
		return ""
	}
	return j.path
}

func (j *PublicationJournal) initialize() error {
	statements := []string{
		`PRAGMA journal_mode=DELETE`,
		`PRAGMA synchronous=FULL`,
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS publication_intents (
			identity TEXT PRIMARY KEY,
			intent_hash BLOB NOT NULL,
			intent_bytes BLOB NOT NULL,
			stage TEXT NOT NULL,
			receipt_path TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS publication_objects (
			identity TEXT NOT NULL,
			object_key TEXT NOT NULL,
			local_path TEXT NOT NULL,
			sha256 BLOB NOT NULL,
			bytes INTEGER NOT NULL,
			remote_key TEXT NOT NULL,
			PRIMARY KEY (identity, object_key),
			FOREIGN KEY (identity) REFERENCES publication_intents(identity)
		)`,
		`CREATE TABLE IF NOT EXISTS publication_transitions (
			identity TEXT NOT NULL,
			stage TEXT NOT NULL,
			ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
			FOREIGN KEY (identity) REFERENCES publication_intents(identity)
		)`,
		`CREATE TABLE IF NOT EXISTS publication_object_states (
			identity TEXT NOT NULL,
			object_key TEXT NOT NULL,
			local_path TEXT NOT NULL,
			size INTEGER NOT NULL,
			sha256 BLOB NOT NULL,
			md5 BLOB NOT NULL,
			upload_method TEXT NOT NULL,
			remote_etag TEXT NOT NULL,
			remote_verified_at_unix_ms INTEGER NOT NULL,
			state TEXT NOT NULL,
			ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
			UNIQUE(identity, object_key, state),
			FOREIGN KEY (identity) REFERENCES publication_intents(identity)
		)`,
	}
	for _, statement := range statements {
		if _, err := j.db.Exec(statement); err != nil {
			return fmt.Errorf("initialize publication journal")
		}
	}
	return nil
}

func (j *PublicationJournal) CreateOrGetIntent(intent PublicationIntent) (JournalRecord, error) {
	if err := validatePublicationIntent(intent); err != nil {
		return JournalRecord{}, err
	}
	intentBytes, err := canonicalIntent(intent)
	if err != nil {
		return JournalRecord{}, err
	}
	hash := sha256.Sum256(append([]byte(publicationIntentDomain), intentBytes...))
	record, found, err := j.lookup(intent.ManifestKey)
	if err != nil {
		return JournalRecord{}, err
	}
	if found {
		if record.IntentHash != hash || string(record.IntentBytes) != string(intentBytes) {
			return JournalRecord{}, fmt.Errorf("%w: publication intent content differs", archive.ErrIntegrity)
		}
		return record, nil
	}
	tx, err := j.db.Begin()
	if err != nil {
		return JournalRecord{}, fmt.Errorf("begin publication intent transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`INSERT INTO publication_intents (identity, intent_hash, intent_bytes, stage, receipt_path)
		 VALUES (?, ?, ?, ?, ?)`,
		intent.ManifestKey, hash[:], intentBytes, StageIntent, intent.ReceiptPath,
	); err != nil {
		return JournalRecord{}, fmt.Errorf("insert publication intent")
	}
	for _, object := range intent.Objects {
		objectBytes, err := sqliteUint64(object.Bytes)
		if err != nil {
			return JournalRecord{}, err
		}
		if _, err := tx.Exec(
			`INSERT INTO publication_objects (identity, object_key, local_path, sha256, bytes, remote_key)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			intent.ManifestKey, object.Key, object.LocalPath, object.SHA256[:], objectBytes, object.RemoteKey,
		); err != nil {
			return JournalRecord{}, fmt.Errorf("insert publication object")
		}
	}
	if _, err := tx.Exec(`INSERT INTO publication_transitions (identity, stage) VALUES (?, ?)`, intent.ManifestKey, StageIntent); err != nil {
		return JournalRecord{}, fmt.Errorf("insert publication transition")
	}
	if err := tx.Commit(); err != nil {
		return JournalRecord{}, fmt.Errorf("commit publication intent")
	}
	return JournalRecord{Identity: intent.ManifestKey, IntentHash: hash, IntentBytes: append([]byte(nil), intentBytes...), Stage: StageIntent}, nil
}

func (j *PublicationJournal) SetStage(identity, stage string) error {
	if !validPublicationStage(stage) {
		return fmt.Errorf("invalid publication stage")
	}
	current, found, err := j.lookup(identity)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("publication intent is not registered")
	}
	if publicationStageRank(stage) < publicationStageRank(current.Stage) {
		return fmt.Errorf("%w: publication stage cannot move backwards", archive.ErrIntegrity)
	}
	if stage == current.Stage {
		return nil
	}
	tx, err := j.db.Begin()
	if err != nil {
		return fmt.Errorf("begin publication stage transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`UPDATE publication_intents SET stage=? WHERE identity=?`, stage, identity); err != nil {
		return fmt.Errorf("update publication stage")
	}
	if _, err := tx.Exec(`INSERT INTO publication_transitions (identity, stage) VALUES (?, ?)`, identity, stage); err != nil {
		return fmt.Errorf("insert publication stage transition")
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit publication stage")
	}
	return nil
}

// AdvanceStage is idempotent and never moves a durable publication backwards.
func (j *PublicationJournal) AdvanceStage(identity, stage string) error {
	if !validPublicationStage(stage) {
		return fmt.Errorf("invalid publication stage")
	}
	current, found, err := j.lookup(identity)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("publication intent is not registered")
	}
	if publicationStageRank(current.Stage) >= publicationStageRank(stage) {
		return nil
	}
	return j.SetStage(identity, stage)
}

func (j *PublicationJournal) Record(identity string) (JournalRecord, bool, error) {
	return j.lookup(identity)
}

// ListUnfinished reconstructs every non-terminal publication intent from the
// durable journal. It intentionally returns only the narrow Publisher input;
// claim and scope fields remain journal-owned and are not copied into local
// coordination state.
func (j *PublicationJournal) ListUnfinished(ctx context.Context) ([]UnfinishedPublication, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := j.db.QueryContext(ctx, `
		SELECT identity, intent_hash, intent_bytes, stage
		FROM publication_intents WHERE stage <> ? ORDER BY identity`, StageReceiptSaved)
	if err != nil {
		return nil, fmt.Errorf("list unfinished publication intents: %w", err)
	}
	defer rows.Close()
	records := make([]JournalRecord, 0)
	for rows.Next() {
		var record JournalRecord
		var hashBytes []byte
		if err := rows.Scan(&record.Identity, &hashBytes, &record.IntentBytes, &record.Stage); err != nil {
			return nil, fmt.Errorf("scan unfinished publication intent: %w", err)
		}
		if len(hashBytes) != 32 || len(record.IntentBytes) == 0 || !validPublicationStage(record.Stage) {
			return nil, fmt.Errorf("%w: unfinished publication intent is malformed", archive.ErrIntegrity)
		}
		copy(record.IntentHash[:], hashBytes)
		wantHash := sha256.Sum256(append([]byte(publicationIntentDomain), record.IntentBytes...))
		if record.IntentHash != wantHash {
			return nil, fmt.Errorf("%w: unfinished publication intent hash changed", archive.ErrIntegrity)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unfinished publication intents: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close unfinished publication intents: %w", err)
	}
	result := make([]UnfinishedPublication, 0, len(records))
	for _, record := range records {
		input, err := j.inputFromIntent(ctx, record)
		if err != nil {
			return nil, err
		}
		result = append(result, UnfinishedPublication{Identity: record.Identity, Stage: record.Stage, Input: input})
	}
	return result, nil
}

type canonicalIntentEnvelope struct {
	ManifestBytes string `json:"manifest_bytes"`
	ManifestKey   string `json:"manifest_key"`
	ManifestPath  string `json:"manifest_path"`
	ReceiptPath   string `json:"receipt_path"`
}

func (j *PublicationJournal) inputFromIntent(ctx context.Context, record JournalRecord) (PublicationInput, error) {
	var envelope canonicalIntentEnvelope
	if err := json.Unmarshal(record.IntentBytes, &envelope); err != nil {
		return PublicationInput{}, fmt.Errorf("%w: decode unfinished publication intent", archive.ErrIntegrity)
	}
	if envelope.ManifestKey != record.Identity || envelope.ManifestPath == "" || envelope.ReceiptPath == "" || envelope.ManifestBytes == "" {
		return PublicationInput{}, fmt.Errorf("%w: unfinished publication intent input is incomplete", archive.ErrIntegrity)
	}
	manifestBytes := []byte(envelope.ManifestBytes)
	manifest, err := archive.VerifyRawDayManifest(manifestBytes)
	if err != nil {
		return PublicationInput{}, fmt.Errorf("%w: unfinished publication manifest is invalid", archive.ErrIntegrity)
	}
	canonical, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil || !bytes.Equal(canonical, manifestBytes) {
		return PublicationInput{}, fmt.Errorf("%w: unfinished publication manifest is not canonical", archive.ErrIntegrity)
	}
	paths, err := j.objectPaths(ctx, record.Identity, manifest)
	if err != nil {
		return PublicationInput{}, err
	}
	return PublicationInput{
		Manifest:      manifest,
		ManifestBytes: manifestBytes,
		ManifestPath:  envelope.ManifestPath,
		ObjectPaths:   paths,
		ReceiptPath:   envelope.ReceiptPath,
	}, nil
}

func (j *PublicationJournal) objectPaths(ctx context.Context, identity string, manifest archive.RawDayManifest) (map[string]string, error) {
	rows, err := j.db.QueryContext(ctx, `
		SELECT object_key, local_path, sha256, bytes, remote_key
		FROM publication_objects WHERE identity=? ORDER BY object_key`, identity)
	if err != nil {
		return nil, fmt.Errorf("read unfinished publication objects: %w", err)
	}
	defer rows.Close()
	paths := make(map[string]string)
	for rows.Next() {
		var key, localPath, remoteKey string
		var shaBytes []byte
		var bytesValue int64
		if err := rows.Scan(&key, &localPath, &shaBytes, &bytesValue, &remoteKey); err != nil {
			return nil, fmt.Errorf("scan unfinished publication object: %w", err)
		}
		if key == "" || localPath == "" || remoteKey == "" || len(shaBytes) != 32 || bytesValue <= 0 {
			return nil, fmt.Errorf("%w: unfinished publication object is malformed", archive.ErrIntegrity)
		}
		var sha [32]byte
		copy(sha[:], shaBytes)
		if _, found := paths[key]; found {
			return nil, fmt.Errorf("%w: unfinished publication object is duplicated", archive.ErrIntegrity)
		}
		paths[key] = localPath
		matched := false
		for _, chainObject := range manifest.ChainObjects {
			if chainObject.Key == key {
				if chainObject.SHA256 != sha || uint64(bytesValue) != chainObject.Bytes {
					return nil, fmt.Errorf("%w: unfinished publication object metadata differs", archive.ErrIntegrity)
				}
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("%w: unfinished publication object is not in manifest", archive.ErrIntegrity)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unfinished publication objects: %w", err)
	}
	if len(paths) != len(manifest.ChainObjects) {
		return nil, fmt.Errorf("%w: unfinished publication intent does not contain every object", archive.ErrIntegrity)
	}
	return paths, nil
}

func (j *PublicationJournal) RecordObjectState(identity string, object PublicationObject, state, remoteETag string, remoteVerifiedAt time.Time) error {
	if identity == "" || object.RemoteKey == "" || object.LocalPath == "" || object.Bytes == 0 || object.SHA256 == ([32]byte{}) || object.MD5 == ([16]byte{}) || !validPublicationObjectState(state) {
		return fmt.Errorf("%w: publication object state is incomplete", archive.ErrIntegrity)
	}
	if state == ObjectStateRemoteVerified && remoteVerifiedAt.IsZero() {
		return fmt.Errorf("%w: remote_verified state requires verification time", archive.ErrIntegrity)
	}
	if state != ObjectStateRemoteVerified {
		remoteVerifiedAt = time.Time{}
	}
	size, err := sqliteUint64(object.Bytes)
	if err != nil {
		return err
	}
	verifiedAt := int64(0)
	if !remoteVerifiedAt.IsZero() {
		verifiedAt = remoteVerifiedAt.UTC().UnixMilli()
	}
	result, err := j.db.Exec(
		`INSERT OR IGNORE INTO publication_object_states
		 (identity, object_key, local_path, size, sha256, md5, upload_method, remote_etag, remote_verified_at_unix_ms, state)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		identity, object.RemoteKey, object.LocalPath, size, object.SHA256[:], object.MD5[:], UploadMethodS3PutObjectV1, remoteETag, verifiedAt, state,
	)
	if err != nil {
		return fmt.Errorf("insert publication object state")
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return j.ensureObjectStateMatches(identity, object, state, remoteETag, verifiedAt)
	}
	return nil
}

func (j *PublicationJournal) ensureObjectStateMatches(identity string, object PublicationObject, state, remoteETag string, verifiedAt int64) error {
	var gotLocalPath, gotUploadMethod, gotRemoteETag string
	var gotSize, gotVerifiedAt int64
	var gotSHA, gotMD5 []byte
	err := j.db.QueryRow(
		`SELECT local_path, size, sha256, md5, upload_method, remote_etag, remote_verified_at_unix_ms
		 FROM publication_object_states WHERE identity=? AND object_key=? AND state=?`,
		identity, object.RemoteKey, state,
	).Scan(&gotLocalPath, &gotSize, &gotSHA, &gotMD5, &gotUploadMethod, &gotRemoteETag, &gotVerifiedAt)
	if err != nil {
		return fmt.Errorf("read existing publication object state")
	}
	if gotLocalPath != object.LocalPath || gotSize < 0 || uint64(gotSize) != object.Bytes || !bytes.Equal(gotSHA, object.SHA256[:]) || !bytes.Equal(gotMD5, object.MD5[:]) || gotUploadMethod != UploadMethodS3PutObjectV1 {
		return fmt.Errorf("%w: publication object state content differs", archive.ErrIntegrity)
	}
	if remoteETag != "" && gotRemoteETag != "" && gotRemoteETag != remoteETag {
		return fmt.Errorf("%w: publication object state ETag differs", archive.ErrIntegrity)
	}
	_ = verifiedAt
	_ = gotVerifiedAt
	return nil
}

func (j *PublicationJournal) ObjectStateRecords(identity string) ([]PublicationObjectStateRecord, error) {
	rows, err := j.db.Query(
		`SELECT identity, object_key, local_path, size, sha256, md5, upload_method, remote_etag, remote_verified_at_unix_ms, state
		 FROM publication_object_states WHERE identity=? ORDER BY ordinal`,
		identity,
	)
	if err != nil {
		return nil, fmt.Errorf("read publication object states")
	}
	defer rows.Close()
	var records []PublicationObjectStateRecord
	for rows.Next() {
		var record PublicationObjectStateRecord
		var size, verifiedAt int64
		var shaBytes, md5Bytes []byte
		if err := rows.Scan(&record.Identity, &record.ObjectKey, &record.LocalPath, &size, &shaBytes, &md5Bytes, &record.UploadMethod, &record.RemoteETag, &verifiedAt, &record.State); err != nil {
			return nil, fmt.Errorf("scan publication object state")
		}
		if size < 0 || len(shaBytes) != 32 || len(md5Bytes) != 16 || !validPublicationObjectState(record.State) {
			return nil, fmt.Errorf("%w: publication object state is malformed", archive.ErrIntegrity)
		}
		record.Size = uint64(size)
		copy(record.SHA256[:], shaBytes)
		copy(record.MD5[:], md5Bytes)
		if verifiedAt != 0 {
			record.RemoteVerifiedAt = time.UnixMilli(verifiedAt).UTC()
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate publication object states")
	}
	return records, nil
}

// FindRemoteVerifiedObject is the deletion-side read boundary. It returns an
// exact durable remote verification for one immutable object identity without
// exposing the journal's broader coordination state to retention planning.
func (j *PublicationJournal) FindRemoteVerifiedObject(remoteKey string, sha [32]byte, size uint64) (PublicationObjectStateRecord, bool, error) {
	return j.findRemoteVerifiedObject(remoteKey, "", sha, size)
}

// FindRemoteVerifiedObjectAtPath additionally binds the verification to the
// exact local raw-outbox path that retention is about to remove.
func (j *PublicationJournal) FindRemoteVerifiedObjectAtPath(remoteKey, localPath string, sha [32]byte, size uint64) (PublicationObjectStateRecord, bool, error) {
	if localPath == "" {
		return PublicationObjectStateRecord{}, false, fmt.Errorf("%w: local verification path is empty", archive.ErrIntegrity)
	}
	return j.findRemoteVerifiedObject(remoteKey, localPath, sha, size)
}

func (j *PublicationJournal) findRemoteVerifiedObject(remoteKey, localPath string, sha [32]byte, size uint64) (PublicationObjectStateRecord, bool, error) {
	if remoteKey == "" || sha == ([32]byte{}) || size == 0 {
		return PublicationObjectStateRecord{}, false, fmt.Errorf("%w: remote verification lookup identity is incomplete", archive.ErrIntegrity)
	}
	sizeValue, err := sqliteUint64(size)
	if err != nil {
		return PublicationObjectStateRecord{}, false, err
	}
	var record PublicationObjectStateRecord
	var sizeRow, verifiedAt int64
	var shaBytes, md5Bytes []byte
	query := `SELECT identity, object_key, local_path, size, sha256, md5, upload_method, remote_etag, remote_verified_at_unix_ms
		FROM publication_object_states
		WHERE object_key=? AND size=? AND sha256=? AND state=?`
	args := []any{remoteKey, sizeValue, sha[:], ObjectStateRemoteVerified}
	if localPath != "" {
		query += ` AND local_path=?`
		args = append(args, localPath)
	}
	query += ` ORDER BY ordinal DESC LIMIT 1`
	err = j.db.QueryRow(query, args...).Scan(&record.Identity, &record.ObjectKey, &record.LocalPath, &sizeRow, &shaBytes, &md5Bytes, &record.UploadMethod, &record.RemoteETag, &verifiedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return PublicationObjectStateRecord{}, false, nil
	}
	if err != nil {
		return PublicationObjectStateRecord{}, false, fmt.Errorf("lookup remote_verified object: %w", err)
	}
	if sizeRow < 0 || uint64(sizeRow) != size || len(shaBytes) != 32 || len(md5Bytes) != 16 || verifiedAt == 0 {
		return PublicationObjectStateRecord{}, false, fmt.Errorf("%w: remote_verified object row is malformed", archive.ErrIntegrity)
	}
	record.Size = uint64(sizeRow)
	copy(record.SHA256[:], shaBytes)
	copy(record.MD5[:], md5Bytes)
	record.RemoteVerifiedAt = time.UnixMilli(verifiedAt).UTC()
	record.State = ObjectStateRemoteVerified
	return record, true, nil
}

// LastRemoteVerifiedAt returns the latest durable full-byte verification time
// without exposing remote object contents or credentials to the status layer.
func (j *PublicationJournal) LastRemoteVerifiedAt(ctx context.Context) (time.Time, bool, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	var value sql.NullInt64
	if err := j.db.QueryRowContext(ctx, `
		SELECT MAX(remote_verified_at_unix_ms)
		FROM publication_object_states WHERE state=?`, ObjectStateRemoteVerified).Scan(&value); err != nil {
		return time.Time{}, false, fmt.Errorf("read last remote verification: %w", err)
	}
	if !value.Valid || value.Int64 <= 0 {
		return time.Time{}, false, nil
	}
	return time.UnixMilli(value.Int64).UTC(), true, nil
}

func (j *PublicationJournal) lookup(identity string) (JournalRecord, bool, error) {
	var hashBytes, intentBytes []byte
	var record JournalRecord
	err := j.db.QueryRow(
		`SELECT identity, intent_hash, intent_bytes, stage FROM publication_intents WHERE identity=?`,
		identity,
	).Scan(&record.Identity, &hashBytes, &intentBytes, &record.Stage)
	if err == sql.ErrNoRows {
		return JournalRecord{}, false, nil
	}
	if err != nil {
		return JournalRecord{}, false, fmt.Errorf("read publication intent")
	}
	if len(hashBytes) != 32 || len(intentBytes) == 0 || !validPublicationStage(record.Stage) {
		return JournalRecord{}, false, fmt.Errorf("%w: publication journal intent is malformed", archive.ErrIntegrity)
	}
	copy(record.IntentHash[:], hashBytes)
	record.IntentBytes = append([]byte(nil), intentBytes...)
	return record, true, nil
}

func canonicalIntent(intent PublicationIntent) ([]byte, error) {
	objects := make([]any, len(intent.Objects))
	for i, object := range intent.Objects {
		objects[i] = map[string]any{
			"bytes":      object.Bytes,
			"key":        object.Key,
			"local_path": object.LocalPath,
			"remote_key": object.RemoteKey,
			"sha256":     hex.EncodeToString(object.SHA256[:]),
		}
	}
	configJSON, err := intent.Scope.CanonicalConfigJSON()
	if err != nil {
		return nil, err
	}
	claimJSON, err := intent.Claim.CanonicalJSON()
	if err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(map[string]any{
		"claim_bytes":             string(claimJSON),
		"claim_hash":              hex.EncodeToString(intent.ClaimHash[:]),
		"claim_key":               intent.ClaimKey,
		"config_json":             string(configJSON),
		"manifest_bytes":          string(intent.ManifestBytes),
		"manifest_key":            intent.ManifestKey,
		"manifest_path":           intent.ManifestPath,
		"objects":                 objects,
		"receipt_path":            intent.ReceiptPath,
		"scope_config_hash":       hex.EncodeToString(intent.Manifest.ConfigHash[:]),
		"scope_descriptor_bytes":  intent.ScopeDescriptorBytes,
		"scope_descriptor_key":    intent.ScopeDescriptorKey,
		"scope_descriptor_path":   intent.ScopeDescriptorPath,
		"scope_descriptor_sha256": hex.EncodeToString(intent.ScopeDescriptorSHA256[:]),
	})
}

func validatePublicationIntent(intent PublicationIntent) error {
	if intent.ManifestKey == "" || len(intent.ManifestBytes) == 0 || intent.ReceiptPath == "" {
		return fmt.Errorf("%w: publication intent is incomplete", archive.ErrIntegrity)
	}
	if intent.ManifestPath == "" {
		return fmt.Errorf("%w: manifest path is missing", archive.ErrIntegrity)
	}
	if intent.ClaimKey == "" || intent.ClaimHash == ([32]byte{}) {
		return fmt.Errorf("%w: publisher claim is incomplete", archive.ErrIntegrity)
	}
	if intent.ScopeDescriptorKey == "" || intent.ScopeDescriptorPath == "" || intent.ScopeDescriptorSHA256 == ([32]byte{}) || intent.ScopeDescriptorBytes == 0 {
		return fmt.Errorf("%w: scope descriptor transfer is incomplete", archive.ErrIntegrity)
	}
	if intent.Manifest.ManifestSHA256 == ([32]byte{}) {
		return fmt.Errorf("%w: manifest digest is missing", archive.ErrIntegrity)
	}
	decoded, err := archive.VerifyRawDayManifest(intent.ManifestBytes)
	if err != nil || decoded.ManifestSHA256 != intent.Manifest.ManifestSHA256 {
		return fmt.Errorf("%w: publication intent manifest bytes are invalid", archive.ErrIntegrity)
	}
	canonical, err := archive.ManifestCanonicalJSON(intent.Manifest)
	if err != nil || !bytes.Equal(canonical, intent.ManifestBytes) {
		return fmt.Errorf("%w: publication intent manifest is not canonical", archive.ErrIntegrity)
	}
	claimHash, err := intent.Claim.Digest()
	if err != nil || claimHash != intent.ClaimHash {
		return fmt.Errorf("%w: publication intent claim hash is invalid", archive.ErrIntegrity)
	}
	if _, err := sqliteUint64(intent.Manifest.PublisherEpoch); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(intent.Objects))
	for _, object := range intent.Objects {
		if object.Key == "" || object.LocalPath == "" || object.RemoteKey == "" || object.Bytes == 0 || object.SHA256 == ([32]byte{}) || object.MD5 == ([16]byte{}) {
			return fmt.Errorf("%w: publication object is incomplete", archive.ErrIntegrity)
		}
		if _, ok := seen[object.Key]; ok {
			return fmt.Errorf("%w: publication object is duplicated", archive.ErrIntegrity)
		}
		var matches int
		for _, chainObject := range intent.Manifest.ChainObjects {
			if object.Key == chainObject.Key && object.SHA256 == chainObject.SHA256 && object.Bytes == chainObject.Bytes {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("%w: publication object is not in manifest chain_objects", archive.ErrIntegrity)
		}
		seen[object.Key] = struct{}{}
	}
	if len(intent.Objects) != len(intent.Manifest.ChainObjects) {
		return fmt.Errorf("%w: publication intent does not contain every chain object", archive.ErrIntegrity)
	}
	return nil
}

func sqliteUint64(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("%w: value does not fit SQLite integer", archive.ErrIntegrity)
	}
	return int64(value), nil
}

func validPublicationStage(stage string) bool {
	return publicationStageRank(stage) > 0
}

func publicationStageRank(stage string) int {
	switch stage {
	case StageIntent:
		return 1
	case StageClaimed:
		return 2
	case StageObjectsCopied:
		return 3
	case StageObjectsVerified:
		return 4
	case StageManifestCopied:
		return 5
	case StageManifestVerified:
		return 6
	case StageReceiptSaved:
		return 7
	default:
		return 0
	}
}

func validPublicationObjectState(state string) bool {
	switch state {
	case ObjectStateSealedLocal, ObjectStateUploading, ObjectStateRemoteCommitted, ObjectStateRemoteVerified:
		return true
	default:
		return false
	}
}
