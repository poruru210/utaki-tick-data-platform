package r2

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"

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
)

type PublicationObject struct {
	Key       string
	LocalPath string
	SHA256    [32]byte
	Bytes     uint64
	RemoteKey string
	RcloneKey string
}

type PublicationIntent struct {
	Scope                    archive.ScopeConfig
	Claim                    PublisherClaim
	ClaimKey                 string
	ClaimHash                [32]byte
	ScopeDescriptorKey       string
	ScopeDescriptorRcloneKey string
	ScopeDescriptorPath      string
	ManifestKey              string
	Manifest                 archive.RawDayManifest
	ManifestBytes            []byte
	ManifestPath             string
	Objects                  []PublicationObject
	ReceiptPath              string
}

type JournalRecord struct {
	Identity    string
	IntentHash  [32]byte
	IntentBytes []byte
	Stage       string
}

type PublicationJournal struct {
	db   *sql.DB
	path string
}

func OpenPublicationJournal(path string) (*PublicationJournal, error) {
	if path == "" {
		return nil, fmt.Errorf("publication journal path is empty")
	}
	if parent := filepath.Dir(path); parent != "." {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return nil, fmt.Errorf("create publication journal directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open publication journal")
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	journal := &PublicationJournal{db: db, path: path}
	if err := journal.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return journal, nil
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
			rclone_key TEXT NOT NULL,
			PRIMARY KEY (identity, object_key),
			FOREIGN KEY (identity) REFERENCES publication_intents(identity)
		)`,
		`CREATE TABLE IF NOT EXISTS publication_transitions (
			identity TEXT NOT NULL,
			stage TEXT NOT NULL,
			ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
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
			`INSERT INTO publication_objects (identity, object_key, local_path, sha256, bytes, remote_key, rclone_key)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			intent.ManifestKey, object.Key, object.LocalPath, object.SHA256[:], objectBytes, object.RemoteKey, object.RcloneKey,
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

func (j *PublicationJournal) Close() error {
	if j == nil || j.db == nil {
		return nil
	}
	err := j.db.Close()
	j.db = nil
	return err
}

func canonicalIntent(intent PublicationIntent) ([]byte, error) {
	objects := make([]any, len(intent.Objects))
	for i, object := range intent.Objects {
		objects[i] = map[string]any{
			"bytes":      object.Bytes,
			"key":        object.Key,
			"local_path": object.LocalPath,
			"remote_key": object.RemoteKey,
			"rclone_key": object.RcloneKey,
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
		"claim_bytes":                 string(claimJSON),
		"claim_hash":                  hex.EncodeToString(intent.ClaimHash[:]),
		"claim_key":                   intent.ClaimKey,
		"config_json":                 string(configJSON),
		"manifest_bytes":              string(intent.ManifestBytes),
		"manifest_key":                intent.ManifestKey,
		"manifest_path":               intent.ManifestPath,
		"objects":                     objects,
		"receipt_path":                intent.ReceiptPath,
		"scope_config_hash":           hex.EncodeToString(intent.Manifest.ConfigHash[:]),
		"scope_descriptor_key":        intent.ScopeDescriptorKey,
		"scope_descriptor_path":       intent.ScopeDescriptorPath,
		"scope_descriptor_rclone_key": intent.ScopeDescriptorRcloneKey,
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
	if intent.ScopeDescriptorKey == "" || intent.ScopeDescriptorRcloneKey == "" || intent.ScopeDescriptorPath == "" {
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
		if object.Key == "" || object.LocalPath == "" || object.RemoteKey == "" || object.RcloneKey == "" || object.Bytes == 0 || object.SHA256 == ([32]byte{}) {
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
