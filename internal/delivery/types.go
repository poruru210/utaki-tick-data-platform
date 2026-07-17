package delivery

import (
	"context"
	"errors"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

var (
	ErrUnsupportedReplay = errors.New("replay delivery is outside M2R-3")
	ErrSelectorInvalid   = errors.New("snapshot selector is invalid")
	ErrSelectorNotFound  = errors.New("snapshot selector was not found")
)

const (
	VerificationScopeAnchoredDay = "anchored_day_slice"
	VerificationScopeFullChain   = "scope_genesis_to_root"
)

type ArchiveReaderV1 interface {
	ListDatasets(ctx context.Context) ([]DatasetDescriptor, error)
	ListScopes(ctx context.Context, datasetID string) ([]ScopeDescriptor, error)
	ListRawSnapshots(ctx context.Context, scope RawDayScope) ([]SnapshotDescriptor, error)
	ResolveSnapshot(ctx context.Context, selector SnapshotSelector) (ResolvedSnapshot, error)
	BuildFetchPlan(ctx context.Context, snapshot ResolvedSnapshot) (FetchPlan, error)
	Fetch(ctx context.Context, plan FetchPlan, destination string) (FetchResult, error)
	VerifyDay(ctx context.Context, selector SnapshotSelector) (DayVerificationReport, error)
	VerifyScope(ctx context.Context, scope RawScopeSelector, throughRoot string) (ScopeVerificationReport, error)
	ListReplaySnapshots(ctx context.Context, scope ReplayDayScope) ([]ReplaySnapshotDescriptor, error)
	ResolveReplaySnapshot(ctx context.Context, selector ReplaySnapshotSelector) (ResolvedReplaySnapshot, error)
	BuildReplayFetchPlan(ctx context.Context, snapshot ResolvedReplaySnapshot) (ReplayFetchPlan, error)
	FetchReplay(ctx context.Context, plan ReplayFetchPlan, destination string) (ReplayFetchResult, error)
	VerifyReplayDay(ctx context.Context, selector ReplaySnapshotSelector) (ReplayDayVerificationReport, error)
}

type DatasetDescriptor struct {
	DatasetID string
}

type ScopeDescriptor struct {
	DatasetID               string
	ProviderID              string
	StableFeedID            string
	ExactSourceSymbol       string
	BrokerServerFingerprint string
	DayDefinitionID         string
	PublisherID             string
	PublisherEpoch          uint64
	ConfigHash              [32]byte
}

type RawDayScope struct {
	DatasetID         string
	ProviderID        string
	ExactSourceSymbol string
	Date              string
}

type SnapshotSelector struct {
	DatasetID         string
	ProviderID        string
	ExactSourceSymbol string
	Date              string
	Manifest          string
}

type SnapshotDescriptor struct {
	DatasetID           string
	ProviderID          string
	ExactSourceSymbol   string
	DayDefinitionID     string
	Date                string
	Revision            uint64
	PublisherID         string
	PublisherEpoch      uint64
	ManifestKey         string
	ManifestSHA256      [32]byte
	ChainSliceStart     uint64
	ChainSliceStartRoot [32]byte
	ChainSliceEnd       uint64
	ChainSliceEndRoot   [32]byte
	AcceptedRecordCount uint64
	ErrorCount          uint64
}

type ResolvedSnapshot struct {
	Descriptor     SnapshotDescriptor
	Scope          archive.ScopeConfig
	Manifest       archive.RawDayManifest
	ManifestKey    string
	ManifestBytes  []byte
	ManifestSHA256 [32]byte
}

type FetchObject struct {
	Key       string
	RemoteKey string
	SHA256    [32]byte
	Bytes     uint64
	CachePath string
}

type FetchPlan struct {
	ManifestKey       string
	ManifestSHA256    [32]byte
	ManifestBytes     []byte
	ManifestCachePath string
	Objects           []FetchObject
}

type FetchResult struct {
	ManifestPath string
	ObjectPaths  map[string]string
	Entries      []RestoredEntry
}

type RestoredEntry struct {
	ObjectKey              string
	GatewayIngestSequence  uint64
	Frame                  []byte
	Batch                  protocol.BatchFrameV1
	PreviousEntryHash      [32]byte
	EntryHash              [32]byte
	SelectedRecordOrdinals []uint32
}

type DayVerificationReport struct {
	GenesisVerified     bool
	VerificationScope   string
	DatasetID           string
	Date                string
	Revision            uint64
	ManifestKey         string
	ManifestSHA256      [32]byte
	PredecessorAnchor   [32]byte
	ChainSliceStart     uint64
	ChainSliceStartRoot [32]byte
	ChainSliceEnd       uint64
	ChainSliceEndRoot   [32]byte
	AcceptedRecordCount uint64
	ErrorCount          uint64
	Entries             []RestoredEntry
}

type RawScopeSelector struct {
	DatasetID         string
	ProviderID        string
	ExactSourceSymbol string
}

type ScopeVerificationReport struct {
	GenesisVerified   bool
	VerificationScope string
	DatasetID         string
	ProviderID        string
	ExactSourceSymbol string
	ThroughRoot       [32]byte
	VerifiedThrough   uint64
	SegmentCount      int
	EntryCount        int
}

const VerificationScopeReplayAnchoredDay = "replay_anchored_day"

type ReplayDayScope struct {
	DatasetID         string
	ProviderID        string
	ExactSourceSymbol string
	DayDefinitionID   string
	Date              string
	ReplayContractID  string
	ConversionID      string
}

type ReplaySnapshotSelector struct {
	ReplayDayScope
	Revision *uint64
	Manifest string
}

type ReplaySnapshotDescriptor struct {
	DatasetID                   string
	ProviderID                  string
	ExactSourceSymbol           string
	DayDefinitionID             string
	Date                        string
	ReplayContractID            string
	ConversionID                string
	Revision                    uint64
	ManifestKey                 string
	ManifestSHA256              [32]byte
	PreviousManifestSHA256      *[32]byte
	RawDayManifestKey           string
	RawDayManifestSHA256        [32]byte
	PartSetRoot                 [32]byte
	CanonicalStreamRowChainRoot [32]byte
	PartCount                   uint64
}

type ResolvedReplaySnapshot struct {
	Descriptor     ReplaySnapshotDescriptor
	Scope          archive.ScopeConfig
	Manifest       protocol.ReplayDayManifest
	ManifestKey    string
	ManifestBytes  []byte
	ManifestSHA256 [32]byte
}

type ReplayFetchObjectKind string

const (
	ReplayFetchManifest     ReplayFetchObjectKind = "replay_manifest"
	ReplayFetchPartManifest ReplayFetchObjectKind = "part_manifest"
	ReplayFetchParquet      ReplayFetchObjectKind = "parquet"
)

type ReplayFetchObject struct {
	Kind           ReplayFetchObjectKind
	Key            string
	RemoteKey      string
	Digest         [32]byte
	Bytes          uint64
	CachePath      string
	CanonicalBytes []byte
}

type ReplayFetchPlan struct {
	Manifest ReplayFetchObject
	Parts    []ReplayFetchObject
	Parquet  []ReplayFetchObject
}

type ReplayFetchResult struct {
	ManifestPath      string
	PartManifestPaths map[string]string
	ParquetPaths      map[string]string
}

type ReplayDayVerificationReport struct {
	GenesisVerified               bool
	VerificationScope             string
	DatasetID                     string
	ProviderID                    string
	ExactSourceSymbol             string
	DayDefinitionID               string
	Date                          string
	ReplayContractID              string
	ConversionID                  string
	Revision                      uint64
	ManifestKey                   string
	ManifestSHA256                [32]byte
	RawBindingVerified            bool
	RawDaySemanticsVerified       bool
	PartManifestChainVerified     bool
	PartSetRootVerified           bool
	ParquetSchemaVerified         bool
	ParquetRowsVerified           bool
	ParquetFileHashesVerified     bool
	CanonicalRowChainRootVerified bool
	EmptyDay                      bool
	PartCount                     uint64
	RowCount                      uint64
}
