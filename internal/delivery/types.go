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
)

const (
	VerificationScopeAnchoredDay = "anchored_day_slice"
	VerificationScopeCampaign    = "campaign_genesis_to_root"
)

type ArchiveReaderV1 interface {
	ListDatasets(ctx context.Context) ([]DatasetDescriptor, error)
	ListCampaigns(ctx context.Context, datasetID string) ([]CampaignDescriptor, error)
	ListRawSnapshots(ctx context.Context, scope RawDayScope) ([]SnapshotDescriptor, error)
	ResolveSnapshot(ctx context.Context, selector SnapshotSelector) (ResolvedSnapshot, error)
	BuildFetchPlan(ctx context.Context, snapshot ResolvedSnapshot) (FetchPlan, error)
	Fetch(ctx context.Context, plan FetchPlan, destination string) (FetchResult, error)
	VerifyDay(ctx context.Context, selector SnapshotSelector) (DayVerificationReport, error)
	VerifyCampaign(ctx context.Context, datasetID, campaignID, throughRoot string) (CampaignVerificationReport, error)
}

type DatasetDescriptor struct {
	DatasetID string
}

type CampaignDescriptor struct {
	DatasetID               string
	CampaignID              string
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
	DatasetID  string
	CampaignID string
	Date       string
}

type SnapshotSelector struct {
	DatasetID  string
	CampaignID string
	Date       string
	Manifest   string
}

type SnapshotDescriptor struct {
	DatasetID           string
	CampaignID          string
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
	CampaignID          string
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

type CampaignVerificationReport struct {
	GenesisVerified   bool
	VerificationScope string
	DatasetID         string
	CampaignID        string
	ThroughRoot       [32]byte
	VerifiedThrough   uint64
	SegmentCount      int
	EntryCount        int
}
