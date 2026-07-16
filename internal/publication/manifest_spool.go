package publication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/wal"
)

const (
	ManifestTerminalSyncStatus = "unknown"
	ManifestCompleteness       = "provisional"
)

// Planner turns verified local raw objects into canonical provisional
// raw-day manifests. It never contacts R2 and never invents a settled state.
type Planner struct {
	scope           archive.ScopeConfig
	manifestRoot    string
	catalog         *Catalog
	clock           Clock
	publicationGate ManifestPublicationGate
}

// ManifestPublicationGate is the narrow boundary used to prevent a local
// manifest successor from overtaking a predecessor that has not been
// durably published. The implementation belongs to the composition root so
// the planner itself remains network-free.
type ManifestPublicationGate interface {
	IsPublished(context.Context, archive.RawDayManifest) (bool, error)
}

func NewPlanner(scope archive.ScopeConfig, manifestRoot string, catalog *Catalog, clock Clock) (*Planner, error) {
	return NewPlannerWithGate(scope, manifestRoot, catalog, clock, nil)
}

func NewPlannerWithGate(scope archive.ScopeConfig, manifestRoot string, catalog *Catalog, clock Clock, publicationGate ManifestPublicationGate) (*Planner, error) {
	if manifestRoot == "" || catalog == nil || clock == nil {
		return nil, fmt.Errorf("manifest planner dependencies are incomplete")
	}
	if _, err := scope.ConfigHash(); err != nil {
		return nil, err
	}
	return &Planner{scope: scope, manifestRoot: manifestRoot, catalog: catalog, clock: clock, publicationGate: publicationGate}, nil
}

// Reconcile rebuilds manifest catalog rows from complete canonical files. A
// missing SQLite catalog therefore cannot cause a later revision to be
// mistaken for a new genesis revision. Temporary files are deliberately
// ignored; an unexpected final JSON file is an integrity error.
func (p *Planner) Reconcile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(p.manifestRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect manifest spool: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("manifest spool is not a directory")
	}
	manifests := make(map[string]map[uint64]archive.RawDayManifest)
	err = filepath.WalkDir(p.manifestRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(entry.Name()) != ".json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read manifest during reconciliation: %w", err)
		}
		manifest, err := archive.VerifyRawDayManifest(data)
		if err != nil {
			return fmt.Errorf("verify manifest during reconciliation: %w", err)
		}
		canonical, err := archive.ManifestCanonicalJSON(manifest)
		if err != nil || !bytes.Equal(canonical, data) {
			return fmt.Errorf("manifest during reconciliation is not canonical")
		}
		if filepath.Clean(path) != filepath.Clean(manifestPath(p.manifestRoot, manifest)) {
			return fmt.Errorf("manifest is outside its canonical spool path")
		}
		byRevision := manifests[manifest.Date]
		if byRevision == nil {
			byRevision = make(map[uint64]archive.RawDayManifest)
			manifests[manifest.Date] = byRevision
		}
		if existing, ok := byRevision[manifest.Revision]; ok && existing.ManifestSHA256 != manifest.ManifestSHA256 {
			return fmt.Errorf("manifest revision has multiple digests")
		}
		byRevision[manifest.Revision] = manifest
		if err := p.catalog.EnsureManifest(ctx, ManifestRecord{
			Identity:  ManifestIdentity(manifest.Date, manifest.Revision, manifest.ManifestSHA256),
			Date:      manifest.Date,
			Revision:  manifest.Revision,
			Path:      path,
			SHA256:    manifest.ManifestSHA256,
			Bytes:     uint64(len(data)),
			State:     ManifestStateSpooled,
			UpdatedAt: p.clock().UTC(),
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	for date, byRevision := range manifests {
		revisions := make([]uint64, 0, len(byRevision))
		for revision := range byRevision {
			revisions = append(revisions, revision)
		}
		sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
		if len(revisions) == 0 || revisions[0] != 1 {
			return fmt.Errorf("manifest revision chain for %s does not start at 1", date)
		}
		for i := 1; i < len(revisions); i++ {
			previousRevision := revisions[i-1]
			currentRevision := revisions[i]
			if currentRevision != previousRevision+1 {
				return fmt.Errorf("manifest revision chain for %s has a gap", date)
			}
			previous := byRevision[previousRevision]
			current := byRevision[currentRevision]
			if current.PreviousManifestSHA256 == nil || *current.PreviousManifestSHA256 != previous.ManifestSHA256 {
				return fmt.Errorf("manifest revision chain for %s has an invalid predecessor", date)
			}
		}
	}
	return nil
}

// Plan creates a new revision only when the selected local data changed. The
// bool is false for duplicate wakeups that produce the same semantic snapshot.
func (p *Planner) Plan(ctx context.Context, date string, objects []archive.RawObject) (ManifestRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ManifestRecord{}, false, err
	}
	if err := validateUTCDate(date); err != nil {
		return ManifestRecord{}, false, err
	}
	if len(objects) == 0 {
		return ManifestRecord{}, false, fmt.Errorf("manifest planner has no raw objects")
	}
	previousRecord, found, err := p.catalog.LatestManifest(ctx, date)
	if err != nil {
		return ManifestRecord{}, false, err
	}
	var previous *archive.RawDayManifest
	if found {
		loaded, err := readCanonicalManifest(previousRecord.Path, previousRecord.SHA256, previousRecord.Bytes)
		if err != nil {
			return ManifestRecord{}, false, err
		}
		previous = &loaded
		if p.publicationGate != nil {
			published, err := p.publicationGate.IsPublished(ctx, *previous)
			if err != nil {
				return ManifestRecord{}, false, fmt.Errorf("check manifest predecessor publication: %w", err)
			}
			if !published {
				return previousRecord, false, nil
			}
		}
	}
	revision := uint64(1)
	if previous != nil {
		revision = previous.Revision + 1
	}
	candidate, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope:              p.scope,
		Date:               date,
		RawObjects:         objects,
		Revision:           revision,
		Previous:           previous,
		TerminalSyncStatus: ManifestTerminalSyncStatus,
		CompletenessStatus: ManifestCompleteness,
		LogicalCloseTimeS:  0,
	})
	if err != nil {
		return ManifestRecord{}, false, err
	}
	if previous != nil && !manifestSelectionChanged(*previous, candidate) {
		return previousRecord, false, nil
	}
	canonical, err := archive.ManifestCanonicalJSON(candidate)
	if err != nil {
		return ManifestRecord{}, false, err
	}
	path := manifestPath(p.manifestRoot, candidate)
	if err := publishCanonical(path, canonical); err != nil {
		return ManifestRecord{}, false, err
	}
	record := ManifestRecord{
		Identity:  ManifestIdentity(candidate.Date, candidate.Revision, candidate.ManifestSHA256),
		Date:      candidate.Date,
		Revision:  candidate.Revision,
		Path:      path,
		SHA256:    candidate.ManifestSHA256,
		Bytes:     uint64(len(canonical)),
		State:     ManifestStateSpooled,
		UpdatedAt: p.clock().UTC(),
	}
	if err := p.catalog.UpsertManifest(ctx, record); err != nil {
		return ManifestRecord{}, false, err
	}
	return record, true, nil
}

// AffectedDates derives every UTC date touched by a verified WAL segment.
// Empty batches are represented by their requested timestamp, matching the
// archive manifest's zero-record sentinel rule.
func AffectedDates(segment wal.VerifiedSegment) ([]string, error) {
	seen := make(map[string]struct{})
	for _, entry := range segment.Entries {
		frame, err := protocol.DecodeFrame(entry.Frame)
		if err != nil {
			return nil, fmt.Errorf("decode WAL entry %d: %w", entry.Sequence, err)
		}
		message, err := protocol.DecodeMessage(frame)
		if err != nil {
			return nil, fmt.Errorf("decode WAL entry %d: %w", entry.Sequence, err)
		}
		batch, ok := message.(protocol.BatchFrameV1)
		if !ok {
			return nil, fmt.Errorf("WAL entry %d is not BatchFrameV1", entry.Sequence)
		}
		if len(batch.Records) == 0 {
			seen[utcDate(batch.RequestedFromMSC)] = struct{}{}
			continue
		}
		for _, record := range batch.Records {
			seen[utcDate(record.TimeMSC)] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for date := range seen {
		result = append(result, date)
	}
	sort.Strings(result)
	return result, nil
}

func manifestPath(root string, manifest archive.RawDayManifest) string {
	return filepath.Join(root, "raw-day", "date="+manifest.Date,
		fmt.Sprintf("raw-day-%020d-%x.json", manifest.Revision, manifest.ManifestSHA256))
}

func readCanonicalManifest(path string, expectedSHA [32]byte, expectedBytes uint64) (archive.RawDayManifest, error) {
	if path == "" {
		return archive.RawDayManifest{}, fmt.Errorf("manifest path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return archive.RawDayManifest{}, fmt.Errorf("read local manifest: %w", err)
	}
	if uint64(len(data)) != expectedBytes {
		return archive.RawDayManifest{}, fmt.Errorf("local manifest size changed")
	}
	manifest, err := archive.VerifyRawDayManifest(data)
	if err != nil {
		return archive.RawDayManifest{}, fmt.Errorf("verify local manifest: %w", err)
	}
	if manifest.ManifestSHA256 != expectedSHA {
		return archive.RawDayManifest{}, fmt.Errorf("local manifest digest changed")
	}
	canonical, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil || !bytes.Equal(canonical, data) {
		return archive.RawDayManifest{}, fmt.Errorf("local manifest is not canonical")
	}
	return manifest, nil
}

func publishCanonical(path string, canonical []byte) error {
	if path == "" || len(canonical) == 0 {
		return fmt.Errorf("manifest publication input is incomplete")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create manifest directory: %w", err)
	}
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, canonical) {
			return nil
		}
		return fmt.Errorf("manifest path already contains different bytes")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect manifest path: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".raw-day-manifest-*.tmp")
	if err != nil {
		return fmt.Errorf("create manifest temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set manifest temporary permissions: %w", err)
	}
	if _, err := temporary.Write(canonical); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write manifest temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync manifest temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close manifest temporary file: %w", err)
	}
	if err := os.Link(temporaryPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := os.ReadFile(path)
			if readErr == nil && bytes.Equal(existing, canonical) {
				return nil
			}
			return fmt.Errorf("manifest publication raced with different bytes")
		}
		return fmt.Errorf("publish manifest without overwrite: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("remove manifest temporary link: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("sync manifest directory: %w", err)
	}
	return nil
}

func manifestSelectionChanged(previous, current archive.RawDayManifest) bool {
	if previous.Date != current.Date || previous.DatasetID != current.DatasetID || previous.CampaignID != current.CampaignID || previous.DayDefinitionID != current.DayDefinitionID {
		return true
	}
	if previous.AcceptedRecordCount != current.AcceptedRecordCount || previous.ErrorCount != current.ErrorCount ||
		previous.ObservedThroughSourceMSC != current.ObservedThroughSourceMSC || previous.ObservedThroughCaptureSeq != current.ObservedThroughCaptureSeq ||
		previous.ChainSliceStartSequence != current.ChainSliceStartSequence || previous.ChainSliceStartRoot != current.ChainSliceStartRoot ||
		previous.ChainSliceEndSequence != current.ChainSliceEndSequence || previous.ChainSliceEndRoot != current.ChainSliceEndRoot ||
		previous.RawSetRoot != current.RawSetRoot {
		return true
	}
	if !equalRawObjectRanges(previous.Objects, current.Objects) || !equalRawChainObjects(previous.ChainObjects, current.ChainObjects) {
		return true
	}
	return false
}

func equalRawObjectRanges(left, right []archive.RawObjectRange) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalRawChainObjects(left, right []archive.RawChainObject) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func utcDate(milliseconds int64) string {
	return time.UnixMilli(milliseconds).UTC().Format("2006-01-02")
}
