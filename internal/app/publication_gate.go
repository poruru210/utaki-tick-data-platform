package app

import (
	"context"
	"fmt"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/publication"
	"tick-data-platform/internal/r2"
)

// r2ManifestPublicationGate adapts the remote journal's terminal stage to
// publication's network-free planner boundary. The local planner may build a
// successor only after the exact predecessor has reached receipt_saved.
type r2ManifestPublicationGate struct {
	journal *r2.PublicationJournal
	layout  r2.Layout
}

func newManifestPublicationGate(journal *r2.PublicationJournal, layout r2.Layout) (publication.ManifestPublicationGate, error) {
	if journal == nil {
		return nil, fmt.Errorf("manifest publication gate journal is nil")
	}
	return &r2ManifestPublicationGate{journal: journal, layout: layout}, nil
}

func (g *r2ManifestPublicationGate) IsPublished(ctx context.Context, manifest archive.RawDayManifest) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key, err := g.layout.ManifestKey(manifest)
	if err != nil {
		return false, err
	}
	record, found, err := g.journal.Record(key)
	if err != nil {
		return false, err
	}
	return found && record.Stage == r2.StageReceiptSaved, nil
}

var _ publication.ManifestPublicationGate = (*r2ManifestPublicationGate)(nil)
