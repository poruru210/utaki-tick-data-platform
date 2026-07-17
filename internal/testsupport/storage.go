// Package testsupport contains explicit lifecycle helpers used only by tests.
package testsupport

import (
	"context"

	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
)

func NewStartedWAL(root, gatewayID string) (*wal.Store, error) {
	store, err := wal.NewStore(root, gatewayID, nil)
	if err != nil {
		return nil, err
	}
	if err := store.Start(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}

func NewStartedWALWithAnchor(root, gatewayID string, anchor *wal.PruneAnchor) (*wal.Store, error) {
	store, err := wal.NewStoreWithAnchor(root, gatewayID, anchor)
	if err != nil {
		return nil, err
	}
	if err := store.Start(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}

func NewStartedPublicationJournal(path string) (*r2.PublicationJournal, error) {
	journal, err := r2.NewPublicationJournal(path)
	if err != nil {
		return nil, err
	}
	if err := journal.Start(context.Background()); err != nil {
		return nil, err
	}
	return journal, nil
}
