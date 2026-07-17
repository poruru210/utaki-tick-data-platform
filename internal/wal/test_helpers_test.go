package wal

import "context"

func newStartedStore(root, gatewayID string) (*Store, error) {
	store, err := NewStore(root, gatewayID, nil)
	if err != nil {
		return nil, err
	}
	if err := store.Start(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}
