package retention

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"tick-data-platform/internal/wal"
)

// WALRecovery loads the durable prune boundary before the WAL opens. This is
// a lifecycle component because reading and verifying the checkpoint is I/O.
type WALRecovery struct {
	root    string
	mu      sync.RWMutex
	started bool
	anchor  *wal.PruneAnchor
}

func NewWALRecovery(root string) (*WALRecovery, error) {
	if root == "" {
		return nil, fmt.Errorf("WAL recovery root is empty")
	}
	return &WALRecovery{root: root}, nil
}

func (r *WALRecovery) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return fmt.Errorf("WAL recovery is already started")
	}
	if err := RecoverPrune(r.root, 1<<20); err != nil {
		return err
	}
	checkpoint, err := LoadLatestCheckpoint(r.root)
	if errors.Is(err, ErrCheckpointAbsent) {
		r.started = true
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := VerifyRetainedWAL(r.root, &checkpoint, 1<<20); err != nil {
		return err
	}
	if checkpoint.EndSequence == 0 || checkpoint.EndSequence == ^uint64(0) || checkpoint.RetainedChainRoot == ([32]byte{}) {
		return fmt.Errorf("WAL checkpoint has an invalid prune boundary")
	}
	r.anchor = &wal.PruneAnchor{EndSequence: checkpoint.EndSequence, ChainRoot: checkpoint.RetainedChainRoot}
	r.started = true
	return nil
}

func (r *WALRecovery) LoadWALAnchor(ctx context.Context) (*wal.PruneAnchor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.started {
		return nil, fmt.Errorf("WAL recovery is not started")
	}
	if r.anchor == nil {
		return nil, nil
	}
	copyAnchor := *r.anchor
	return &copyAnchor, nil
}

func (r *WALRecovery) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	r.started = false
	r.mu.Unlock()
	return nil
}

var _ wal.AnchorProvider = (*WALRecovery)(nil)
