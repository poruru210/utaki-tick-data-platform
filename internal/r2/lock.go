package r2

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
	"tick-data-platform/internal/archive"
)

var ErrPublicationLock = errors.New("publication lock is held")

type CampaignLock struct {
	file *flock.Flock
	path string
}

func PublicationLockPath(root string, scope archive.ScopeConfig) (string, error) {
	if root == "" {
		return "", fmt.Errorf("publication lock root is empty")
	}
	scopeKey, err := archive.ScopePathKey(scope)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, fmt.Sprintf("campaign-%s-epoch-%d.lock", scopeKey, scope.PublisherEpoch)), nil
}

func AcquirePublicationLock(path string) (*CampaignLock, error) {
	if path == "" {
		return nil, fmt.Errorf("publication lock path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create publication lock directory: %w", err)
	}
	file := flock.New(path, flock.SetPermissions(0o600))
	locked, err := file.TryLock()
	if err != nil {
		return nil, fmt.Errorf("acquire publication lock: %w", err)
	}
	if !locked {
		return nil, ErrPublicationLock
	}
	return &CampaignLock{file: file, path: path}, nil
}

func (l *CampaignLock) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

func (l *CampaignLock) Held() bool {
	return l != nil && l.file != nil
}

func (l *CampaignLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Unlock()
	l.file = nil
	return err
}
