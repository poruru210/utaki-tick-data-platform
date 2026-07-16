//go:build !windows

package ingest

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func (OSDiskUsageProvider) Usage(root string) (DiskUsage, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(root, &stat); err != nil {
		return DiskUsage{}, fmt.Errorf("stat filesystem: %w", err)
	}
	free, err := checkedMultiply(uint64(stat.Bavail), uint64(stat.Bsize))
	if err != nil {
		return DiskUsage{}, err
	}
	total, err := checkedMultiply(uint64(stat.Blocks), uint64(stat.Bsize))
	if err != nil {
		return DiskUsage{}, err
	}
	return DiskUsage{FreeBytes: free, TotalBytes: total}, nil
}
