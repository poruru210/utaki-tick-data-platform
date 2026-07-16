//go:build windows

package ingest

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func (OSDiskUsageProvider) Usage(root string) (DiskUsage, error) {
	path, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return DiskUsage{}, fmt.Errorf("encode filesystem path: %w", err)
	}
	var free, total uint64
	if err := windows.GetDiskFreeSpaceEx(path, &free, &total, nil); err != nil {
		return DiskUsage{}, fmt.Errorf("stat filesystem: %w", err)
	}
	return DiskUsage{FreeBytes: free, TotalBytes: total}, nil
}
