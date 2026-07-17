//go:build linux

package credentials

import (
	"os"

	"golang.org/x/sys/unix"
)

func openCredentialFile(path string, protection ProtectionMode) (*os.File, error) {
	if protection != ProtectionNativeACL {
		return os.Open(path)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}
