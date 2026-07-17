//go:build linux

package credentials

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func validateCredentialFileSecurity(path string, file *os.File, protection ProtectionMode) error {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: credential file is not regular", ErrCredentialFileUnsafe)
	}
	if protection == ProtectionManagedMount {
		return nil
	}
	if permission := info.Mode().Perm(); permission != 0o400 && permission != 0o600 {
		return fmt.Errorf("%w: credential file mode must be 0400 or 0600", ErrCredentialFileUnsafe)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%w: credential owner is unavailable", ErrCredentialFileUnsafe)
	}
	uid := uint32(os.Getuid())
	if uid != 0 && stat.Uid != uid && stat.Uid != 0 {
		return fmt.Errorf("%w: credential owner is not the service user", ErrCredentialFileUnsafe)
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("%w: credential parent directory cannot be checked", ErrCredentialFileUnsafe)
	}
	if parent.Mode().Perm()&0o002 != 0 {
		return fmt.Errorf("%w: credential parent directory is world-writable", ErrCredentialFileUnsafe)
	}
	return nil
}
