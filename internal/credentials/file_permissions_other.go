//go:build !linux && !windows

package credentials

import (
	"fmt"
	"os"
)

func validateCredentialFileSecurity(_ string, file *os.File, protection ProtectionMode) error {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: credential file is not regular", ErrCredentialFileUnsafe)
	}
	if protection == ProtectionNativeACL && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%w: group or other permission is present", ErrCredentialFileUnsafe)
	}
	return nil
}
