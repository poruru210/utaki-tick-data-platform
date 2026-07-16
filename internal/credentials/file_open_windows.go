//go:build windows

package credentials

import "os"

func openCredentialFile(path string, _ ProtectionMode) (*os.File, error) {
	return os.Open(path)
}
