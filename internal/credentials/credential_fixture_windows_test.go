//go:build windows

package credentials

import (
	"fmt"
	"testing"

	"golang.org/x/sys/windows"
)

func secureCredentialFixtureForOS(t *testing.T, path string) {
	t.Helper()
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil {
		t.Fatalf("read test service identity: %v", err)
	}
	sid := user.User.Sid.String()
	descriptor, err := windows.SecurityDescriptorFromString(fmt.Sprintf("D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;%s)", sid))
	if err != nil {
		t.Fatalf("build protected credential ACL fixture: %v", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatalf("read protected credential ACL fixture: %v", err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		t.Fatalf("apply protected credential ACL fixture: %v", err)
	}
}
