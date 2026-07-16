//go:build windows

package credentials

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func validateCredentialFileSecurity(path string, file *os.File, protection ProtectionMode) error {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("%w: credential file is not regular", ErrCredentialFileUnsafe)
	}
	if protection == ProtectionManagedMount {
		return nil
	}

	descriptor, err := windows.GetSecurityInfo(windows.Handle(file.Fd()), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Errorf("%w: read Windows ACL: %v", ErrCredentialFileUnsafe, err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return fmt.Errorf("%w: read Windows ACL owner: %v", ErrCredentialFileUnsafe, err)
	}
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil {
		return fmt.Errorf("%w: read service identity: %v", ErrCredentialFileUnsafe, err)
	}
	allowedOwner := owner.Equals(user.User.Sid) || owner.IsWellKnown(windows.WinLocalSystemSid) || owner.IsWellKnown(windows.WinBuiltinAdministratorsSid)
	if !allowedOwner {
		return fmt.Errorf("%w: credential owner is not an allowed service identity", ErrCredentialFileUnsafe)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil {
		return fmt.Errorf("%w: credential DACL is unavailable", ErrCredentialFileUnsafe)
	}
	for index := uint16(0); index < dacl.AceCount; index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, uint32(index), &ace); err != nil {
			return fmt.Errorf("%w: read credential DACL entry: %v", ErrCredentialFileUnsafe, err)
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			continue
		}
		mask := uint32(ace.Mask)
		readMask := uint32(windows.FILE_READ_DATA | windows.FILE_READ_ATTRIBUTES | windows.FILE_READ_EA | windows.GENERIC_READ)
		if mask&readMask == 0 {
			continue
		}
		entrySID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if entrySID.IsWellKnown(windows.WinWorldSid) || entrySID.IsWellKnown(windows.WinBuiltinUsersSid) || entrySID.IsWellKnown(windows.WinAuthenticatedUserSid) {
			return fmt.Errorf("%w: credential DACL grants broad read access", ErrCredentialFileUnsafe)
		}
		if !entrySID.Equals(user.User.Sid) && !entrySID.IsWellKnown(windows.WinLocalSystemSid) && !entrySID.IsWellKnown(windows.WinBuiltinAdministratorsSid) {
			return fmt.Errorf("%w: credential DACL grants read access to an unapproved identity", ErrCredentialFileUnsafe)
		}
	}
	_ = path
	return nil
}
