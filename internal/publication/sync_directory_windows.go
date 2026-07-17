//go:build windows

package publication

// Windows does not expose directory fsync through os.File. The temporary file
// is flushed before the no-clobber link; startup reconciliation remains the
// authority if a directory entry is lost during a crash.
func syncDirectory(string) error { return nil }
