//go:build !windows

package credentials

import "testing"

func secureCredentialFixtureForOS(t *testing.T, _ string) {
	t.Helper()
}
