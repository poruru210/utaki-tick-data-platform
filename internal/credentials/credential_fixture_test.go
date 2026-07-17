package credentials

import "testing"

func secureCredentialFixtureForTest(t *testing.T, path string) {
	t.Helper()
	secureCredentialFixtureForOS(t, path)
}
