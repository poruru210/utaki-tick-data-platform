package r2

import "testing"

func TestValidateHTTPSHostEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"http://example.invalid",
		"https://user@example.invalid",
		"https://example.invalid/path",
		"https://example.invalid?query=1",
		"https://example.invalid#fragment",
		"https://example.invalid",
		"https://r2.cloudflarestorage.com",
	} {
		if err := ValidateHTTPSHostEndpoint(endpoint); err == nil {
			t.Fatalf("unsafe endpoint %q was accepted", endpoint)
		}
	}
	if err := ValidateHTTPSHostEndpoint("https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com"); err != nil {
		t.Fatalf("valid endpoint was rejected: %v", err)
	}
}
