package r2

import "testing"

func TestValidateHTTPSHostEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"http://example.invalid",
		"https://user@example.invalid",
		"https://example.invalid/path",
		"https://example.invalid?query=1",
		"https://example.invalid#fragment",
	} {
		if err := ValidateHTTPSHostEndpoint(endpoint); err == nil {
			t.Fatalf("unsafe endpoint %q was accepted", endpoint)
		}
	}
	if err := ValidateHTTPSHostEndpoint("https://example.invalid"); err != nil {
		t.Fatalf("valid endpoint was rejected: %v", err)
	}
}
