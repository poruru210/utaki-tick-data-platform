package r2

import (
	"fmt"
	"net/url"
)

// ValidateHTTPSHostEndpoint accepts only the production R2 endpoint shape.
// Local HTTP test servers must be wired through an explicitly separate test
// path; they must not be accepted by production configuration loaders.
func ValidateHTTPSHostEndpoint(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("endpoint must be an HTTPS host-only URL")
	}
	return nil
}
