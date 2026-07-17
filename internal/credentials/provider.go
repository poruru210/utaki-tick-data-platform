// Package credentials owns the narrow, secret-free credential loading contract
// used by the Gateway's R2 client.
package credentials

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const MaxBundleBytes int64 = 64 << 10

var (
	ErrCredentialPathRequired = errors.New("credential path is required")
	ErrCredentialFileUnsafe   = errors.New("credential file permissions are unsafe")
	ErrCredentialTooLarge     = errors.New("credential file is too large")
	ErrCredentialMalformed    = errors.New("credential file is malformed")
	ErrCredentialVersion      = errors.New("unsupported credential format version")
	ErrCredentialIncomplete   = errors.New("credential fields are incomplete")
)

type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// CredentialError classifies a credential-loading failure without exposing
// credential values. Its Unwrap method preserves errors.Is compatibility with
// the package sentinel errors while callers can use errors.As to inspect the
// stable classification boundary.
type CredentialError struct {
	Kind  error
	Cause error
}

func (e *CredentialError) Error() string {
	if e == nil {
		return "credential error"
	}
	if e.Cause == nil || errors.Is(e.Cause, e.Kind) {
		return e.Kind.Error()
	}
	return fmt.Sprintf("%s: %v", e.Kind, e.Cause)
}

func (e *CredentialError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Kind
}

func newCredentialError(kind, cause error) error {
	if kind == nil {
		return cause
	}
	return &CredentialError{Kind: kind, Cause: cause}
}

func classifyCredentialError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*CredentialError); ok {
		return err
	}
	for _, kind := range []error{
		ErrCredentialPathRequired,
		ErrCredentialFileUnsafe,
		ErrCredentialTooLarge,
		ErrCredentialMalformed,
		ErrCredentialVersion,
		ErrCredentialIncomplete,
	} {
		if errors.Is(err, kind) {
			return newCredentialError(kind, err)
		}
	}
	return err
}

// Format prevents accidental disclosure when the public value is logged with
// any fmt verb. String and GoString are intentionally not implemented.
func (Credentials) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, "[credentials redacted]")
}

type Provider interface {
	Load(context.Context) (Credentials, error)
}

type FileConfig struct {
	Path       string
	Protection ProtectionMode
}

type ProtectionMode string

const (
	ProtectionNativeACL    ProtectionMode = "native-acl"
	ProtectionManagedMount ProtectionMode = "managed-mount"
)

type FileProvider struct {
	path              string
	protection        ProtectionMode
	securityValidator func(string, *os.File, ProtectionMode) error
}

type bundle struct {
	FormatVersion   int    `json:"format_version"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func NewFileProvider(config FileConfig) (*FileProvider, error) {
	if strings.TrimSpace(config.Path) == "" {
		return nil, newCredentialError(ErrCredentialPathRequired, nil)
	}
	if config.Protection == "" {
		config.Protection = ProtectionNativeACL
	}
	if config.Protection != ProtectionNativeACL && config.Protection != ProtectionManagedMount {
		return nil, newCredentialError(ErrCredentialFileUnsafe, fmt.Errorf("unknown protection mode %q", config.Protection))
	}
	return &FileProvider{path: config.Path, protection: config.Protection}, nil
}

func (p *FileProvider) Load(ctx context.Context) (Credentials, error) {
	if p == nil || strings.TrimSpace(p.path) == "" {
		return Credentials{}, newCredentialError(ErrCredentialPathRequired, nil)
	}
	if err := ctx.Err(); err != nil {
		return Credentials{}, err
	}

	file, err := openCredentialFile(p.path, p.protection)
	if err != nil {
		return Credentials{}, newCredentialError(ErrCredentialFileUnsafe, fmt.Errorf("open credential file: %w", err))
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return Credentials{}, newCredentialError(ErrCredentialFileUnsafe, fmt.Errorf("stat credential file: %w", err))
	}
	if !info.Mode().IsRegular() {
		return Credentials{}, newCredentialError(ErrCredentialFileUnsafe, errors.New("credential path is not a regular file"))
	}
	if info.Size() > MaxBundleBytes {
		return Credentials{}, newCredentialError(ErrCredentialTooLarge, nil)
	}
	validator := p.securityValidator
	if validator == nil {
		validator = validateCredentialFileSecurity
	}
	if err := validator(p.path, file, p.protection); err != nil {
		return Credentials{}, classifyCredentialError(err)
	}

	data, err := io.ReadAll(io.LimitReader(file, MaxBundleBytes+1))
	if err != nil {
		return Credentials{}, newCredentialError(ErrCredentialFileUnsafe, fmt.Errorf("read credential file: %w", err))
	}
	if int64(len(data)) > MaxBundleBytes {
		return Credentials{}, newCredentialError(ErrCredentialTooLarge, nil)
	}
	if err := ctx.Err(); err != nil {
		return Credentials{}, err
	}
	loaded, err := decodeBundle(data)
	if err != nil {
		return Credentials{}, classifyCredentialError(err)
	}
	return loaded, nil
}

func decodeBundle(data []byte) (Credentials, error) {
	if bytes.HasPrefix(data, []byte{0xef, 0xbb, 0xbf}) {
		return Credentials{}, ErrCredentialMalformed
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return Credentials{}, fmt.Errorf("%w: invalid JSON", ErrCredentialMalformed)
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return Credentials{}, fmt.Errorf("%w: bundle must be a JSON object", ErrCredentialMalformed)
	}
	seen := make(map[string]struct{}, 3)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return Credentials{}, fmt.Errorf("%w: invalid object key", ErrCredentialMalformed)
		}
		key, ok := keyToken.(string)
		if !ok {
			return Credentials{}, fmt.Errorf("%w: object key is not a string", ErrCredentialMalformed)
		}
		if _, exists := seen[key]; exists {
			return Credentials{}, fmt.Errorf("%w: duplicate field", ErrCredentialMalformed)
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return Credentials{}, fmt.Errorf("%w: invalid field value", ErrCredentialMalformed)
		}
	}
	if token, err := decoder.Token(); err != nil {
		return Credentials{}, fmt.Errorf("%w: unterminated object", ErrCredentialMalformed)
	} else if delimiter, ok := token.(json.Delim); !ok || delimiter != '}' {
		return Credentials{}, fmt.Errorf("%w: invalid object end", ErrCredentialMalformed)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Credentials{}, fmt.Errorf("%w: trailing JSON value", ErrCredentialMalformed)
		}
		return Credentials{}, fmt.Errorf("%w: trailing bytes", ErrCredentialMalformed)
	}

	var decoded bundle
	strict := json.NewDecoder(bytes.NewReader(data))
	strict.DisallowUnknownFields()
	if err := strict.Decode(&decoded); err != nil {
		return Credentials{}, fmt.Errorf("%w: invalid bundle fields", ErrCredentialMalformed)
	}
	if decoded.FormatVersion != 1 {
		return Credentials{}, ErrCredentialVersion
	}
	if decoded.AccessKeyID == "" || decoded.SecretAccessKey == "" {
		return Credentials{}, ErrCredentialIncomplete
	}
	return Credentials{AccessKeyID: decoded.AccessKeyID, SecretAccessKey: decoded.SecretAccessKey}, nil
}
