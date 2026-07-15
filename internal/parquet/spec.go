package parquet

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"

	"tick-data-platform/internal/protocol"
)

const (
	parquetGoModule      = "github.com/parquet-go/parquet-go"
	parquetGoVersion     = "v0.30.1"
	parquetGoGoVersion   = "go1.24.9"
	parquetGoModuleSum   = "h1:Oy6ganNrAdFiVwy7wNmWagfPTWA2X9Z3tVHBc7JtuX8="
	conversionSpecDomain = "tick-data-platform/conversion-spec/v1\x00"
	writerConfigDomain   = "tick-data-platform/writer-config/v1\x00"
	dependencyLockDomain = "tick-data-platform/dependency-lock/v1\x00"

	WriterApplication = "utaki-tick-data-platform"
	WriterVersion     = "ticks-parquet-v1"
	WriterDataPage    = 1
	WriterPageStats   = true
	WriterCompression = "UNCOMPRESSED"
	WriterPageBuffer  = 256 * 1024
	WriterWriteBuffer = 32 * 1024
)

// ConversionSpec is the complete immutable input to ticks-parquet-v1.
// The limits are logical part-boundary inputs and never depend on Parquet
// compression, a filesystem, or the size of a temporary file.
type ConversionSpec struct {
	ReplayContractID         string
	FormatID                 string
	ConversionID             string
	ConverterBuildID         string
	TargetPlatformContract   string
	MaxRowsPerPart           uint64
	MaxCanonicalBytesPerPart uint64
	MaxRowsPerRowGroup       uint64
	DependencyLockHash       [32]byte
	WriterConfigurationHash  [32]byte
}

// NewConversionSpec fills the derived dependency and writer hashes after
// validating the explicit tuple and bounded part limits.
func NewConversionSpec(replayContractID, conversionID, converterBuildID, targetPlatform string, maxRows, maxBytes, maxRowsPerGroup uint64) (ConversionSpec, error) {
	spec := ConversionSpec{
		ReplayContractID:         replayContractID,
		FormatID:                 protocol.ReplayFormatID,
		ConversionID:             conversionID,
		ConverterBuildID:         converterBuildID,
		TargetPlatformContract:   targetPlatform,
		MaxRowsPerPart:           maxRows,
		MaxCanonicalBytesPerPart: maxBytes,
		MaxRowsPerRowGroup:       maxRowsPerGroup,
	}
	if err := spec.validateTuple(); err != nil {
		return ConversionSpec{}, err
	}
	var err error
	spec.DependencyLockHash, err = DeriveDependencyLockHash()
	if err != nil {
		return ConversionSpec{}, err
	}
	spec.WriterConfigurationHash, err = spec.DeriveWriterConfigurationHash()
	if err != nil {
		return ConversionSpec{}, err
	}
	return spec, nil
}

func (s ConversionSpec) validateTuple() error {
	for name, value := range map[string]string{
		"replay_contract_id":       s.ReplayContractID,
		"format_id":                s.FormatID,
		"conversion_id":            s.ConversionID,
		"converter_build_id":       s.ConverterBuildID,
		"target_platform_contract": s.TargetPlatformContract,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if s.FormatID != protocol.ReplayFormatID {
		return fmt.Errorf("unsupported format id %q", s.FormatID)
	}
	if s.MaxRowsPerPart == 0 || s.MaxCanonicalBytesPerPart == 0 || s.MaxRowsPerRowGroup == 0 {
		return fmt.Errorf("conversion limits must be nonzero")
	}
	if s.MaxRowsPerPart > math.MaxInt64 || s.MaxRowsPerRowGroup > math.MaxInt64 || s.MaxCanonicalBytesPerPart > math.MaxInt64 {
		return fmt.Errorf("conversion limits exceed implementation bounds")
	}
	return nil
}

// Validate proves that a spec contains the exact derived hashes for this
// pinned converter and that no unbounded limit is accepted.
func (s ConversionSpec) Validate() error {
	if err := s.validateTuple(); err != nil {
		return err
	}
	dependency, err := DeriveDependencyLockHash()
	if err != nil {
		return err
	}
	if s.DependencyLockHash != dependency {
		return fmt.Errorf("dependency lock hash does not match pinned parquet-go")
	}
	writer, err := s.DeriveWriterConfigurationHash()
	if err != nil {
		return err
	}
	if s.WriterConfigurationHash != writer {
		return fmt.Errorf("writer configuration hash does not match fixed writer")
	}
	return nil
}

func DeriveDependencyLockHash() ([32]byte, error) {
	value, err := protocol.CanonicalJSON(map[string]any{
		"go_version":      parquetGoGoVersion,
		"module":          parquetGoModule,
		"module_checksum": parquetGoModuleSum,
		"version":         parquetGoVersion,
	})
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(dependencyLockDomain), value...)), nil
}

func (s ConversionSpec) DeriveWriterConfigurationHash() ([32]byte, error) {
	if err := s.validateTuple(); err != nil {
		return [32]byte{}, err
	}
	value, err := protocol.CanonicalJSON(map[string]any{
		"application":            WriterApplication,
		"compression":            WriterCompression,
		"data_page_statistics":   WriterPageStats,
		"data_page_version":      WriterDataPage,
		"format_id":              s.FormatID,
		"max_rows_per_row_group": s.MaxRowsPerRowGroup,
		"page_buffer_size":       WriterPageBuffer,
		"schema_id":              "ticks-parquet-v1-fixed-schema",
		"write_buffer_size":      WriterWriteBuffer,
	})
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(writerConfigDomain), value...)), nil
}

func (s ConversionSpec) CanonicalTupleBytes() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(map[string]any{
		"conversion_id":                s.ConversionID,
		"converter_build_id":           s.ConverterBuildID,
		"dependency_lock_hash":         fmt.Sprintf("%x", s.DependencyLockHash[:]),
		"format_id":                    s.FormatID,
		"max_canonical_bytes_per_part": s.MaxCanonicalBytesPerPart,
		"max_rows_per_part":            s.MaxRowsPerPart,
		"max_rows_per_row_group":       s.MaxRowsPerRowGroup,
		"replay_contract_id":           s.ReplayContractID,
		"target_platform_contract":     s.TargetPlatformContract,
		"writer_configuration_hash":    fmt.Sprintf("%x", s.WriterConfigurationHash[:]),
	})
}

func (s ConversionSpec) IdentityHash() ([32]byte, error) {
	value, err := s.CanonicalTupleBytes()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(conversionSpecDomain), value...)), nil
}

func sameBytes(a, b []byte) bool { return bytes.Equal(a, b) }
