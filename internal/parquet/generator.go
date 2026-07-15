package parquet

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/uncompressed"

	"tick-data-platform/internal/protocol"
)

var (
	errClosed                 = errors.New("parquet generator is closed")
	errStreamSequenceOverflow = errors.New("replay stream sequence overflow")
	errInvalidParquetRow      = errors.New("invalid parquet replay row")
)

// PartArtifact is the verified local result of one deterministic part write.
// It contains metadata only, not the rows of the day.
type PartArtifact struct {
	PartSequence         uint32
	Path                 string
	PartKey              string
	PartSHA256           [32]byte
	PartBytes            uint64
	RowCount             uint64
	CanonicalRowBytes    uint64
	FirstStreamSequence  uint64
	LastStreamSequence   uint64
	PreviousRowChainHash [32]byte
	FirstRowChainHash    [32]byte
	LastRowChainHash     [32]byte
}

type GenerationResult struct {
	Scope             protocol.ReplayScope
	Parts             []PartArtifact
	RowCount          uint64
	CanonicalRowBytes uint64
	RowChainRoot      [32]byte
}

type Generator struct {
	spec       ConversionSpec
	scope      protocol.ReplayScope
	outputRoot string

	rows           []parquetRow
	replayRows     []protocol.ReplayRow
	canonical      [][]byte
	chains         [][32]byte
	rowCount       uint64
	canonicalBytes uint64
	rowChainRoot   [32]byte
	parts          []PartArtifact
	closed         bool
}

func NewGenerator(spec ConversionSpec, scope protocol.ReplayScope, outputRoot string) (*Generator, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if scope.ReplayContractID != spec.ReplayContractID || scope.ConversionID != spec.ConversionID {
		return nil, fmt.Errorf("conversion spec does not match replay scope")
	}
	if outputRoot == "" {
		return nil, fmt.Errorf("output root is required")
	}
	return &Generator{spec: spec, scope: scope, outputRoot: outputRoot}, nil
}

// WriteRow is a streaming sink. It validates and canonicalizes the row before
// it becomes visible to the Parquet writer, and only commits chain state after
// the bounded part buffer accepts it.
func (g *Generator) WriteRow(row protocol.ReplayRow) error {
	if g.closed {
		return errClosed
	}
	if row.Scope() != g.scope {
		return fmt.Errorf("replay row scope does not match generator scope")
	}
	if row.StreamSequence() != g.rowCount {
		return fmt.Errorf("replay stream sequence %d is not %d", row.StreamSequence(), g.rowCount)
	}
	physical, canonical, next, err := makeParquetRow(row, g.rowChainRoot)
	if err != nil {
		return fmt.Errorf("canonicalize replay row %d: %w", row.StreamSequence(), err)
	}
	rowBytes := uint64(len(canonical))
	if rowBytes > g.spec.MaxCanonicalBytesPerPart {
		return fmt.Errorf("row %d exceeds MaxCanonicalBytesPerPart", row.StreamSequence())
	}
	if len(g.rows) > 0 && (uint64(len(g.rows)) >= g.spec.MaxRowsPerPart || g.canonicalBytes > g.spec.MaxCanonicalBytesPerPart-rowBytes) {
		if err := g.flushPart(); err != nil {
			return err
		}
	}
	if len(g.rows) >= int(g.spec.MaxRowsPerPart) {
		return fmt.Errorf("part row limit is not representable")
	}
	g.rows = append(g.rows, physical)
	g.replayRows = append(g.replayRows, row)
	g.canonical = append(g.canonical, canonical)
	g.chains = append(g.chains, next)
	g.canonicalBytes += rowBytes
	g.rowChainRoot = next
	g.rowCount++
	return nil
}

func (g *Generator) Close() (GenerationResult, error) {
	if g.closed {
		return GenerationResult{}, errClosed
	}
	g.closed = true
	if err := g.flushPart(); err != nil {
		return GenerationResult{}, err
	}
	return GenerationResult{
		Scope: g.scope, Parts: append([]PartArtifact(nil), g.parts...),
		RowCount: g.rowCount, CanonicalRowBytes: g.totalCanonicalBytes(), RowChainRoot: g.rowChainRoot,
	}, nil
}

func (g *Generator) totalCanonicalBytes() uint64 {
	var total uint64
	for _, part := range g.parts {
		total += part.CanonicalRowBytes
	}
	return total
}

func (g *Generator) flushPart() error {
	if len(g.rows) == 0 {
		return nil
	}
	if len(g.parts) > int(^uint32(0)) {
		return fmt.Errorf("too many replay parts")
	}
	artifact, err := writeAndVerifyPart(g.outputRoot, uint32(len(g.parts)), g.scope, g.spec, g.rows, g.replayRows, g.canonical, g.chains)
	if err != nil {
		return err
	}
	g.parts = append(g.parts, artifact)
	g.rows = nil
	g.replayRows = nil
	g.canonical = nil
	g.chains = nil
	g.canonicalBytes = 0
	return nil
}

func writeAndVerifyPart(root string, sequence uint32, scope protocol.ReplayScope, spec ConversionSpec, rows []parquetRow, replayRows []protocol.ReplayRow, canonical [][]byte, chains [][32]byte) (PartArtifact, error) {
	directory := filepath.Join(root, filepath.FromSlash(".replay-tmp"))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return PartArtifact{}, fmt.Errorf("create parquet directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".part-*.tmp")
	if err != nil {
		return PartArtifact{}, fmt.Errorf("create parquet temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	keepTemporary := true
	defer func() {
		_ = temporary.Close()
		if keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()

	writerConfig := pq.DefaultWriterConfig()
	writerConfig.CreatedBy = fmt.Sprintf("%s version %s (build %s)", WriterApplication, WriterVersion, spec.ConverterBuildID)
	writerConfig.PageBufferSize = WriterPageBuffer
	writerConfig.WriteBufferSize = WriterWriteBuffer
	writerConfig.DataPageVersion = WriterDataPage
	writerConfig.DataPageStatistics = WriterPageStats
	writerConfig.MaxRowsPerRowGroup = int64(spec.MaxRowsPerRowGroup)
	writerConfig.Compression = &uncompressed.Codec{}
	writerConfig.Schema = pq.SchemaOf(parquetRow{})
	writer := pq.NewGenericWriter[parquetRow](temporary, writerConfig)
	if _, err := writer.Write(rows); err != nil {
		return PartArtifact{}, fmt.Errorf("write parquet rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return PartArtifact{}, fmt.Errorf("close parquet writer: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return PartArtifact{}, fmt.Errorf("sync parquet temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return PartArtifact{}, fmt.Errorf("close parquet temporary file: %w", err)
	}
	partSHA, partBytes, err := hashFile(temporaryPath)
	if err != nil {
		return PartArtifact{}, err
	}
	partKey, err := protocol.ReplayPartObjectKey(scope, rows[0].StreamSequence, rows[len(rows)-1].StreamSequence, partSHA)
	if err != nil {
		return PartArtifact{}, err
	}
	finalPath := filepath.Join(root, filepath.FromSlash(partKey))
	artifact := PartArtifact{
		PartSequence: sequence, Path: temporaryPath, PartKey: partKey, PartSHA256: partSHA, PartBytes: partBytes,
		RowCount: uint64(len(rows)), CanonicalRowBytes: canonicalBytes(canonical),
		FirstStreamSequence: rows[0].StreamSequence, LastStreamSequence: rows[len(rows)-1].StreamSequence,
		PreviousRowChainHash: rows[0].PreviousRowChainHash,
		FirstRowChainHash:    chains[0], LastRowChainHash: chains[len(chains)-1],
	}
	if err := verifyPartFile(temporaryPath, artifact, scope, replayRows); err != nil {
		return PartArtifact{}, err
	}
	if err := promoteNoClobber(temporaryPath, finalPath, partSHA, partBytes); err != nil {
		return PartArtifact{}, err
	}
	keepTemporary = false
	artifact.Path = finalPath
	if err := VerifyPartFile(finalPath, artifact, scope); err != nil {
		return PartArtifact{}, err
	}
	return artifact, nil
}

func canonicalBytes(values [][]byte) uint64 {
	var result uint64
	for _, value := range values {
		result += uint64(len(value))
	}
	return result
}

func promoteNoClobber(temporaryPath, finalPath string, digest [32]byte, size uint64) error {
	if existing, err := os.Open(finalPath); err == nil {
		defer existing.Close()
		existingDigest, existingSize, hashErr := hashReader(existing)
		if hashErr != nil || existingDigest != digest || existingSize != size {
			return fmt.Errorf("existing parquet object collides with content-addressed key")
		}
		return os.Remove(temporaryPath)
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o700); err != nil {
		return fmt.Errorf("create parquet object directory: %w", err)
	}
	source, err := os.Open(temporaryPath)
	if err != nil {
		return err
	}
	target, err := os.OpenFile(finalPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		source.Close()
		if errors.Is(err, os.ErrExist) {
			return promoteNoClobber(temporaryPath, finalPath, digest, size)
		}
		return fmt.Errorf("create parquet object: %w", err)
	}
	_, copyErr := io.Copy(target, source)
	syncErr := target.Sync()
	closeTargetErr := target.Close()
	closeSourceErr := source.Close()
	if copyErr != nil || syncErr != nil || closeTargetErr != nil || closeSourceErr != nil {
		_ = os.Remove(finalPath)
		return fmt.Errorf("promote parquet object: copy=%v sync=%v close=%v/%v", copyErr, syncErr, closeTargetErr, closeSourceErr)
	}
	return os.Remove(temporaryPath)
}

func hashFile(path string) ([32]byte, uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("open parquet for hashing: %w", err)
	}
	defer file.Close()
	return hashReader(file)
}

func hashReader(reader io.Reader) ([32]byte, uint64, error) {
	hash := sha256.New()
	bytesCopied, err := io.Copy(hash, reader)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("hash parquet bytes: %w", err)
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, uint64(bytesCopied), nil
}

// VerifyPartFile reopens a sealed Parquet part and checks schema, typed values,
// raw bit patterns, canonical row bytes, row-chain anchors, size, and hash.
func VerifyPartFile(path string, artifact PartArtifact, scope protocol.ReplayScope) error {
	return verifyPartFile(path, artifact, scope, nil)
}

func verifyPartFile(path string, artifact PartArtifact, scope protocol.ReplayScope, expectedRows []protocol.ReplayRow) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("parquet verifier panic: %v", recovered)
		}
	}()
	if err := scope.Validate(); err != nil {
		return err
	}
	digest, size, err := hashFile(path)
	if err != nil {
		return err
	}
	expectedKey, err := protocol.ReplayPartObjectKey(scope, artifact.FirstStreamSequence, artifact.LastStreamSequence, digest)
	if err != nil || digest != artifact.PartSHA256 || size != artifact.PartBytes || artifact.PartKey != expectedKey {
		return fmt.Errorf("parquet artifact hash, size, or key mismatch")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	opened, err := pq.OpenFile(file, info.Size())
	if err != nil {
		return fmt.Errorf("open parquet footer: %w", err)
	}
	if !pq.EqualNodes(opened.Schema(), pq.SchemaOf(parquetRow{})) {
		return fmt.Errorf("parquet schema mismatch")
	}
	if opened.NumRows() != int64(artifact.RowCount) {
		return fmt.Errorf("parquet row count mismatch")
	}
	reader := pq.NewGenericReader[parquetRow](file)
	defer reader.Close()
	if artifact.RowCount > uint64(^uint(0)>>1) {
		return fmt.Errorf("parquet row count is not representable")
	}
	rows := make([]parquetRow, int(artifact.RowCount))
	read, readErr := reader.Read(rows)
	if readErr != nil && readErr != io.EOF {
		return fmt.Errorf("read parquet rows: %w", readErr)
	}
	if uint64(read) != artifact.RowCount {
		return fmt.Errorf("parquet row count read mismatch")
	}
	if expectedRows != nil && len(expectedRows) != len(rows) {
		return fmt.Errorf("expected replay row count does not match parquet row count")
	}
	previous := artifact.PreviousRowChainHash
	if artifact.PartSequence == 0 && previous != ([32]byte{}) {
		return fmt.Errorf("first parquet part has a nonzero predecessor row-chain hash")
	}
	var canonicalTotal uint64
	for index, physical := range rows {
		if uint64(index) > ^uint64(0)-artifact.FirstStreamSequence || physical.StreamSequence != artifact.FirstStreamSequence+uint64(index) {
			return fmt.Errorf("parquet stream sequence is not contiguous")
		}
		if physical.PreviousRowChainHash != previous {
			return fmt.Errorf("parquet previous row-chain anchor mismatch")
		}
		row, err := physical.replayRow()
		if err != nil {
			return err
		}
		if row.Scope() != scope {
			return fmt.Errorf("parquet row scope mismatch")
		}
		canonical, err := row.CanonicalBytes()
		if err != nil {
			return err
		}
		if expectedRows != nil {
			expectedCanonical, err := expectedRows[index].CanonicalBytes()
			if err != nil || !bytes.Equal(expectedCanonical, canonical) {
				return fmt.Errorf("parquet canonical row value differs from streaming input")
			}
		}
		next := protocol.RowChainStep(physical.StreamSequence, previous, canonical)
		if physical.RowChainHash != next {
			return fmt.Errorf("parquet row-chain hash mismatch")
		}
		previous = next
		canonicalTotal += uint64(len(canonical))
	}
	if rows[0].StreamSequence != artifact.FirstStreamSequence || rows[len(rows)-1].StreamSequence != artifact.LastStreamSequence || previous != artifact.LastRowChainHash || rows[0].RowChainHash != artifact.FirstRowChainHash || canonicalTotal != artifact.CanonicalRowBytes {
		return fmt.Errorf("parquet row boundary metadata mismatch")
	}
	return nil
}
