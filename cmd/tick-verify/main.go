package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"tick-data-platform/internal/delivery"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, output, errorsOut io.Writer) int {
	configPath, ok := configPathFor(args, errorsOut)
	if !ok {
		return 2
	}
	config, err := delivery.LoadReaderConfig(configPath)
	if err != nil {
		fmt.Fprintln(errorsOut, "reader configuration is invalid")
		return 1
	}
	reader, err := delivery.NewArchiveReaderV1(context.Background(), config)
	if err != nil {
		fmt.Fprintln(errorsOut, "archive reader is unavailable")
		return 1
	}
	return runWithReader(args, reader, output, errorsOut)
}

func configPathFor(args []string, errorsOut io.Writer) (string, bool) {
	for index, arg := range args {
		if arg == "--config" && index+1 < len(args) {
			return args[index+1], true
		}
	}
	fmt.Fprintln(errorsOut, "--config is required")
	return "", false
}

func runWithReader(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errorsOut, "a tick-verify command is required")
		return 2
	}
	switch args[0] {
	case "day":
		return runDay(args[1:], reader, output, errorsOut)
	case "scope":
		return runScope(args[1:], reader, output, errorsOut)
	case "replay-day":
		return runReplayDay(args[1:], reader, output, errorsOut)
	default:
		fmt.Fprintln(errorsOut, "unknown tick-verify command")
		return 2
	}
}

func runReplayDay(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("replay-day", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	manifest := flags.String("manifest", "", "immutable replay manifest key or digest")
	if err := flags.Parse(args); err != nil || *manifest == "" {
		fmt.Fprintln(errorsOut, "--manifest is required")
		return 2
	}
	report, err := reader.VerifyReplayDay(context.Background(), delivery.ReplaySnapshotSelector{Manifest: *manifest})
	if err != nil {
		return reportError(err, errorsOut)
	}
	return writeJSON(replayDayOutput{
		GenesisVerified: report.GenesisVerified, VerificationScope: report.VerificationScope,
		DatasetID: report.DatasetID, Source: report.ProviderID, Symbol: report.ExactSourceSymbol, DayDefinitionID: report.DayDefinitionID,
		Date: report.Date, ReplayContractID: report.ReplayContractID, ConversionID: report.ConversionID,
		Revision: report.Revision, ManifestKey: report.ManifestKey, ManifestSHA256: hex.EncodeToString(report.ManifestSHA256[:]),
		RawBindingVerified: report.RawBindingVerified, RawDaySemanticsVerified: report.RawDaySemanticsVerified,
		PartManifestChainVerified: report.PartManifestChainVerified, PartSetRootVerified: report.PartSetRootVerified,
		ParquetSchemaVerified: report.ParquetSchemaVerified, ParquetRowsVerified: report.ParquetRowsVerified,
		ParquetFileHashesVerified: report.ParquetFileHashesVerified, CanonicalRowChainRootVerified: report.CanonicalRowChainRootVerified,
		EmptyDay: report.EmptyDay, PartCount: report.PartCount, RowCount: report.RowCount,
	}, output, errorsOut)
}

func runDay(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("day", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	manifest := flags.String("manifest", "", "immutable manifest key or digest")
	if err := flags.Parse(args); err != nil || *manifest == "" {
		fmt.Fprintln(errorsOut, "--manifest is required")
		return 2
	}
	report, err := reader.VerifyDay(context.Background(), delivery.SnapshotSelector{Manifest: *manifest})
	if err != nil {
		return reportError(err, errorsOut)
	}
	return writeJSON(dayOutput{
		GenesisVerified: report.GenesisVerified, VerificationScope: report.VerificationScope,
		DatasetID: report.DatasetID, Date: report.Date, Revision: report.Revision,
		ManifestKey: report.ManifestKey, ManifestSHA256: hex.EncodeToString(report.ManifestSHA256[:]),
		PredecessorAnchor: hex.EncodeToString(report.PredecessorAnchor[:]),
		ChainSliceStart:   report.ChainSliceStart, ChainSliceStartRoot: hex.EncodeToString(report.ChainSliceStartRoot[:]),
		ChainSliceEnd: report.ChainSliceEnd, ChainSliceEndRoot: hex.EncodeToString(report.ChainSliceEndRoot[:]),
		AcceptedRecordCount: report.AcceptedRecordCount, ErrorCount: report.ErrorCount,
		EntryCount: len(report.Entries),
	}, output, errorsOut)
}

func runScope(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("scope", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	dataset := flags.String("dataset", "", "dataset identity")
	source := flags.String("source", "", "source identity")
	symbol := flags.String("symbol", "", "exact source symbol")
	throughRoot := flags.String("through-root", "", "scope root")
	if err := flags.Parse(args); err != nil || *dataset == "" || *source == "" || *symbol == "" || *throughRoot == "" {
		fmt.Fprintln(errorsOut, "--dataset, --source, --symbol, and --through-root are required")
		return 2
	}
	report, err := reader.VerifyScope(context.Background(), delivery.RawScopeSelector{DatasetID: *dataset, ProviderID: *source, ExactSourceSymbol: *symbol}, *throughRoot)
	if err != nil {
		return reportError(err, errorsOut)
	}
	return writeJSON(scopeOutput{
		GenesisVerified: report.GenesisVerified, VerificationScope: report.VerificationScope,
		DatasetID: report.DatasetID, Source: report.ProviderID, Symbol: report.ExactSourceSymbol,
		ThroughRoot: hex.EncodeToString(report.ThroughRoot[:]), VerifiedThrough: report.VerifiedThrough,
		SegmentCount: report.SegmentCount, EntryCount: report.EntryCount,
	}, output, errorsOut)
}

type dayOutput struct {
	GenesisVerified     bool   `json:"genesis_verified"`
	VerificationScope   string `json:"verification_scope"`
	DatasetID           string `json:"dataset_id"`
	Date                string `json:"date"`
	Revision            uint64 `json:"revision"`
	ManifestKey         string `json:"manifest_key"`
	ManifestSHA256      string `json:"manifest_sha256"`
	PredecessorAnchor   string `json:"predecessor_anchor"`
	ChainSliceStart     uint64 `json:"chain_slice_start"`
	ChainSliceStartRoot string `json:"chain_slice_start_root"`
	ChainSliceEnd       uint64 `json:"chain_slice_end"`
	ChainSliceEndRoot   string `json:"chain_slice_end_root"`
	AcceptedRecordCount uint64 `json:"accepted_record_count"`
	ErrorCount          uint64 `json:"error_count"`
	EntryCount          int    `json:"entry_count"`
}

type scopeOutput struct {
	GenesisVerified   bool   `json:"genesis_verified"`
	VerificationScope string `json:"verification_scope"`
	DatasetID         string `json:"dataset_id"`
	Source            string `json:"source"`
	Symbol            string `json:"symbol"`
	ThroughRoot       string `json:"through_root"`
	VerifiedThrough   uint64 `json:"verified_through"`
	SegmentCount      int    `json:"segment_count"`
	EntryCount        int    `json:"entry_count"`
}

type replayDayOutput struct {
	GenesisVerified               bool   `json:"genesis_verified"`
	VerificationScope             string `json:"verification_scope"`
	DatasetID                     string `json:"dataset_id"`
	Source                        string `json:"source"`
	Symbol                        string `json:"symbol"`
	DayDefinitionID               string `json:"day_definition_id"`
	Date                          string `json:"date"`
	ReplayContractID              string `json:"replay_contract_id"`
	ConversionID                  string `json:"conversion_id"`
	Revision                      uint64 `json:"revision"`
	ManifestKey                   string `json:"manifest_key"`
	ManifestSHA256                string `json:"manifest_sha256"`
	RawBindingVerified            bool   `json:"raw_binding_verified"`
	RawDaySemanticsVerified       bool   `json:"raw_day_semantics_verified"`
	PartManifestChainVerified     bool   `json:"part_manifest_chain_verified"`
	PartSetRootVerified           bool   `json:"part_set_root_verified"`
	ParquetSchemaVerified         bool   `json:"parquet_schema_verified"`
	ParquetRowsVerified           bool   `json:"parquet_rows_verified"`
	ParquetFileHashesVerified     bool   `json:"parquet_file_hashes_verified"`
	CanonicalRowChainRootVerified bool   `json:"canonical_row_chain_root_verified"`
	EmptyDay                      bool   `json:"empty_day"`
	PartCount                     uint64 `json:"part_count"`
	RowCount                      uint64 `json:"row_count"`
}

func writeJSON(value any, output, errorsOut io.Writer) int {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		fmt.Fprintln(errorsOut, "write JSON output failed")
		return 1
	}
	return 0
}

func reportError(err error, errorsOut io.Writer) int {
	fmt.Fprintln(errorsOut, err.Error())
	return 1
}
