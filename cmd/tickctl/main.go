package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

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
		fmt.Fprintln(errorsOut, "a tickctl command is required")
		return 2
	}
	switch args[0] {
	case "datasets":
		return runDatasets(args[1:], reader, output, errorsOut)
	case "scopes":
		return runScopes(args[1:], reader, output, errorsOut)
	case "snapshots":
		if len(args) < 2 {
			fmt.Fprintln(errorsOut, "snapshots requires raw or replay")
			return 2
		}
		switch args[1] {
		case "raw":
			return runSnapshotsRaw(args[2:], reader, output, errorsOut)
		case "replay":
			return runSnapshotsReplay(args[2:], reader, output, errorsOut)
		default:
			fmt.Fprintln(errorsOut, "snapshots requires raw or replay")
			return 2
		}
	case "fetch":
		return runFetch(args[1:], reader, output, errorsOut)
	default:
		fmt.Fprintln(errorsOut, "unknown tickctl command")
		return 2
	}
}

func runSnapshotsReplay(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("snapshots replay", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	dataset := flags.String("dataset", "", "dataset identity")
	source := flags.String("source", "", "source identity")
	symbol := flags.String("symbol", "", "exact source symbol")
	date := flags.String("date", "", "UTC date")
	stream := flags.String("stream", "", "replay contract identity")
	conversion := flags.String("conversion", "", "conversion identity")
	dayDefinition := flags.String("day-definition", "", "day definition identity")
	revision := flags.Uint64("revision", 0, "exact replay revision")
	manifest := flags.String("manifest", "", "immutable replay manifest key or digest")
	if err := flags.Parse(args); err != nil || *dataset == "" || *source == "" || *symbol == "" || *date == "" || *stream == "" || *conversion == "" || *revision != 0 && *manifest != "" {
		fmt.Fprintln(errorsOut, "--dataset, --source, --symbol, --date, --stream, and --conversion are required; --revision and --manifest are mutually exclusive")
		return 2
	}
	scope := delivery.ReplayDayScope{DatasetID: *dataset, ProviderID: *source, ExactSourceSymbol: *symbol, DayDefinitionID: *dayDefinition, Date: *date, ReplayContractID: *stream, ConversionID: *conversion}
	var items []delivery.ReplaySnapshotDescriptor
	if *revision != 0 || *manifest != "" {
		selector := delivery.ReplaySnapshotSelector{ReplayDayScope: scope, Manifest: *manifest}
		if *revision != 0 {
			selector.Revision = revision
		}
		resolved, err := reader.ResolveReplaySnapshot(context.Background(), selector)
		if err != nil {
			return reportError(err, errorsOut)
		}
		items = []delivery.ReplaySnapshotDescriptor{resolved.Descriptor}
	} else {
		var err error
		items, err = reader.ListReplaySnapshots(context.Background(), scope)
		if err != nil {
			return reportError(err, errorsOut)
		}
	}
	result := make([]replaySnapshotOutput, len(items))
	for index, item := range items {
		previous := ""
		if item.PreviousManifestSHA256 != nil {
			previous = hex.EncodeToString(item.PreviousManifestSHA256[:])
		}
		result[index] = replaySnapshotOutput{DatasetID: item.DatasetID, Source: item.ProviderID, Symbol: item.ExactSourceSymbol, DayDefinitionID: item.DayDefinitionID, Date: item.Date, ReplayContractID: item.ReplayContractID, ConversionID: item.ConversionID, Revision: item.Revision, ManifestKey: item.ManifestKey, ManifestSHA256: hex.EncodeToString(item.ManifestSHA256[:]), PreviousManifestSHA256: previous, RawDayManifestKey: item.RawDayManifestKey, RawDayManifestSHA256: hex.EncodeToString(item.RawDayManifestSHA256[:]), PartSetRoot: hex.EncodeToString(item.PartSetRoot[:]), CanonicalStreamRowChainRoot: hex.EncodeToString(item.CanonicalStreamRowChainRoot[:]), PartCount: item.PartCount}
	}
	return writeJSON(result, output, errorsOut)
}

func runDatasets(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("datasets", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	if err := flags.Parse(args); err != nil {
		fmt.Fprintln(errorsOut, "invalid datasets arguments")
		return 2
	}
	items, err := reader.ListDatasets(context.Background())
	if err != nil {
		return reportError(err, errorsOut)
	}
	result := make([]datasetOutput, 0, len(items))
	for _, item := range items {
		result = append(result, datasetOutput{DatasetID: item.DatasetID})
	}
	return writeJSON(result, output, errorsOut)
}

func runScopes(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("scopes", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	dataset := flags.String("dataset", "", "dataset identity")
	if err := flags.Parse(args); err != nil || *dataset == "" {
		fmt.Fprintln(errorsOut, "--dataset is required")
		return 2
	}
	items, err := reader.ListScopes(context.Background(), *dataset)
	if err != nil {
		return reportError(err, errorsOut)
	}
	result := make([]scopeOutput, 0, len(items))
	for _, item := range items {
		result = append(result, scopeOutput{
			DatasetID: item.DatasetID, Source: item.ProviderID,
			StableFeedID: item.StableFeedID,
			Symbol:       item.ExactSourceSymbol, BrokerServerFingerprint: item.BrokerServerFingerprint,
			DayDefinitionID: item.DayDefinitionID, PublisherID: item.PublisherID,
			PublisherEpoch: item.PublisherEpoch, ConfigHash: hex.EncodeToString(item.ConfigHash[:]),
		})
	}
	return writeJSON(result, output, errorsOut)
}

func runSnapshotsRaw(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("snapshots raw", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	dataset := flags.String("dataset", "", "dataset identity")
	source := flags.String("source", "", "source identity")
	symbol := flags.String("symbol", "", "exact source symbol")
	date := flags.String("date", "", "UTC date")
	if err := flags.Parse(args); err != nil || *dataset == "" || *source == "" || *symbol == "" || *date == "" {
		fmt.Fprintln(errorsOut, "--dataset, --source, --symbol, and --date are required")
		return 2
	}
	items, err := reader.ListRawSnapshots(context.Background(), delivery.RawDayScope{DatasetID: *dataset, ProviderID: *source, ExactSourceSymbol: *symbol, Date: *date})
	if err != nil {
		return reportError(err, errorsOut)
	}
	result := make([]snapshotOutput, 0, len(items))
	for _, item := range items {
		result = append(result, snapshotOutput{
			DatasetID: item.DatasetID, Source: item.ProviderID, Symbol: item.ExactSourceSymbol, DayDefinitionID: item.DayDefinitionID,
			Date: item.Date, Revision: item.Revision, PublisherID: item.PublisherID,
			PublisherEpoch: item.PublisherEpoch, ManifestKey: item.ManifestKey,
			ManifestSHA256:  hex.EncodeToString(item.ManifestSHA256[:]),
			ChainSliceStart: item.ChainSliceStart, ChainSliceStartRoot: hex.EncodeToString(item.ChainSliceStartRoot[:]),
			ChainSliceEnd: item.ChainSliceEnd, ChainSliceEndRoot: hex.EncodeToString(item.ChainSliceEndRoot[:]),
			AcceptedRecordCount: item.AcceptedRecordCount, ErrorCount: item.ErrorCount,
		})
	}
	return writeJSON(result, output, errorsOut)
}

func runFetch(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("fetch", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	manifest := flags.String("manifest", "", "immutable manifest key or digest")
	destination := flags.String("output", "", "output directory")
	kind := flags.String("kind", "auto", "auto, raw, or replay")
	if err := flags.Parse(args); err != nil || *manifest == "" || *destination == "" || *kind != "auto" && *kind != "raw" && *kind != "replay" {
		fmt.Fprintln(errorsOut, "--manifest and --output are required")
		return 2
	}
	if *kind == "replay" || *kind == "auto" && strings.Contains(*manifest, "/replay-day-") {
		return runReplayFetch(*manifest, *destination, reader, output, errorsOut)
	}
	snapshot, err := reader.ResolveSnapshot(context.Background(), delivery.SnapshotSelector{Manifest: *manifest})
	if err != nil {
		if *kind == "auto" {
			return runReplayFetch(*manifest, *destination, reader, output, errorsOut)
		}
		return reportError(err, errorsOut)
	}
	plan, err := reader.BuildFetchPlan(context.Background(), snapshot)
	if err != nil {
		return reportError(err, errorsOut)
	}
	result, err := reader.Fetch(context.Background(), plan, *destination)
	if err != nil {
		return reportError(err, errorsOut)
	}
	return writeJSON(fetchOutput{ManifestPath: result.ManifestPath, ObjectPaths: result.ObjectPaths, EntryCount: len(result.Entries)}, output, errorsOut)
}

func runReplayFetch(manifest, destination string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	snapshot, err := reader.ResolveReplaySnapshot(context.Background(), delivery.ReplaySnapshotSelector{Manifest: manifest})
	if err != nil {
		return reportError(err, errorsOut)
	}
	plan, err := reader.BuildReplayFetchPlan(context.Background(), snapshot)
	if err != nil {
		return reportError(err, errorsOut)
	}
	result, err := reader.FetchReplay(context.Background(), plan, destination)
	if err != nil {
		return reportError(err, errorsOut)
	}
	paths := make(map[string]string, 1+len(result.PartManifestPaths)+len(result.ParquetPaths))
	for key, path := range result.PartManifestPaths {
		paths[key] = path
	}
	for key, path := range result.ParquetPaths {
		paths[key] = path
	}
	return writeJSON(fetchOutput{ManifestPath: result.ManifestPath, ObjectPaths: paths, EntryCount: 0}, output, errorsOut)
}

type datasetOutput struct {
	DatasetID string `json:"dataset_id"`
}

type scopeOutput struct {
	DatasetID               string `json:"dataset_id"`
	Source                  string `json:"source"`
	StableFeedID            string `json:"stable_feed_id"`
	Symbol                  string `json:"symbol"`
	BrokerServerFingerprint string `json:"broker_server_fingerprint"`
	DayDefinitionID         string `json:"day_definition_id"`
	PublisherID             string `json:"publisher_id"`
	PublisherEpoch          uint64 `json:"publisher_epoch"`
	ConfigHash              string `json:"config_hash"`
}

type snapshotOutput struct {
	DatasetID           string `json:"dataset_id"`
	Source              string `json:"source"`
	Symbol              string `json:"symbol"`
	DayDefinitionID     string `json:"day_definition_id"`
	Date                string `json:"date"`
	Revision            uint64 `json:"revision"`
	PublisherID         string `json:"publisher_id"`
	PublisherEpoch      uint64 `json:"publisher_epoch"`
	ManifestKey         string `json:"manifest_key"`
	ManifestSHA256      string `json:"manifest_sha256"`
	ChainSliceStart     uint64 `json:"chain_slice_start"`
	ChainSliceStartRoot string `json:"chain_slice_start_root"`
	ChainSliceEnd       uint64 `json:"chain_slice_end"`
	ChainSliceEndRoot   string `json:"chain_slice_end_root"`
	AcceptedRecordCount uint64 `json:"accepted_record_count"`
	ErrorCount          uint64 `json:"error_count"`
}

type fetchOutput struct {
	ManifestPath string            `json:"manifest_path"`
	ObjectPaths  map[string]string `json:"object_paths"`
	EntryCount   int               `json:"entry_count"`
}

type replaySnapshotOutput struct {
	DatasetID                   string `json:"dataset_id"`
	Source                      string `json:"source"`
	Symbol                      string `json:"symbol"`
	DayDefinitionID             string `json:"day_definition_id"`
	Date                        string `json:"date"`
	ReplayContractID            string `json:"replay_contract_id"`
	ConversionID                string `json:"conversion_id"`
	Revision                    uint64 `json:"revision"`
	ManifestKey                 string `json:"manifest_key"`
	ManifestSHA256              string `json:"manifest_sha256"`
	PreviousManifestSHA256      string `json:"previous_manifest_sha256,omitempty"`
	RawDayManifestKey           string `json:"raw_day_manifest_key"`
	RawDayManifestSHA256        string `json:"raw_day_manifest_sha256"`
	PartSetRoot                 string `json:"part_set_root"`
	CanonicalStreamRowChainRoot string `json:"canonical_stream_row_chain_root"`
	PartCount                   uint64 `json:"part_count"`
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
