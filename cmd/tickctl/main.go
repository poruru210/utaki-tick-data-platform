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
		fmt.Fprintln(errorsOut, "a tickctl command is required")
		return 2
	}
	switch args[0] {
	case "datasets":
		return runDatasets(args[1:], reader, output, errorsOut)
	case "campaigns":
		return runCampaigns(args[1:], reader, output, errorsOut)
	case "snapshots":
		if len(args) < 2 || args[1] != "raw" {
			fmt.Fprintln(errorsOut, "snapshots requires the raw subcommand")
			return 2
		}
		return runSnapshotsRaw(args[2:], reader, output, errorsOut)
	case "fetch":
		return runFetch(args[1:], reader, output, errorsOut)
	default:
		fmt.Fprintln(errorsOut, "unknown tickctl command")
		return 2
	}
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

func runCampaigns(args []string, reader delivery.ArchiveReaderV1, output, errorsOut io.Writer) int {
	flags := flag.NewFlagSet("campaigns", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("config", "", "reader configuration")
	dataset := flags.String("dataset", "", "dataset identity")
	if err := flags.Parse(args); err != nil || *dataset == "" {
		fmt.Fprintln(errorsOut, "--dataset is required")
		return 2
	}
	items, err := reader.ListCampaigns(context.Background(), *dataset)
	if err != nil {
		return reportError(err, errorsOut)
	}
	result := make([]campaignOutput, 0, len(items))
	for _, item := range items {
		result = append(result, campaignOutput{
			DatasetID: item.DatasetID, CampaignID: item.CampaignID,
			ProviderID: item.ProviderID, StableFeedID: item.StableFeedID,
			ExactSourceSymbol: item.ExactSourceSymbol, BrokerServerFingerprint: item.BrokerServerFingerprint,
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
	campaign := flags.String("campaign", "", "campaign identity")
	date := flags.String("date", "", "UTC date")
	if err := flags.Parse(args); err != nil || *dataset == "" || *campaign == "" || *date == "" {
		fmt.Fprintln(errorsOut, "--dataset, --campaign, and --date are required")
		return 2
	}
	items, err := reader.ListRawSnapshots(context.Background(), delivery.RawDayScope{DatasetID: *dataset, CampaignID: *campaign, Date: *date})
	if err != nil {
		return reportError(err, errorsOut)
	}
	result := make([]snapshotOutput, 0, len(items))
	for _, item := range items {
		result = append(result, snapshotOutput{
			DatasetID: item.DatasetID, CampaignID: item.CampaignID, DayDefinitionID: item.DayDefinitionID,
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
	if err := flags.Parse(args); err != nil || *manifest == "" || *destination == "" {
		fmt.Fprintln(errorsOut, "--manifest and --output are required")
		return 2
	}
	snapshot, err := reader.ResolveSnapshot(context.Background(), delivery.SnapshotSelector{Manifest: *manifest})
	if err != nil {
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

type datasetOutput struct {
	DatasetID string `json:"dataset_id"`
}

type campaignOutput struct {
	DatasetID               string `json:"dataset_id"`
	CampaignID              string `json:"campaign_id"`
	ProviderID              string `json:"provider_id"`
	StableFeedID            string `json:"stable_feed_id"`
	ExactSourceSymbol       string `json:"exact_source_symbol"`
	BrokerServerFingerprint string `json:"broker_server_fingerprint"`
	DayDefinitionID         string `json:"day_definition_id"`
	PublisherID             string `json:"publisher_id"`
	PublisherEpoch          uint64 `json:"publisher_epoch"`
	ConfigHash              string `json:"config_hash"`
}

type snapshotOutput struct {
	DatasetID           string `json:"dataset_id"`
	CampaignID          string `json:"campaign_id"`
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
