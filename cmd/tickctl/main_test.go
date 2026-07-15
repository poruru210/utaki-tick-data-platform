package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"tick-data-platform/internal/delivery"
)

type tickctlReaderStub struct{}

func (tickctlReaderStub) ListDatasets(context.Context) ([]delivery.DatasetDescriptor, error) {
	return []delivery.DatasetDescriptor{{DatasetID: "dataset-1"}}, nil
}
func (tickctlReaderStub) ListCampaigns(context.Context, string) ([]delivery.CampaignDescriptor, error) {
	return []delivery.CampaignDescriptor{{DatasetID: "dataset-1", CampaignID: "campaign-1"}}, nil
}
func (tickctlReaderStub) ListRawSnapshots(context.Context, delivery.RawDayScope) ([]delivery.SnapshotDescriptor, error) {
	return []delivery.SnapshotDescriptor{{DatasetID: "dataset-1", CampaignID: "campaign-1", Date: "2024-03-09", ManifestKey: "v1/manifest.json"}}, nil
}
func (tickctlReaderStub) ResolveSnapshot(context.Context, delivery.SnapshotSelector) (delivery.ResolvedSnapshot, error) {
	return delivery.ResolvedSnapshot{}, errors.New("unused")
}
func (tickctlReaderStub) BuildFetchPlan(context.Context, delivery.ResolvedSnapshot) (delivery.FetchPlan, error) {
	return delivery.FetchPlan{}, errors.New("unused")
}
func (tickctlReaderStub) Fetch(context.Context, delivery.FetchPlan, string) (delivery.FetchResult, error) {
	return delivery.FetchResult{ManifestPath: "out/manifest.json", ObjectPaths: map[string]string{"objects/raw/wal-x.rtw": "out/wal-x.rtw"}}, nil
}
func (tickctlReaderStub) VerifyDay(context.Context, delivery.SnapshotSelector) (delivery.DayVerificationReport, error) {
	return delivery.DayVerificationReport{}, errors.New("unused")
}
func (tickctlReaderStub) VerifyCampaign(context.Context, string, string, string) (delivery.CampaignVerificationReport, error) {
	return delivery.CampaignVerificationReport{}, errors.New("unused")
}

func TestTickctlCommandsEmitStableJSON(t *testing.T) {
	reader := tickctlReaderStub{}
	commands := [][]string{
		{"datasets"},
		{"campaigns", "--dataset", "dataset-1"},
		{"snapshots", "raw", "--dataset", "dataset-1", "--campaign", "campaign-1", "--date", "2024-03-09"},
	}
	for _, command := range commands {
		var output, errorsOut bytes.Buffer
		if code := runWithReader(command, reader, &output, &errorsOut); code != 0 {
			t.Fatalf("command %v exit=%d errors=%q", command, code, errorsOut.String())
		}
		if !strings.HasSuffix(output.String(), "\n") || strings.Contains(output.String(), "TICK_READER") {
			t.Fatalf("command %v output=%q", command, output.String())
		}
	}
}

func TestTickctlFetchRequiresManifestAndOutput(t *testing.T) {
	var output, errorsOut bytes.Buffer
	if code := runWithReader([]string{"fetch", "--manifest", "m"}, tickctlReaderStub{}, &output, &errorsOut); code != 2 {
		t.Fatalf("missing output exit=%d, want 2", code)
	}
	if errorsOut.Len() == 0 || output.Len() != 0 {
		t.Fatalf("invalid fetch output=%q errors=%q", output.String(), errorsOut.String())
	}
}

var _ delivery.ArchiveReaderV1 = tickctlReaderStub{}
