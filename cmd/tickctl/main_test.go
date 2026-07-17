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
func (tickctlReaderStub) ListScopes(context.Context, string) ([]delivery.ScopeDescriptor, error) {
	return []delivery.ScopeDescriptor{{DatasetID: "dataset-1", ProviderID: "source-1", ExactSourceSymbol: "EURUSD"}}, nil
}
func (tickctlReaderStub) ListRawSnapshots(context.Context, delivery.RawDayScope) ([]delivery.SnapshotDescriptor, error) {
	return []delivery.SnapshotDescriptor{{DatasetID: "dataset-1", ProviderID: "source-1", ExactSourceSymbol: "EURUSD", Date: "2024-03-09", ManifestKey: "v1/manifest.json"}}, nil
}
func (tickctlReaderStub) ResolveSnapshot(context.Context, delivery.SnapshotSelector) (delivery.ResolvedSnapshot, error) {
	return delivery.ResolvedSnapshot{}, nil
}
func (tickctlReaderStub) BuildFetchPlan(context.Context, delivery.ResolvedSnapshot) (delivery.FetchPlan, error) {
	return delivery.FetchPlan{}, nil
}
func (tickctlReaderStub) Fetch(context.Context, delivery.FetchPlan, string) (delivery.FetchResult, error) {
	return delivery.FetchResult{ManifestPath: "out/manifest.json", ObjectPaths: map[string]string{"objects/raw/wal-x.rtw": "out/wal-x.rtw"}}, nil
}
func (tickctlReaderStub) VerifyDay(context.Context, delivery.SnapshotSelector) (delivery.DayVerificationReport, error) {
	return delivery.DayVerificationReport{}, errors.New("unused")
}
func (tickctlReaderStub) VerifyScope(context.Context, delivery.RawScopeSelector, string) (delivery.ScopeVerificationReport, error) {
	return delivery.ScopeVerificationReport{}, errors.New("unused")
}
func (tickctlReaderStub) ListReplaySnapshots(context.Context, delivery.ReplayDayScope) ([]delivery.ReplaySnapshotDescriptor, error) {
	return []delivery.ReplaySnapshotDescriptor{{DatasetID: "dataset-1", ProviderID: "source-1", ExactSourceSymbol: "EURUSD", Date: "2024-03-09", ReplayContractID: "stream-1", ConversionID: "conversion-1", Revision: 1, ManifestKey: "v1/replay-day-1.json"}}, nil
}
func (tickctlReaderStub) ResolveReplaySnapshot(context.Context, delivery.ReplaySnapshotSelector) (delivery.ResolvedReplaySnapshot, error) {
	return delivery.ResolvedReplaySnapshot{Descriptor: delivery.ReplaySnapshotDescriptor{ManifestKey: "v1/replay-day-1.json"}}, nil
}
func (tickctlReaderStub) BuildReplayFetchPlan(context.Context, delivery.ResolvedReplaySnapshot) (delivery.ReplayFetchPlan, error) {
	return delivery.ReplayFetchPlan{}, nil
}
func (tickctlReaderStub) FetchReplay(context.Context, delivery.ReplayFetchPlan, string) (delivery.ReplayFetchResult, error) {
	return delivery.ReplayFetchResult{ManifestPath: "out/replay.json", PartManifestPaths: map[string]string{}, ParquetPaths: map[string]string{}}, nil
}
func (tickctlReaderStub) VerifyReplayDay(context.Context, delivery.ReplaySnapshotSelector) (delivery.ReplayDayVerificationReport, error) {
	return delivery.ReplayDayVerificationReport{}, errors.New("unused")
}

func TestTickctlCommandsEmitStableJSON(t *testing.T) {
	reader := tickctlReaderStub{}
	commands := [][]string{
		{"datasets"},
		{"scopes", "--dataset", "dataset-1"},
		{"snapshots", "raw", "--dataset", "dataset-1", "--source", "source-1", "--symbol", "EURUSD", "--date", "2024-03-09"},
		{"snapshots", "replay", "--dataset", "dataset-1", "--source", "source-1", "--symbol", "EURUSD", "--date", "2024-03-09", "--stream", "stream-1", "--conversion", "conversion-1"},
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

func TestTickctlReplayFlagsAndFetchJSON(t *testing.T) {
	reader := tickctlReaderStub{}
	var output, errorsOut bytes.Buffer
	if code := runWithReader([]string{"snapshots", "replay", "--dataset", "d", "--source", "s1", "--symbol", "EURUSD", "--date", "2024-03-09", "--stream", "s", "--conversion", "x", "--revision", "1", "--manifest", "m"}, reader, &output, &errorsOut); code != 2 || output.Len() != 0 {
		t.Fatalf("invalid replay flags exit=%d output=%q errors=%q", code, output.String(), errorsOut.String())
	}
	output.Reset()
	errorsOut.Reset()
	if code := runWithReader([]string{"fetch", "--kind", "replay", "--manifest", "m", "--output", "out"}, reader, &output, &errorsOut); code != 0 {
		t.Fatalf("replay fetch exit=%d errors=%q", code, errorsOut.String())
	}
	if !strings.Contains(output.String(), `"manifest_path":"out/replay.json"`) {
		t.Fatalf("replay fetch JSON=%q", output.String())
	}
}

func TestTickctlRawFetchCompatibility(t *testing.T) {
	var output, errorsOut bytes.Buffer
	if code := runWithReader([]string{"fetch", "--kind", "raw", "--manifest", "m", "--output", "out"}, tickctlReaderStub{}, &output, &errorsOut); code != 0 {
		t.Fatalf("raw fetch exit=%d errors=%q", code, errorsOut.String())
	}
	if !strings.Contains(output.String(), `"entry_count":0`) || !strings.Contains(output.String(), `"manifest_path":"out/manifest.json"`) {
		t.Fatalf("raw fetch JSON=%q", output.String())
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
