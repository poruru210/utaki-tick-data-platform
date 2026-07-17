package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"tick-data-platform/internal/delivery"
)

type tickVerifyReaderStub struct{}

func (tickVerifyReaderStub) ListDatasets(context.Context) ([]delivery.DatasetDescriptor, error) {
	return nil, errors.New("unused")
}
func (tickVerifyReaderStub) ListScopes(context.Context, string) ([]delivery.ScopeDescriptor, error) {
	return nil, errors.New("unused")
}
func (tickVerifyReaderStub) ListRawSnapshots(context.Context, delivery.RawDayScope) ([]delivery.SnapshotDescriptor, error) {
	return nil, errors.New("unused")
}
func (tickVerifyReaderStub) ResolveSnapshot(context.Context, delivery.SnapshotSelector) (delivery.ResolvedSnapshot, error) {
	return delivery.ResolvedSnapshot{}, errors.New("unused")
}
func (tickVerifyReaderStub) BuildFetchPlan(context.Context, delivery.ResolvedSnapshot) (delivery.FetchPlan, error) {
	return delivery.FetchPlan{}, errors.New("unused")
}
func (tickVerifyReaderStub) Fetch(context.Context, delivery.FetchPlan, string) (delivery.FetchResult, error) {
	return delivery.FetchResult{}, errors.New("unused")
}
func (tickVerifyReaderStub) VerifyDay(context.Context, delivery.SnapshotSelector) (delivery.DayVerificationReport, error) {
	return delivery.DayVerificationReport{VerificationScope: delivery.VerificationScopeAnchoredDay}, nil
}
func (tickVerifyReaderStub) VerifyScope(context.Context, delivery.RawScopeSelector, string) (delivery.ScopeVerificationReport, error) {
	return delivery.ScopeVerificationReport{GenesisVerified: true, VerificationScope: delivery.VerificationScopeFullChain}, nil
}
func (tickVerifyReaderStub) ListReplaySnapshots(context.Context, delivery.ReplayDayScope) ([]delivery.ReplaySnapshotDescriptor, error) {
	return nil, errors.New("unused")
}
func (tickVerifyReaderStub) ResolveReplaySnapshot(context.Context, delivery.ReplaySnapshotSelector) (delivery.ResolvedReplaySnapshot, error) {
	return delivery.ResolvedReplaySnapshot{}, errors.New("unused")
}
func (tickVerifyReaderStub) BuildReplayFetchPlan(context.Context, delivery.ResolvedReplaySnapshot) (delivery.ReplayFetchPlan, error) {
	return delivery.ReplayFetchPlan{}, errors.New("unused")
}
func (tickVerifyReaderStub) FetchReplay(context.Context, delivery.ReplayFetchPlan, string) (delivery.ReplayFetchResult, error) {
	return delivery.ReplayFetchResult{}, errors.New("unused")
}
func (tickVerifyReaderStub) VerifyReplayDay(context.Context, delivery.ReplaySnapshotSelector) (delivery.ReplayDayVerificationReport, error) {
	return delivery.ReplayDayVerificationReport{VerificationScope: delivery.VerificationScopeReplayAnchoredDay, RawBindingVerified: true}, nil
}

func TestTickVerifyDayAndScopeUseExplicitVerificationScopes(t *testing.T) {
	reader := tickVerifyReaderStub{}
	var dayOutput, dayErrors bytes.Buffer
	if code := runWithReader([]string{"day", "--manifest", "manifest"}, reader, &dayOutput, &dayErrors); code != 0 {
		t.Fatalf("day exit=%d errors=%q", code, dayErrors.String())
	}
	var day map[string]any
	if err := json.Unmarshal(dayOutput.Bytes(), &day); err != nil {
		t.Fatal(err)
	}
	if day["genesis_verified"] != false || day["verification_scope"] != delivery.VerificationScopeAnchoredDay {
		t.Fatalf("day output = %v", day)
	}
	var scopeOutput, scopeErrors bytes.Buffer
	if code := runWithReader([]string{"scope", "--config", "local/tick-reader.toml.example", "--dataset", "d", "--source", "s", "--symbol", "EURUSD", "--through-root", "root"}, reader, &scopeOutput, &scopeErrors); code != 0 {
		t.Fatalf("scope exit=%d errors=%q", code, scopeErrors.String())
	}
	var scope map[string]any
	if err := json.Unmarshal(scopeOutput.Bytes(), &scope); err != nil {
		t.Fatal(err)
	}
	if scope["genesis_verified"] != true || scope["verification_scope"] != delivery.VerificationScopeFullChain {
		t.Fatalf("scope output = %v", scope)
	}
}

func TestTickVerifyReplayDayEmitsMachineReadableScope(t *testing.T) {
	var output, errorsOut bytes.Buffer
	if code := runWithReader([]string{"replay-day", "--manifest", "manifest"}, tickVerifyReaderStub{}, &output, &errorsOut); code != 0 {
		t.Fatalf("replay-day exit=%d errors=%q", code, errorsOut.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["verification_scope"] != delivery.VerificationScopeReplayAnchoredDay || decoded["genesis_verified"] != false || decoded["raw_binding_verified"] != true {
		t.Fatalf("replay-day JSON = %v", decoded)
	}
	output.Reset()
	errorsOut.Reset()
	if code := runWithReader([]string{"replay-day"}, tickVerifyReaderStub{}, &output, &errorsOut); code != 2 || output.Len() != 0 {
		t.Fatalf("invalid replay-day exit=%d output=%q errors=%q", code, output.String(), errorsOut.String())
	}
}

func TestTickVerifyInvalidArgumentsAreNonzeroAndSilentOnStdout(t *testing.T) {
	var output, errorsOut bytes.Buffer
	if code := runWithReader([]string{"day"}, tickVerifyReaderStub{}, &output, &errorsOut); code == 0 || output.Len() != 0 || errorsOut.Len() == 0 {
		t.Fatalf("invalid day output=%q errors=%q", output.String(), errorsOut.String())
	}
}

var _ delivery.ArchiveReaderV1 = tickVerifyReaderStub{}
