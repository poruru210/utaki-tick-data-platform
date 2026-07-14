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
func (tickVerifyReaderStub) ListCampaigns(context.Context, string) ([]delivery.CampaignDescriptor, error) {
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
func (tickVerifyReaderStub) VerifyCampaign(context.Context, string, string, string) (delivery.CampaignVerificationReport, error) {
	return delivery.CampaignVerificationReport{GenesisVerified: true, VerificationScope: delivery.VerificationScopeCampaign}, nil
}

func TestTickVerifyDayAndCampaignUseExplicitVerificationScopes(t *testing.T) {
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
	var campaignOutput, campaignErrors bytes.Buffer
	if code := runWithReader([]string{"campaign", "--config", "local/tick-reader.toml.example", "--dataset", "d", "--campaign", "c", "--through-root", "root"}, reader, &campaignOutput, &campaignErrors); code != 0 {
		t.Fatalf("campaign exit=%d errors=%q", code, campaignErrors.String())
	}
	var campaign map[string]any
	if err := json.Unmarshal(campaignOutput.Bytes(), &campaign); err != nil {
		t.Fatal(err)
	}
	if campaign["genesis_verified"] != true || campaign["verification_scope"] != delivery.VerificationScopeCampaign {
		t.Fatalf("campaign output = %v", campaign)
	}
}

func TestTickVerifyInvalidArgumentsAreNonzeroAndSilentOnStdout(t *testing.T) {
	var output, errorsOut bytes.Buffer
	if code := runWithReader([]string{"day"}, tickVerifyReaderStub{}, &output, &errorsOut); code == 0 || output.Len() != 0 || errorsOut.Len() == 0 {
		t.Fatalf("invalid day output=%q errors=%q", output.String(), errorsOut.String())
	}
}

var _ delivery.ArchiveReaderV1 = tickVerifyReaderStub{}
