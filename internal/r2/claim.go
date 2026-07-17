package r2

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

const (
	PublisherClaimVersion = "publisher-claim-v1"
	PublisherClaimDomain  = "tick-data-platform/publisher-claim/v1\x00"
)

type PublisherClaim struct {
	ClaimVersion            string
	ScopeKey                string
	DatasetID               string
	ProviderID              string
	StableFeedID            string
	ExactSourceSymbol       string
	BrokerServerFingerprint string
	DayDefinitionID         string
	SettlePolicy            string
	ConfigHash              [32]byte
	PublisherID             string
	PublisherEpoch          uint64
}

func NewPublisherClaim(scope archive.ScopeConfig) (PublisherClaim, error) {
	scopeKey, err := archive.ScopePathKey(scope)
	if err != nil {
		return PublisherClaim{}, err
	}
	configHash, err := scope.ConfigHash()
	if err != nil {
		return PublisherClaim{}, err
	}
	return PublisherClaim{
		ClaimVersion:            PublisherClaimVersion,
		ScopeKey:                scopeKey,
		DatasetID:               scope.DatasetID,
		ProviderID:              scope.ProviderID,
		StableFeedID:            scope.StableFeedID,
		ExactSourceSymbol:       scope.ExactSourceSymbol,
		BrokerServerFingerprint: scope.BrokerServerFingerprint,
		DayDefinitionID:         scope.DayDefinitionID,
		SettlePolicy:            scope.SettlePolicy,
		ConfigHash:              configHash,
		PublisherID:             scope.PublisherID,
		PublisherEpoch:          scope.PublisherEpoch,
	}, nil
}

func (c PublisherClaim) Value() map[string]any {
	return map[string]any{
		"broker_server_fingerprint": c.BrokerServerFingerprint,
		"claim_version":             c.ClaimVersion,
		"config_hash":               hex.EncodeToString(c.ConfigHash[:]),
		"dataset_id":                c.DatasetID,
		"day_definition_id":         c.DayDefinitionID,
		"exact_source_symbol":       c.ExactSourceSymbol,
		"provider_id":               c.ProviderID,
		"publisher_epoch":           c.PublisherEpoch,
		"publisher_id":              c.PublisherID,
		"scope_key":                 c.ScopeKey,
		"settle_policy":             c.SettlePolicy,
		"stable_feed_id":            c.StableFeedID,
	}
}

func (c PublisherClaim) CanonicalJSON() ([]byte, error) {
	if c.ClaimVersion != PublisherClaimVersion || c.PublisherID == "" || c.ScopeKey == "" {
		return nil, fmt.Errorf("publisher claim identity is incomplete")
	}
	return protocol.CanonicalJSON(c.Value())
}

func (c PublisherClaim) Digest() ([32]byte, error) {
	bytes, err := c.CanonicalJSON()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(PublisherClaimDomain), bytes...)), nil
}
