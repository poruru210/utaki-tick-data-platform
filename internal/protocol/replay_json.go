package protocol

import "fmt"

func exactObject(value any, expected map[string]bool) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("canonical JSON value must be an object")
	}
	if len(object) != len(expected) {
		return nil, fmt.Errorf("canonical JSON object has an unexpected key set")
	}
	for key := range object {
		if !expected[key] {
			return nil, fmt.Errorf("unknown canonical JSON key %q", key)
		}
	}
	return object, nil
}

func stringValue(object map[string]any, key string) (string, error) {
	value, ok := object[key].(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return value, nil
}

func uintValue(object map[string]any, key string) (uint64, error) {
	value, ok := object[key].(uint64)
	if !ok {
		return 0, fmt.Errorf("%s must be a non-negative JSON integer", key)
	}
	return value, nil
}

func hashValue(object map[string]any, key string) ([32]byte, error) {
	value, err := stringValue(object, key)
	if err != nil {
		return [32]byte{}, err
	}
	return ParseHashHex(value)
}

func nullableHashValue(object map[string]any, key string) (*[32]byte, error) {
	value := object[key]
	if value == nil {
		return nil, nil
	}
	hash, err := hashValue(object, key)
	if err != nil {
		return nil, err
	}
	return &hash, nil
}

func stringArrayValue(object map[string]any, key string) ([]string, error) {
	value, ok := object[key].([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an array", key)
	}
	result := make([]string, len(value))
	for index, item := range value {
		stringItem, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s[%d] must be a string", key, index)
		}
		result[index] = stringItem
	}
	return result, nil
}

var partManifestKeys = map[string]bool{
	"campaign_id": true, "canonical_row_bytes": true, "conversion_id": true,
	"converter_build_id": true, "dataset_id": true, "date": true,
	"day_definition_id": true, "dependency_lock_hash": true, "first_row_chain_hash": true,
	"first_stream_sequence": true, "format_id": true, "last_row_chain_hash": true,
	"last_stream_sequence": true, "manifest_version": true, "part_bytes": true,
	"part_key": true, "part_sequence": true, "part_sha256": true,
	"previous_manifest_sha256": true, "previous_row_chain_hash": true, "raw_day_manifest_key": true,
	"raw_day_manifest_sha256": true, "replay_contract_id": true, "row_count": true,
	"target_platform_contract": true, "writer_configuration_hash": true,
}

// VerifyPartManifest strictly decodes one immutable part-manifest-v1 object.
func VerifyPartManifest(data []byte) (PartManifest, error) {
	value, err := DecodeCanonicalJSON(data)
	if err != nil {
		return PartManifest{}, err
	}
	object, err := exactObject(value, partManifestKeys)
	if err != nil {
		return PartManifest{}, err
	}
	version, err := stringValue(object, "manifest_version")
	if err != nil || version != PartManifestVersion {
		return PartManifest{}, fmt.Errorf("invalid part manifest version")
	}
	partSequence, err := uintValue(object, "part_sequence")
	if err != nil || partSequence > uint64(^uint32(0)) {
		return PartManifest{}, fmt.Errorf("invalid part sequence")
	}
	partKey, err := stringValue(object, "part_key")
	if err != nil {
		return PartManifest{}, err
	}
	partSHA, err := hashValue(object, "part_sha256")
	if err != nil {
		return PartManifest{}, err
	}
	firstChain, err := hashValue(object, "first_row_chain_hash")
	if err != nil {
		return PartManifest{}, err
	}
	lastChain, err := hashValue(object, "last_row_chain_hash")
	if err != nil {
		return PartManifest{}, err
	}
	rowCount, err := uintValue(object, "row_count")
	if err != nil {
		return PartManifest{}, err
	}
	previous, err := nullableHashValue(object, "previous_manifest_sha256")
	if err != nil {
		return PartManifest{}, err
	}
	datasetID, err := stringValue(object, "dataset_id")
	if err != nil {
		return PartManifest{}, err
	}
	campaignID, err := stringValue(object, "campaign_id")
	if err != nil {
		return PartManifest{}, err
	}
	dayDefinitionID, err := stringValue(object, "day_definition_id")
	if err != nil {
		return PartManifest{}, err
	}
	date, err := stringValue(object, "date")
	if err != nil {
		return PartManifest{}, err
	}
	replayContractID, err := stringValue(object, "replay_contract_id")
	if err != nil {
		return PartManifest{}, err
	}
	formatID, err := stringValue(object, "format_id")
	if err != nil {
		return PartManifest{}, err
	}
	conversionID, err := stringValue(object, "conversion_id")
	if err != nil {
		return PartManifest{}, err
	}
	converterBuildID, err := stringValue(object, "converter_build_id")
	if err != nil {
		return PartManifest{}, err
	}
	targetPlatformContract, err := stringValue(object, "target_platform_contract")
	if err != nil {
		return PartManifest{}, err
	}
	rawDayManifestKey, err := stringValue(object, "raw_day_manifest_key")
	if err != nil {
		return PartManifest{}, err
	}
	dependencyLockHash, err := hashValue(object, "dependency_lock_hash")
	if err != nil {
		return PartManifest{}, err
	}
	writerConfigurationHash, err := hashValue(object, "writer_configuration_hash")
	if err != nil {
		return PartManifest{}, err
	}
	rawDayManifestSHA256, err := hashValue(object, "raw_day_manifest_sha256")
	if err != nil {
		return PartManifest{}, err
	}
	previousRowChainHash, err := hashValue(object, "previous_row_chain_hash")
	if err != nil {
		return PartManifest{}, err
	}
	part := PartManifest{
		ManifestVersion:         version,
		DatasetID:               datasetID,
		CampaignID:              campaignID,
		DayDefinitionID:         dayDefinitionID,
		Date:                    date,
		ReplayContractID:        replayContractID,
		FormatID:                formatID,
		ConversionID:            conversionID,
		ConverterBuildID:        converterBuildID,
		DependencyLockHash:      dependencyLockHash,
		WriterConfigurationHash: writerConfigurationHash,
		TargetPlatformContract:  targetPlatformContract,
		RawDayManifestKey:       rawDayManifestKey,
		RawDayManifestSHA256:    rawDayManifestSHA256,
		PartSequence:            uint32(partSequence),
		PartKey:                 partKey,
		PartSHA256:              partSHA,
		PartBytes:               mustUint(object, "part_bytes"),
		RowCount:                rowCount,
		CanonicalRowBytes:       mustUint(object, "canonical_row_bytes"),
		FirstStreamSequence:     mustUint(object, "first_stream_sequence"),
		LastStreamSequence:      mustUint(object, "last_stream_sequence"),
		PreviousRowChainHash:    previousRowChainHash,
		FirstRowChainHash:       firstChain,
		LastRowChainHash:        lastChain,
		PreviousManifestSHA256:  previous,
	}
	if _, err := uintValue(object, "part_bytes"); err != nil {
		return PartManifest{}, err
	}
	if _, err := uintValue(object, "canonical_row_bytes"); err != nil {
		return PartManifest{}, err
	}
	if _, err := uintValue(object, "first_stream_sequence"); err != nil {
		return PartManifest{}, err
	}
	if _, err := uintValue(object, "last_stream_sequence"); err != nil {
		return PartManifest{}, err
	}
	if err := part.Validate(); err != nil {
		return PartManifest{}, err
	}
	digest, err := PartManifestDigest(part)
	if err != nil {
		return PartManifest{}, err
	}
	part.ManifestSHA256 = digest
	return part, nil
}

func mustUint(object map[string]any, key string) uint64 {
	value, _ := uintValue(object, key)
	return value
}

var replayDayManifestM3Keys = map[string]bool{
	"campaign_id": true, "canonical_stream_row_chain_root": true,
	"completeness_status": true, "conversion_id": true, "converter_build_id": true,
	"dataset_id": true, "date": true, "day_definition_id": true,
	"dependency_lock_hash": true, "format_id": true, "manifest_id": true,
	"manifest_version": true, "part_manifest_keys": true, "part_set_root": true,
	"previous_manifest_sha256": true, "raw_day_manifest_key": true,
	"raw_day_manifest_sha256": true, "replay_contract_id": true, "revision": true,
	"target_platform_contract": true, "writer_configuration_hash": true,
}

var replayDayManifestM0Keys = map[string]bool{
	"campaign_id": true, "canonical_stream_row_chain_root": true,
	"completeness_status": true, "conversion_id": true, "converter_build_id": true,
	"dataset_id": true, "date": true, "day_definition_id": true,
	"dependency_lock_hash": true, "format_id": true, "manifest_id": true,
	"manifest_version": true, "part_manifest_keys": true, "part_set_root": true,
	"raw_day_manifest_sha256": true, "replay_contract_id": true,
	"target_platform_contract": true, "writer_configuration_hash": true,
}

// VerifyReplayDayManifest accepts the pre-M3 empty-parts compatibility form
// and the strict M3 form. Only the latter is publishable and binding-complete.
func VerifyReplayDayManifest(data []byte) (ReplayDayManifest, error) {
	value, err := DecodeCanonicalJSON(data)
	if err != nil {
		return ReplayDayManifest{}, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return ReplayDayManifest{}, fmt.Errorf("replay manifest must be an object")
	}
	compatibility := len(object) == len(replayDayManifestM0Keys)
	if compatibility {
		for key := range object {
			if !replayDayManifestM0Keys[key] {
				compatibility = false
				break
			}
		}
	}
	if compatibility {
		manifest, err := decodeReplayManifestCommon(object)
		if err != nil {
			return ReplayDayManifest{}, err
		}
		if manifest.PartManifestKeys == nil || len(manifest.PartManifestKeys) != 0 || object["part_set_root"] != nil || object["canonical_stream_row_chain_root"] != nil {
			return ReplayDayManifest{}, fmt.Errorf("invalid M0 empty-parts compatibility form")
		}
		manifest.M0EmptyPartsCompatibility = true
		return manifest, nil
	}
	object, err = exactObject(value, replayDayManifestM3Keys)
	if err != nil {
		return ReplayDayManifest{}, err
	}
	manifest, err := decodeReplayManifestCommon(object)
	if err != nil {
		return ReplayDayManifest{}, err
	}
	manifest.Revision, err = uintValue(object, "revision")
	if err != nil {
		return ReplayDayManifest{}, err
	}
	manifest.RawDayManifestKey, err = stringValue(object, "raw_day_manifest_key")
	if err != nil {
		return ReplayDayManifest{}, err
	}
	manifest.PartSetRoot, err = hashValue(object, "part_set_root")
	if err != nil {
		return ReplayDayManifest{}, err
	}
	manifest.CanonicalStreamRowChainRoot, err = hashValue(object, "canonical_stream_row_chain_root")
	if err != nil {
		return ReplayDayManifest{}, err
	}
	manifest.PreviousManifestSHA256, err = nullableHashValue(object, "previous_manifest_sha256")
	if err != nil {
		return ReplayDayManifest{}, err
	}
	if err := manifest.Validate(); err != nil {
		return ReplayDayManifest{}, err
	}
	digest, err := ReplayDayManifestDigest(manifest)
	if err != nil {
		return ReplayDayManifest{}, err
	}
	manifest.ManifestSHA256 = digest
	return manifest, nil
}

func decodeReplayManifestCommon(object map[string]any) (ReplayDayManifest, error) {
	var result ReplayDayManifest
	var err error
	if result.ManifestVersion, err = stringValue(object, "manifest_version"); err != nil || result.ManifestVersion != ReplayDayManifestVersion {
		return result, fmt.Errorf("invalid replay manifest version")
	}
	for key, target := range map[string]*string{
		"manifest_id": &result.ManifestID, "dataset_id": &result.DatasetID,
		"campaign_id": &result.CampaignID, "day_definition_id": &result.DayDefinitionID,
		"date": &result.Date, "replay_contract_id": &result.ReplayContractID,
		"format_id": &result.FormatID, "conversion_id": &result.ConversionID,
		"converter_build_id":       &result.ConverterBuildID,
		"target_platform_contract": &result.TargetPlatformContract,
		"completeness_status":      &result.CompletenessStatus,
	} {
		*target, err = stringValue(object, key)
		if err != nil {
			return result, err
		}
	}
	if result.RawDayManifestSHA256, err = hashValue(object, "raw_day_manifest_sha256"); err != nil {
		return result, err
	}
	if result.DependencyLockHash, err = hashValue(object, "dependency_lock_hash"); err != nil {
		return result, err
	}
	if result.WriterConfigurationHash, err = hashValue(object, "writer_configuration_hash"); err != nil {
		return result, err
	}
	if result.PartManifestKeys, err = stringArrayValue(object, "part_manifest_keys"); err != nil {
		return result, err
	}
	return result, nil
}
