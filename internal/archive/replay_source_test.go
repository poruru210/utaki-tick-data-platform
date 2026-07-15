package archive

import "testing"

func TestValidateReplayResourceLimitsRejectsChainByteOverflow(t *testing.T) {
	manifest := RawDayManifest{ChainObjects: []RawChainObject{
		{Bytes: ^uint64(0)},
		{Bytes: 1},
	}}
	limits := ReplayResourceLimits{
		MaxChainObjects: 2,
		MaxObjectBytes:  ^uint64(0),
		MaxChainBytes:   ^uint64(0),
	}
	if err := validateReplayResourceLimits(manifest, limits); err == nil {
		t.Fatal("overflowing chain byte sum was accepted")
	}
}

func TestValidateReplayResourceLimitsRejectsChainObjectCount(t *testing.T) {
	manifest := RawDayManifest{ChainObjects: []RawChainObject{{Bytes: 1}, {Bytes: 1}}}
	limits := ReplayResourceLimits{MaxChainObjects: 1, MaxObjectBytes: 1, MaxChainBytes: 2}
	if err := validateReplayResourceLimits(manifest, limits); err == nil {
		t.Fatal("chain object count over-limit was accepted")
	}
}
