package protocol

import (
	"bytes"
	"testing"
)

func TestCanonicalJSONUsesProtocolV1EscapingAndOrdering(t *testing.T) {
	value := map[string]any{
		"z": "日本語😀",
		"a": uint64(1),
	}
	want := []byte(`{"a":1,"z":"\u65e5\u672c\u8a9e\ud83d\ude00"}`)
	got, err := CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("canonical JSON = %s, want %s", got, want)
	}
	decoded, err := DecodeCanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := CanonicalJSON(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, got) {
		t.Fatal("canonical JSON did not round-trip")
	}
}

func TestDecodeCanonicalJSONRejectsNonCanonicalInputs(t *testing.T) {
	tests := [][]byte{
		[]byte(`{"a":1,"a":2}`),
		[]byte(`{"a":1.0}`),
		[]byte(`{"a":01}`),
		[]byte(`{"a":-0}`),
		[]byte(`{"a":1} `),
		[]byte(`{"a":"日本"}`),
		{0xef, 0xbb, 0xbf, '{', '}', '\n'},
		{'{', '"', 'a', '"', ':', '"', 0xff, '"', '}'},
	}
	for _, input := range tests {
		if _, err := DecodeCanonicalJSON(input); err == nil {
			t.Fatalf("DecodeCanonicalJSON accepted %q", input)
		}
	}
}

func TestCanonicalJSONRejectsFloatingPointValues(t *testing.T) {
	if _, err := CanonicalJSON(map[string]any{"value": 1.5}); err == nil {
		t.Fatal("CanonicalJSON accepted a floating point value")
	}
}
