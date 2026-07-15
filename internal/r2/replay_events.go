package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"tick-data-platform/internal/protocol"
)

const replayPublicationEventDomain = "tick-data-platform/replay-publication-event/v1\x00"

type ReplayEventKind string

const (
	ReplayEventBundleRegistered     ReplayEventKind = "BundleRegistered"
	ReplayEventObservationCompleted ReplayEventKind = "ObservationCompleted"
	ReplayEventActionPlanned        ReplayEventKind = "ActionPlanned"
	ReplayEventActionStarted        ReplayEventKind = "ActionStarted"
	ReplayEventActionFinished       ReplayEventKind = "ActionFinished"
	ReplayEventReceiptSaved         ReplayEventKind = "ReceiptSaved"
)

type ReplayPublicationEvent struct {
	EventID           [32]byte
	Kind              ReplayEventKind
	BundleDigest      [32]byte
	ObservationDigest [32]byte
	ActionPlanDigest  [32]byte
	ActionKind        ReplayActionKind
	ObjectID          ReplayObjectID
	ResultClass       ReplayActionResultClass
	ErrorClass        ReplayActionErrorClass
}

func NewReplayPublicationEvent(event ReplayPublicationEvent) (ReplayPublicationEvent, error) {
	event.EventID = [32]byte{}
	canonical, err := replayPublicationEventCanonical(event)
	if err != nil {
		return ReplayPublicationEvent{}, err
	}
	event.EventID = sha256.Sum256(append([]byte(replayPublicationEventDomain), canonical...))
	return event, nil
}

func replayPublicationEventCanonical(event ReplayPublicationEvent) ([]byte, error) {
	if err := validateReplayEventShape(event); err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(map[string]any{
		"action_kind": string(event.ActionKind), "action_plan_digest": hex.EncodeToString(event.ActionPlanDigest[:]),
		"bundle_digest": hex.EncodeToString(event.BundleDigest[:]), "error_class": string(event.ErrorClass),
		"event_kind": string(event.Kind), "object_id": string(event.ObjectID),
		"observation_digest": hex.EncodeToString(event.ObservationDigest[:]), "result_class": string(event.ResultClass),
	})
}

func validateReplayEventShape(event ReplayPublicationEvent) error {
	if event.BundleDigest == ([32]byte{}) {
		return fmt.Errorf("replay event bundle digest is zero")
	}
	zeroObservation := event.ObservationDigest == ([32]byte{})
	zeroPlan := event.ActionPlanDigest == ([32]byte{})
	hasAction := event.ActionKind != "" || event.ObjectID != ""
	hasResult := event.ResultClass != ""
	switch event.Kind {
	case ReplayEventBundleRegistered:
		if !zeroObservation || !zeroPlan || hasAction || hasResult || event.ErrorClass != ReplayActionErrorNone {
			return fmt.Errorf("BundleRegistered event has action or observation fields")
		}
	case ReplayEventObservationCompleted:
		if zeroObservation || !zeroPlan || hasAction || hasResult || event.ErrorClass != ReplayActionErrorNone {
			return fmt.Errorf("ObservationCompleted event fields are invalid")
		}
	case ReplayEventActionPlanned, ReplayEventActionStarted:
		if zeroObservation || zeroPlan || event.ActionKind == "" || event.ObjectID == "" || hasResult || event.ErrorClass != ReplayActionErrorNone {
			return fmt.Errorf("action event fields are incomplete")
		}
	case ReplayEventActionFinished:
		if zeroObservation || zeroPlan || event.ActionKind == "" || event.ObjectID == "" {
			return fmt.Errorf("ActionFinished event fields are incomplete")
		}
		switch event.ResultClass {
		case ReplayActionCompleted:
			if event.ErrorClass != ReplayActionErrorNone {
				return fmt.Errorf("completed action event has an error class")
			}
		case ReplayActionDifferent:
			if event.ErrorClass != ReplayActionErrorLocalDifferent && event.ErrorClass != ReplayActionErrorCollision && event.ErrorClass != ReplayActionErrorCheckMismatch {
				return fmt.Errorf("Different action event has an invalid error class")
			}
		case ReplayActionUnavailable:
			if event.ErrorClass != ReplayActionErrorTimeout && event.ErrorClass != ReplayActionErrorUnknownOutcome {
				return fmt.Errorf("Unavailable action event has an invalid error class")
			}
		default:
			return fmt.Errorf("ActionFinished event has unknown result class")
		}
	case ReplayEventReceiptSaved:
		if zeroObservation || zeroPlan || hasAction || hasResult || event.ErrorClass != ReplayActionErrorNone {
			return fmt.Errorf("ReceiptSaved event fields are invalid")
		}
	default:
		return fmt.Errorf("unknown replay event kind %q", event.Kind)
	}
	return nil
}

type replayEventRecord struct {
	event     ReplayPublicationEvent
	canonical []byte
}

// ReplayDiagnosticEventStore is an in-memory secret-free R4 adapter. It is
// append-only diagnostic data and is intentionally not imported by the
// observer, reconciler, or executor as action authority.
type ReplayDiagnosticEventStore struct {
	mu      sync.Mutex
	records map[[32]byte][]replayEventRecord
	byID    map[[32]byte][]byte
}

type ReplayEventStore interface {
	Append(ctx context.Context, bundle ReplayPublicationBundle, event ReplayPublicationEvent) error
	Load(ctx context.Context, bundle ReplayPublicationBundle) ([]ReplayPublicationEvent, error)
}

func NewReplayDiagnosticEventStore() *ReplayDiagnosticEventStore {
	return &ReplayDiagnosticEventStore{records: make(map[[32]byte][]replayEventRecord), byID: make(map[[32]byte][]byte)}
}

func (s *ReplayDiagnosticEventStore) Append(ctx context.Context, bundle ReplayPublicationBundle, event ReplayPublicationEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := verifySealedReplayBundle(bundle); err != nil {
		return err
	}
	if event.BundleDigest != bundle.Digest {
		return fmt.Errorf("replay event bundle digest mismatch")
	}
	if event.ActionKind != "" || event.ObjectID != "" {
		if !replayActionMatchesContract(bundle.Contract, ReplayAction{Kind: event.ActionKind, ObjectID: event.ObjectID}) {
			return fmt.Errorf("replay event action or object is unknown")
		}
	}
	canonical, err := replayPublicationEventCanonical(event)
	if err != nil {
		return err
	}
	wantID := sha256.Sum256(append([]byte(replayPublicationEventDomain), canonical...))
	if event.EventID != wantID {
		return fmt.Errorf("replay event ID does not match canonical bytes")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.byID[event.EventID]; ok {
		if bytes.Equal(existing, canonical) {
			return nil
		}
		return fmt.Errorf("replay event ID conflicts with different canonical bytes")
	}
	if err := validateReplayEventLineage(s.records[bundle.Digest], event); err != nil {
		return err
	}
	s.byID[event.EventID] = append([]byte(nil), canonical...)
	s.records[bundle.Digest] = append(s.records[bundle.Digest], replayEventRecord{event: event, canonical: append([]byte(nil), canonical...)})
	return nil
}

func (s *ReplayDiagnosticEventStore) Load(ctx context.Context, bundle ReplayPublicationBundle) ([]ReplayPublicationEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := verifySealedReplayBundle(bundle); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records := s.records[bundle.Digest]
	result := make([]ReplayPublicationEvent, len(records))
	for index, record := range records {
		canonical, err := replayPublicationEventCanonical(record.event)
		if err != nil || !bytes.Equal(canonical, record.canonical) {
			return nil, fmt.Errorf("stored replay event %d is malformed", index)
		}
		wantID := sha256.Sum256(append([]byte(replayPublicationEventDomain), canonical...))
		if record.event.EventID != wantID || record.event.BundleDigest != bundle.Digest {
			return nil, fmt.Errorf("stored replay event %d identity is invalid", index)
		}
		result[index] = record.event
	}
	return result, nil
}

func validateReplayEventLineage(records []replayEventRecord, event ReplayPublicationEvent) error {
	matchingReceiptObservation := event.Kind != ReplayEventReceiptSaved
	sawObservation := false
	for _, record := range records {
		existing := record.event
		if event.ObjectID != "" && existing.ObjectID == event.ObjectID && existing.ActionKind == event.ActionKind {
			if existing.ObservationDigest != event.ObservationDigest || existing.ActionPlanDigest != event.ActionPlanDigest {
				return fmt.Errorf("replay event observation or action plan conflicts")
			}
		}
		if event.Kind == ReplayEventReceiptSaved && existing.Kind == ReplayEventObservationCompleted {
			sawObservation = true
			if existing.ObservationDigest == event.ObservationDigest {
				matchingReceiptObservation = true
			}
		}
	}
	if sawObservation && !matchingReceiptObservation {
		return fmt.Errorf("receipt event observation digest conflicts")
	}
	return nil
}

func replayActionMatchesContract(bundle protocol.ReplayPublicationBundle, action ReplayAction) bool {
	switch action.Kind {
	case ReplayActionUploadParquet:
		for _, object := range bundle.ParquetObjects {
			if ReplayObjectID(object.ObjectID) == action.ObjectID {
				return true
			}
		}
	case ReplayActionUploadPartManifest:
		for _, object := range bundle.PartManifests {
			if ReplayObjectID(object.ObjectID) == action.ObjectID {
				return true
			}
		}
	case ReplayActionUploadReplayManifest:
		return replayManifestObjectID(bundle) == action.ObjectID
	}
	return false
}
