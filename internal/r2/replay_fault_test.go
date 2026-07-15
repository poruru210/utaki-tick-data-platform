package r2

import (
	"context"
	"errors"
	"os"
	"testing"

	"tick-data-platform/internal/protocol"
)

var errInjectedReplayCrash = errors.New("injected replay publisher crash")

func TestReplayFaultRestartAfterEveryActionBarrier(t *testing.T) {
	for _, actionKind := range []ReplayActionKind{
		ReplayActionUploadParquet,
		ReplayActionUploadPartManifest,
		ReplayActionUploadReplayManifest,
	} {
		t.Run(string(actionKind), func(t *testing.T) {
			fixture := newReplayPublicationFixture(t, false)
			var crashedAction ReplayAction
			fixture.publisher.hooks.afterExecute = func(_ uint64, result ReplayActionResult) error {
				if result.Action.Kind != actionKind || crashedAction.ObjectID != "" {
					return nil
				}
				crashedAction = result.Action
				return errInjectedReplayCrash
			}

			if _, err := fixture.publish(t); !errors.Is(err, errInjectedReplayCrash) {
				t.Fatalf("injected crash = %v", err)
			}
			if crashedAction.ObjectID == "" {
				t.Fatal("target action was not reached")
			}
			if _, err := os.Stat(fixture.receipt); !os.IsNotExist(err) {
				t.Fatalf("receipt exists at action crash: %v", err)
			}

			fixture.publisher.hooks.afterExecute = nil
			receipt, err := fixture.publish(t)
			if err != nil || !receipt.VerificationComplete {
				t.Fatalf("restart did not complete: receipt=%+v err=%v", receipt, err)
			}
			bundle := fixture.sealedBundle(t)
			resolved, err := resolveReplayAction(bundle, crashedAction)
			if err != nil {
				t.Fatal(err)
			}
			copies := 0
			for _, key := range fixture.tool.copyKeys {
				if key == resolved.rcloneKey {
					copies++
				}
			}
			if copies != 1 {
				t.Fatalf("already completed action was copied %d times after restart", copies)
			}
		})
	}
}

func TestReplayFaultRestartDiscardsObservationBeforeAction(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	observations := 0
	fixture.publisher.hooks.afterObservation = func(_ uint64, _ ReplayRemoteObservation) error {
		observations++
		return errInjectedReplayCrash
	}
	if _, err := fixture.publish(t); !errors.Is(err, errInjectedReplayCrash) {
		t.Fatalf("injected crash = %v", err)
	}
	if copies, _ := fixture.tool.counts(); copies != 0 {
		t.Fatalf("action ran from crashed observation: copies=%d", copies)
	}

	fixture.publisher.hooks.afterObservation = func(_ uint64, _ ReplayRemoteObservation) error {
		observations++
		return nil
	}
	if _, err := fixture.publish(t); err != nil {
		t.Fatal(err)
	}
	if observations < 2 {
		t.Fatalf("restart reused stale observation: observations=%d", observations)
	}
}

type crashOnceReplayReceiptStore struct {
	crashed bool
}

func (s *crashOnceReplayReceiptStore) SaveNoClobber(_ context.Context, path string, receipt ReplayVerificationReceipt) error {
	if !s.crashed {
		s.crashed = true
		return errInjectedReplayCrash
	}
	return SaveReplayVerificationReceipt(path, receipt)
}

func TestReplayFaultRestartAfterFinalObservationBeforeReceipt(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	store := &crashOnceReplayReceiptStore{}
	fixture.publisher.receiptStore = store
	observations := 0
	fixture.publisher.hooks.afterObservation = func(_ uint64, _ ReplayRemoteObservation) error {
		observations++
		return nil
	}

	if _, err := fixture.publish(t); !errors.Is(err, errInjectedReplayCrash) {
		t.Fatalf("receipt crash = %v", err)
	}
	beforeRestart := observations
	if _, err := os.Stat(fixture.receipt); !os.IsNotExist(err) {
		t.Fatalf("receipt exists after failed save: %v", err)
	}
	receipt, err := fixture.publish(t)
	if err != nil || !receipt.VerificationComplete {
		t.Fatalf("receipt restart did not complete: receipt=%+v err=%v", receipt, err)
	}
	if observations <= beforeRestart {
		t.Fatal("receipt restart did not obtain a fresh final observation")
	}
}

func TestReplayFaultRemoteMutationAfterObservationCannotAuthorizeNextAction(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	bundle := fixture.sealedBundle(t)
	target := bundle.Contract.ParquetObjects[0]
	mutated := false
	fixture.publisher.hooks.afterObservation = func(round uint64, _ ReplayRemoteObservation) error {
		if round == 1 && !mutated {
			mutated = true
			fixture.backend.force(target.FullKey, []byte("conflicting-remote-object"))
		}
		return nil
	}

	if _, err := fixture.publish(t); !errors.Is(err, ErrReplayPublicationIntegrity) {
		t.Fatalf("stale-observation collision = %v", err)
	}
	if copies, _ := fixture.tool.counts(); copies != 1 {
		t.Fatalf("publisher continued after stale-observation collision: copies=%d", copies)
	}
	if _, err := os.Stat(fixture.receipt); !os.IsNotExist(err) {
		t.Fatalf("receipt exists after stale-observation collision: %v", err)
	}
}

func TestReplayFaultAggregateObservationBudgetExhaustion(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		limits func(protocol.ReplayPublicationLimits) protocol.ReplayPublicationLimits
	}{
		{
			name: "requests",
			limits: func(limits protocol.ReplayPublicationLimits) protocol.ReplayPublicationLimits {
				limits.MaxObservationRequests = 15
				return limits
			},
		},
		{
			name: "bytes",
			limits: func(limits protocol.ReplayPublicationLimits) protocol.ReplayPublicationLimits {
				limits.MaxObservationBytes = 100_000
				limits.MaxParquetObjectBytes = 100_000
				limits.MaxTotalParquetBytes = 100_000
				return limits
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newReplayPublicationFixture(t, false)
			fixture.input.Limits = testCase.limits(fixture.input.Limits)
			if _, err := fixture.publish(t); !errors.Is(err, ErrReplayPublicationResource) {
				t.Fatalf("aggregate budget error = %v", err)
			}
			if _, err := os.Stat(fixture.receipt); !os.IsNotExist(err) {
				t.Fatalf("receipt exists after aggregate budget stop: %v", err)
			}
		})
	}
}
