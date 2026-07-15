package r2

import (
	"context"
	"errors"
	"os"
	"testing"
)

type narrowReplayTool struct {
	copyErr    error
	checkErr   error
	copyCalls  int
	checkCalls int
	keys       []string
	copied     [][]byte
}

func (t *narrowReplayTool) CopyToImmutable(_ context.Context, localPath, remoteKey string) error {
	t.copyCalls++
	t.keys = append(t.keys, remoteKey)
	body, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	t.copied = append(t.copied, body)
	return t.copyErr
}

func (t *narrowReplayTool) CheckDownload(_ context.Context, localPath, remoteKey string) error {
	t.checkCalls++
	t.keys = append(t.keys, remoteKey)
	if _, err := os.Stat(localPath); err != nil {
		return err
	}
	return t.checkErr
}

func sealedReplayExecutorBundle(t *testing.T) ReplayPublicationBundle {
	t.Helper()
	fixture := newReplayPublicationFixture(t, false)
	bundle, err := SealReplayPublicationBundle(replayBundleInputFromFixture(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestReplayExecutorApprovedExecutionUsesSealedObjectID(t *testing.T) {
	bundle := sealedReplayExecutorBundle(t)
	object := bundle.Contract.ParquetObjects[0]
	tool := &narrowReplayTool{}
	executor, _ := NewNarrowReplayActionExecutor(tool)
	result, err := executor.Execute(context.Background(), bundle, ReplayAction{Kind: ReplayActionUploadParquet, ObjectID: ReplayObjectID(object.ObjectID)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Class != ReplayActionCompleted || result.ErrorClass != ReplayActionErrorNone || tool.copyCalls != 1 || tool.checkCalls != 1 {
		t.Fatalf("execution result=%+v tool=%+v", result, tool)
	}
	if len(tool.keys) != 2 || tool.keys[0] != object.RcloneKey || tool.keys[1] != object.RcloneKey {
		t.Fatalf("executor did not use sealed rclone key: %v", tool.keys)
	}
}

func TestReplayExecutorCanonicalMetadataIsMaterializedAndVerified(t *testing.T) {
	bundle := sealedReplayExecutorBundle(t)
	object := bundle.Contract.PartManifests[0]
	tool := &narrowReplayTool{}
	executor, _ := NewNarrowReplayActionExecutor(tool)
	result, err := executor.Execute(context.Background(), bundle, ReplayAction{Kind: ReplayActionUploadPartManifest, ObjectID: ReplayObjectID(object.ObjectID)})
	if err != nil || result.Class != ReplayActionCompleted {
		t.Fatalf("metadata execution result=%+v err=%v", result, err)
	}
	if len(tool.copied) != 1 || string(tool.copied[0]) != string(bundle.LocalSources.Artifacts[ReplayObjectID(object.ObjectID)].CanonicalBytes) {
		t.Fatal("executor did not copy exact sealed canonical metadata")
	}
}

func TestReplayExecutorRejectsUnknownMismatchedAndUnsupportedActionBeforeRemoteCall(t *testing.T) {
	bundle := sealedReplayExecutorBundle(t)
	known := ReplayObjectID(bundle.Contract.ParquetObjects[0].ObjectID)
	tests := []ReplayAction{
		{Kind: ReplayActionUploadParquet, ObjectID: ReplayObjectID("unknown")},
		{Kind: ReplayActionUploadPartManifest, ObjectID: known},
		{Kind: ReplayActionKind("Delete"), ObjectID: known},
	}
	for _, action := range tests {
		tool := &narrowReplayTool{}
		executor, _ := NewNarrowReplayActionExecutor(tool)
		if _, err := executor.Execute(context.Background(), bundle, action); err == nil {
			t.Fatalf("action %+v was accepted", action)
		}
		if tool.copyCalls != 0 || tool.checkCalls != 0 {
			t.Fatalf("remote call occurred for %+v", action)
		}
	}
}

func TestReplayExecutorRejectsBundleDigestMismatchBeforeRemoteCall(t *testing.T) {
	bundle := sealedReplayExecutorBundle(t)
	action := ReplayAction{Kind: ReplayActionUploadParquet, ObjectID: ReplayObjectID(bundle.Contract.ParquetObjects[0].ObjectID)}
	bundle.Digest[0] ^= 0xff
	tool := &narrowReplayTool{}
	executor, _ := NewNarrowReplayActionExecutor(tool)
	if _, err := executor.Execute(context.Background(), bundle, action); err == nil {
		t.Fatal("bundle digest mismatch was accepted")
	}
	if tool.copyCalls != 0 || tool.checkCalls != 0 {
		t.Fatal("remote call occurred for mismatched bundle")
	}
}

func TestReplayExecutorLocalMutationStopsAsDifferent(t *testing.T) {
	bundle := sealedReplayExecutorBundle(t)
	object := bundle.Contract.ParquetObjects[0]
	artifact := bundle.LocalSources.Artifacts[ReplayObjectID(object.ObjectID)]
	file, err := os.OpenFile(artifact.Path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	tool := &narrowReplayTool{}
	executor, _ := NewNarrowReplayActionExecutor(tool)
	result, err := executor.Execute(context.Background(), bundle, ReplayAction{Kind: ReplayActionUploadParquet, ObjectID: ReplayObjectID(object.ObjectID)})
	if err != nil || result.Class != ReplayActionDifferent || result.ErrorClass != ReplayActionErrorLocalDifferent {
		t.Fatalf("mutation result=%+v err=%v", result, err)
	}
	if tool.copyCalls != 0 || tool.checkCalls != 0 {
		t.Fatal("mutated local source reached remote tool")
	}
}

func TestReplayExecutorTimeoutCollisionAndCheckMismatchAreFailClosed(t *testing.T) {
	bundle := sealedReplayExecutorBundle(t)
	action := ReplayAction{Kind: ReplayActionUploadParquet, ObjectID: ReplayObjectID(bundle.Contract.ParquetObjects[0].ObjectID)}
	tests := []struct {
		name      string
		copyErr   error
		checkErr  error
		wantClass ReplayActionResultClass
		wantError ReplayActionErrorClass
	}{
		{"timeout", context.DeadlineExceeded, nil, ReplayActionUnavailable, ReplayActionErrorTimeout},
		{"collision", ErrRcloneCollision, nil, ReplayActionDifferent, ReplayActionErrorCollision},
		{"check_mismatch", nil, ErrRcloneCheckMismatch, ReplayActionDifferent, ReplayActionErrorCheckMismatch},
		{"unknown", errors.New("unknown outcome"), nil, ReplayActionUnavailable, ReplayActionErrorUnknownOutcome},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tool := &narrowReplayTool{copyErr: testCase.copyErr, checkErr: testCase.checkErr}
			executor, _ := NewNarrowReplayActionExecutor(tool)
			result, err := executor.Execute(context.Background(), bundle, action)
			if err != nil || result.Class != testCase.wantClass || result.ErrorClass != testCase.wantError {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if testCase.copyErr != nil && tool.checkCalls != 0 {
				t.Fatal("check ran after failed immutable copy")
			}
		})
	}
}

func TestReplayExecutorAllowlistExposesOnlyImmutableCopyAndDownloadCheck(t *testing.T) {
	var _ ReplayActionTool = (*narrowReplayTool)(nil)
	var _ ReplayActionTool = (*RcloneRunner)(nil)
}
