package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

type ReplayActionResultClass string

const (
	ReplayActionCompleted   ReplayActionResultClass = "Completed"
	ReplayActionDifferent   ReplayActionResultClass = "Different"
	ReplayActionUnavailable ReplayActionResultClass = "Unavailable"
)

type ReplayActionErrorClass string

const (
	ReplayActionErrorNone           ReplayActionErrorClass = "None"
	ReplayActionErrorLocalDifferent ReplayActionErrorClass = "LocalDifferent"
	ReplayActionErrorCollision      ReplayActionErrorClass = "Collision"
	ReplayActionErrorCheckMismatch  ReplayActionErrorClass = "CheckMismatch"
	ReplayActionErrorTimeout        ReplayActionErrorClass = "Timeout"
	ReplayActionErrorUnknownOutcome ReplayActionErrorClass = "UnknownOutcome"
)

type ReplayActionResult struct {
	BundleDigest [32]byte
	Action       ReplayAction
	Class        ReplayActionResultClass
	Bytes        uint64
	Digest       string
	ErrorClass   ReplayActionErrorClass
}

type ReplayActionExecutor interface {
	Execute(ctx context.Context, bundle ReplayPublicationBundle, action ReplayAction) (ReplayActionResult, error)
}

type NarrowReplayActionExecutor struct {
	tool ReplayActionTool
}

func NewNarrowReplayActionExecutor(tool ReplayActionTool) (*NarrowReplayActionExecutor, error) {
	if tool == nil {
		return nil, fmt.Errorf("replay action tool is nil")
	}
	return &NarrowReplayActionExecutor{tool: tool}, nil
}

type resolvedReplayAction struct {
	artifact  ReplayLocalArtifact
	rcloneKey string
	digest    string
	bytes     uint64
	relative  string
}

func (e *NarrowReplayActionExecutor) Execute(ctx context.Context, bundle ReplayPublicationBundle, action ReplayAction) (ReplayActionResult, error) {
	if err := verifySealedReplayBundle(bundle); err != nil {
		return ReplayActionResult{}, err
	}
	resolved, err := resolveReplayAction(bundle, action)
	if err != nil {
		return ReplayActionResult{}, err
	}
	result := ReplayActionResult{BundleDigest: bundle.Digest, Action: action, Bytes: resolved.bytes, Digest: resolved.digest}
	path, cleanup, err := materializeReplayActionSource(resolved)
	if err != nil {
		result.Class = ReplayActionDifferent
		result.ErrorClass = ReplayActionErrorLocalDifferent
		return result, nil
	}
	defer cleanup()

	if err := e.tool.CopyToImmutable(ctx, path, resolved.rcloneKey); err != nil {
		return classifyReplayActionToolError(result, err, true), nil
	}
	if err := e.tool.CheckDownload(ctx, path, resolved.rcloneKey); err != nil {
		return classifyReplayActionToolError(result, err, false), nil
	}
	result.Class = ReplayActionCompleted
	result.ErrorClass = ReplayActionErrorNone
	return result, nil
}

func resolveReplayAction(bundle ReplayPublicationBundle, action ReplayAction) (resolvedReplayAction, error) {
	if action.ObjectID == "" {
		return resolvedReplayAction{}, fmt.Errorf("replay action object ID is empty")
	}
	var kind, rcloneKey, digest, relative string
	var bytes uint64
	switch action.Kind {
	case ReplayActionUploadParquet:
		for _, object := range bundle.Contract.ParquetObjects {
			if ReplayObjectID(object.ObjectID) == action.ObjectID {
				kind, rcloneKey, digest, relative, bytes = "parquet", object.RcloneKey, object.SHA256, object.RelativeKey, object.Bytes
				break
			}
		}
	case ReplayActionUploadPartManifest:
		for _, object := range bundle.Contract.PartManifests {
			if ReplayObjectID(object.ObjectID) == action.ObjectID {
				kind, rcloneKey, digest, relative, bytes = "part_manifest", object.RcloneKey, object.DomainDigest, object.RelativeKey, object.Bytes
				break
			}
		}
	case ReplayActionUploadReplayManifest:
		if replayManifestObjectID(bundle.Contract) == action.ObjectID {
			object := bundle.Contract.ReplayManifest
			kind, rcloneKey, digest, relative, bytes = "replay_manifest", object.RcloneKey, object.DomainDigest, object.RelativeKey, object.Bytes
		}
	default:
		return resolvedReplayAction{}, fmt.Errorf("unsupported replay action kind %q", action.Kind)
	}
	if kind == "" {
		return resolvedReplayAction{}, fmt.Errorf("replay action object ID is unknown or mismatched for %s", action.Kind)
	}
	artifact, ok := bundle.LocalSources.Artifacts[action.ObjectID]
	if !ok || artifact.Kind != kind || artifact.Bytes != bytes || artifact.Digest != digest || artifact.ContentSHA256 == "" {
		return resolvedReplayAction{}, fmt.Errorf("sealed local source does not match replay action")
	}
	if kind == "parquet" && artifact.ContentSHA256 != digest {
		return resolvedReplayAction{}, fmt.Errorf("sealed Parquet content hash does not match replay action")
	}
	return resolvedReplayAction{artifact: artifact, rcloneKey: rcloneKey, digest: digest, bytes: bytes, relative: relative}, nil
}

func materializeReplayActionSource(action resolvedReplayAction) (string, func(), error) {
	var source io.Reader
	var closeSource func() error
	switch action.artifact.Kind {
	case "parquet":
		if action.artifact.Path == "" {
			return "", func() {}, fmt.Errorf("Parquet local source path is empty")
		}
		file, err := os.Open(action.artifact.Path)
		if err != nil {
			return "", func() {}, err
		}
		source = file
		closeSource = file.Close
	case "part_manifest":
		if _, err := archive.VerifyPartManifestObject(action.artifact.CanonicalBytes, action.relative, mustHash(action.digest)); err != nil {
			return "", func() {}, err
		}
		source = bytes.NewReader(action.artifact.CanonicalBytes)
		closeSource = func() error { return nil }
	case "replay_manifest":
		manifest, err := protocol.VerifyReplayDayManifest(action.artifact.CanonicalBytes)
		if err != nil {
			return "", func() {}, err
		}
		canonical, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
		digest, digestErr := protocol.ReplayDayManifestDigest(manifest)
		key, keyErr := protocol.ReplayDayManifestKey(manifest)
		if err != nil || digestErr != nil || keyErr != nil || !bytes.Equal(canonical, action.artifact.CanonicalBytes) || digest != mustHash(action.digest) || key != action.relative {
			return "", func() {}, fmt.Errorf("replay manifest local source is not canonical or bound")
		}
		source = bytes.NewReader(action.artifact.CanonicalBytes)
		closeSource = func() error { return nil }
	default:
		return "", func() {}, fmt.Errorf("local source kind is not executable")
	}

	temporary, err := os.CreateTemp("", ".replay-action-*.tmp")
	if err != nil {
		_ = closeSource()
		return "", func() {}, err
	}
	cleanup := func() { _ = os.Remove(temporary.Name()) }
	hash := sha256.New()
	read, copyErr := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(source, int64(action.bytes)+1))
	closeSourceErr := closeSource()
	syncErr := temporary.Sync()
	closeErr := temporary.Close()
	if copyErr != nil || closeSourceErr != nil || syncErr != nil || closeErr != nil || read < 0 || uint64(read) != action.bytes || hex.EncodeToString(hash.Sum(nil)) != action.artifact.ContentSHA256 {
		cleanup()
		return "", func() {}, fmt.Errorf("local source bytes changed after sealing")
	}
	return temporary.Name(), cleanup, nil
}

func classifyReplayActionToolError(result ReplayActionResult, err error, copyPhase bool) ReplayActionResult {
	if copyPhase && errors.Is(err, ErrRcloneCollision) {
		result.Class = ReplayActionDifferent
		result.ErrorClass = ReplayActionErrorCollision
		return result
	}
	if !copyPhase && (errors.Is(err, ErrRcloneCheckMismatch) || errors.Is(err, ErrReplayCheckDifferent)) {
		result.Class = ReplayActionDifferent
		result.ErrorClass = ReplayActionErrorCheckMismatch
		return result
	}
	result.Class = ReplayActionUnavailable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		result.ErrorClass = ReplayActionErrorTimeout
	} else {
		result.ErrorClass = ReplayActionErrorUnknownOutcome
	}
	return result
}
