package operations

import (
	"context"
	"errors"
	"fmt"
)

const ApplicationStatusVersion = "application-status-v1"

const (
	OverallHealthy  = "healthy"
	OverallDegraded = "degraded"
	OverallBlocked  = "blocked"
	OverallFailed   = "failed"

	ErrorClassRemoteTransient = "remote_transient"
	ErrorClassRemoteTimeout   = "remote_timeout"
	ErrorClassPermission      = "permission"
	ErrorClassCollision       = "collision"
	ErrorClassLocalIntegrity  = "local_integrity"
	ErrorClassRemoteIntegrity = "remote_integrity"
	ErrorClassUnknown         = "unknown"
)

var ErrStatusUnavailable = errors.New("application status unavailable")

type IngestStatus struct {
	ReadyForACK     bool   `json:"ready_for_ack"`
	ActiveSession   bool   `json:"active_session"`
	AcceptedBatches uint64 `json:"accepted_batches"`
	Connections     uint64 `json:"connections"`
}

type PublicationStatus struct {
	PendingSegments                  uint64 `json:"pending_segments"`
	PendingBytes                     uint64 `json:"pending_bytes"`
	PendingManifests                 uint64 `json:"pending_manifests"`
	OldestPendingAtUnixMS            uint64 `json:"oldest_pending_at_unix_ms"`
	RetryCount                       uint64 `json:"retry_count"`
	LastSuccessfulVerificationUnixMS uint64 `json:"last_successful_verification_unix_ms"`
	LastErrorClass                   string `json:"last_error_class"`
	RemoteAvailable                  bool   `json:"remote_available"`
}

type DiskStatus struct {
	Class           string `json:"class"`
	FreeBytes       uint64 `json:"free_bytes"`
	TotalBytes      uint64 `json:"total_bytes"`
	Ready           bool   `json:"ready"`
	ACKAllowed      bool   `json:"ack_allowed"`
	BlockedReason   string `json:"blocked_reason"`
	PendingSegments uint64 `json:"pending_publication_segments"`
	PendingBytes    uint64 `json:"pending_publication_bytes"`
	WorkerPriority  bool   `json:"publication_worker_priority"`
}

type ApplicationStatus struct {
	StatusVersion string            `json:"status_version"`
	Overall       string            `json:"overall"`
	Ingest        IngestStatus      `json:"ingest"`
	Publication   PublicationStatus `json:"publication"`
	Disk          DiskStatus        `json:"disk"`
}

type IngestStatusReader interface {
	IngestStatus(context.Context) (IngestStatus, error)
}

type PublicationStatusReader interface {
	PublicationStatus(context.Context) (PublicationStatus, error)
}

type DiskStatusReader interface {
	DiskStatus(context.Context) (DiskStatus, error)
}

type StatusService struct {
	ingest      IngestStatusReader
	publication PublicationStatusReader
	disk        DiskStatusReader
}

func NewStatusService(ingestReader IngestStatusReader, publicationReader PublicationStatusReader, diskReader DiskStatusReader) (*StatusService, error) {
	if ingestReader == nil || publicationReader == nil || diskReader == nil {
		return nil, fmt.Errorf("application status sources are incomplete")
	}
	return &StatusService{ingest: ingestReader, publication: publicationReader, disk: diskReader}, nil
}

func (s *StatusService) Snapshot(ctx context.Context) (ApplicationStatus, error) {
	if err := ctx.Err(); err != nil {
		return ApplicationStatus{}, err
	}
	if s == nil {
		return ApplicationStatus{}, ErrStatusUnavailable
	}
	ingestStatus, err := s.ingest.IngestStatus(ctx)
	if err != nil {
		return ApplicationStatus{}, ErrStatusUnavailable
	}
	publicationStatus, err := s.publication.PublicationStatus(ctx)
	if err != nil {
		return ApplicationStatus{}, ErrStatusUnavailable
	}
	diskStatus, err := s.disk.DiskStatus(ctx)
	if err != nil {
		return ApplicationStatus{}, ErrStatusUnavailable
	}
	publicationStatus.LastErrorClass = normalizeErrorClass(publicationStatus.LastErrorClass)
	status := ApplicationStatus{
		StatusVersion: ApplicationStatusVersion,
		Overall:       overallStatus(ingestStatus, publicationStatus, diskStatus),
		Ingest:        ingestStatus,
		Publication:   publicationStatus,
		Disk:          diskStatus,
	}
	return status, nil
}

func overallStatus(ingestStatus IngestStatus, publicationStatus PublicationStatus, diskStatus DiskStatus) string {
	switch publicationStatus.LastErrorClass {
	case ErrorClassCollision, ErrorClassLocalIntegrity, ErrorClassRemoteIntegrity:
		return OverallFailed
	}
	if !ingestStatus.ReadyForACK || !diskStatus.Ready || !diskStatus.ACKAllowed {
		return OverallBlocked
	}
	if diskStatus.Class == "high" || diskStatus.WorkerPriority {
		return OverallDegraded
	}
	if !publicationStatus.RemoteAvailable || publicationStatus.LastErrorClass == ErrorClassRemoteTransient || publicationStatus.LastErrorClass == ErrorClassRemoteTimeout || publicationStatus.LastErrorClass == ErrorClassPermission || publicationStatus.PendingBytes != 0 || publicationStatus.PendingManifests != 0 {
		return OverallDegraded
	}
	return OverallHealthy
}

func normalizeErrorClass(value string) string {
	switch value {
	case "", ErrorClassRemoteTransient, ErrorClassRemoteTimeout, ErrorClassPermission, ErrorClassCollision, ErrorClassLocalIntegrity, ErrorClassRemoteIntegrity:
		return value
	default:
		return ErrorClassUnknown
	}
}
