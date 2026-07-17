package operations

import (
	"context"
	"testing"
)

type fakeIngestStatusReader struct {
	status IngestStatus
	err    error
}

func (f fakeIngestStatusReader) IngestStatus(context.Context) (IngestStatus, error) {
	return f.status, f.err
}

type fakePublicationStatusReader struct {
	status PublicationStatus
	err    error
}

func (f fakePublicationStatusReader) PublicationStatus(context.Context) (PublicationStatus, error) {
	return f.status, f.err
}

type fakeDiskStatusReader struct {
	status DiskStatus
	err    error
}

func (f fakeDiskStatusReader) DiskStatus(context.Context) (DiskStatus, error) {
	return f.status, f.err
}

func TestStatusServiceClassifiesApplicationState(t *testing.T) {
	baseIngest := fakeIngestStatusReader{status: IngestStatus{ReadyForACK: true}}
	baseDisk := fakeDiskStatusReader{status: DiskStatus{Ready: true, ACKAllowed: true}}
	cases := []struct {
		name        string
		publication PublicationStatus
		disk        DiskStatus
		wantOverall string
	}{
		{name: "healthy", publication: PublicationStatus{RemoteAvailable: true}, wantOverall: OverallHealthy},
		{name: "backlog", publication: PublicationStatus{RemoteAvailable: true, PendingBytes: 1}, wantOverall: OverallDegraded},
		{name: "remote timeout", publication: PublicationStatus{RemoteAvailable: false, LastErrorClass: ErrorClassRemoteTimeout}, wantOverall: OverallDegraded},
		{name: "permission", publication: PublicationStatus{RemoteAvailable: false, LastErrorClass: ErrorClassPermission}, wantOverall: OverallDegraded},
		{name: "disk high warning", publication: PublicationStatus{RemoteAvailable: true}, disk: DiskStatus{Class: "high", Ready: true, ACKAllowed: true, WorkerPriority: true}, wantOverall: OverallDegraded},
		{name: "disk blocked", publication: PublicationStatus{RemoteAvailable: true}, disk: DiskStatus{Ready: false, ACKAllowed: false}, wantOverall: OverallBlocked},
		{name: "collision", publication: PublicationStatus{LastErrorClass: ErrorClassCollision, RemoteAvailable: false}, wantOverall: OverallFailed},
		{name: "local integrity", publication: PublicationStatus{LastErrorClass: ErrorClassLocalIntegrity, RemoteAvailable: false}, wantOverall: OverallFailed},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			disk := baseDisk
			if test.name == "disk blocked" || test.name == "disk high warning" {
				disk = fakeDiskStatusReader{status: test.disk}
			}
			service, err := NewStatusService(baseIngest, fakePublicationStatusReader{status: test.publication}, disk)
			if err != nil {
				t.Fatal(err)
			}
			status, err := service.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if status.StatusVersion != ApplicationStatusVersion || status.Overall != test.wantOverall {
				t.Fatalf("status = %+v", status)
			}
		})
	}
}

func TestStatusServiceDoesNotExposeSourceError(t *testing.T) {
	service, err := NewStatusService(
		fakeIngestStatusReader{err: context.DeadlineExceeded},
		fakePublicationStatusReader{},
		fakeDiskStatusReader{},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Snapshot(context.Background())
	if err != ErrStatusUnavailable {
		t.Fatalf("status error = %v, want sanitized ErrStatusUnavailable", err)
	}
}
