package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/fx/fxtest"

	"tick-data-platform/internal/archive"
	appconfig "tick-data-platform/internal/config"
	"tick-data-platform/internal/credentials"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/publication"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/wal"
	"tick-data-platform/producers/fake"
	protocolv1 "tick-data-platform/protocol/v1/go"
)

func TestFxApplicationG3UsesGatewayAndManualPublicationTick(t *testing.T) {
	config := testConfig(t)
	config.Publication.SealMaxBytes = 1
	config.Publication.SealIntervalMS = uint64(time.Hour / time.Millisecond)
	config.Publication.ScanIntervalMS = uint64(time.Hour / time.Millisecond)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config.ListenAddress = listener.Addr().String()
	_ = listener.Close()

	fixedNow := time.Date(2024, 3, 9, 12, 0, 0, 0, time.UTC)
	clock := publication.Clock(func() time.Time { return fixedNow })
	tickerReady := make(chan *appManualTicker, 1)
	tickerFactory := publication.TickerFactory(func(time.Duration) publication.Ticker {
		ticker := &appManualTicker{ticks: make(chan time.Time, 1), stopped: make(chan struct{})}
		tickerReady <- ticker
		return ticker
	})
	backend := newAppFakeBackend()
	var catalog *publication.Catalog
	application := fxtest.New(t,
		fx.Options(
			TestOptionsWithRemoteBackend(config, &staticProvider{}, backend),
			fx.Supply(clock, tickerFactory),
		),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(30*time.Second),
		fx.Populate(&catalog),
	)
	application.RequireStart()
	t.Cleanup(func() {
		application.RequireStop()
		removeEventually(t, config.Publication.CatalogPath)
	})

	var ticker *appManualTicker
	select {
	case ticker = <-tickerReady:
	case <-time.After(5 * time.Second):
		t.Fatal("Fx publication ticker was not created")
	}

	values := config.Gateway()
	hello := protocolv1.HelloV1{
		ProducerInstanceID: "fake-ingest-producer",
		ProducerSessionID:  "fake-ingest-session",
		ProducerBuildID:    values.ProducerBuildID,
		MQLCompilerBuild:   "fake",
		TerminalBuild:      "fake",
		OSContract:         "test",
		ClockAPIID:         "test-clock", ProviderID: values.ProviderID,
		StableFeedID:            values.StableFeedID,
		BrokerServerFingerprint: values.BrokerServerFingerprint,
		ExactSourceSymbol:       values.ExactSourceSymbol,
		SourceSchemaID:          protocolv1.SourceSchemaMT5,
		AcquisitionMode:         1,
		InitialFromMSC:          values.InitialFromMSC,
	}
	client, err := fake.Dial(context.Background(), config.ListenAddress, hello)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	day := time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC)
	ack, err := client.SendBatch(protocolv1.BatchFrameV1{
		ProducerSessionID:     hello.ProducerSessionID,
		BatchSequence:         1,
		RequestedFromMSC:      day.UnixMilli(),
		RequestedCount:        1,
		FetchWallStartS:       day.Unix(),
		FetchWallEndS:         day.Unix() + 1,
		FetchMonotonicStartUS: 1,
		FetchMonotonicEndUS:   2,
		ReturnedCount:         1,
		SourceSchemaID:        protocolv1.SourceSchemaMT5,
		Records: []protocolv1.RawMqlTickV1{{
			Time:            day.Unix(),
			TimeMSC:         day.Add(time.Second).UnixMilli(),
			CaptureSequence: 1,
			BidBits:         100,
			AskBits:         200,
		}},
	})
	if err != nil || ack.Status != protocolv1.AckAcceptedAdvanced {
		t.Fatalf("fake ingest ack = %+v err=%v", ack, err)
	}
	ticker.ticks <- fixedNow

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		record, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
		if err != nil {
			t.Fatal(err)
		}
		if found && record.State == publication.ManifestStateSpooled {
			data, err := os.ReadFile(record.Path)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := archive.VerifyRawDayManifest(data)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.LogicalCloseTimeS != 0 {
				t.Fatalf("automatic manifest logical close time = %d, want 0", manifest.LogicalCloseTimeS)
			}
			segments, err := catalog.ListSegments(context.Background())
			if err != nil || len(segments) != 1 {
				t.Fatalf("promoted segments = %+v err=%v", segments, err)
			}
			if _, err := os.Stat(segments[0].RawPath); err != nil {
				t.Fatalf("promoted raw object missing: %v", err)
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("manifest was not spooled: record=%+v found=%v", record, found)
		case <-poll.C:
		}
	}
}

type appManualTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func (t *appManualTicker) C() <-chan time.Time { return t.ticks }
func (t *appManualTicker) Stop()               { t.once.Do(func() { close(t.stopped) }) }

func TestFakeR2PublicationThroughFxApplication(t *testing.T) {
	config := testConfig(t)
	config.Publication.SealMaxBytes = 1
	config.Publication.ScanIntervalMS = 10
	config.Publication.RetryMinMS = 10
	config.Publication.RetryMaxMS = 100
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config.ListenAddress = listener.Addr().String()
	_ = listener.Close()

	transport := newAppFakeS3Transport()
	client := &http.Client{Transport: transport}
	provider := &staticProvider{accessKey: "app-g4-access", secret: "app-g4-secret-canary"}
	var catalog *publication.Catalog
	var remoteJournal *r2.PublicationJournal
	var backend *r2.CredentialBackend
	logger := &appFxEventLogger{}
	application := fxtest.New(t,
		testOptionsWithSDKRemoteBackend(config, provider, client),
		fx.WithLogger(func() fxevent.Logger { return logger }),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(30*time.Second),
		fx.Populate(&catalog, &remoteJournal, &backend),
	)
	application.RequireStart()
	t.Cleanup(func() {
		application.RequireStop()
		removeEventually(t, config.Publication.CatalogPath)
	})

	values := config.Gateway()
	hello := protocolv1.HelloV1{
		ProducerInstanceID: "fake-g4-producer",
		ProducerSessionID:  "fake-g4-session",
		ProducerBuildID:    values.ProducerBuildID,
		MQLCompilerBuild:   "fake",
		TerminalBuild:      "fake",
		OSContract:         "test",
		ClockAPIID:         "test-clock", ProviderID: values.ProviderID,
		StableFeedID:            values.StableFeedID,
		BrokerServerFingerprint: values.BrokerServerFingerprint,
		ExactSourceSymbol:       values.ExactSourceSymbol,
		SourceSchemaID:          protocolv1.SourceSchemaMT5,
		AcquisitionMode:         1,
		InitialFromMSC:          values.InitialFromMSC,
	}
	producer, err := fake.Dial(context.Background(), config.ListenAddress, hello)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	day := time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC)
	ack, err := producer.SendBatch(protocolv1.BatchFrameV1{
		ProducerSessionID:     hello.ProducerSessionID,
		BatchSequence:         1,
		RequestedFromMSC:      day.UnixMilli(),
		RequestedCount:        1,
		FetchWallStartS:       day.Unix(),
		FetchWallEndS:         day.Unix() + 1,
		FetchMonotonicStartUS: 1,
		FetchMonotonicEndUS:   2,
		ReturnedCount:         1,
		SourceSchemaID:        protocolv1.SourceSchemaMT5,
		Records: []protocolv1.RawMqlTickV1{{
			Time:            day.Unix(),
			TimeMSC:         day.Add(time.Second).UnixMilli(),
			CaptureSequence: 1,
			BidBits:         100,
			AskBits:         200,
		}},
	})
	if err != nil || ack.Status != protocolv1.AckAcceptedAdvanced {
		t.Fatalf("fake ingest ack = %+v err=%v", ack, err)
	}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		record, found, err := catalog.LatestManifest(context.Background(), "2024-03-09")
		if err != nil {
			t.Fatal(err)
		}
		if found && record.State == publication.ManifestStatePublished {
			manifestBytes, readErr := os.ReadFile(record.Path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			manifest, verifyErr := archive.VerifyRawDayManifest(manifestBytes)
			if verifyErr != nil {
				t.Fatal(verifyErr)
			}
			layout, layoutErr := r2.NewLayout(config.R2.ImmutableRoot, toArchiveScope(config))
			if layoutErr != nil {
				t.Fatal(layoutErr)
			}
			manifestKey, keyErr := layout.ManifestKey(manifest)
			if keyErr != nil {
				t.Fatal(keyErr)
			}
			journalRecord, journalFound, journalErr := remoteJournal.Record(manifestKey)
			if journalErr != nil || !journalFound || journalRecord.Stage != r2.StageReceiptSaved {
				t.Fatalf("remote journal = %+v found=%v err=%v", journalRecord, journalFound, journalErr)
			}
			if bytes.Contains(journalRecord.IntentBytes, []byte(provider.secretValue())) {
				t.Fatal("credential secret appeared in remote journal intent")
			}
			states, stateErr := remoteJournal.ObjectStateRecords(manifestKey)
			if stateErr != nil || len(states) == 0 {
				t.Fatalf("remote object states = %+v err=%v", states, stateErr)
			}
			verifiedCount := 0
			for _, state := range states {
				if state.State == r2.ObjectStateRemoteVerified {
					verifiedCount++
					if state.RemoteVerifiedAt.IsZero() {
						t.Fatalf("remote_verified state has no durable timestamp: %+v", state)
					}
				}
			}
			if verifiedCount != len(manifest.ChainObjects) {
				t.Fatalf("remote_verified object states = %d, want %d: %+v", verifiedCount, len(manifest.ChainObjects), states)
			}
			remoteManifest, getErr := backend.Get(context.Background(), manifestKey)
			if getErr != nil || !bytes.Equal(remoteManifest, manifestBytes) {
				t.Fatalf("SDK-backed remote manifest mismatch: err=%v", getErr)
			}
			if transport.count() < 4 {
				t.Fatalf("fake R2 object count = %d, want claim, descriptor, raw object, manifest", transport.count())
			}
			if logger.contains(provider.secretValue()) || transport.contains(provider.secretValue()) {
				t.Fatal("credential secret appeared in Fx event or remote object bytes")
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("Fx publication did not reach local published state: record=%+v found=%v", record, found)
		case <-poll.C:
		}
	}
}

func TestFxApplicationDoesNotLeakCredentialSecretOnRemoteFailure(t *testing.T) {
	config := testConfig(t)
	config.Publication.SealMaxBytes = 1
	config.Publication.ScanIntervalMS = 10
	config.Publication.RetryMinMS = 10
	config.Publication.RetryMaxMS = 100
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config.ListenAddress = listener.Addr().String()
	_ = listener.Close()

	secret := "app-g4-error-secret-canary"
	transport := newAppFakeS3Transport()
	layout, err := r2.NewLayout(config.R2.ImmutableRoot, toArchiveScope(config))
	if err != nil {
		t.Fatal(err)
	}
	claimKey, err := layout.ClaimKey(config.PublisherEpoch)
	if err != nil {
		t.Fatal(err)
	}
	transport.seed(claimKey, []byte("different publisher"))
	transport.failPutsWithStatus(100, http.StatusPreconditionFailed, []byte("<Error><Code>PreconditionFailed</Code><Message>"+secret+"</Message></Error>"))
	provider := &staticProvider{accessKey: "app-g4-error-access", secret: secret}
	logger := &appFxEventLogger{}
	var uploader *publication.Uploader
	var remoteJournal *r2.PublicationJournal
	application := fxtest.New(t,
		testOptionsWithSDKRemoteBackend(config, provider, &http.Client{Transport: transport}),
		fx.WithLogger(func() fxevent.Logger { return logger }),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(30*time.Second),
		fx.Populate(&uploader, &remoteJournal),
	)
	application.RequireStart()
	t.Cleanup(func() {
		application.RequireStop()
		removeEventually(t, config.Publication.CatalogPath)
	})

	producer := sendAppFakeBatch(t, config, "fake-g4-error-producer", "fake-g4-error-session")
	defer producer.Close()
	select {
	case <-application.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Fx application did not stop after remote failure")
	}
	lastError := uploader.LastError()
	if lastError == nil || strings.Contains(lastError.Error(), secret) {
		t.Fatalf("remote failure error = %v, secret leaked or error absent", lastError)
	}
	if logger.contains(secret) || transport.contains(secret) {
		t.Fatal("credential secret appeared in Fx event or remote object bytes")
	}
	unfinished, err := remoteJournal.ListUnfinished(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, pending := range unfinished {
		if bytes.Contains(pending.Input.ManifestBytes, []byte(secret)) {
			t.Fatal("credential secret appeared in recovered journal manifest")
		}
	}
}

func sendAppFakeBatch(t *testing.T, config appconfig.Config, instanceID, sessionID string) *fake.Client {
	t.Helper()
	values := config.Gateway()
	hello := protocolv1.HelloV1{
		ProducerInstanceID: instanceID,
		ProducerSessionID:  sessionID,
		ProducerBuildID:    values.ProducerBuildID,
		MQLCompilerBuild:   "fake",
		TerminalBuild:      "fake",
		OSContract:         "test",
		ClockAPIID:         "test-clock", ProviderID: values.ProviderID,
		StableFeedID:            values.StableFeedID,
		BrokerServerFingerprint: values.BrokerServerFingerprint,
		ExactSourceSymbol:       values.ExactSourceSymbol,
		SourceSchemaID:          protocolv1.SourceSchemaMT5,
		AcquisitionMode:         1,
		InitialFromMSC:          values.InitialFromMSC,
	}
	producer, err := fake.Dial(context.Background(), config.ListenAddress, hello)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC)
	ack, err := producer.SendBatch(protocolv1.BatchFrameV1{
		ProducerSessionID:     hello.ProducerSessionID,
		BatchSequence:         1,
		RequestedFromMSC:      day.UnixMilli(),
		RequestedCount:        1,
		FetchWallStartS:       day.Unix(),
		FetchWallEndS:         day.Unix() + 1,
		FetchMonotonicStartUS: 1,
		FetchMonotonicEndUS:   2,
		ReturnedCount:         1,
		SourceSchemaID:        protocolv1.SourceSchemaMT5,
		Records: []protocolv1.RawMqlTickV1{{
			Time:            day.Unix(),
			TimeMSC:         day.Add(time.Second).UnixMilli(),
			CaptureSequence: 1,
			BidBits:         100,
			AskBits:         200,
		}},
	})
	if err != nil || ack.Status != protocolv1.AckAcceptedAdvanced {
		_ = producer.Close()
		t.Fatalf("fake ingest ack = %+v err=%v", ack, err)
	}
	return producer
}

func removeEventually(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("remove %s after application stop: %v", path, err)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func testOptionsWithSDKRemoteBackend(configValue appconfig.Config, provider credentials.Provider, httpClient *http.Client) fx.Option {
	return fx.Options(
		fx.Supply(configValue),
		CoreOptions(),
		fx.Provide(
			func(configValue appconfig.Config, provider credentials.Provider) (remoteBackendResult, error) {
				backend, err := r2.NewCredentialBackend(r2.S3BackendConfig{
					Bucket: configValue.R2.Bucket, Endpoint: configValue.R2.Endpoint, Region: configValue.R2.Region,
					HTTPClient: httpClient,
				}, provider)
				if err != nil {
					return remoteBackendResult{}, err
				}
				return remoteBackendResult{Backend: backend, Writer: backend}, nil
			},
			newPublicationJournal,
			newLayout,
		),
		fx.Supply(fx.Annotate(provider, fx.As(new(credentials.Provider)))),
	)
}

type appFxEventLogger struct {
	mu     sync.Mutex
	events []string
}

func (l *appFxEventLogger) LogEvent(event fxevent.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, fmt.Sprintf("%#v", event))
}

func (l *appFxEventLogger) contains(value string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, event := range l.events {
		if strings.Contains(event, value) {
			return true
		}
	}
	return false
}

type appFakeS3Transport struct {
	mu            sync.Mutex
	objects       map[string][]byte
	failPutCount  int
	failureStatus int
	failureBody   []byte
}

func newAppFakeS3Transport() *appFakeS3Transport {
	return &appFakeS3Transport{objects: make(map[string][]byte)}
}

func (s *appFakeS3Transport) failPuts(count int, body []byte) {
	s.failPutsWithStatus(count, http.StatusInternalServerError, body)
}

func (s *appFakeS3Transport) failPutsWithStatus(count, status int, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failPutCount = count
	s.failureStatus = status
	s.failureBody = append([]byte(nil), body...)
}

func (s *appFakeS3Transport) seed(key string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = append([]byte(nil), body...)
}

func (s *appFakeS3Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	if request.Method == http.MethodGet && request.URL.Query().Get("list-type") == "2" {
		return s.listResponse(request, request.URL.Query().Get("prefix")), nil
	}
	key, ok := appFakeS3ObjectKey(request.URL.Path)
	if !ok {
		return appFakeS3Error(request, http.StatusBadRequest, "InvalidURI"), nil
	}
	switch request.Method {
	case http.MethodPut:
		return s.putResponse(request, key), nil
	case http.MethodGet:
		return s.getResponse(request, key), nil
	default:
		return appFakeS3Error(request, http.StatusMethodNotAllowed, "MethodNotAllowed"), nil
	}
}

func appFakeS3ObjectKey(requestPath string) (string, bool) {
	trimmed := strings.TrimPrefix(requestPath, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", false
	}
	key, err := url.PathUnescape(parts[1])
	return key, err == nil && key != ""
}

func (s *appFakeS3Transport) putResponse(request *http.Request, key string) *http.Response {
	s.mu.Lock()
	if s.failPutCount > 0 {
		s.failPutCount--
		status := s.failureStatus
		body := append([]byte(nil), s.failureBody...)
		s.mu.Unlock()
		return appFakeS3ErrorWithBody(request, status, body)
	}
	s.mu.Unlock()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return appFakeS3Error(request, http.StatusBadRequest, "InvalidRequest")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.objects[key]; found && request.Header.Get("If-None-Match") == "*" {
		return appFakeS3Error(request, http.StatusPreconditionFailed, "PreconditionFailed")
	}
	s.objects[key] = append([]byte(nil), body...)
	return appFakeS3Response(request, http.StatusOK, nil, nil)
}

func (s *appFakeS3Transport) getResponse(request *http.Request, key string) *http.Response {
	s.mu.Lock()
	body, found := s.objects[key]
	body = append([]byte(nil), body...)
	s.mu.Unlock()
	if !found {
		return appFakeS3Error(request, http.StatusNotFound, "NoSuchKey")
	}
	headers := make(http.Header)
	headers.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	return appFakeS3Response(request, http.StatusOK, headers, body)
}

func (s *appFakeS3Transport) listResponse(request *http.Request, prefix string) *http.Response {
	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	items := make([]appFakeS3ListItem, 0, len(keys))
	for _, key := range keys {
		items = append(items, appFakeS3ListItem{Key: key, Size: len(s.objects[key])})
	}
	s.mu.Unlock()
	body, err := xml.Marshal(appFakeS3ListResult{Contents: items})
	if err != nil {
		return appFakeS3Error(request, http.StatusInternalServerError, "InternalError")
	}
	return appFakeS3Response(request, http.StatusOK, nil, append(body, '\n'))
}

func (s *appFakeS3Transport) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

func (s *appFakeS3Transport) contains(value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, body := range s.objects {
		if strings.Contains(key, value) || bytes.Contains(body, []byte(value)) {
			return true
		}
	}
	return false
}

type appFakeS3ListResult struct {
	XMLName     xml.Name            `xml:"ListBucketResult"`
	Contents    []appFakeS3ListItem `xml:"Contents"`
	IsTruncated bool                `xml:"IsTruncated"`
}

type appFakeS3ListItem struct {
	Key  string `xml:"Key"`
	Size int    `xml:"Size"`
}

func appFakeS3Response(request *http.Request, status int, headers http.Header, body []byte) *http.Response {
	if headers == nil {
		headers = make(http.Header)
	}
	if body != nil {
		headers.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	}
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     headers,
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    request,
	}
}

func appFakeS3Error(request *http.Request, status int, code string) *http.Response {
	body := []byte(fmt.Sprintf("<Error><Code>%s</Code><Message>fake R2 response</Message></Error>", code))
	return appFakeS3ErrorWithBody(request, status, body)
}

func appFakeS3ErrorWithBody(request *http.Request, status int, body []byte) *http.Response {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/xml")
	return appFakeS3Response(request, status, headers, body)
}

func TestFxApplicationShutsDownOnPublicationWorkerError(t *testing.T) {
	config := testConfig(t)
	config.Publication.SealMaxBytes = 1
	config.Publication.ScanIntervalMS = 10
	config.Publication.RetryMinMS = 10
	config.Publication.RetryMaxMS = 100
	backend := &failingAppBackend{appFakeBackend: newAppFakeBackend()}
	var store *wal.Store
	application := fxtest.New(t,
		TestOptionsWithRemoteBackend(config, &staticProvider{}, backend),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(30*time.Second),
		fx.Populate(&store),
	)
	application.RequireStart()
	t.Cleanup(func() { application.RequireStop() })

	frame, err := protocol.EncodeMessage(protocol.BatchFrameV1{
		RequestedFromMSC: time.Date(2024, 3, 9, 0, 0, 0, 0, time.UTC).UnixMilli(),
		SourceSchemaID:   protocol.SourceSchemaMT5,
		ReturnedCount:    1,
		Records: []protocol.RawMqlTickV1{{
			TimeMSC:         time.Date(2024, 3, 9, 0, 0, 1, 0, time.UTC).UnixMilli(),
			CaptureSequence: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(frame, 1710000000, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-application.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Fx application did not shut down after publication worker error")
	}
	application.RequireStop()
}

type appFakeBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newAppFakeBackend() *appFakeBackend {
	return &appFakeBackend{objects: make(map[string][]byte)}
}

func (b *appFakeBackend) PutIfAbsent(_ context.Context, key string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, found := b.objects[key]; found {
		return r2.ErrObjectExists
	}
	b.objects[key] = append([]byte(nil), body...)
	return nil
}

func (b *appFakeBackend) Get(_ context.Context, key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	body, found := b.objects[key]
	if !found {
		return nil, r2.ErrObjectNotFound
	}
	return append([]byte(nil), body...), nil
}

func (b *appFakeBackend) Open(_ context.Context, key string) (io.ReadCloser, int64, error) {
	body, err := b.Get(context.Background(), key)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func (b *appFakeBackend) List(_ context.Context, prefix string) ([]r2.RemoteObject, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, 0)
	for key := range b.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	objects := make([]r2.RemoteObject, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, r2.RemoteObject{Key: key, Size: int64(len(b.objects[key]))})
	}
	return objects, nil
}

func (b *appFakeBackend) PutFileIfAbsent(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (r2.RemoteObjectCommit, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return r2.RemoteObjectCommit{}, err
	}
	if uint64(len(body)) != expectedBytes || sha256.Sum256(body) != expectedSHA256 {
		return r2.RemoteObjectCommit{}, r2.ErrLocalObjectChanged
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, found := b.objects[key]; found {
		if !bytes.Equal(existing, body) {
			return r2.RemoteObjectCommit{}, r2.ErrImmutableCollision
		}
		return r2.RemoteObjectCommit{ETag: fmt.Sprintf("etag-%x", expectedSHA256[:4])}, nil
	}
	b.objects[key] = append([]byte(nil), body...)
	return r2.RemoteObjectCommit{ETag: fmt.Sprintf("etag-%x", expectedSHA256[:4])}, nil
}

func (b *appFakeBackend) VerifyFile(_ context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (r2.RemoteObjectVerification, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return r2.RemoteObjectVerification{}, err
	}
	if uint64(len(body)) != expectedBytes || sha256.Sum256(body) != expectedSHA256 {
		return r2.RemoteObjectVerification{}, r2.ErrLocalObjectChanged
	}
	remote, err := b.Get(context.Background(), key)
	if err != nil {
		return r2.RemoteObjectVerification{}, err
	}
	if !bytes.Equal(remote, body) {
		return r2.RemoteObjectVerification{}, r2.ErrRemoteCheckMismatch
	}
	return r2.RemoteObjectVerification{ETag: fmt.Sprintf("etag-%x", expectedSHA256[:4])}, nil
}

func (b *appFakeBackend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.objects)
}

var _ r2.WriteBackend = (*appFakeBackend)(nil)

type failingAppBackend struct {
	*appFakeBackend
}

func (*failingAppBackend) PutFileIfAbsent(context.Context, string, string, [32]byte, uint64) (r2.RemoteObjectCommit, error) {
	return r2.RemoteObjectCommit{}, r2.ErrImmutableCollision
}

func (*failingAppBackend) VerifyFile(context.Context, string, string, [32]byte, uint64) (r2.RemoteObjectVerification, error) {
	return r2.RemoteObjectVerification{}, r2.ErrImmutableCollision
}

var _ r2.WriteBackend = (*failingAppBackend)(nil)
