package r2

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func TestS3BackendPutFileIfAbsentUsesConditionalVerifiedSDKBoundary(t *testing.T) {
	server := newSDKFakeR2Server(t)
	backend := newSDKFakeR2Backend(server)
	body := []byte("conditional sdk boundary")
	localPath, digest := writeSDKFixture(t, body)

	commit, err := backend.PutFileIfAbsent(context.Background(), "objects/raw/a.zst", localPath, digest, uint64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	if commit.ETag == "" {
		t.Fatal("successful conditional upload did not return an ETag")
	}
	stored, err := backend.Get(context.Background(), "objects/raw/a.zst")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, body) {
		t.Fatalf("stored object = %q, want %q", stored, body)
	}
	request := server.lastPut("objects/raw/a.zst")
	if request.IfNoneMatch != "*" {
		t.Fatalf("If-None-Match = %q, want *", request.IfNoneMatch)
	}
	wantMD5 := md5.Sum(body)
	if request.ContentMD5 != base64.StdEncoding.EncodeToString(wantMD5[:]) {
		t.Fatalf("Content-MD5 = %q, want %q", request.ContentMD5, base64.StdEncoding.EncodeToString(wantMD5[:]))
	}
}

func TestS3BackendPutFileIfAbsentTreatsResponseLossAsIdempotentSuccess(t *testing.T) {
	server := newSDKFakeR2Server(t)
	backend := newSDKFakeR2Backend(server)
	body := []byte("response lost after durable PUT")
	localPath, digest := writeSDKFixture(t, body)
	server.loseNextResponse("objects/raw/lost.zst")

	if _, err := backend.PutFileIfAbsent(context.Background(), "objects/raw/lost.zst", localPath, digest, uint64(len(body))); err != nil {
		t.Fatalf("response-loss recovery: %v", err)
	}
	stored, err := backend.Get(context.Background(), "objects/raw/lost.zst")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, body) {
		t.Fatalf("recovered object = %q, want %q", stored, body)
	}
}

func TestS3BackendPutFileIfAbsentChecks412ContentBeforeSuccess(t *testing.T) {
	tests := []struct {
		name       string
		remoteBody []byte
		wantErr    error
	}{
		{name: "same bytes", remoteBody: []byte("already durable"), wantErr: nil},
		{name: "different bytes", remoteBody: []byte("different immutable bytes"), wantErr: ErrImmutableCollision},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newSDKFakeR2Server(t)
			backend := newSDKFakeR2Backend(server)
			localPath, digest := writeSDKFixture(t, []byte("already durable"))
			server.seed("objects/raw/existing.zst", tt.remoteBody)

			_, err := backend.PutFileIfAbsent(context.Background(), "objects/raw/existing.zst", localPath, digest, uint64(len([]byte("already durable"))))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			stored, getErr := backend.Get(context.Background(), "objects/raw/existing.zst")
			if getErr != nil || !bytes.Equal(stored, tt.remoteBody) {
				t.Fatalf("remote object after conditional result = %q err=%v, want original %q", stored, getErr, tt.remoteBody)
			}
		})
	}
}

type sdkFakeR2Server struct {
	server *httptest.Server

	mu            sync.Mutex
	objects       map[string][]byte
	puts          map[string]int
	lastRequests  map[string]sdkFakePutRequest
	loseResponses map[string]bool
}

type sdkFakePutRequest struct {
	IfNoneMatch string
	ContentMD5  string
}

func newSDKFakeR2Server(t *testing.T) *sdkFakeR2Server {
	t.Helper()
	fake := &sdkFakeR2Server{
		objects:       make(map[string][]byte),
		puts:          make(map[string]int),
		lastRequests:  make(map[string]sdkFakePutRequest),
		loseResponses: make(map[string]bool),
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func newSDKFakeR2Backend(server *sdkFakeR2Server) *S3Backend {
	awsConfig := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider("sdk-test-access", "sdk-test-secret", ""),
		HTTPClient:  server.server.Client(),
	}
	return newS3BackendFromConfig(awsConfig, S3BackendConfig{Bucket: "tick-raw", Endpoint: server.server.URL})
}

func writeSDKFixture(t *testing.T, body []byte) (string, [32]byte) {
	t.Helper()
	localPath := path.Join(t.TempDir(), "object.zst")
	if err := os.WriteFile(localPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return localPath, sha256.Sum256(body)
}

func (s *sdkFakeR2Server) seed(key string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = append([]byte(nil), body...)
}

func (s *sdkFakeR2Server) loseNextResponse(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loseResponses[key] = true
}

func (s *sdkFakeR2Server) lastPut(key string) sdkFakePutRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRequests[key]
}

func (s *sdkFakeR2Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		s.serveList(w, r.URL.Query().Get("prefix"))
		return
	}
	key, ok := sdkFakeObjectKey(r.URL.Path)
	if !ok {
		sdkFakeError(w, http.StatusBadRequest, "InvalidURI")
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.servePut(w, r, key)
	case http.MethodGet:
		s.serveGet(w, key)
	default:
		sdkFakeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func sdkFakeObjectKey(requestPath string) (string, bool) {
	trimmed := strings.TrimPrefix(requestPath, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", false
	}
	key, err := url.PathUnescape(parts[1])
	if err != nil || key == "" {
		return "", false
	}
	return key, true
}

func (s *sdkFakeR2Server) servePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		sdkFakeError(w, http.StatusBadRequest, "InvalidRequest")
		return
	}
	s.mu.Lock()
	s.puts[key]++
	s.lastRequests[key] = sdkFakePutRequest{IfNoneMatch: r.Header.Get("If-None-Match"), ContentMD5: r.Header.Get("Content-MD5")}
	_, exists := s.objects[key]
	if exists && r.Header.Get("If-None-Match") == "*" {
		s.mu.Unlock()
		sdkFakeError(w, http.StatusPreconditionFailed, "PreconditionFailed")
		return
	}
	s.objects[key] = append([]byte(nil), body...)
	loseResponse := s.loseResponses[key]
	delete(s.loseResponses, key)
	s.mu.Unlock()
	if loseResponse {
		if hijacker, ok := w.(http.Hijacker); ok {
			connection, _, hijackErr := hijacker.Hijack()
			if hijackErr == nil {
				_ = connection.Close()
				return
			}
		}
		return
	}
	w.Header().Set("ETag", sdkFakeETag(body))
	w.WriteHeader(http.StatusOK)
}

func (s *sdkFakeR2Server) serveGet(w http.ResponseWriter, key string) {
	s.mu.Lock()
	body, found := s.objects[key]
	body = append([]byte(nil), body...)
	s.mu.Unlock()
	if !found {
		sdkFakeError(w, http.StatusNotFound, "NoSuchKey")
		return
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.Header().Set("ETag", sdkFakeETag(body))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *sdkFakeR2Server) serveList(w http.ResponseWriter, prefix string) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	s.mu.Unlock()
	sort.Strings(keys)
	type content struct {
		Key  string `xml:"Key"`
		Size int    `xml:"Size"`
	}
	type result struct {
		XMLName     xml.Name  `xml:"ListBucketResult"`
		Contents    []content `xml:"Contents"`
		IsTruncated bool      `xml:"IsTruncated"`
	}
	items := make([]content, 0, len(keys))
	for _, key := range keys {
		s.mu.Lock()
		size := len(s.objects[key])
		s.mu.Unlock()
		items = append(items, content{Key: key, Size: size})
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result{Contents: items})
}

func sdkFakeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>fake R2 response</Message></Error>", code)
}

func sdkFakeETag(body []byte) string {
	digest := md5.Sum(body)
	return fmt.Sprintf("\"%x\"", digest[:])
}
