package ingest_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestGatewayStartStopIsIdempotentAndBounded(t *testing.T) {
	runtime := newStartedGatewayRuntime(t, testConfig(t))
	gateway := runtime.Gateway()
	defer runtime.Stop(context.Background())

	if err := gateway.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Start(context.Background()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("second Start error = %v, want net.ErrClosed", err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gateway.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop error = %v", err)
	}
}

func TestGatewayLifecycleDoesNotLeakGoroutines(t *testing.T) {
	runtime := newStartedGatewayRuntime(t, testConfig(t))
	gateway := runtime.Gateway()
	if err := gateway.Start(context.Background()); err != nil {
		_ = runtime.Stop(context.Background())
		t.Fatal(err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gateway.Stop(stopCtx); err != nil {
		_ = runtime.Stop(context.Background())
		t.Fatal(err)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	goleak.VerifyNone(t)
}

func TestGatewayServeReportsUnexpectedListenerFailure(t *testing.T) {
	runtime := newStartedGatewayRuntime(t, testConfig(t))
	gateway := runtime.Gateway()
	defer runtime.Stop(context.Background())

	want := errors.New("listener fixture failed")
	listener := &failingListener{err: want}
	serveDone := make(chan error, 1)
	go func() { serveDone <- gateway.Serve(context.Background(), listener) }()
	select {
	case err := <-serveDone:
		if !errors.Is(err, want) {
			t.Fatalf("Serve error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not report listener failure")
	}
	select {
	case err := <-gateway.Errors():
		if !errors.Is(err, want) {
			t.Fatalf("Gateway.Errors = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("listener failure was not published to Gateway.Errors")
	}
}

func TestGatewayStopAfterAcceptClosesHandler(t *testing.T) {
	runtime := newStartedGatewayRuntime(t, testConfig(t))
	gateway := runtime.Gateway()
	defer runtime.Stop(context.Background())
	peer, raw := net.Pipe()
	defer peer.Close()
	connection := &trackingConn{Conn: raw, closed: make(chan struct{})}
	accepted := make(chan struct{})
	listener := &closeOnAcceptListener{
		connection: connection,
		closed:     make(chan struct{}),
		onAccept:   func() { close(accepted) },
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- gateway.Serve(context.Background(), listener) }()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not accept fixture connection")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gateway.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connection.closed:
	case <-time.After(time.Second):
		t.Fatal("Stop did not close accepted connection")
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after handler close")
	}
}

func TestGatewayStopHonorsTimeoutForStuckHandler(t *testing.T) {
	runtime := newStartedGatewayRuntime(t, testConfig(t))
	gateway := runtime.Gateway()
	defer runtime.Stop(context.Background())
	connection := newBlockingConn()
	accepted := make(chan struct{})
	listener := &closeOnAcceptListener{
		connection: connection,
		closed:     make(chan struct{}),
		onAccept:   func() { close(accepted) },
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- gateway.Serve(context.Background(), listener) }()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not accept stuck connection")
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := gateway.Stop(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("short Stop error = %v, want deadline exceeded", err)
	}
	connection.release()
	longCtx, longCancel := context.WithTimeout(context.Background(), time.Second)
	defer longCancel()
	if err := gateway.Stop(longCtx); err != nil {
		t.Fatalf("Stop after handler release = %v", err)
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after stuck handler release")
	}
}

type failingListener struct {
	err error
}

func (*failingListener) Close() error { return nil }

func (*failingListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

func (l *failingListener) Accept() (net.Conn, error) { return nil, l.err }

type blockingConn struct {
	released chan struct{}
	once     sync.Once
}

func newBlockingConn() *blockingConn {
	return &blockingConn{released: make(chan struct{})}
}

func (c *blockingConn) Read([]byte) (int, error) {
	<-c.released
	return 0, io.EOF
}

func (*blockingConn) Write([]byte) (int, error) { return 0, net.ErrClosed }

func (c *blockingConn) Close() error {
	// Deliberately does not release Read. This lets the test prove that Stop
	// returns its context deadline rather than waiting forever for a handler.
	return nil
}

func (*blockingConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*blockingConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*blockingConn) SetDeadline(time.Time) error      { return nil }
func (*blockingConn) SetReadDeadline(time.Time) error  { return nil }
func (*blockingConn) SetWriteDeadline(time.Time) error { return nil }

func (c *blockingConn) release() {
	c.once.Do(func() { close(c.released) })
}
