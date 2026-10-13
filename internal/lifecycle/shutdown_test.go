package lifecycle

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

// These tests exercise AwaitShutdown (the lifecycle decision logic) and
// ShutdownHTTP (bounded graceful HTTP shutdown) with plain channels and
// never-started/stuck servers. No Kafka broker, Redis server, PostgreSQL,
// or Docker is involved; the consumer, application stores, and real API
// wiring are never constructed.

func TestAwaitShutdown_SignalIsNormal(t *testing.T) {
	sig := make(chan os.Signal, 1)
	sig <- os.Interrupt // pre-filled: must win immediately

	reason, err := AwaitShutdown(context.Background(), sig, make(chan error), nil)
	if reason != ReasonSignal {
		t.Fatalf("reason = %v, want ReasonSignal", reason)
	}
	if err != nil {
		t.Fatalf("err = %v, want nil for signal shutdown", err)
	}
}

func TestAwaitShutdown_ServerError(t *testing.T) {
	wantErr := errors.New("address already in use")
	serverErr := make(chan error, 1)
	serverErr <- wantErr

	reason, err := AwaitShutdown(context.Background(), make(chan os.Signal), serverErr, nil)
	if reason != ReasonServerExit {
		t.Fatalf("reason = %v, want ReasonServerExit", reason)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

func TestAwaitShutdown_ConsumerExitWhileActiveIsFatal(t *testing.T) {
	// The core regression guard: the consume loop exited (done closed)
	// while the application context is STILL ACTIVE — e.g. offset-commit
	// retries exhausted. This must be reported as the fatal case so the
	// caller stops the HTTP API and exits non-zero.
	done := make(chan struct{})
	close(done)

	reason, err := AwaitShutdown(context.Background(), make(chan os.Signal), make(chan error), done)
	if reason != ReasonConsumerExit {
		t.Fatalf("reason = %v, want ReasonConsumerExit (ctx still active)", reason)
	}
	if !errors.Is(err, ErrConsumerExited) {
		t.Fatalf("err = %v, want ErrConsumerExited", err)
	}
}

func TestAwaitShutdown_ConsumerExitAfterCancelIsNormal(t *testing.T) {
	// Shutdown in progress: the context was cancelled first, so the
	// consumer exiting afterwards is the EXPECTED cleanup step, not a
	// fatal failure (requirement: normal shutdown must not be mistaken
	// for a crash).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	close(done)

	reason, err := AwaitShutdown(ctx, make(chan os.Signal), make(chan error), done)
	if reason != ReasonCancelled {
		t.Fatalf("reason = %v, want ReasonCancelled (ctx already cancelled)", reason)
	}
	if err != nil {
		t.Fatalf("err = %v, want nil for cancelled-context consumer exit", err)
	}
}

func TestAwaitShutdown_NilConsumerWatchNeverFires(t *testing.T) {
	// With the consumer disabled, main passes a nil consumerWatch. A nil
	// channel blocks forever in select, so a later signal must still be
	// the winner — a pre-closed done channel in that position would
	// otherwise kill the disabled-consumer app immediately.
	sig := make(chan os.Signal, 1)
	sig <- os.Interrupt

	reason, err := AwaitShutdown(context.Background(), sig, make(chan error), nil)
	if reason != ReasonSignal {
		t.Fatalf("reason = %v, want ReasonSignal even with nil consumer watch", reason)
	}
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestAwaitShutdown_ServerClosedErrorPassesThrough(t *testing.T) {
	// http.ErrServerClosed is how ListenAndServe reports "closed by
	// Shutdown"; the helper must pass it through untouched so the caller
	// can distinguish it from a real server failure.
	serverErr := make(chan error, 1)
	serverErr <- http.ErrServerClosed

	reason, err := AwaitShutdown(context.Background(), make(chan os.Signal), serverErr, nil)
	if reason != ReasonServerExit {
		t.Fatalf("reason = %v, want ReasonServerExit", reason)
	}
	if !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("err = %v, want http.ErrServerClosed passthrough", err)
	}
}

func TestShutdownHTTP_TimesOutOnStuckConnection(t *testing.T) {
	// Requirement: HTTP shutdown must be bounded so a stuck connection
	// can never hang cleanup — it returns ctx.DeadlineExceeded after the
	// timeout, which the caller reports after running the remaining
	// resource cleanup (never instead of it).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	// A connection that stays in StateNew (its header never read to
	// completion... it sends nothing at all) blocks Shutdown: Go only
	// treats StateNew connections as idle after 5 seconds.
	// The ConnState callback makes the registration deterministic:
	// without it, Shutdown can win the race against Serve's Accept loop
	// and find an empty activeConn set, returning nil instead of timing
	// out.
	stateNew := make(chan struct{}, 1)
	srv := &http.Server{
		Handler: http.NewServeMux(),
		ConnState: func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				select {
				case stateNew <- struct{}{}:
				default:
				}
			}
		},
	}
	go srv.Serve(ln)
	defer srv.Close()
	// Dial and never send a byte, so the connection never becomes idle.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial: %v", err)
	}
	defer conn.Close()
	select {
	case <-stateNew: // server accepted and registered the connection
	case <-time.After(2 * time.Second):
		t.Fatal("server never reached StateNew for the dialled connection")
	}

	err = ShutdownHTTP(srv, 150*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded from the stuck connection", err)
	}
}

func TestShutdownHTTP_NeverStartedServerReturnsNil(t *testing.T) {
	// A server that was never started shuts down immediately: the API's
	// normal cleanup path must get a nil error, not a failure.
	srv := &http.Server{}
	if err := ShutdownHTTP(srv, time.Second); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}
