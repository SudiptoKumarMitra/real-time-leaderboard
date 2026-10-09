package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

// The tests below exercise awaitShutdown — the lifecycle decision logic —
// with plain channels. No Kafka broker, Redis server, PostgreSQL, or
// Docker is involved; the consumer, HTTP server, and stores are never
// constructed.

func TestCleanupRuntime_StopsHTTPBeforeConsumer(t *testing.T) {
	// Ordering regression guard for the "don't accept scores while the
	// consumer group is shutting down" fix: HTTP Shutdown must begin (and
	// be in progress) BEFORE the consumer is told to stop. RegisterOnShutdown
	// callbacks fire during Shutdown, so stopConsumer waiting on that signal
	// can only succeed if HTTP shutdown came first — a reversed order would
	// block forever and the timeout below would fail the test.
	srv := &http.Server{} // never started; Shutdown closes immediately
	httpShuttingDown := make(chan struct{})
	srv.RegisterOnShutdown(func() { close(httpShuttingDown) })

	var order []string
	err := cleanupRuntime(srv, func() {
		select {
		case <-httpShuttingDown:
			order = append(order, "consumer")
		case <-time.After(2 * time.Second):
			order = append(order, "consumer-after-timeout") // would mark wrong order
		}
	}, time.Second)
	if err != nil {
		t.Fatalf("cleanupRuntime err = %v, want nil for a never-started server", err)
	}
	if len(order) != 1 || order[0] != "consumer" {
		t.Fatalf("consumer stop ran without HTTP shutdown in progress first, order = %v", order)
	}
}

func TestCleanupRuntime_ConsumerStopsEvenWhenHTTPShutdownTimesOut(t *testing.T) {
	// Requirement: a shutdown timeout must not skip the consumer stop (or
	// the store closes that follow it). A server with a connection stuck
	// mid-request makes Shutdown return ctx.DeadlineExceeded, and
	// stopConsumer must still run.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := &http.Server{Handler: http.NewServeMux()}
	go srv.Serve(ln)
	defer srv.Close()
	// Dial and never complete a request, so the connection never becomes
	// idle and Shutdown cannot finish within the short budget below.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial: %v", err)
	}
	defer conn.Close()

	stopped := make(chan struct{}, 1)
	err = cleanupRuntime(srv, func() { stopped <- struct{}{} }, 150*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded from the stuck connection", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stopConsumer was not invoked after HTTP shutdown timed out")
	}
}

func TestAwaitShutdown_SignalIsNormal(t *testing.T) {
	sig := make(chan os.Signal, 1)
	sig <- os.Interrupt // pre-filled: must win immediately

	reason, err := awaitShutdown(context.Background(), sig, make(chan error), nil)
	if reason != reasonSignal {
		t.Fatalf("reason = %v, want reasonSignal", reason)
	}
	if err != nil {
		t.Fatalf("err = %v, want nil for signal shutdown", err)
	}
}

func TestAwaitShutdown_ServerError(t *testing.T) {
	wantErr := errors.New("address already in use")
	serverErr := make(chan error, 1)
	serverErr <- wantErr

	reason, err := awaitShutdown(context.Background(), make(chan os.Signal), serverErr, nil)
	if reason != reasonServerExit {
		t.Fatalf("reason = %v, want reasonServerExit", reason)
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

	reason, err := awaitShutdown(context.Background(), make(chan os.Signal), make(chan error), done)
	if reason != reasonConsumerExit {
		t.Fatalf("reason = %v, want reasonConsumerExit (ctx still active)", reason)
	}
	if !errors.Is(err, errConsumerExited) {
		t.Fatalf("err = %v, want errConsumerExited", err)
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

	reason, err := awaitShutdown(ctx, make(chan os.Signal), make(chan error), done)
	if reason != reasonCancelled {
		t.Fatalf("reason = %v, want reasonCancelled (ctx already cancelled)", reason)
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

	reason, err := awaitShutdown(context.Background(), sig, make(chan error), nil)
	if reason != reasonSignal {
		t.Fatalf("reason = %v, want reasonSignal even with nil consumer watch", reason)
	}
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

func TestAwaitShutdown_ServerClosedErrorPassesThrough(t *testing.T) {
	// http.ErrServerClosed is how ListenAndServe reports "closed by
	// Shutdown"; the helper must pass it through untouched so run() can
	// distinguish it from a real server failure.
	serverErr := make(chan error, 1)
	serverErr <- http.ErrServerClosed

	reason, err := awaitShutdown(context.Background(), make(chan os.Signal), serverErr, nil)
	if reason != reasonServerExit {
		t.Fatalf("reason = %v, want reasonServerExit", reason)
	}
	if !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("err = %v, want http.ErrServerClosed passthrough", err)
	}
}
