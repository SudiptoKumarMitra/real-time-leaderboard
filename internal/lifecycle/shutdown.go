// Package lifecycle holds the process-lifecycle decision logic shared by
// the application's entry points: why the process is stopping (signal,
// HTTP server exit, consumer exit) and whether that reason is normal or
// fatal.
//
// It exists so the fatal-vs-normal shutdown distinction has exactly one
// tested implementation instead of being duplicated per binary.
package lifecycle

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"
)

// Reason states why the application is stopping.
type Reason int

const (
	// ReasonSignal: a termination signal (Ctrl+C / SIGTERM) was received —
	// the normal graceful-shutdown path.
	ReasonSignal Reason = iota
	// ReasonCancelled: the consumer context was already cancelled when the
	// consumer goroutine exited — also normal (shutdown in progress).
	ReasonCancelled
	// ReasonServerExit: the HTTP server stopped serving on its own.
	ReasonServerExit
	// ReasonConsumerExit: the consume loop exited while the application
	// context was still active — an unexpected, fatal condition.
	ReasonConsumerExit
)

// ErrConsumerExited marks the fatal case: the consume loop stopped
// without the application context having been cancelled.
var ErrConsumerExited = errors.New("leaderboard consumer exited unexpectedly")

// AwaitShutdown blocks until the first shutdown trigger fires and reports
// which one it was:
//
//   - sig: a termination signal (reasonSignal);
//   - serverErr: the HTTP server's serve loop returned (ReasonServerExit,
//     carrying the server error, if any);
//   - consumerWatch: the consumer goroutine exited (closed its done
//     channel). If ctx is still active this is the FATAL case
//     (ReasonConsumerExit): the consumer stopped on its own — e.g. after
//     exhausting its offset-commit retries — and the API must not keep
//     accepting scores nobody will consume. If ctx was already cancelled,
//     the exit is the expected result of shutdown (ReasonCancelled).
//
// consumerWatch must be a nil channel when no consumer runs, so that case
// can never fire (likewise serverErr for processes without an HTTP
// server).
func AwaitShutdown(ctx context.Context, sig <-chan os.Signal, serverErr <-chan error, consumerWatch <-chan struct{}) (Reason, error) {
	select {
	case <-sig:
		return ReasonSignal, nil
	case err := <-serverErr:
		return ReasonServerExit, err
	case <-consumerWatch:
		if ctx.Err() != nil {
			return ReasonCancelled, nil
		}
		return ReasonConsumerExit, ErrConsumerExited
	}
}

// ShutdownHTTP gracefully stops srv within the given timeout, so cleanup
// can never hang indefinitely on a stuck request. It always returns
// within the timeout: callers must continue with the remaining resource
// cleanup regardless of the returned error, then surface that error
// (e.g. via a non-zero exit code) only after cleanup has completed.
func ShutdownHTTP(srv *http.Server, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return srv.Shutdown(ctx)
}
