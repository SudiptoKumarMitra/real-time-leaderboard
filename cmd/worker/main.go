// Command worker is the leaderboard projection worker: an independent
// process that consumes score.submitted events from Kafka and updates the
// Redis leaderboard (ZADD ... GT).
//
// It never starts an HTTP server and never touches PostgreSQL or the
// Kafka producer — the event flow (PG insert + publish in cmd/api) is its
// only input. Consumer group, event schema, retry/malformed/commit-safety
// behavior all live in internal/consumer and are unchanged by this entry
// point. There is intentionally no restart loop: after an unexpected
// consumer exit the process ends with a non-zero code and a supervisor
// (or an operator) restarts it.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"real_time_leaderboard/db"
	"real_time_leaderboard/internal/consumer"
	"real_time_leaderboard/internal/lifecycle"
	"real_time_leaderboard/internal/repository"
)

// run initializes Redis and the score consumer, runs the consume loop
// until a shutdown trigger fires, and always performs cleanup — cancel the
// consumer context, wait for the goroutine (final commit attempt + group
// leave), then close Redis — before returning the process exit code.
// main() exits with that code so an unexpected consumer exit surfaces as
// a non-zero status for the process supervisor. No log.Fatal is used
// after initialization: errors propagate as exit codes so cleanup always
// completes first.
func run() int {
	// Fail fast if Redis is unreachable (db.InitRedis pings with a
	// bounded timeout): a worker that cannot update the projection would
	// only consume and retry — better to exit before joining the group.
	redisClient := db.InitRedis()
	lbRepo := repository.NewLeaderboardRepository(redisClient)

	// Constructed ONCE at startup: kafka-go's group reader joins the
	// leaderboard-writer consumer group at construction time, so this
	// happens only where the consume loop will actually run.
	scoreConsumer := consumer.NewScoreConsumer(consumer.DefaultReaderConfig(), lbRepo)

	// Shutdown is driven by context cancellation: cancel() unblocks
	// FetchMessage, Run returns and closes the reader (final commit
	// attempt + leaving the group).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		scoreConsumer.Run(ctx)
	}()

	log.Println("leaderboard worker running (process=cmd/worker, group=leaderboard-writer, topic=score.submitted)")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// serverErr is a nil channel: this process has no HTTP server, so the
	// ReasonServerExit arm can never fire. The consumer exit observed
	// here is fatal only while ctx is still active — a consumer exit
	// after our own cancel() would be reported as ReasonCancelled (but
	// this call has already returned by then).
	reason, cause := lifecycle.AwaitShutdown(ctx, sigCh, nil, consumerDone)

	exitCode := 0
	switch reason {
	case lifecycle.ReasonSignal:
		log.Println("shutdown: termination signal received")
	case lifecycle.ReasonCancelled:
		log.Println("shutdown: consumer stopped after context cancellation")
	case lifecycle.ReasonConsumerExit:
		// Unexpected: the consume loop exited while ctx is still active
		// (e.g. offset-commit retries exhausted — the consumer stops
		// rather than let a later offset implicitly commit an unresolved
		// event). Cleanup below still runs; the non-zero exit code tells
		// the supervisor to restart, which resumes from the last
		// committed offset. Never log.Fatal here: cleanup must run first.
		log.Printf("fatal: leaderboard consumer exited unexpectedly while the worker context is still active "+
			"(%v); no scores will be applied to the leaderboard until the process is restarted; "+
			"process supervisor should restart the worker", cause)
		exitCode = 1
	case lifecycle.ReasonServerExit:
		// Unreachable with a nil serverErr — defensive only, so an
		// unexpected reason can never be silently treated as success.
		log.Printf("unexpected shutdown reason %v (this process runs no HTTP server): %v", reason, cause)
		exitCode = 1
	}

	// Cleanup for BOTH normal and fatal paths, before the exit code is
	// returned: cancel unblocks FetchMessage, waiting lets Run close the
	// reader (group leave — can take ~20s) and finish any in-flight
	// event, and only then is the Redis client closed. On the fatal path
	// the goroutine has already exited, so both steps return immediately.
	cancel()
	<-consumerDone
	db.CloseRedis(redisClient)
	log.Println("shutdown complete")
	return exitCode
}

func main() {
	os.Exit(run())
}
