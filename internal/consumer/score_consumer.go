// Package consumer owns inbound Kafka event consumption.
//
// It mirrors the other per-store components (one component, one client,
// constructor-injected, no globals): ScoreRepository -> PostgreSQL,
// LeaderboardRepository -> Redis, EventPublisher -> Kafka out,
// ScoreConsumer -> Kafka in. A single kafka.Reader is created once at
// application startup — never per message, never per request.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"real_time_leaderboard/internal/config"
	"real_time_leaderboard/internal/publisher"
	"real_time_leaderboard/internal/repository"
)

const (
	// consumerGroupID mirrors the producer's contract (topic from
	// config.ScoreSubmittedTopic, created by infrastructure, key = user_id,
	// value = JSON event).
	consumerGroupID = "leaderboard-writer"

	retryInitialBackoff = 500 * time.Millisecond
	retryMaxBackoff     = 30 * time.Second
	commitMaxAttempts   = 5
)

// errPermanent marks events that no amount of retrying can fix (malformed
// JSON or invalid fields). They are logged loudly and then committed
// (skipped) so a single bad event cannot block its partition forever —
// the smallest safe policy: PostgreSQL stays the source of truth and the
// projection can be rebuilt from it at any time.
var errPermanent = errors.New("permanent event error")

// messageReader is the consume loop's view of the Kafka reader: fetch one
// message, commit one message, close. *kafka.Reader satisfies it in
// production; unit tests substitute an in-memory fake so the loop's
// process-then-commit ordering can be verified without a broker.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// bestScoreUpdater is the consume loop's view of the leaderboard
// projection: the single ZADD ... GT operation, named after the operation
// rather than the store. *repository.LeaderboardRepository satisfies it in
// production; unit tests substitute a fake so no Redis server is needed.
type bestScoreUpdater interface {
	UpdateBestScore(userID int, score int) error
}

// ScoreConsumer consumes score.submitted events and updates the Redis
// leaderboard projection with ZADD ... GT.
//
// Both dependencies are the narrow interfaces above: production wiring is
// unchanged (the constructor still builds one *kafka.Reader and accepts
// *repository.LeaderboardRepository), while tests can drive the loop with
// fakes.
type ScoreConsumer struct {
	reader messageReader
	lbRepo bestScoreUpdater
}

// NewScoreConsumer creates the application's single Kafka consumer.
//
// readerConfig comes from DefaultReaderConfig(): consumer-group mode
// (GroupID leaderboard-writer, no manual partition), StartOffset
// FirstOffset for the group's very first run. CommitInterval stays 0 —
// the kafka-go default — so commits are synchronous: CommitMessages
// returns only after the group coordinator acknowledged the offset
// (process-then-commit, at-least-once).
//
// NOTE: constructing a group reader joins the consumer group immediately
// (kafka-go spawns its generation loop in NewReader), so this must be
// called exactly once at startup and only when the consume loop will
// actually run.
func NewScoreConsumer(readerConfig kafka.ReaderConfig, lbRepo *repository.LeaderboardRepository) *ScoreConsumer {
	return &ScoreConsumer{
		reader: kafka.NewReader(readerConfig),
		lbRepo: lbRepo,
	}
}

// DefaultReaderConfig returns the shared ReaderConfig for the leaderboard
// consumer: Brokers from KAFKA_BROKERS (default localhost:9092), topic
// score.submitted, consumer group leaderboard-writer, earliest start for a
// group without committed offsets. Partitions are never assigned manually —
// the group protocol owns assignment (topic has 2 partitions, key = user_id).
func DefaultReaderConfig() kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:     config.Brokers(),
		Topic:       config.ScoreSubmittedTopic,
		GroupID:     consumerGroupID,
		StartOffset: kafka.FirstOffset,
	}
}

// Run processes events until ctx is cancelled, strictly in the order
// FetchMessage -> process -> CommitMessages. ReadMessage is never used:
// it commits BEFORE processing, which would turn a crash into a
// permanently missed leaderboard update.
//
// Failure policy:
//   - transient (e.g., Redis down): the event is retried in place with
//     capped exponential backoff and is NEVER committed while failing, so
//     it stays eligible for redelivery (at-least-once, self-healing).
//   - permanent (malformed/invalid): logged with the raw payload and
//     committed, so the partition keeps moving.
//   - unrecoverable offset commit: after bounded commit retries the
//     consumer STOPS instead of continuing (see commit). Kafka commits
//     are per-partition monotonic — a later commit would implicitly
//     commit this unresolved event and lose it after a restart.
//
// Shutdown: ctx cancellation unblocks FetchMessage; the deferred reader
// Close performs a final commit attempt and leaves the group. Any message
// fetched but not committed is redelivered after restart.
func (c *ScoreConsumer) Run(ctx context.Context) {
	log.Printf("leaderboard consumer started (group=%s topic=%s)", consumerGroupID, config.ScoreSubmittedTopic)
	defer func() {
		c.reader.Close()
		log.Println("leaderboard consumer stopped")
	}()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return // clean shutdown — reader closed
			}
			log.Printf("warning: consumer fetch failed, backing off %s: %v", retryInitialBackoff, err)
			if !sleep(ctx, retryInitialBackoff) {
				return
			}
			continue
		}

		if !c.processUntilDone(ctx, msg) {
			return // context cancelled while pending — no commit, redelivery
		}
		if !c.commit(ctx, msg) {
			return // shutdown or unrecoverable commit failure — stop; no
			// later offset in this partition is committed, so the event
			// stays eligible for redelivery (at-least-once).
		}
	}
}

// processUntilDone applies the event to Redis, retrying transient failures
// with capped exponential backoff until success or shutdown. Returns false
// only when ctx is cancelled — the message is then left uncommitted and
// stays eligible for redelivery. Permanent errors are logged and return
// true so the caller commits (skips) them, keeping the partition moving.
func (c *ScoreConsumer) processUntilDone(ctx context.Context, msg kafka.Message) bool {
	backoff := retryInitialBackoff
	for {
		err := c.process(msg)
		if err == nil {
			return true
		}
		if errors.Is(err, errPermanent) {
			log.Printf("error: skipping malformed score.submitted event (partition=%d offset=%d): raw=%s: %v",
				msg.Partition, msg.Offset, msg.Value, err)
			return true
		}
		log.Printf("warning: leaderboard update failed (partition=%d offset=%d), not committing; retrying in %s: %v",
			msg.Partition, msg.Offset, backoff, err)
		if !sleep(ctx, backoff) {
			return false
		}
		if backoff *= 2; backoff > retryMaxBackoff {
			backoff = retryMaxBackoff
		}
	}
}

// process decodes one score.submitted event and applies it to the Redis
// projection via LeaderboardRepository.UpdateBestScore (ZADD ... GT).
//
// The wire type is the producer's own publisher.ScoreSubmittedEvent — one
// shared definition so producer and consumer cannot drift. Fields needed
// for the leaderboard (user_id, score) are validated; score_id/created_at
// are carried for future dedup needs but not required here — ZADD GT is
// convergent, which is what makes redelivered (duplicate) events safe.
func (c *ScoreConsumer) process(msg kafka.Message) error {
	var event publisher.ScoreSubmittedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("%w: invalid json: %v", errPermanent, err)
	}
	if event.UserID <= 0 || event.Score < 0 {
		return fmt.Errorf("%w: invalid event fields (user_id=%d score=%d)", errPermanent, event.UserID, event.Score)
	}
	if err := c.lbRepo.UpdateBestScore(event.UserID, event.Score); err != nil {
		return err // transient — caller retries without committing
	}
	log.Printf("leaderboard consumer: applied user=%d score=%d (partition=%d offset=%d)",
		event.UserID, event.Score, msg.Partition, msg.Offset)
	return nil
}

// commit durably records the processed offset (stores msg.Offset+1 via the
// group coordinator). Sync commits are retried briefly with the standard
// backoff; if the broker never acknowledges within commitMaxAttempts the
// consumer STOPS (returns false) rather than moving on.
//
// Why stop: Kafka offset commits are per-partition monotonic — committing
// offset N+1 implicitly commits every offset below it in that partition.
// Continuing to the next message and successfully committing ITS offset
// would silently mark this unresolved event committed too, and a restart
// would skip it forever (message loss). Stopping leaves the event
// uncommitted: on restart the group resumes from the last durably
// committed offset and redelivers it, preserving at-least-once delivery.
//
// Operational consequence: an unrecoverable commit failure (coordinator
// unreachable for the whole retry budget) is not self-healing — the
// consume loop exits and the process must be restarted to resume. The
// failure is logged loudly; it is never reported as a successful commit.
//
// Returns false on ctx cancellation OR unrecoverable commit failure —
// both leave the message uncommitted and the loop stopped.
func (c *ScoreConsumer) commit(ctx context.Context, msg kafka.Message) bool {
	for attempt := 1; attempt <= commitMaxAttempts; attempt++ {
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return false
			}
			log.Printf("warning: offset commit failed (partition=%d offset=%d) attempt %d/%d: %v",
				msg.Partition, msg.Offset, attempt, commitMaxAttempts, err)
			if !sleep(ctx, retryInitialBackoff) {
				return false
			}
			continue
		}
		return true
	}
	log.Printf("error: offset commit not acknowledged after %d attempts (partition=%d offset=%d); "+
		"stopping consumer to avoid committing a later offset that would implicitly commit this one "+
		"(Kafka commits are per-partition monotonic); restart to redeliver from the last committed offset",
		commitMaxAttempts, msg.Partition, msg.Offset)
	return false
}

// sleep waits for d and returns false if ctx was cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
