// Package publisher owns outbound event publishing to Kafka.
//
// It follows the one-component-per-external-store rule used everywhere in
// this project: ScoreRepository -> PostgreSQL, LeaderboardRepository ->
// Redis, EventPublisher -> Kafka. A single kafka.Writer is created once at
// application startup and injected into ScoreService — never per request,
// never as a package-level global.
package publisher

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// scoreSubmittedTopic is the topic that score submission events are
// published to. The topic already exists (2 partitions) — this component
// only produces to it, it never creates it.
const scoreSubmittedTopic = "score.submitted"

// ScoreSubmittedEvent is the payload published after a score row has been
// committed to PostgreSQL.
//
// ScoreID and CreatedAt come from the INSERT ... RETURNING result — the
// PostgreSQL row identity and the row's own timestamp. They are never
// synthesized with time.Now() in the service layer.
type ScoreSubmittedEvent struct {
	Event     string `json:"event"`
	UserID    int    `json:"user_id"`
	Score     int    `json:"score"`
	ScoreID   int    `json:"score_id"`
	CreatedAt string `json:"created_at"`
}

// EventPublisher publishes domain events to Kafka through one shared writer.
type EventPublisher struct {
	writer *kafka.Writer
}

// NewEventPublisher creates the application's single Kafka publisher.
//
// Brokers come from KAFKA_BROKERS (comma-separated, default localhost:9092).
// A bounded metadata request verifies broker and topic reachability at
// startup, matching the fail-fast behavior of db.InitDB and db.InitRedis:
// a wrong address kills the process before it accepts traffic instead of
// silently degrading every submission.
//
// Producer configuration:
//   - RequiredAcks RequireAll: leader + all in-sync replicas must durably
//     append the record before WriteMessages returns nil
//   - WriteTimeout 5s: a dead broker cannot hang a request path indefinitely
//   - MaxAttempts 3: bounded retries inside WriteMessages (no infinite loop)
//   - Balancer Hash: partition = hash(key) % 2, so every event for one user
//     reaches the same partition and stays ordered
func NewEventPublisher() *EventPublisher {
	brokers := brokerAddrs()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        scoreSubmittedTopic,
		RequiredAcks: kafka.RequireAll,
		WriteTimeout: 5 * time.Second,
		BatchTimeout: 10 * time.Millisecond,
		MaxAttempts:  3,
		Balancer:     &kafka.Hash{},
	}

	verifyTopic(brokers)
	return &EventPublisher{writer: writer}
}

// verifyTopic performs a bounded metadata fetch for score.submitted and
// exits the process if the broker is unreachable or the topic is missing.
// The topic is pre-created infrastructure; this code must never create it.
func verifyTopic(brokers []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := (&kafka.Client{
		Addr:    kafka.TCP(brokers...),
		Timeout: 3 * time.Second,
	}).Metadata(ctx, &kafka.MetadataRequest{Topics: []string{scoreSubmittedTopic}})
	if err != nil {
		log.Fatalf("failed to fetch kafka metadata from %s: %v", strings.Join(brokers, ","), err)
	}

	for _, t := range resp.Topics {
		if t.Name != scoreSubmittedTopic {
			continue
		}
		if t.Error != nil {
			log.Fatalf("kafka topic %q metadata error: %v", scoreSubmittedTopic, t.Error)
		}
		if len(t.Partitions) == 0 {
			log.Fatalf("kafka topic %q has no partitions", scoreSubmittedTopic)
		}
		log.Printf("kafka producer ready (brokers=%s topic=%s partitions=%d)",
			strings.Join(brokers, ","), scoreSubmittedTopic, len(t.Partitions))
		return
	}
	log.Fatalf("kafka topic %q not found on broker(s) %s", scoreSubmittedTopic, strings.Join(brokers, ","))
}

// PublishScoreSubmitted writes one score.submitted event and returns only
// after the broker has acknowledged it (RequireAll) or the bounded
// retries/timeout are exhausted.
//
// The message key is the user_id as bytes: the Hash balancer computes
// hash(key) % partitionCount, so all events for one user land in the same
// partition (per-user ordering) while different users spread across both
// partitions.
//
// Any returned error must be treated as soft-fail by the caller: the score
// is already durably stored in PostgreSQL.
func (p *EventPublisher) PublishScoreSubmitted(ctx context.Context, event ScoreSubmittedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.Itoa(event.UserID)),
		Value: payload,
	})
}

// Close releases the writer and its pooled broker connections.
// Called once during application shutdown.
func (p *EventPublisher) Close() {
	p.writer.Close()
	log.Println("kafka producer closed")
}

// brokerAddrs parses the comma-separated KAFKA_BROKERS environment
// variable, defaulting to localhost:9092.
func brokerAddrs() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		raw = "localhost:9092"
	}
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			addrs = append(addrs, part)
		}
	}
	return addrs
}
