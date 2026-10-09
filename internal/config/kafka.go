// Package config holds configuration knowledge shared by more than one
// component. It exists so contracts like the Kafka connection settings and
// the score submission topic name have exactly one definition — publisher
// and consumer cannot drift apart.
//
// Component-specific settings (consumer GroupID, retry limits, writer
// timeouts) stay in their own packages.
package config

import (
	"os"
	"strings"
)

// ScoreSubmittedTopic is the Kafka topic that score submission events are
// published to and consumed from. Defined once so the EventPublisher and
// the ScoreConsumer reference the same string. The topic is created by
// infrastructure (2 partitions) — application code never creates it.
const ScoreSubmittedTopic = "score.submitted"

// Brokers parses the comma-separated KAFKA_BROKERS environment variable,
// defaulting to localhost:9092. Shared by the EventPublisher (kafka.Writer)
// and the ScoreConsumer (kafka.Reader).
func Brokers() []string {
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
