package db

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// InitRedis creates a Redis client and verifies connectivity with a Ping.
//
// The client is created from environment variables with safe local defaults,
// mirroring the PostgreSQL configuration in DSN(). redis.NewClient does not
// open any connection — the actual dial happens in Ping, which is bounded by
// a context timeout so startup can never hang on an unresponsive server.
//
// If Redis is unreachable, the application exits before the HTTP server
// starts, matching the fail-fast behavior of InitDB().
func InitRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       redisDB(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		log.Fatalf("Failed to ping redis: %v", err)
	}

	log.Println("✅ Redis client connected")
	return client
}

// CloseRedis closes the client and all pooled connections.
func CloseRedis(client *redis.Client) {
	client.Close()
	log.Println("🔌 Redis client closed")
}

// redisDB returns the Redis database index from the REDIS_DB environment
// variable, defaulting to 0. Invalid or empty values fall back to 0.
func redisDB() int {
	val := getEnv("REDIS_DB", "0")
	db, err := strconv.Atoi(val)
	if err != nil || db < 0 || db > 15 {
		return 0
	}
	return db
}
