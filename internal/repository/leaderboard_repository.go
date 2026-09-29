package repository

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// leaderboardKey is the Redis Sorted Set key that holds the leaderboard.
// The key may not be a Go const because it is a string literal used in commands.
const leaderboardKey = "leaderboard"

// LeaderboardRepository handles Redis operations for the leaderboard.
//
// The leaderboard is a Redis Sorted Set:
//   - member = user_id as a string (e.g. "31")
//   - score  = that user's highest score
//
// This is separate from ScoreRepository (PostgreSQL history) because it
// stores derived state: rebuildable from PostgreSQL at any time.
type LeaderboardRepository struct {
	Redis *redis.Client
}

// NewLeaderboardRepository creates a LeaderboardRepository with the given Redis client.
// The client is shared — created once in db.InitRedis(), injected via constructor.
func NewLeaderboardRepository(rdb *redis.Client) *LeaderboardRepository {
	return &LeaderboardRepository{Redis: rdb}
}

// UpdateBestScore updates the user's leaderboard entry only if the new score
// is strictly greater than the stored best.
//
// Command: ZADD leaderboard GT <score> <user_id>
//
// GT (greater-than) is required: plain ZADD would overwrite unconditionally,
// allowing a lower score to reduce a user's best. The comparison happens
// server-side inside Redis, so concurrent submissions cannot race.
// GT does not prevent adding members that do not exist yet.
//
// Errors are soft at the service layer: PostgreSQL already stored the score,
// so a Redis failure only leaves the projection temporarily stale.
func (r *LeaderboardRepository) UpdateBestScore(userID int, score int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return r.Redis.ZAddGT(ctx, leaderboardKey, redis.Z{
		Score:  float64(score),
		Member: strconv.Itoa(userID),
	}).Err()
}
