package repository

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// leaderboardKey is the Redis Sorted Set key that holds the leaderboard.
// The key may not be a Go const because it is a string literal used in commands.
const leaderboardKey = "leaderboard"

// ErrNotRanked indicates the user has no entry in the leaderboard zset.
// It is a normal state (never submitted, or Redis projection missing),
// not an operational failure — callers respond 200, not 500.
var ErrNotRanked = errors.New("user is not ranked in the leaderboard")

// LeaderboardEntry is one row of leaderboard data: a user's best score.
// It is shared by the Redis fast path and the PostgreSQL fallback path.
type LeaderboardEntry struct {
	UserID int
	Score  int
}

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

// GetTopN returns the top `limit` entries ordered by best score, descending.
//
// Command: ZREVRANGE leaderboard 0 (limit-1) WITHSCORES
//
// Redis returns members in descending score order with scores inline, so
// no per-member follow-up query is needed. A missing key yields an empty
// slice (not an error) — the service treats that as a cache miss and
// falls back to PostgreSQL. Parse failures skip the offending member
// rather than failing the whole read.
func (r *LeaderboardRepository) GetTopN(limit int) ([]LeaderboardEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	results, err := r.Redis.ZRevRangeWithScores(ctx, leaderboardKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	entries := make([]LeaderboardEntry, 0, len(results))
	for _, z := range results {
		member, ok := z.Member.(string)
		if !ok {
			continue
		}
		userID, err := strconv.Atoi(member)
		if err != nil {
			continue
		}
		entries = append(entries, LeaderboardEntry{
			UserID: userID,
			Score:  int(z.Score),
		})
	}
	return entries, nil
}

// GetRank returns the user's 0-based descending rank in the leaderboard.
//
// Command: ZREVRANK leaderboard <user_id>
//
// The returned value counts members with strictly higher scores: 0 means
// best. Redis replies nil for members absent from the sorted set, which
// go-redis surfaces as redis.Nil — translated here to ErrNotRanked so
// callers never deal with protocol-level errors. Any other error is a real
// Redis failure (unreachable, timeout, WRONGTYPE) and must propagate.
func (r *LeaderboardRepository) GetRank(userID int) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rank, err := r.Redis.ZRevRank(ctx, leaderboardKey, strconv.Itoa(userID)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotRanked
	}
	if err != nil {
		return 0, err
	}
	return int(rank), nil
}

// GetBestScore returns the user's best score from the leaderboard projection.
//
// Command: ZSCORE leaderboard <user_id>
//
// O(1) hash-table lookup of the member's score — the value maintained by
// the write path's ZADD ... GT (highest score only). Missing member is
// translated to ErrNotRanked, same contract as GetRank.
func (r *LeaderboardRepository) GetBestScore(userID int) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	score, err := r.Redis.ZScore(ctx, leaderboardKey, strconv.Itoa(userID)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotRanked
	}
	if err != nil {
		return 0, err
	}
	return score, nil
}
