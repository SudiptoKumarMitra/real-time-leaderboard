package service

import (
	"errors"
	"log"

	"real_time_leaderboard/internal/repository"
)

// maxScore is the maximum acceptable score value.
const maxScore = 1000000

// ErrInvalidScore is returned when the score fails business validation.
var ErrInvalidScore = errors.New("score must be an integer between 0 and 1000000")

// ScoreService handles score submission business logic.
type ScoreService struct {
	repo     repository.ScoreRepository
	lbRepo   repository.LeaderboardRepository
	userRepo repository.UserRepository
}

// NewScoreService creates a new ScoreService with the given repositories.
//
// repo persists score history (PostgreSQL, source of truth).
// lbRepo maintains and reads the leaderboard projection (Redis Sorted Set).
// userRepo resolves user IDs to display names in one batch query.
func NewScoreService(repo repository.ScoreRepository, lbRepo repository.LeaderboardRepository, userRepo repository.UserRepository) *ScoreService {
	return &ScoreService{repo: repo, lbRepo: lbRepo, userRepo: userRepo}
}

// LeaderboardRow is one display-ready leaderboard row.
// Rank is 1-based (API/display rank) — Redis's 0-based index never leaks out.
type LeaderboardRow struct {
	Rank   int
	UserID int
	Name   string
	Score  int
}

// SubmitScore validates the score against business rules and persists it.
//
// Business rules:
//   - score must be >= 0 (negative scores are meaningless)
//   - score must be <= 1,000,000 (reasonable maximum)
//   - score 0 is accepted
//   - every submission creates a new row (history preserved)
//
// Persistence order: PostgreSQL first (source of truth), then Redis
// (derived leaderboard, ZADD ... GT so a lower score never reduces the best).
// A Redis failure is only logged — the submission still succeeds because the
// score is durably stored in PostgreSQL.
//
// The userID comes from the verified JWT context — never from the request body.
func (s *ScoreService) SubmitScore(userID, score int) (int, error) {
	// Validate business rules
	if score < 0 || score > maxScore {
		return 0, ErrInvalidScore
	}

	// Step 1: PostgreSQL — durable history. Failure here fails the submission.
	scoreID, err := s.repo.InsertScore(userID, score)
	if err != nil {
		return 0, errors.New("database error while saving score")
	}

	// Step 2: Redis — leaderboard projection (best score only).
	// Runs strictly after a successful INSERT so Redis never claims a score
	// that PostgreSQL does not have. Failure is non-fatal: the projection
	// is rebuildable and will self-heal on the next higher score.
	if err := s.lbRepo.UpdateBestScore(userID, score); err != nil {
		log.Printf("warning: failed to update leaderboard for user %d score %d: %v", userID, score, err)
	}

	return scoreID, nil
}

// GetLeaderboard returns the top `limit` players with 1-based ranks and names.
//
// Path selection:
//   - Redis fast path: ZREVRANGE over the pre-sorted zset (O(log N + limit))
//   - PostgreSQL fallback: used when Redis is empty (cache miss) OR returns
//     an error (unavailable). Neither condition is an HTTP error — PostgreSQL
//     is the source of truth and can recompute the same projection.
//
// Names are resolved in ONE batch query for all IDs (no N+1).
// Entries whose user no longer exists in PostgreSQL are skipped; Rank stays
// tied to the original position so gaps honestly reflect missing users.
// An empty leaderboard is a valid 200 with an empty (non-nil) slice.
func (s *ScoreService) GetLeaderboard(limit int) ([]LeaderboardRow, error) {
	entries, err := s.lbRepo.GetTopN(limit)
	if err != nil {
		// Redis unavailable — log and fall back to PostgreSQL.
		log.Printf("warning: redis leaderboard unavailable, falling back to postgres: %v", err)
		entries = nil
	}
	if len(entries) == 0 {
		// Cache miss (empty/missing key) or Redis down — recompute from truth.
		entries, err = s.repo.GetTopBestScores(limit)
		if err != nil {
			return nil, errors.New("database error while loading leaderboard")
		}
	}
	if len(entries) == 0 {
		return []LeaderboardRow{}, nil
	}

	ids := make([]int, len(entries))
	for i, e := range entries {
		ids[i] = e.UserID
	}

	names, err := s.userRepo.GetNamesByIDs(ids)
	if err != nil {
		return nil, errors.New("database error while loading leaderboard")
	}

	rows := make([]LeaderboardRow, 0, len(entries))
	for i, e := range entries {
		name, ok := names[e.UserID]
		if !ok {
			continue // user deleted from PostgreSQL; stale Redis member
		}
		rows = append(rows, LeaderboardRow{
			Rank:   i + 1, // display rank: Redis index is 0-based, API is 1-based
			UserID: e.UserID,
			Name:   name,
			Score:  e.Score,
		})
	}
	return rows, nil
}
