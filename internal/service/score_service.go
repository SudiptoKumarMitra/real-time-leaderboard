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
	repo   repository.ScoreRepository
	lbRepo repository.LeaderboardRepository
}

// NewScoreService creates a new ScoreService with the given repositories.
//
// repo persists score history (PostgreSQL, source of truth).
// lbRepo maintains the leaderboard projection (Redis Sorted Set).
func NewScoreService(repo repository.ScoreRepository, lbRepo repository.LeaderboardRepository) *ScoreService {
	return &ScoreService{repo: repo, lbRepo: lbRepo}
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
