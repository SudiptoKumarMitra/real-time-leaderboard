package service

import (
	"errors"

	"real_time_leaderboard/internal/repository"
)

// maxScore is the maximum acceptable score value.
const maxScore = 1000000

// ErrInvalidScore is returned when the score fails business validation.
var ErrInvalidScore = errors.New("score must be an integer between 0 and 1000000")

// ScoreService handles score submission business logic.
type ScoreService struct {
	repo repository.ScoreRepository
}

// NewScoreService creates a new ScoreService with the given repository.
func NewScoreService(repo repository.ScoreRepository) *ScoreService {
	return &ScoreService{repo: repo}
}

// SubmitScore validates the score against business rules and persists it.
//
// Business rules:
//   - score must be >= 0 (negative scores are meaningless)
//   - score must be <= 1,000,000 (reasonable maximum)
//   - score 0 is accepted
//   - every submission creates a new row (history preserved)
//
// The userID comes from the verified JWT context — never from the request body.
func (s *ScoreService) SubmitScore(userID, score int) (int, error) {
	// Validate business rules
	if score < 0 || score > maxScore {
		return 0, ErrInvalidScore
	}

	// Persist the score
	scoreID, err := s.repo.InsertScore(userID, score)
	if err != nil {
		return 0, errors.New("database error while saving score")
	}

	return scoreID, nil
}
