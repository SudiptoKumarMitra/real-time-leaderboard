package service

import (
	"context"
	"errors"
	"log"
	"time"

	"real_time_leaderboard/internal/publisher"
	"real_time_leaderboard/internal/repository"
)

// maxScore is the maximum acceptable score value.
const maxScore = 1000000

// ErrInvalidScore is returned when the score fails business validation.
var ErrInvalidScore = errors.New("score must be an integer between 0 and 1000000")

// LeaderboardEntry is one projected leaderboard row as seen by the
// service: identity, best score, and (when the source can resolve it)
// the display name. Name may be empty — the service then fills it with
// ONE batch PostgreSQL lookup, or skips the row as a stale member.
type LeaderboardEntry struct {
	UserID int
	Score  int
	Name   string
}

// LeaderboardReader supplies the leaderboard projection. Production API
// wiring injects the gRPC query-client adapter (internal/queryclient);
// the methods mirror the Redis repository's read contract (GetRank is
// 0-based, repository.ErrNotRanked marks a missing member — a normal
// state, not a failure), plus the caller's context so request
// cancellation propagates into the backend call.
type LeaderboardReader interface {
	GetTopN(ctx context.Context, limit int) ([]LeaderboardEntry, error)
	GetRank(ctx context.Context, userID int) (int, error)
	GetBestScore(ctx context.Context, userID int) (float64, error)
}

// ScoreService handles score submission business logic.
type ScoreService struct {
	repo      repository.ScoreRepository
	lb        LeaderboardReader
	userRepo  repository.UserRepository
	publisher *publisher.EventPublisher
}

// NewScoreService creates a new ScoreService with the given dependencies.
//
// repo persists score history (PostgreSQL, source of truth).
// lb reads the leaderboard projection (gRPC query service in front of
// the Redis sorted set).
// userRepo resolves user IDs to display names in one batch query.
// publisher emits score.submitted events to Kafka (one writer per app,
// created in main and injected here — never per request, never a global).
func NewScoreService(repo repository.ScoreRepository, lb LeaderboardReader, userRepo repository.UserRepository, pub *publisher.EventPublisher) *ScoreService {
	return &ScoreService{repo: repo, lb: lb, userRepo: userRepo, publisher: pub}
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
// Persistence order: PostgreSQL first (source of truth), then the
// Kafka score.submitted event. The Redis leaderboard is NOT written here —
// it is updated only by the Kafka consumer, which applies the event
// asynchronously. PostgreSQL and Kafka failure policies are unchanged:
// a PostgreSQL failure fails the submission, a Kafka failure is only
// logged because the score is durably stored in PostgreSQL.
//
// The userID comes from the verified JWT context — never from the request body.
func (s *ScoreService) SubmitScore(userID, score int) (int, error) {
	// Validate business rules
	if score < 0 || score > maxScore {
		return 0, ErrInvalidScore
	}

	// Step 1: PostgreSQL — durable history. Failure here fails the submission
	// and prevents Kafka from running.
	scoreID, createdAt, err := s.repo.InsertScore(userID, score)
	if err != nil {
		return 0, errors.New("database error while saving score")
	}

	// Step 2: Kafka — score.submitted event, strictly AFTER PostgreSQL
	// succeeded, because the event's score_id and created_at come from the
	// INSERT ... RETURNING result. This is a submission event: it is
	// published for every accepted submission, including scores that are
	// not a new best.
	//
	// Soft-fail policy: a Kafka outage must never roll back the PostgreSQL
	// row nor turn a successful submission into an HTTP error. The score is
	// already durable; the event is best-effort here (no outbox in scope).
	event := publisher.ScoreSubmittedEvent{
		Event:     "score.submitted",
		UserID:    userID,
		Score:     score,
		ScoreID:   scoreID,
		CreatedAt: createdAt.Format(time.RFC3339),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.publisher.PublishScoreSubmitted(ctx, event); err != nil {
		log.Printf("warning: failed to publish score.submitted for user %d score %d: %v", userID, score, err)
	}

	return scoreID, nil
}

// GetLeaderboard returns the top `limit` players with 1-based ranks and names.
//
// Path selection:
//   - Projection fast path: GetTopN over the pre-sorted zset via the
//     query service. Entries may already carry display names.
//   - PostgreSQL fallback: used when the projection call fails OR returns
//     zero entries (cache miss). Neither condition is an HTTP error —
//     PostgreSQL is the source of truth and can recompute the projection.
//
// Names are resolved in ONE batch query — but only for entries whose
// source did not carry a name (PostgreSQL fallback rows). No N+1.
// Entries whose user no longer exists in PostgreSQL are skipped; Rank
// stays tied to the original position so gaps honestly reflect missing
// users. An empty leaderboard is a valid 200 with an empty (non-nil) slice.
// The caller's context bounds the projection read; cancellation aborts it.
func (s *ScoreService) GetLeaderboard(ctx context.Context, limit int) ([]LeaderboardRow, error) {
	entries, err := s.lb.GetTopN(ctx, limit)
	if err != nil {
		// Projection unavailable — log and fall back to PostgreSQL.
		log.Printf("warning: leaderboard projection unavailable, falling back to postgres: %v", err)
		entries = nil
	}
	if len(entries) == 0 {
		// Cache miss (empty projection) or outage — recompute from truth.
		raw, err := s.repo.GetTopBestScores(limit)
		if err != nil {
			return nil, errors.New("database error while loading leaderboard")
		}
		entries = make([]LeaderboardEntry, len(raw))
		for i, e := range raw {
			entries[i] = LeaderboardEntry{UserID: e.UserID, Score: e.Score} // names filled below
		}
	}
	if len(entries) == 0 {
		return []LeaderboardRow{}, nil
	}

	// One batch lookup for entries whose source did not carry a name.
	var missing []int
	for _, e := range entries {
		if e.Name == "" {
			missing = append(missing, e.UserID)
		}
	}
	names := map[int]string{}
	if len(missing) > 0 {
		names, err = s.userRepo.GetNamesByIDs(missing)
		if err != nil {
			return nil, errors.New("database error while loading leaderboard")
		}
	}

	rows := make([]LeaderboardRow, 0, len(entries))
	for i, e := range entries {
		name := e.Name
		if name == "" {
			name = names[e.UserID]
		}
		if name == "" {
			continue // user deleted from PostgreSQL; stale projection member
		}
		rows = append(rows, LeaderboardRow{
			Rank:   i + 1, // display rank: projection index is 0-based, API is 1-based
			UserID: e.UserID,
			Name:   name,
			Score:  e.Score,
		})
	}
	return rows, nil
}

// UserRank is the ranking for a single user.
// Ranked=false means the user has no entry in the Redis leaderboard —
// a normal state (200), not an error.
type UserRank struct {
	UserID int
	Ranked bool
	Rank   int // 1-based API rank; meaningful only when Ranked is true
	Score  int // best score from Redis; meaningful only when Ranked is true
}

// GetUserRank returns the user's current leaderboard position and best score.
//
// Reads come ONLY from the projection via the query service
// (GetRank + GetBestScore) — no PostgreSQL lookup, because the projection
// already holds the authoritative current ranking state. The reader's
// 0-based rank is converted to the 1-based API rank here (apiRank =
// readerRank + 1); the raw value never leaves this layer.
//
// A user missing from the projection maps to repository.ErrNotRanked and
// is returned as UserRank{Ranked: false} — NOT rank 1. Real backend
// failures propagate as errors so the handler can respond 500.
// The caller's context bounds the projection reads; cancellation aborts them.
func (s *ScoreService) GetUserRank(ctx context.Context, userID int) (UserRank, error) {
	projRank, err := s.lb.GetRank(ctx, userID)
	if errors.Is(err, repository.ErrNotRanked) {
		return UserRank{UserID: userID, Ranked: false}, nil
	}
	if err != nil {
		return UserRank{}, err
	}

	best, err := s.lb.GetBestScore(ctx, userID)
	if errors.Is(err, repository.ErrNotRanked) {
		// Member removed between the two commands — treat as unranked.
		return UserRank{UserID: userID, Ranked: false}, nil
	}
	if err != nil {
		return UserRank{}, err
	}

	return UserRank{
		UserID: userID,
		Ranked: true,
		Rank:   projRank + 1, // 0-based reader rank → 1-based API rank
		Score:  int(best),
	}, nil
}
