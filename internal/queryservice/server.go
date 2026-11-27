// Package queryservice implements the LeaderboardQuery gRPC service: the
// read-only query surface over the Redis leaderboard projection, plus the
// one batch PostgreSQL name lookup needed to return display-ready rows.
//
// Dependencies are narrow interfaces (same pattern as internal/consumer):
// production wiring injects *repository.LeaderboardRepository and
// *repository.UserRepository; unit tests substitute fakes, so the logic
// is verifiable without Redis, PostgreSQL, or a gRPC listener.
package queryservice

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	leaderboardv1 "real_time_leaderboard/internal/gen/leaderboard/v1"
	"real_time_leaderboard/internal/repository"
)

// Mirror the API handler's leaderboard limits (handler constants:
// default 10, min 1, max 100). The gRPC boundary validates them itself.
const (
	minLeaderboardLimit = 1
	maxLeaderboardLimit = 100
)

// leaderboardReader is this service's view of the Redis projection:
// the three read operations ScoreService consumes. Satisfied by
// *repository.LeaderboardRepository in production.
type leaderboardReader interface {
	GetTopN(limit int) ([]repository.LeaderboardEntry, error)
	GetRank(userID int) (int, error)
	GetBestScore(userID int) (float64, error)
}

// nameLookup resolves user IDs to display names in ONE batch query.
// Satisfied by *repository.UserRepository in production. Names are part
// of the HTTP leaderboard response, so the query service must provide
// them — Redis alone cannot.
type nameLookup interface {
	GetNamesByIDs(ids []int) (map[int]string, error)
}

// Server implements leaderboardv1.LeaderboardQueryServer.
type Server struct {
	leaderboardv1.UnimplementedLeaderboardQueryServer
	lbRepo leaderboardReader
	names  nameLookup
}

// NewServer wires the service with its two dependencies.
func NewServer(lbRepo leaderboardReader, names nameLookup) *Server {
	return &Server{lbRepo: lbRepo, names: names}
}

// GetLeaderboard returns the top `limit` players, descending by best
// score, with 1-based ranks and display names.
//
// Behavior mirrors ScoreService.GetLeaderboard: ranks are the original
// positions (i+1) so entries whose user is gone from PostgreSQL keep
// their rank and honest gaps remain; an empty leaderboard is a
// successful empty response, not an error.
//
// Errors: limit outside 1..100 → InvalidArgument; Redis or name-lookup
// failures → Internal (the API's PostgreSQL fallback covers the former).
func (s *Server) GetLeaderboard(ctx context.Context, req *leaderboardv1.GetLeaderboardRequest) (*leaderboardv1.GetLeaderboardResponse, error) {
	limit := int(req.GetLimit())
	if limit < minLeaderboardLimit || limit > maxLeaderboardLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be between %d and %d", minLeaderboardLimit, maxLeaderboardLimit)
	}

	entries, err := s.lbRepo.GetTopN(limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "leaderboard unavailable: %v", err)
	}
	if len(entries) == 0 {
		return &leaderboardv1.GetLeaderboardResponse{
			Entries: make([]*leaderboardv1.LeaderboardEntry, 0),
		}, nil
	}

	// ONE batch query for all names (no N+1), same as the API service.
	ids := make([]int, len(entries))
	for i, e := range entries {
		ids[i] = e.UserID
	}
	names, err := s.names.GetNamesByIDs(ids)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "user names unavailable: %v", err)
	}

	resp := &leaderboardv1.GetLeaderboardResponse{
		Entries: make([]*leaderboardv1.LeaderboardEntry, 0, len(entries)),
	}
	for i, e := range entries {
		resp.Entries = append(resp.Entries, &leaderboardv1.LeaderboardEntry{
			Rank:   int32(i + 1), // Redis 0-based position → 1-based display rank (converted ONCE, here)
			UserId: int32(e.UserID),
			Score:  int32(e.Score),
			Name:   names[e.UserID], // absent user → empty name, rank gap preserved
		})
	}
	return resp, nil
}

// GetUserRank returns one user's current position.
//
// An unranked user (no zset member) is a SUCCESSFUL response with
// ranked=false — rank and best_score are left at zero placeholders and
// must be ignored by callers, per the proto contract. It is never
// mapped to rank 1 and never an error.
//
// Errors: user_id < 1 → InvalidArgument; Redis failures → Internal.
func (s *Server) GetUserRank(ctx context.Context, req *leaderboardv1.GetUserRankRequest) (*leaderboardv1.GetUserRankResponse, error) {
	userID := int(req.GetUserId())
	if userID < 1 {
		return nil, status.Errorf(codes.InvalidArgument, "user_id must be a positive integer")
	}

	redisRank, err := s.lbRepo.GetRank(userID)
	if errors.Is(err, repository.ErrNotRanked) {
		return &leaderboardv1.GetUserRankResponse{UserId: int32(userID), Ranked: false}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rank lookup unavailable: %v", err)
	}

	best, err := s.lbRepo.GetBestScore(userID)
	if errors.Is(err, repository.ErrNotRanked) {
		// Member vanished between the two commands — treat as unranked.
		return &leaderboardv1.GetUserRankResponse{UserId: int32(userID), Ranked: false}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "best-score lookup unavailable: %v", err)
	}

	return &leaderboardv1.GetUserRankResponse{
		UserId:    int32(userID),
		Ranked:    true,
		Rank:      int32(redisRank + 1), // Redis 0-based → 1-based display rank (converted ONCE, here)
		BestScore: int32(best),          // scores are integers by contract ([0, 1000000])
	}, nil
}
