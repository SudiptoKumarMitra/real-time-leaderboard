// Package queryclient adapts the generated leaderboard.v1 gRPC client to
// the service.LeaderboardReader interface consumed by ScoreService.
//
// Connection policy: grpc.NewClient with insecure credentials — lazy
// connection establishment, NO startup health ping, no blocking of API
// startup while the Query Service is down. Each RPC derives its own
// 2-second deadline from the caller's context, so HTTP-request
// cancellation propagates into the gRPC call. gRPC's built-in reconnect
// (with backoff) handles transient outages between calls, so failures
// surface per-call and the existing service fallback behavior
// (PostgreSQL for GET /leaderboard, HTTP 500 for rank queries) is
// preserved unchanged.
package queryclient

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	leaderboardv1 "real_time_leaderboard/internal/gen/leaderboard/v1"
	"real_time_leaderboard/internal/repository"
	"real_time_leaderboard/internal/service"
)

// rpcTimeout bounds each leaderboard query RPC. Package-level var (not
// const) so tests can shrink it without a multi-second sleep.
var rpcTimeout = 2 * time.Second

// Client is a service.LeaderboardReader backed by the Query Service
// (leaderboard.v1.LeaderboardQuery) over gRPC.
type Client struct {
	conn   *grpc.ClientConn
	client leaderboardv1.LeaderboardQueryClient
}

// New creates a Client for the Query Service at QUERY_ADDR
// (default localhost:50051). It does not block or ping: the connection is
// established lazily on the first RPC. A malformed target is returned as
// an error — the caller decides how to fail startup — never via
// log.Fatalf here.
func New() (*Client, error) {
	addr := os.Getenv("QUERY_ADDR")
	if addr == "" {
		addr = "localhost:50051"
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("invalid QUERY_ADDR %q: %w", addr, err)
	}
	return &Client{conn: conn, client: leaderboardv1.NewLeaderboardQueryClient(conn)}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// GetTopN returns the top `limit` entries from the Query Service. The
// protobuf rank is dropped: the service recomputes display ranks
// positionally (i+1), matching the previous repository contract.
// The caller's context bounds the RPC; cancellation aborts it.
func (c *Client) GetTopN(ctx context.Context, limit int) ([]service.LeaderboardEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := c.client.GetLeaderboard(ctx, &leaderboardv1.GetLeaderboardRequest{Limit: int32(limit)})
	if err != nil {
		return nil, fmt.Errorf("query service GetLeaderboard: %w", err)
	}
	entries := make([]service.LeaderboardEntry, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		entries = append(entries, service.LeaderboardEntry{
			UserID: int(e.UserId),
			Score:  int(e.Score),
			Name:   e.Name,
		})
	}
	return entries, nil
}

// GetRank returns the user's 0-based projection rank — the Query
// Service's 1-based protobuf rank is converted here, exactly once.
// A user absent from the projection (ranked=false) maps to
// repository.ErrNotRanked, never rank 1.
func (c *Client) GetRank(ctx context.Context, userID int) (int, error) {
	resp, err := c.getUserRank(ctx, userID)
	if err != nil {
		return 0, err
	}
	if !resp.Ranked {
		return 0, repository.ErrNotRanked
	}
	return int(resp.Rank) - 1, nil // 1-based proto rank → 0-based reader contract
}

// GetBestScore returns the user's best score from the projection, as
// float64 to match the repository contract. A user absent from the
// projection (ranked=false) maps to repository.ErrNotRanked.
func (c *Client) GetBestScore(ctx context.Context, userID int) (float64, error) {
	resp, err := c.getUserRank(ctx, userID)
	if err != nil {
		return 0, err
	}
	if !resp.Ranked {
		return 0, repository.ErrNotRanked
	}
	return float64(resp.BestScore), nil
}

// getUserRank issues one GetUserRank RPC with the caller's context and
// the standard deadline.
func (c *Client) getUserRank(ctx context.Context, userID int) (*leaderboardv1.GetUserRankResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := c.client.GetUserRank(ctx, &leaderboardv1.GetUserRankRequest{UserId: int32(userID)})
	if err != nil {
		return nil, fmt.Errorf("query service GetUserRank: %w", err)
	}
	return resp, nil
}
