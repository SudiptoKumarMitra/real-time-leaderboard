package queryclient

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	leaderboardv1 "real_time_leaderboard/internal/gen/leaderboard/v1"
	"real_time_leaderboard/internal/queryservice"
	"real_time_leaderboard/internal/repository"
)

// readerFake implements queryservice's leaderboardReader seam
// (GetTopN returns repository.LeaderboardEntry; names come separately).
type readerFake struct {
	topN    []repository.LeaderboardEntry
	topNErr error
	rank    int
	rankErr error
	best    float64
	bestErr error
	delay   time.Duration // blocks GetTopN to exercise the RPC deadline
}

func (f *readerFake) GetTopN(limit int) ([]repository.LeaderboardEntry, error) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.topNErr != nil {
		return nil, f.topNErr
	}
	return f.topN, nil
}

func (f *readerFake) GetRank(userID int) (int, error) {
	if f.rankErr != nil {
		return 0, f.rankErr
	}
	return f.rank, nil
}

func (f *readerFake) GetBestScore(userID int) (float64, error) {
	if f.bestErr != nil {
		return 0, f.bestErr
	}
	return f.best, nil
}

// namesFake implements queryservice's nameLookup seam.
type namesFake struct {
	names map[int]string
	err   error
}

func (f *namesFake) GetNamesByIDs(ids []int) (map[int]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.names, nil
}

// startServer runs the real queryservice.Server on an in-memory bufconn
// listener and returns a Client wired to it. No network, no Redis,
// no PostgreSQL, no Docker.
func startServer(t *testing.T, reader *readerFake, names *namesFake) *Client {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	leaderboardv1.RegisterLeaderboardQueryServer(srv, queryservice.NewServer(reader, names))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &Client{conn: conn, client: leaderboardv1.NewLeaderboardQueryClient(conn)}
}

func TestGetTopN_MapsEntriesNamesAndDropsProtoRank(t *testing.T) {
	c := startServer(t, &readerFake{
		topN: []repository.LeaderboardEntry{
			{UserID: 7, Score: 9000},
			{UserID: 9, Score: 5000},
		},
	}, &namesFake{names: map[int]string{7: "Ada", 9: "Grace"}})

	got, err := c.GetTopN(context.Background(), 5)
	if err != nil {
		t.Fatalf("GetTopN: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2", len(got))
	}
	if got[0].UserID != 7 || got[0].Score != 9000 || got[0].Name != "Ada" {
		t.Errorf("entry[0] = %+v, want {7 9000 Ada}", got[0])
	}
	if got[1].UserID != 9 || got[1].Score != 5000 || got[1].Name != "Grace" {
		t.Errorf("entry[1] = %+v, want {9 5000 Grace}", got[1])
	}
}

func TestGetTopN_PreservesEmptyIsSuccess(t *testing.T) {
	c := startServer(t, &readerFake{topN: []repository.LeaderboardEntry{}}, &namesFake{})

	got, err := c.GetTopN(context.Background(), 10)
	if err != nil {
		t.Fatalf("GetTopN empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len=%d, want 0", len(got))
	}
}

func TestGetTopN_ServerFailureIsPlainError(t *testing.T) {
	c := startServer(t, &readerFake{topNErr: errors.New("redis down")}, &namesFake{})

	_, err := c.GetTopN(context.Background(), 10)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	// Must NOT be ErrNotRanked — service fallback treats any error the
	// same way (log + PostgreSQL), but the distinction matters for rank.
	if errors.Is(err, repository.ErrNotRanked) {
		t.Error("server failure must not map to ErrNotRanked")
	}
}

func TestGetRank_Converts1BasedTo0BasedExactlyOnce(t *testing.T) {
	c := startServer(t, &readerFake{rank: 0}, &namesFake{})

	got, err := c.GetRank(context.Background(), 42)
	if err != nil {
		t.Fatalf("GetRank: %v", err)
	}
	if got != 0 {
		t.Errorf("rank=%d, want 0 (proto rank 1 → reader rank 0)", got)
	}
}

func TestGetRank_FourthPlaceIsReaderRankThree(t *testing.T) {
	c := startServer(t, &readerFake{rank: 3}, &namesFake{})

	got, err := c.GetRank(context.Background(), 42)
	if err != nil {
		t.Fatalf("GetRank: %v", err)
	}
	if got != 3 {
		t.Errorf("rank=%d, want 3 (proto rank 4 → reader rank 3)", got)
	}
}

func TestGetRank_UnrankedIsErrNotRanked(t *testing.T) {
	c := startServer(t, &readerFake{rankErr: repository.ErrNotRanked}, &namesFake{})

	got, err := c.GetRank(context.Background(), 999999)
	if !errors.Is(err, repository.ErrNotRanked) {
		t.Fatalf("err=%v, want ErrNotRanked", err)
	}
	if got != 0 {
		t.Errorf("rank=%d, want 0 placeholder", got)
	}
}

func TestGetRank_InternalFailureIsPlainError(t *testing.T) {
	c := startServer(t, &readerFake{rankErr: errors.New("redis timeout")}, &namesFake{})

	_, err := c.GetRank(context.Background(), 42)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if errors.Is(err, repository.ErrNotRanked) {
		t.Error("backend failure must not map to ErrNotRanked (would silently 200)")
	}
	if !strings.Contains(err.Error(), "redis timeout") {
		t.Errorf("err=%v, want underlying cause preserved", err)
	}
}

func TestGetBestScore_Ranked(t *testing.T) {
	c := startServer(t, &readerFake{best: 1234.0}, &namesFake{})

	got, err := c.GetBestScore(context.Background(), 42)
	if err != nil {
		t.Fatalf("GetBestScore: %v", err)
	}
	if got != 1234.0 {
		t.Errorf("score=%v, want 1234", got)
	}
}

func TestGetBestScore_UnrankedIsErrNotRanked(t *testing.T) {
	c := startServer(t, &readerFake{bestErr: repository.ErrNotRanked}, &namesFake{})

	got, err := c.GetBestScore(context.Background(), 999999)
	if !errors.Is(err, repository.ErrNotRanked) {
		t.Fatalf("err=%v, want ErrNotRanked", err)
	}
	if got != 0 {
		t.Errorf("score=%v, want 0 placeholder", got)
	}
}

func TestGetTopN_DeadlineExceeded(t *testing.T) {
	// Shrink the per-RPC deadline so the test stays fast; the production
	// value is 2s (see rpcTimeout).
	old := rpcTimeout
	rpcTimeout = 50 * time.Millisecond
	t.Cleanup(func() { rpcTimeout = old })

	c := startServer(t, &readerFake{delay: 500 * time.Millisecond}, &namesFake{})

	_, err := c.GetTopN(context.Background(), 10)
	if err == nil {
		t.Fatal("want deadline error, got nil")
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("err=%v, want codes.DeadlineExceeded", err)
	}
}

func TestGetTopN_StaleNameSemanticsPreserved(t *testing.T) {
	// The service skips empty-name entries (stale members) and preserves
	// rank gaps; the client must faithfully forward empty names from the
	// wire (server leaves name empty for users absent from PostgreSQL).
	c := startServer(t, &readerFake{
		topN: []repository.LeaderboardEntry{
			{UserID: 1, Score: 100},
			{UserID: 2, Score: 90},
			{UserID: 3, Score: 80},
		},
	}, &namesFake{names: map[int]string{1: "Ada", 3: "Grace"}}) // user 2 deleted

	got, err := c.GetTopN(context.Background(), 10)
	if err != nil {
		t.Fatalf("GetTopN: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len=%d, want 3", len(got))
	}
	if got[1].Name != "" || got[1].UserID != 2 {
		t.Errorf("stale entry mangled: %+v", got[1])
	}
	if got[2].Name != "Grace" {
		t.Errorf("entry[2].Name=%q, want Grace", got[2].Name)
	}
}

func TestGetTopN_CallerCancellationAbortsBeforeDeadline(t *testing.T) {
	// Production rpcTimeout (2s) stays in force; the server delays far
	// longer. The caller cancels after 50ms — the RPC must abort with
	// Canceled well before the deadline would fire.
	c := startServer(t, &readerFake{delay: 5 * time.Second}, &namesFake{})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := c.GetTopN(ctx, 10)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("want cancellation error, got nil")
	}
	if status.Code(err) != codes.Canceled {
		t.Errorf("err=%v, want codes.Canceled", err)
	}
	if elapsed >= rpcTimeout {
		t.Errorf("aborted after %v — caller cancellation did not propagate; only the %v deadline fired", elapsed, rpcTimeout)
	}
}

func TestNew_MalformedQueryAddrReturnsError(t *testing.T) {
	// grpc.NewClient fails at construction on an unparseable target
	// (invalid URL escape), so the constructor must surface the error —
	// never log.Fatalf — for the API to clean up and exit non-zero.
	t.Setenv("QUERY_ADDR", "%zz")
	c, err := New()
	if err == nil {
		c.Close()
		t.Fatal("want error for malformed QUERY_ADDR, got nil")
	}
	if c != nil {
		t.Error("client must be nil when construction fails")
	}
	if !strings.Contains(err.Error(), "%zz") {
		t.Errorf("err=%v, want it to name the bad address", err)
	}
}
