package queryservice

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	leaderboardv1 "real_time_leaderboard/internal/gen/leaderboard/v1"
	"real_time_leaderboard/internal/repository"
)

// fakes — no Redis, no PostgreSQL, no gRPC listener.

type fakeReader struct {
	topN        []repository.LeaderboardEntry
	topNErr     error
	rank        int
	rankErr     error
	best        float64
	bestErr     error
	topNLimits  []int // records limits passed to GetTopN
	rankLookups []int
}

func (f *fakeReader) GetTopN(limit int) ([]repository.LeaderboardEntry, error) {
	f.topNLimits = append(f.topNLimits, limit)
	return f.topN, f.topNErr
}

func (f *fakeReader) GetRank(userID int) (int, error) {
	f.rankLookups = append(f.rankLookups, userID)
	return f.rank, f.rankErr
}

func (f *fakeReader) GetBestScore(userID int) (float64, error) {
	return f.best, f.bestErr
}

type fakeNames struct {
	names map[int]string
	err   error
	calls int
}

func (f *fakeNames) GetNamesByIDs(ids []int) (map[int]string, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.names, nil
}

func TestGetLeaderboard_Success_1BasedRanksAndNames(t *testing.T) {
	reader := &fakeReader{topN: []repository.LeaderboardEntry{
		{UserID: 32, Score: 9500},
		{UserID: 34, Score: 4200},
		{UserID: 31, Score: 2000},
	}}
	names := &fakeNames{names: map[int]string{32: "LB Tester", 34: "Smoke Tester", 31: "MW User"}}
	srv := NewServer(reader, names)

	resp, err := srv.GetLeaderboard(context.Background(), &leaderboardv1.GetLeaderboardRequest{Limit: 10})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if got := reader.topNLimits; len(got) != 1 || got[0] != 10 {
		t.Fatalf("GetTopN limits = %v, want [10]", got)
	}
	if names.calls != 1 {
		t.Fatalf("name lookups = %d, want exactly 1 (batch, no N+1)", names.calls)
	}
	want := []struct {
		rank int32
		uid  int32
		sc   int32
		name string
	}{
		{1, 32, 9500, "LB Tester"},
		{2, 34, 4200, "Smoke Tester"},
		{3, 31, 2000, "MW User"},
	}
	if len(resp.Entries) != len(want) {
		t.Fatalf("entries = %d, want %d", len(resp.Entries), len(want))
	}
	for i, w := range want {
		e := resp.Entries[i]
		if e.Rank != w.rank || e.UserId != w.uid || e.Score != w.sc || e.Name != w.name {
			t.Errorf("entry[%d] = rank=%d user=%d score=%d name=%q, want rank=%d user=%d score=%d name=%q",
				i, e.Rank, e.UserId, e.Score, e.Name, w.rank, w.uid, w.sc, w.name)
		}
	}
}

func TestGetLeaderboard_MissingUser_PreservesRankGapAndEmptyName(t *testing.T) {
	// Deleted user still in Redis: entry returned, name empty, rank still
	// tied to the original position (mirrors ScoreService behavior).
	reader := &fakeReader{topN: []repository.LeaderboardEntry{
		{UserID: 32, Score: 9500},
		{UserID: 99, Score: 5000}, // gone from PostgreSQL
	}}
	names := &fakeNames{names: map[int]string{32: "LB Tester"}}
	srv := NewServer(reader, names)

	resp, err := srv.GetLeaderboard(context.Background(), &leaderboardv1.GetLeaderboardRequest{Limit: 10})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("entries = %d, want 2 (stale member kept)", len(resp.Entries))
	}
	if resp.Entries[1].Rank != 2 || resp.Entries[1].UserId != 99 || resp.Entries[1].Name != "" {
		t.Errorf("stale entry = %+v, want rank=2 user=99 empty name", resp.Entries[1])
	}
}

func TestGetLeaderboard_EmptyIsSuccessNotError(t *testing.T) {
	srv := NewServer(&fakeReader{topN: nil}, &fakeNames{names: map[int]string{}})
	resp, err := srv.GetLeaderboard(context.Background(), &leaderboardv1.GetLeaderboardRequest{Limit: 10})
	if err != nil {
		t.Fatalf("err = %v, want nil for empty leaderboard", err)
	}
	if resp.Entries == nil || len(resp.Entries) != 0 {
		t.Fatalf("entries = %v, want non-nil empty slice", resp.Entries)
	}
}

func TestGetLeaderboard_InvalidLimits(t *testing.T) {
	srv := NewServer(&fakeReader{}, &fakeNames{})
	for _, limit := range []int32{0, -1, 101} {
		_, err := srv.GetLeaderboard(context.Background(), &leaderboardv1.GetLeaderboardRequest{Limit: limit})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("limit=%d: code = %v, want InvalidArgument (err=%v)", limit, status.Code(err), err)
		}
	}
}

func TestGetLeaderboard_RepositoryFailureIsInternal(t *testing.T) {
	srv := NewServer(&fakeReader{topNErr: errors.New("redis down")}, &fakeNames{})
	_, err := srv.GetLeaderboard(context.Background(), &leaderboardv1.GetLeaderboardRequest{Limit: 10})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal (err=%v)", status.Code(err), err)
	}
}

func TestGetLeaderboard_NameLookupFailureIsInternal(t *testing.T) {
	srv := NewServer(
		&fakeReader{topN: []repository.LeaderboardEntry{{UserID: 32, Score: 9500}}},
		&fakeNames{err: errors.New("pg down")},
	)
	_, err := srv.GetLeaderboard(context.Background(), &leaderboardv1.GetLeaderboardRequest{Limit: 10})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal (err=%v)", status.Code(err), err)
	}
}

func TestGetUserRank_Ranked_ConvertsTo1BasedOnce(t *testing.T) {
	// Redis ZREVRANK is 0-based: rank 4 means fifth place → proto rank 5.
	reader := &fakeReader{rank: 4, best: 77777}
	srv := NewServer(reader, &fakeNames{})

	resp, err := srv.GetUserRank(context.Background(), &leaderboardv1.GetUserRankRequest{UserId: 36})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !resp.Ranked || resp.Rank != 5 || resp.BestScore != 77777 || resp.UserId != 36 {
		t.Fatalf("resp = %+v, want ranked rank=5 best=77777 user=36", resp)
	}
}

func TestGetUserRank_RankZeroIsRankOne(t *testing.T) {
	// Guard against the classic off-by-one: Redis rank 0 = best = proto rank 1.
	srv := NewServer(&fakeReader{rank: 0, best: 999}, &fakeNames{})
	resp, err := srv.GetUserRank(context.Background(), &leaderboardv1.GetUserRankRequest{UserId: 31})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !resp.Ranked || resp.Rank != 1 {
		t.Fatalf("resp = %+v, want ranked=true rank=1", resp)
	}
}

func TestGetUserRank_Unranked_NeverRankOne(t *testing.T) {
	srv := NewServer(&fakeReader{rankErr: repository.ErrNotRanked}, &fakeNames{})
	resp, err := srv.GetUserRank(context.Background(), &leaderboardv1.GetUserRankRequest{UserId: 424242})
	if err != nil {
		t.Fatalf("err = %v, want success for unranked user", err)
	}
	if resp.Ranked {
		t.Fatal("ranked = true, want false")
	}
	if resp.Rank != 0 || resp.BestScore != 0 {
		t.Fatalf("rank/best = %d/%d, want zero placeholders (ignored when ranked=false)", resp.Rank, resp.BestScore)
	}
}

func TestGetUserRank_MemberVanishesBetweenCommands(t *testing.T) {
	// GetRank ok but GetBestScore says not-ranked → unranked, not an error.
	srv := NewServer(&fakeReader{rank: 2, bestErr: repository.ErrNotRanked}, &fakeNames{})
	resp, err := srv.GetUserRank(context.Background(), &leaderboardv1.GetUserRankRequest{UserId: 7})
	if err != nil {
		t.Fatalf("err = %v, want success", err)
	}
	if resp.Ranked {
		t.Fatal("ranked = true, want false when member vanished")
	}
}

func TestGetUserRank_InvalidUserID(t *testing.T) {
	srv := NewServer(&fakeReader{}, &fakeNames{})
	for _, id := range []int32{0, -1} {
		_, err := srv.GetUserRank(context.Background(), &leaderboardv1.GetUserRankRequest{UserId: id})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("user_id=%d: code = %v, want InvalidArgument", id, status.Code(err))
		}
	}
}

func TestGetUserRank_RedisFailuresAreInternal(t *testing.T) {
	srv := NewServer(&fakeReader{rankErr: errors.New("redis down")}, &fakeNames{})
	_, err := srv.GetUserRank(context.Background(), &leaderboardv1.GetUserRankRequest{UserId: 1})
	if status.Code(err) != codes.Internal {
		t.Fatalf("rank failure: code = %v, want Internal", status.Code(err))
	}

	srv = NewServer(&fakeReader{rank: 1, bestErr: errors.New("redis down")}, &fakeNames{})
	_, err = srv.GetUserRank(context.Background(), &leaderboardv1.GetUserRankRequest{UserId: 1})
	if status.Code(err) != codes.Internal {
		t.Fatalf("best-score failure: code = %v, want Internal", status.Code(err))
	}
}
