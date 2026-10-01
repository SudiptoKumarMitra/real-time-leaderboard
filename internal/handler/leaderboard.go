package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"real_time_leaderboard/internal/service"
)

// LeaderboardHandler handles leaderboard read requests.
// It contains no Redis commands and no SQL — all store access lives
// behind the service layer.
type LeaderboardHandler struct {
	Service *service.ScoreService
}

// NewLeaderboardHandler creates a new LeaderboardHandler with the given service.
func NewLeaderboardHandler(svc *service.ScoreService) *LeaderboardHandler {
	return &LeaderboardHandler{Service: svc}
}

// LeaderboardEntryResponse is one entry in the leaderboard JSON payload.
// Rank is 1-based — Redis's internal 0-based index is never exposed.
type LeaderboardEntryResponse struct {
	Rank   int    `json:"rank"`
	UserID int    `json:"user_id"`
	Name   string `json:"name"`
	Score  int    `json:"score"`
}

// LeaderboardResponse is the JSON body of GET /leaderboard.
type LeaderboardResponse struct {
	Leaderboard []LeaderboardEntryResponse `json:"leaderboard"`
}

const (
	defaultLeaderboardLimit = 10
	minLeaderboardLimit     = 1
	maxLeaderboardLimit     = 100
)

// GetLeaderboard handles GET /leaderboard?limit=N.
//
// Handler responsibilities:
//   - accept only GET
//   - parse and validate limit (default 10, range 1..100, else 400)
//   - call ScoreService.GetLeaderboard
//   - format the JSON response (200, even when the leaderboard is empty)
func (h *LeaderboardHandler) GetLeaderboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := defaultLeaderboardLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < minLeaderboardLimit || n > maxLeaderboardLimit {
			http.Error(w, "limit must be an integer between 1 and 100", http.StatusBadRequest)
			return
		}
		limit = n
	}

	rows, err := h.Service.GetLeaderboard(limit)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// make(..., 0) guarantees "leaderboard": [] (not null) when empty.
	resp := LeaderboardResponse{
		Leaderboard: make([]LeaderboardEntryResponse, 0, len(rows)),
	}
	for _, row := range rows {
		resp.Leaderboard = append(resp.Leaderboard, LeaderboardEntryResponse{
			Rank:   row.Rank,
			UserID: row.UserID,
			Name:   row.Name,
			Score:  row.Score,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// UserRankResponse is the JSON body of GET /leaderboard/{userID}/rank.
// Rank/BestScore are pointers so they can be omitted for unranked users
// (a nil pointer is omitted; a legitimate score of 0 is still emitted).
type UserRankResponse struct {
	UserID    int    `json:"user_id"`
	Ranked    bool   `json:"ranked"`
	Rank      *int   `json:"rank,omitempty"`
	BestScore *int   `json:"best_score,omitempty"`
	Message   string `json:"message,omitempty"`
}

// GetUserRank handles GET /leaderboard/{userID}/rank.
//
// Handler responsibilities:
//   - accept only GET
//   - parse and validate the path userID (positive integer, else 400)
//   - call ScoreService.GetUserRank
//   - format the JSON response:
//     ranked   → 200 with 1-based rank and best_score
//     unranked → 200 with ranked=false and an explicit message
//     Redis failure → 500
//
// No Redis commands and no SQL live here.
func (h *LeaderboardHandler) GetUserRank(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rawID := r.PathValue("userID")
	userID, err := strconv.Atoi(rawID)
	if err != nil || userID <= 0 {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}

	result, err := h.Service.GetUserRank(userID)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	resp := UserRankResponse{
		UserID: result.UserID,
		Ranked: result.Ranked,
	}
	if result.Ranked {
		rank := result.Rank
		score := result.Score
		resp.Rank = &rank
		resp.BestScore = &score
	} else {
		resp.Message = "user is not currently ranked"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}
