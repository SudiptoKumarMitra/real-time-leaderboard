package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"real_time_leaderboard/internal/middleware"
	"real_time_leaderboard/internal/service"
)

// ScoreHandler handles score submission HTTP requests.
type ScoreHandler struct {
	Service *service.ScoreService
}

// NewScoreHandler creates a new ScoreHandler with the given service.
func NewScoreHandler(svc *service.ScoreService) *ScoreHandler {
	return &ScoreHandler{Service: svc}
}

// SubmitScoreRequest is the JSON request body for score submission.
// It contains ONLY the score — user_id comes from the verified JWT context.
// Score is a pointer so a missing "score" key (nil) can be distinguished
// from an explicit score of 0 (valid).
type SubmitScoreRequest struct {
	Score *int `json:"score"`
}

// SubmitScoreResponse is the JSON response from a successful score submission.
type SubmitScoreResponse struct {
	Status  string `json:"status"`
	ScoreID int    `json:"score_id"`
	Score   int    `json:"score"`
}

// Submit handles POST /scores.
// It is protected by JWT middleware, which places the verified user_id
// into the request context before this handler runs.
//
// Handler responsibilities:
//   - accept only POST
//   - decode JSON
//   - get verified user_id from context (NOT from JSON body)
//   - validate score field is present
//   - call ScoreService.SubmitScore
//   - format HTTP response
func (h *ScoreHandler) Submit(w http.ResponseWriter, r *http.Request) {
	// Accept only POST method
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the verified user_id from the request context (set by JWT middleware)
	userID, ok := middleware.GetUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Decode JSON request body — only {"score": N} is expected.
	// Any user_id in the body is silently ignored (not in the struct).
	var req SubmitScoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Structural validation: score field must be present
	if req.Score == nil {
		http.Error(w, "score is required", http.StatusBadRequest)
		return
	}

	// Call service layer for business logic (validation + DB)
	scoreID, err := h.Service.SubmitScore(userID, *req.Score)
	if err != nil {
		// Invalid score (negative, too large)
		if errors.Is(err, service.ErrInvalidScore) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Unexpected database error
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Success: return 201 Created
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	resp := SubmitScoreResponse{
		Status:  "ok",
		ScoreID: scoreID,
		Score:   *req.Score,
	}
	json.NewEncoder(w).Encode(resp)
}
