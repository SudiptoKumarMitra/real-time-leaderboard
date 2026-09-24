package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"real_time_leaderboard/internal/service"
)

// LoginRequest is the JSON request body for user login.
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// LoginResponse is the JSON response from login.
type LoginResponse struct {
	UserID  int    `json:"user_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// Login handles POST /login.
func (h *UserHandler) Login(w http.ResponseWriter, r *http.Request) {
	// Accept only POST method
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode JSON request body
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate basic fields
	if req.Email == "" || req.Password == "" {
		http.Error(w, "email and password are required", http.StatusBadRequest)
		return
	}

	// Call service layer for business logic (validation + password check + DB)
	userID, err := h.Service.Login(req.Email, req.Password)
	if err != nil {
		// Authentication failed (bad email or bad password)
		if errors.Is(err, service.ErrAuthFailed) {
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}
		// Unexpected database error
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Success: return 200 OK
	w.Header().Set("Content-Type", "application/json")
	resp := LoginResponse{
		UserID:  userID,
		Status:  "ok",
		Message: "user logged in successfully",
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}