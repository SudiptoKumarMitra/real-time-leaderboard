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

// LoginResponse is the JSON response from a successful login.
type LoginResponse struct {
	UserID  int    `json:"user_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Token   string `json:"token"`
}

// Login handles POST /login.
// It decodes JSON, validates input, calls the service, and formats the response.
// It does NOT perform SQL or bcrypt operations.
func (h *UserHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Email == "" || req.Password == "" {
		http.Error(w, "email and password are required", http.StatusBadRequest)
		return
	}

	userID, token, err := h.Service.Login(req.Email, req.Password)
	if err != nil {
		if errors.Is(err, service.ErrAuthFailed) {
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}
		if errors.Is(err, service.ErrInvalidInput) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := LoginResponse{
		UserID:  userID,
		Status:  "ok",
		Message: "user logged in successfully",
		Token:   token,
	}
	json.NewEncoder(w).Encode(resp)
}
