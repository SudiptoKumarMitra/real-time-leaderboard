package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"real_time_leaderboard/internal/service"
)

// UserHandler handles registration HTTP requests.
type UserHandler struct {
	Service *service.UserService
}

// NewUserService creates a new UserHandler with the given service.
func NewUserHandler(svc *service.UserService) *UserHandler {
	return &UserHandler{Service: svc}
}

// RegisterRequest is the JSON request body for user registration.
type RegisterRequest struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

// RegisterResponse is the JSON response from registration.
type RegisterResponse struct {
	UserID  int    `json:"user_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// Register handles POST /register.
func (h *UserHandler) Register(w http.ResponseWriter, r *http.Request) {
	// Accept only POST method
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode JSON request body
	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate basic fields
	if req.Name == "" || req.Email == "" || req.Password == "" {
		http.Error(w, "name, email, and password are required", http.StatusBadRequest)
		return
	}

	// Call service layer for business logic (validation + hashing + DB)
	userID, err := h.Service.Register(req.Name, req.Email, req.Password)
	if err != nil {
		// Duplicate email
		if errors.Is(err, service.ErrDuplicateEmail) {
			http.Error(w, "duplicate email", http.StatusConflict)
			return
		}
		// Invalid input
		if errors.Is(err, service.ErrInvalidInput) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Unexpected database error
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Success: return 201 Created
	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	resp := RegisterResponse{
		UserID:  userID,
		Status:  "ok",
		Message: "user registered successfully",
	}
	json.NewEncoder(w).Encode(resp)
}