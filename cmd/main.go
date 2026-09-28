package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"real_time_leaderboard/db"
	"real_time_leaderboard/internal/handler"
	"real_time_leaderboard/internal/repository"
	"real_time_leaderboard/internal/service"
)

// Application holds the shared application state, including the database connection pool.
type Application struct {
	DB        *sql.DB
	Hnd       *handler.UserHandler
	JWTSecret string
}

// NewApplication initializes the application with a database connection pool and handler.
func NewApplication() *Application {
	databasePool := db.InitDB()

	// Create repository, service, and handler layers
	jwtSecret := service.GetJWTSecret()
	repo := repository.NewUserRepository(databasePool)
	svc := service.NewUserService(*repo, jwtSecret)
	hnd := handler.NewUserHandler(svc)

	return &Application{
		DB:        databasePool,
		Hnd:       hnd,
		JWTSecret: jwtSecret,
	}
}

// RegisterRoutes registers HTTP handlers onto the given servemux.
// This keeps the main() function clean and centralizes route configuration.
func (app *Application) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/register", app.Hnd.Register)
	mux.HandleFunc("/login", app.Hnd.Login)
}

// HealthHandler handles GET /health endpoint.
func (app *Application) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	resp := HealthResponse{Status: "ok"}
	json.NewEncoder(w).Encode(resp)
}

// HealthResponse is the JSON response structure for the /health endpoint.
type HealthResponse struct {
	Status string `json:"status"`
}

func main() {
	// Initialize the application with database connectivity and handler.
	// If db.InitDB() fails (e.g., PostgreSQL unreachable), the program exits
	// before the HTTP server starts, preventing an unhealthy server from serving traffic.
	app := NewApplication()

	// Set up the HTTP request routes, passing the handler struct so handlers
	// can access the service via app.hnd.
	mux := http.NewServeMux()
	app.RegisterRoutes(mux)

	port := "8080"
	fmt.Printf("Server starting on http://localhost:%s\n", port)
	fmt.Printf("Health endpoint: GET /health\n")
	fmt.Printf("Register endpoint: POST /register\n")
	fmt.Printf("Login endpoint: POST /login\n")

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}

	// The HTTP server has stopped (e.g., via Ctrl+C).
	// Close the database pool to clean up PostgreSQL connections.
	db.CloseDB(app.DB)
}
