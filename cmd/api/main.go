// Command api is the HTTP API service: user registration/login, JWT
// authentication, score submission (PostgreSQL insert + score.submitted
// publish), and leaderboard/rank reads via the gRPC Query Service
// (leaderboard.v1.LeaderboardQuery). PostgreSQL remains the source of
// truth for score history; the Query Service fronts the Redis projection
// in the separate cmd/query process.
//
// It never joins the leaderboard-writer consumer group — event consumption
// runs in the separate cmd/worker process — and it never touches Redis
// directly.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"real_time_leaderboard/db"
	"real_time_leaderboard/internal/handler"
	"real_time_leaderboard/internal/lifecycle"
	"real_time_leaderboard/internal/middleware"
	"real_time_leaderboard/internal/publisher"
	"real_time_leaderboard/internal/queryclient"
	"real_time_leaderboard/internal/repository"
	"real_time_leaderboard/internal/service"
)

// httpShutdownTimeout bounds the graceful HTTP shutdown so cleanup can
// never hang indefinitely on a stuck request.
const httpShutdownTimeout = 10 * time.Second

// Application holds the API's application state, including the database
// connection pool and the gRPC query client. It deliberately has no Kafka
// consumer and no Redis client: consumption belongs to cmd/worker, and
// leaderboard reads go through the Query Service.
type Application struct {
	DB          *sql.DB
	QueryClient *queryclient.Client
	Publisher   *publisher.EventPublisher
	Hnd         *handler.UserHandler
	ScHnd       *handler.ScoreHandler
	LbHnd       *handler.LeaderboardHandler
	JWTSecret   string
}

// NewApplication initializes the application with a database connection
// pool, a lazy gRPC query client, and handlers. The query client performs
// no startup health ping — the API comes up even if the Query Service is
// down (per-request reads then fail over to the existing fallbacks).
// A malformed QUERY_ADDR returns an error after closing the already
// opened database pool, so no resource leaks on the failure path.
func NewApplication() (*Application, error) {
	databasePool := db.InitDB()

	// Create repository, service, and handler layers
	jwtSecret := service.GetJWTSecret()
	userRepo := repository.NewUserRepository(databasePool)
	userSvc := service.NewUserService(*userRepo, jwtSecret)
	userHnd := handler.NewUserHandler(userSvc)

	scoreRepo := repository.NewScoreRepository(databasePool)
	queryCl, err := queryclient.New()
	if err != nil {
		db.CloseDB(databasePool)
		return nil, err
	}
	eventPub := publisher.NewEventPublisher()

	scoreSvc := service.NewScoreService(*scoreRepo, queryCl, *userRepo, eventPub)
	scoreHnd := handler.NewScoreHandler(scoreSvc)
	lbHnd := handler.NewLeaderboardHandler(scoreSvc)

	return &Application{
		DB:          databasePool,
		QueryClient: queryCl,
		Publisher:   eventPub,
		Hnd:         userHnd,
		ScHnd:       scoreHnd,
		LbHnd:       lbHnd,
		JWTSecret:   jwtSecret,
	}, nil
}

// RegisterRoutes registers HTTP handlers onto the given servemux.
// This keeps the main() function clean and centralizes route configuration.
func (app *Application) RegisterRoutes(mux *http.ServeMux) {
	// Public routes — no JWT required
	mux.HandleFunc("/health", app.healthHandler)
	mux.HandleFunc("/register", app.Hnd.Register)
	mux.HandleFunc("/login", app.Hnd.Login)
	mux.HandleFunc("/leaderboard", app.LbHnd.GetLeaderboard)
	mux.HandleFunc("/leaderboard/{userID}/rank", app.LbHnd.GetUserRank)

	// Protected routes — JWT middleware verifies token before handler runs
	mux.HandleFunc("/scores", middleware.JWTAuth(app.JWTSecret)(app.ScHnd.Submit))
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

// run initializes the API application, serves HTTP, and waits for a
// shutdown trigger. It always runs the full cleanup sequence — gracefully
// stop the HTTP server first (stop accepting new requests, drain in-flight
// ones within httpShutdownTimeout), then close the database pool, the
// gRPC query client, and the Kafka writer — and only then returns the
// process exit code. main() exits with that code so failures surface to
// the process supervisor. No log.Fatal is used after initialization:
// errors propagate as exit codes so cleanup always completes first.
func run() int {
	// Initialize the application with database connectivity and handler.
	// If db.InitDB() fails (e.g., PostgreSQL unreachable), the program exits
	// before the HTTP server starts, preventing an unhealthy server from
	// serving traffic. A malformed QUERY_ADDR likewise fails startup —
	// NewApplication has already closed the database pool at that point.
	app, err := NewApplication()
	if err != nil {
		log.Printf("fatal: %v", err)
		return 1
	}

	// Set up the HTTP request routes, passing the handler struct so handlers
	// can access the service via app.hnd.
	mux := http.NewServeMux()
	app.RegisterRoutes(mux)

	port := "8080"
	srv := &http.Server{Addr: ":" + port, Handler: mux}
	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()

	fmt.Printf("API server starting on http://localhost:%s\n", port)
	fmt.Printf("Health endpoint: GET /health\n")
	fmt.Printf("Register endpoint: POST /register\n")
	fmt.Printf("Login endpoint: POST /login\n")
	fmt.Printf("Score endpoint: POST /scores (authenticated)\n")
	fmt.Printf("Leaderboard endpoint: GET /leaderboard\n")
	fmt.Printf("Leaderboard reads: gRPC query service (QUERY_ADDR, default localhost:50051)\n")
	fmt.Printf("Leaderboard writer: separate process (run cmd/worker)\n")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// This process runs no consumer, so consumerWatch is nil: the
	// consumer-exit arm can never fire here (it is watched in cmd/worker).
	reason, cause := lifecycle.AwaitShutdown(context.Background(), sigCh, serverErr, nil)

	exitCode := 0
	switch reason {
	case lifecycle.ReasonSignal:
		log.Println("shutdown: termination signal received")
	case lifecycle.ReasonServerExit:
		if cause != nil && !errors.Is(cause, http.ErrServerClosed) {
			log.Printf("fatal: HTTP server stopped unexpectedly: %v", cause)
			exitCode = 1
		} else {
			log.Println("shutdown: HTTP server stopped")
		}
	case lifecycle.ReasonCancelled, lifecycle.ReasonConsumerExit:
		// Unreachable with a nil consumerWatch — defensive only, so an
		// unexpected reason can never be silently treated as success.
		log.Printf("unexpected shutdown reason %v (this process runs no consumer): %v", reason, cause)
		exitCode = 1
	}

	// Cleanup for BOTH normal and fatal paths: graceful HTTP shutdown
	// first (bounded, so cleanup cannot hang), then the stores. The
	// shutdown error — including a timeout — never skips the closes
	// below; it is only reflected in the exit code afterwards.
	shutdownErr := lifecycle.ShutdownHTTP(srv, httpShutdownTimeout)
	<-serverErr // wait for ListenAndServe to return (http.ErrServerClosed)

	if shutdownErr != nil {
		log.Printf("warning: HTTP server graceful shutdown failed: %v", shutdownErr)
		if exitCode == 0 {
			exitCode = 1
		}
	}

	db.CloseDB(app.DB)
	if err := app.QueryClient.Close(); err != nil {
		log.Printf("warning: failed to close query client: %v", err)
	}
	app.Publisher.Close()
	log.Println("shutdown complete")
	return exitCode
}

func main() {
	os.Exit(run())
}
