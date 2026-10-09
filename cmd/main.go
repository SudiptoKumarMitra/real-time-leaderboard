package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/redis/go-redis/v9"

	"real_time_leaderboard/db"
	"real_time_leaderboard/internal/consumer"
	"real_time_leaderboard/internal/handler"
	"real_time_leaderboard/internal/middleware"
	"real_time_leaderboard/internal/publisher"
	"real_time_leaderboard/internal/repository"
	"real_time_leaderboard/internal/service"
)

// Application holds the shared application state, including the database connection pool.
type Application struct {
	DB        *sql.DB
	Redis     *redis.Client
	Publisher *publisher.EventPublisher
	// ScoreConsumer is the leaderboard consumer (group leaderboard-writer).
	// It is nil when disabled via CONSUMER_ENABLED=false: constructing a
	// group reader joins the consumer group immediately, so the reader must
	// only be built when this process will actually run the consume loop.
	ScoreConsumer *consumer.ScoreConsumer
	Hnd           *handler.UserHandler
	ScHnd         *handler.ScoreHandler
	LbHnd         *handler.LeaderboardHandler
	JWTSecret     string
}

// NewApplication initializes the application with a database connection pool and handler.
func NewApplication() *Application {
	databasePool := db.InitDB()
	redisClient := db.InitRedis()

	// Create repository, service, and handler layers
	jwtSecret := service.GetJWTSecret()
	userRepo := repository.NewUserRepository(databasePool)
	userSvc := service.NewUserService(*userRepo, jwtSecret)
	userHnd := handler.NewUserHandler(userSvc)

	scoreRepo := repository.NewScoreRepository(databasePool)
	lbRepo := repository.NewLeaderboardRepository(redisClient)
	eventPub := publisher.NewEventPublisher()

	// Leaderboard consumer: created ONCE during application startup (one
	// kafka.Reader for the whole process — never per message/request, no
	// globals). It consumes score.submitted and applies ZADD GT to Redis,
	// running alongside the API's direct Redis write during the migration's
	// dual-write phase.
	var scoreConsumer *consumer.ScoreConsumer
	if os.Getenv("CONSUMER_ENABLED") != "false" {
		scoreConsumer = consumer.NewScoreConsumer(consumer.DefaultReaderConfig(), lbRepo)
	}

	scoreSvc := service.NewScoreService(*scoreRepo, *lbRepo, *userRepo, eventPub)
	scoreHnd := handler.NewScoreHandler(scoreSvc)
	lbHnd := handler.NewLeaderboardHandler(scoreSvc)

	return &Application{
		DB:            databasePool,
		Redis:         redisClient,
		Publisher:     eventPub,
		ScoreConsumer: scoreConsumer,
		Hnd:           userHnd,
		ScHnd:         scoreHnd,
		LbHnd:         lbHnd,
		JWTSecret:     jwtSecret,
	}
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

func main() {
	// Initialize the application with database connectivity and handler.
	// If db.InitDB() fails (e.g., PostgreSQL unreachable), the program exits
	// before the HTTP server starts, preventing an unhealthy server from serving traffic.
	app := NewApplication()

	// Set up the HTTP request routes, passing the handler struct so handlers
	// can access the service via app.hnd.
	mux := http.NewServeMux()
	app.RegisterRoutes(mux)

	// Start the leaderboard consumer in a goroutine. Shutdown is driven by
	// context cancellation: cancel() unblocks FetchMessage, Run returns and
	// closes the reader (final commit attempt + leaving the group).
	ctx, cancel := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	if app.ScoreConsumer != nil {
		go func() {
			defer close(consumerDone)
			app.ScoreConsumer.Run(ctx)
		}()
	} else {
		log.Println("leaderboard consumer disabled (CONSUMER_ENABLED=false)")
		close(consumerDone)
	}

	port := "8080"
	fmt.Printf("Server starting on http://localhost:%s\n", port)
	fmt.Printf("Health endpoint: GET /health\n")
	fmt.Printf("Register endpoint: POST /register\n")
	fmt.Printf("Login endpoint: POST /login\n")
	fmt.Printf("Score endpoint: POST /scores (authenticated)\n")
	fmt.Printf("Leaderboard endpoint: GET /leaderboard\n")
	fmt.Printf("Leaderboard consumer: group leaderboard-writer (disable with CONSUMER_ENABLED=false)\n")

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}

	// The HTTP server has stopped (e.g., via Ctrl+C).
	// Stop the consumer loop first (context cancellation), wait for it to
	// close the reader and leave the group, then close the database pool,
	// Redis client, and Kafka writer.
	cancel()
	<-consumerDone
	db.CloseDB(app.DB)
	db.CloseRedis(app.Redis)
	app.Publisher.Close()
}
