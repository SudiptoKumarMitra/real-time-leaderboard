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

	"github.com/redis/go-redis/v9"

	"real_time_leaderboard/db"
	"real_time_leaderboard/internal/consumer"
	"real_time_leaderboard/internal/handler"
	"real_time_leaderboard/internal/middleware"
	"real_time_leaderboard/internal/publisher"
	"real_time_leaderboard/internal/repository"
	"real_time_leaderboard/internal/service"
)

// httpShutdownTimeout bounds the graceful HTTP shutdown so cleanup can
// never hang indefinitely on a stuck request.
const httpShutdownTimeout = 10 * time.Second

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

// shutdownReason states why the application is stopping.
type shutdownReason int

const (
	// reasonSignal: a termination signal (Ctrl+C / SIGTERM) was received —
	// the normal graceful-shutdown path.
	reasonSignal shutdownReason = iota
	// reasonCancelled: the consumer context was already cancelled when the
	// consumer goroutine exited — also normal (shutdown in progress).
	reasonCancelled
	// reasonServerExit: the HTTP server stopped serving on its own.
	reasonServerExit
	// reasonConsumerExit: the consume loop exited while the application
	// context was still active — an unexpected, fatal condition.
	reasonConsumerExit
)

// errConsumerExited marks the fatal case: the consume loop stopped
// without the application context having been cancelled.
var errConsumerExited = errors.New("leaderboard consumer exited unexpectedly")

// awaitShutdown blocks until the first shutdown trigger fires and reports
// which one it was:
//
//   - sig: a termination signal (reasonSignal);
//   - serverErr: the HTTP server's serve loop returned (reasonServerExit,
//     carrying the server error, if any);
//   - consumerWatch: the consumer goroutine exited (closed its done
//     channel). If ctx is still active this is the FATAL case
//     (reasonConsumerExit): the consumer stopped on its own — e.g. after
//     exhausting its offset-commit retries — and the API must not keep
//     accepting scores nobody will consume. If ctx was already cancelled,
//     the exit is the expected result of shutdown (reasonCancelled).
//
// consumerWatch must be a nil channel when no consumer runs, so that case
// can never fire.
func awaitShutdown(ctx context.Context, sig <-chan os.Signal, serverErr <-chan error, consumerWatch <-chan struct{}) (shutdownReason, error) {
	select {
	case <-sig:
		return reasonSignal, nil
	case err := <-serverErr:
		return reasonServerExit, err
	case <-consumerWatch:
		if ctx.Err() != nil {
			return reasonCancelled, nil
		}
		return reasonConsumerExit, errConsumerExited
	}
}

// cleanupRuntime gracefully stops the HTTP server FIRST — within timeout
// (callers pass httpShutdownTimeout) — and only then stops the consumer
// loop. This order matters: the consumer-group shutdown can take tens of
// seconds (kafka-go waits out the group session timeout on Close), so
// HTTP must reject new requests before that wait begins; otherwise the
// API would keep accepting score submissions that nothing is consuming.
// The consumer stop is invoked even when the HTTP shutdown times out or
// fails, so remaining cleanup never gets skipped. Returns the HTTP
// shutdown error, if any.
func cleanupRuntime(srv *http.Server, stopConsumer func(), timeout time.Duration) error {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
	defer shutdownCancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	stopConsumer()
	return shutdownErr
}

// run initializes the application, serves HTTP, and supervises the
// consumer goroutine until a shutdown trigger fires. It always runs the
// full cleanup sequence — gracefully stop the HTTP server first (stop
// accepting new requests, drain in-flight ones), stop the consumer
// (cancel + wait), then close the database pool, Redis client, and Kafka
// writer — and then returns the process exit code. main() exits with that
// code, so a fatal consumer exit surfaces as a non-zero status for the
// process supervisor. No log.Fatal is used after initialization: errors
// propagate as exit codes so deferred cleanup always completes first.
func run() int {
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
	defer cancel()
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
	srv := &http.Server{Addr: ":" + port, Handler: mux}
	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()

	fmt.Printf("Server starting on http://localhost:%s\n", port)
	fmt.Printf("Health endpoint: GET /health\n")
	fmt.Printf("Register endpoint: POST /register\n")
	fmt.Printf("Login endpoint: POST /login\n")
	fmt.Printf("Score endpoint: POST /scores (authenticated)\n")
	fmt.Printf("Leaderboard endpoint: GET /leaderboard\n")
	fmt.Printf("Leaderboard consumer: group leaderboard-writer (disable with CONSUMER_ENABLED=false)\n")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// A nil consumerWatch blocks forever — with the consumer disabled there
	// is no consume loop whose exit could be fatal.
	var consumerWatch <-chan struct{}
	if app.ScoreConsumer != nil {
		consumerWatch = consumerDone
	}

	reason, cause := awaitShutdown(ctx, sigCh, serverErr, consumerWatch)

	exitCode := 0
	switch reason {
	case reasonSignal:
		log.Println("shutdown: termination signal received")
	case reasonCancelled:
		log.Println("shutdown: consumer stopped after context cancellation")
	case reasonServerExit:
		if cause != nil && !errors.Is(cause, http.ErrServerClosed) {
			log.Printf("fatal: HTTP server stopped unexpectedly: %v", cause)
			exitCode = 1
		} else {
			log.Println("shutdown: HTTP server stopped")
		}
	case reasonConsumerExit:
		// Unexpected: the consume loop exited while ctx was still active
		// (e.g. offset-commit retries exhausted). The API must stop
		// accepting traffic — otherwise it would publish events nobody
		// consumes — and the process must exit non-zero so a supervisor
		// restarts it. Never log.Fatal here: cleanup below must run first.
		log.Printf("fatal: leaderboard consumer exited unexpectedly while the application context is still active "+
			"(%v); stopping the HTTP API so no further scores are accepted without a consumer; "+
			"process supervisor should restart the application", cause)
		exitCode = 1
	}

	// Unified cleanup for BOTH normal and fatal paths: stop HTTP FIRST
	// (stops accepting new requests, drains in-flight ones within the
	// timeout), then stop the consumer loop (context cancellation + wait
	// for reader close / group leave — can take ~20s), then close the
	// stores.
	shutdownErr := cleanupRuntime(srv, func() {
		cancel()
		<-consumerDone
	}, httpShutdownTimeout)
	<-serverErr // wait for ListenAndServe to return (http.ErrServerClosed)

	if shutdownErr != nil {
		log.Printf("warning: HTTP server graceful shutdown failed: %v", shutdownErr)
		if exitCode == 0 {
			exitCode = 1
		}
	}

	db.CloseDB(app.DB)
	db.CloseRedis(app.Redis)
	app.Publisher.Close()
	log.Println("shutdown complete")
	return exitCode
}

func main() {
	os.Exit(run())
}
