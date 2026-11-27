// Command query is the leaderboard Query Service: a gRPC server that
// serves read-only leaderboard data (GetLeaderboard, GetUserRank) from
// the Redis projection maintained by cmd/worker, plus the one batch
// PostgreSQL name lookup needed for display-ready rows.
//
// It never publishes or consumes Kafka events and never writes Redis —
// score events keep flowing through the existing Kafka pipeline.
// There is intentionally no restart loop: after an unexpected server
// exit the process ends with a non-zero code and a supervisor (or an
// operator) restarts it.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"real_time_leaderboard/db"
	leaderboardv1 "real_time_leaderboard/internal/gen/leaderboard/v1"
	"real_time_leaderboard/internal/lifecycle"
	"real_time_leaderboard/internal/queryservice"
	"real_time_leaderboard/internal/repository"
)

// grpcShutdownTimeout bounds GracefulStop so cleanup can never hang
// indefinitely on a stuck RPC; Stop() then force-closes.
const grpcShutdownTimeout = 10 * time.Second

func getEnv(key, defaultValue string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return defaultValue
}

// run initializes the stores, serves gRPC until a shutdown trigger
// fires, and always performs cleanup — graceful stop (bounded), force
// stop fallback, close PostgreSQL, close Redis — before returning the
// process exit code. main() exits with that code so an unexpected server
// exit surfaces as a non-zero status for the process supervisor. No
// log.Fatal is used after initialization: errors propagate as exit
// codes so cleanup always completes first.
func run() int {
	// Redis: the leaderboard projection this service reads (fail-fast
	// ping via db.InitRedis, matching the other entry points).
	//
	// PostgreSQL: needed ONLY for the batch display-name lookup
	// (UserRepository.GetNamesByIDs — one query per GetLeaderboard call,
	// never N+1). The HTTP /leaderboard response includes names, so the
	// query contract carries them; Redis alone cannot provide them.
	redisClient := db.InitRedis()
	databasePool := db.InitDB()

	lbRepo := repository.NewLeaderboardRepository(redisClient)
	userRepo := repository.NewUserRepository(databasePool)
	srv := grpc.NewServer()
	leaderboardv1.RegisterLeaderboardQueryServer(srv, queryservice.NewServer(lbRepo, userRepo))

	// Standard gRPC health service + reflection: grpcurl-friendly local
	// development and supervisor health checks. No auth on this port —
	// the default listen address is localhost-only.
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("leaderboard.v1.LeaderboardQuery", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthServer)
	reflection.Register(srv)

	addr := getEnv("QUERY_GRPC_ADDR", "localhost:50051")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("fatal: failed to listen on %s: %v", addr, err)
		db.CloseDB(databasePool)
		db.CloseRedis(redisClient)
		return 1
	}

	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.Serve(lis) }()

	log.Printf("leaderboard query service listening on %s (grpc: LeaderboardQuery, health, reflection)", addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// serverErr carries Serve's return; consumerWatch is nil — this
	// process has no consume loop (that is cmd/worker's job).
	reason, cause := lifecycle.AwaitShutdown(context.Background(), sigCh, serverErr, nil)

	exitCode := 0
	switch reason {
	case lifecycle.ReasonSignal:
		log.Println("shutdown: termination signal received")
	case lifecycle.ReasonCancelled:
		log.Println("shutdown: cancelled")
	case lifecycle.ReasonServerExit:
		if cause != nil {
			log.Printf("fatal: gRPC server stopped unexpectedly: %v", cause)
			exitCode = 1
		} else {
			log.Println("shutdown: gRPC server stopped")
		}
	case lifecycle.ReasonConsumerExit:
		// Unreachable with a nil consumerWatch — defensive only.
		log.Printf("unexpected shutdown reason %v: %v", reason, cause)
		exitCode = 1
	}

	// Cleanup for BOTH normal and fatal paths, before the exit code is
	// returned: fail health checks first, then bounded graceful stop
	// (in-flight RPCs finish), force Stop() if it stalls, then close the
	// stores. No log.Fatal below — errors only affect the exit code.
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), grpcShutdownTimeout)
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-stopCtx.Done():
		log.Printf("warning: graceful stop exceeded %s, forcing Stop()", grpcShutdownTimeout)
		srv.Stop()
	}
	stopCancel()
	<-serverErr // Serve returns (grpc.ErrServerStopped or nil) after stop

	db.CloseDB(databasePool)
	db.CloseRedis(redisClient)
	log.Println("shutdown complete")
	return exitCode
}

func main() {
	os.Exit(run())
}
