package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
	_ "github.com/jackc/pgx/v5/stdlib"

)

// InitDB opens a connection to PostgreSQL and verifies it with a Ping.
func InitDB() *sql.DB {
	dsn := DSN()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Set maximum number of connections in the pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5) // 5 minutes

	// Verify the connection is alive by pinging the database
	if err := db.Ping(); err != nil {
		db.Close()
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Println("✅ PostgreSQL connection pool established")
	return db
}

// CloseDB closes all connections in the pool.
func CloseDB(db *sql.DB) {
	db.Close()
	log.Println("🔌 PostgreSQL connection pool closed")
}

// DSN returns the PostgreSQL Data Source Name from environment variables.
func DSN() string {
	host := getEnv("POSTGRES_HOST", "localhost")
	port := getEnv("POSTGRES_PORT", "5432")
	user := getEnv("POSTGRES_USER", "postgres")
	password := getEnv("POSTGRES_PASSWORD", "db")
	dbname := getEnv("POSTGRES_DB", "leaderboard")

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
}

func getEnv(key, defaultValue string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return defaultValue
}
