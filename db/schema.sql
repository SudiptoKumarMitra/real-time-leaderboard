-- SQL schema for the real_time_leaderboard application.
-- Run this via: psql "host=localhost port=5432 user=postgres dbname=leaderboard" < schema.sql
-- OR via Go's db.Exec() after InitDB() is called.

-- Create the users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create the scores table
-- one user → many scores (one-to-many via Foreign Key)
-- score can be 0 (no CHECK constraint)
-- same user can submit same score multiple times (no UNIQUE user_id + score)
-- deleting a user deletes their score history (ON DELETE CASCADE)
CREATE TABLE IF NOT EXISTS scores (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    score INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index: fast lookup of a user's scores by user_id
-- Used when retrieving score history for a specific user
CREATE INDEX IF NOT EXISTS idx_scores_user_id ON scores(user_id);

-- Index: composite index for efficient "get highest score per user"
-- (user_id, score DESC) means: within each user, scores are sorted DESC
-- This makes MAX(score) operations and leaderboard queries efficient
CREATE INDEX IF NOT EXISTS idx_scores_user_score ON scores(user_id, score DESC);