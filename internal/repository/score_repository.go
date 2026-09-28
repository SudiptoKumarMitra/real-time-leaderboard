package repository

import (
	"database/sql"
)

// ScoreRepository handles database operations for score submissions.
type ScoreRepository struct {
	DB *sql.DB
}

// NewScoreRepository creates a new ScoreRepository with the given database connection pool.
func NewScoreRepository(db *sql.DB) *ScoreRepository {
	return &ScoreRepository{DB: db}
}

// InsertScore inserts a new score row into the PostgreSQL scores table.
//
// Every submission creates a new row — score history is preserved.
// The same user may submit the same score multiple times.
// Score 0 is accepted (no CHECK constraint).
//
// Uses parameterized placeholders ($1, $2) to prevent SQL injection.
// Returns the newly created score ID on success.
func (r *ScoreRepository) InsertScore(userID, score int) (int, error) {
	const query = `INSERT INTO scores (user_id, score) VALUES ($1, $2) RETURNING id`

	var id int
	err := r.DB.QueryRow(query, userID, score).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}
