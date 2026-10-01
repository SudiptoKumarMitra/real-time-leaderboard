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

// GetTopBestScores computes the top `limit` users by highest score from
// PostgreSQL alone — the fallback path when the Redis leaderboard is empty
// or unavailable.
//
// SELECT user_id, MAX(score) AS best_score
// FROM scores
// GROUP BY user_id
// ORDER BY best_score DESC
// LIMIT $1
//
// Returns rows already sorted descending, identical in shape to the Redis
// fast path (LeaderboardEntry) so the service can merge names the same way.
func (r *ScoreRepository) GetTopBestScores(limit int) ([]LeaderboardEntry, error) {
	const query = `SELECT user_id, MAX(score) AS best_score
		FROM scores
		GROUP BY user_id
		ORDER BY best_score DESC
		LIMIT $1`

	rows, err := r.DB.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]LeaderboardEntry, 0, limit)
	for rows.Next() {
		var e LeaderboardEntry
		if err := rows.Scan(&e.UserID, &e.Score); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}
