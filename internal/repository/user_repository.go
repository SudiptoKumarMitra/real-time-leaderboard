package repository

import (
	"database/sql"
	"errors"
)

// ErrDuplicateEmail is returned when the email already exists in the database.
var ErrDuplicateEmail = errors.New("duplicate email")

// UserRepository handles database operations for user accounts.
type UserRepository struct {
	DB *sql.DB
}

// NewUserRepository creates a new UserRepository with the given database connection pool.
func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{DB: db}
}

// CreateUser inserts a new user into the PostgreSQL users table.
//
// Parameters:
//
//	name: full name of the user
//	email: email address (must be unique)
//	passwordHash: bcrypt hash of the user's password
//
// Returns the newly created user ID on success, or an error on failure.
//
// The SQL query uses parameterized placeholders ($1, $2, $3) to prevent SQL injection.
func (r *UserRepository) CreateUser(name, email, passwordHash string) (int, error) {
	const query = `INSERT INTO users (name, email, password_hash)
	VALUES ($1, $2, $3) RETURNING id`

	var id int
	err := r.DB.QueryRow(query, name, email, passwordHash).Scan(&id)
	if err != nil {
		// Check for unique constraint violation on email
		if err.Error() == "pq: duplicate key value violates unique constraint \"users_email_key\"" {
			return 0, errors.New("duplicate email")
		}
		return 0, err
	}

	return id, nil
}
