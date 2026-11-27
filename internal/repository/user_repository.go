package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ErrDuplicateEmail is returned when the email already exists in the database.
var ErrDuplicateEmail = errors.New("duplicate email")

// ErrUserNotFound is returned when a user with the given email does not exist.
var ErrUserNotFound = errors.New("user not found")

// User represents a user account returned from the repository.
type User struct {
	ID    int
	Name  string
	Email string
	Hash  string
}

// UserRepository handles database operations for user accounts.
type UserRepository struct {
	DB *sql.DB
}

// NewUserRepository creates a new UserRepository with the given database connection pool.
func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{DB: db}
}

// FindByEmail retrieves a user by email from the PostgreSQL users table.
// Returns ErrUserNotFound if no user matches the given email.
func (r *UserRepository) FindByEmail(email string) (*User, error) {
	const query = `SELECT id, name, email, password_hash FROM users WHERE email = $1`

	var u User
	err := r.DB.QueryRow(query, email).Scan(&u.ID, &u.Name, &u.Email, &u.Hash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return &u, nil
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

// GetNamesByIDs fetches names for a set of user IDs in ONE query.
//
// SELECT id, name FROM users WHERE id IN ($1, $2, ..., $n)
//
// This avoids the N+1 anti-pattern: a single round trip regardless of how
// many leaderboard rows need names. Placeholders are built per element and
// values are passed as query arguments (still parameterized — no string
// interpolation of data, so no SQL injection).
//
// Returns a map keyed by user ID for O(1) merging with ranking results.
// IDs not found (e.g. deleted users still lingering in Redis) are simply
// absent from the map. An empty id list short-circuits without querying.
func (r *UserRepository) GetNamesByIDs(ids []int) (map[int]string, error) {
	names := make(map[int]string, len(ids))
	if len(ids) == 0 {
		return names, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`SELECT id, name FROM users WHERE id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := r.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		names[id] = name
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, nil
}
