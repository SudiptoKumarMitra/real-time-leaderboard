package service

import (
	"errors"
	"log"

	"golang.org/x/crypto/bcrypt"

	"real_time_leaderboard/internal/repository"
)

// bcryptCost is the work factor for bcrypt hashing. 10 is a reasonable default.
const bcryptCost = 10

// ErrDuplicateEmail is returned when the email already exists in the database.
var ErrDuplicateEmail = errors.New("duplicate email")

// ErrInvalidInput is returned when required fields are missing.
var ErrInvalidInput = errors.New("invalid input: email and password are required")
var ErrAuthFailed = errors.New("authentication failed")

// UserService handles registration business logic.
type UserService struct {
	repo repository.UserRepository
}

// NewUserService creates a new UserService with the given repository.
func NewUserService(repo repository.UserRepository) *UserService {
	return &UserService{repo: repo}
}

// bcryptHash wraps bcrypt.GenerateFromPassword for clarity.
func bcryptHash(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

// Register performs user registration business logic.
//
// Parameters:
//
//	name: full name of the user
//	email: email address
//	password: plain-text password from the HTTP request
//
// Returns:
//
//	userID: the newly created user's ID (on success)
//	err: nil on success, or an error describing what went wrong
//
// Business rules applied:
//   - Email and password must not be empty
//   - If the email already exists in the database, return ErrDuplicateEmail
//   - The password is hashed using bcrypt before storage
func (s *UserService) Register(name, email, password string) (int, error) {
	// Basic validation: email and password must not be empty
	if email == "" || password == "" {
		return 0, ErrInvalidInput
	}

	// Hash the password using bcrypt
	hashedPassword, err := bcryptHash(password)
	if err != nil {
		log.Printf("Failed to hash password: %v", err)
		return 0, errors.New("internal error while processing password")
	}

	// Call repository to create the user
	userID, err := s.repo.CreateUser(name, email, hashedPassword)
	if err != nil {
		// Check specifically for duplicate email
		if errors.Is(err, repository.ErrDuplicateEmail) {
			return 0, ErrDuplicateEmail
		}
		return 0, errors.New("database error while creating user")
	}

	return userID, nil
}

// Login performs user login authentication logic.
//
// Parameters:
//
//	email: user's email address
//	password: plain-text password from the HTTP request
//
// Returns:
//
//	userID: the authenticated user's ID (on success)
//	err: nil on success, or ErrAuthFailed if email/password are invalid
//
// Business rules applied:
//   - Email and password must not be empty
//   - User is looked up by email from the database
//   - If user not found, ErrAuthFailed is returned (do not reveal why)
//   - If password does not match the stored bcrypt hash, ErrAuthFailed is returned
//     (do not reveal if it was the email or password that was wrong)
//   - The password is verified using bcrypt.CompareHashAndPassword
func (s *UserService) Login(email, password string) (int, error) {
	// Basic validation: email and password must not be empty
	if email == "" || password == "" {
		return 0, ErrInvalidInput
	}

	// Call repository to find user by email
	user, err := s.repo.FindByEmail(email)
	if err != nil {
		// User not found - return generic auth error
		return 0, ErrAuthFailed
	}

	// Verify password using bcrypt comparison
	err = bcrypt.CompareHashAndPassword([]byte(user.Hash), []byte(password))
	if err != nil {
		// Password mismatch - return generic auth error
		return 0, ErrAuthFailed
	}

	return user.ID, nil
}
