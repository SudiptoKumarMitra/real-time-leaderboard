package service

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"real_time_leaderboard/internal/repository"
)

// bcryptCost is the work factor for bcrypt hashing. 10 is a reasonable default.
const bcryptCost = 10

// jwtExpiryDuration is how long a login token remains valid.
const jwtExpiryDuration = 24 * time.Hour

// ErrDuplicateEmail is returned when the email already exists in the database.
var ErrDuplicateEmail = errors.New("duplicate email")

// ErrInvalidInput is returned when required fields are missing.
var ErrInvalidInput = errors.New("invalid input: email and password are required")

// ErrAuthFailed is returned when authentication fails (bad email or bad password).
var ErrAuthFailed = errors.New("authentication failed")

// UserService handles authentication and registration business logic.
type UserService struct {
	repo      repository.UserRepository
	jwtSecret []byte
}

// NewUserService creates a new UserService with the given repository and JWT secret.
func NewUserService(repo repository.UserRepository, jwtSecret string) *UserService {
	return &UserService{
		repo:      repo,
		jwtSecret: []byte(jwtSecret),
	}
}

// bcryptHash wraps bcrypt.GenerateFromPassword for clarity.
func bcryptHash(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

// generateToken creates a signed JWT containing the user ID and expiration.
func (s *UserService) generateToken(userID int) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"user_id": userID,
		"iat":     now.Unix(),
		"exp":     now.Add(jwtExpiryDuration).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(s.jwtSecret)
	if err != nil {
		log.Printf("Failed to sign JWT: %v", err)
		return "", errors.New("internal error while generating token")
	}
	return signed, nil
}

// GetJWTSecret returns the JWT secret from the environment.
// It does NOT provide a hardcoded default — the caller must set JWT_SECRET.
func GetJWTSecret() string {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		log.Fatal("JWT_SECRET environment variable is required")
	}
	return secret
}

// Register performs user registration business logic.
func (s *UserService) Register(name, email, password string) (int, error) {
	if email == "" || password == "" {
		return 0, ErrInvalidInput
	}

	hashedPassword, err := bcryptHash(password)
	if err != nil {
		log.Printf("Failed to hash password: %v", err)
		return 0, errors.New("internal error while processing password")
	}

	userID, err := s.repo.CreateUser(name, email, hashedPassword)
	if err != nil {
		if errors.Is(err, repository.ErrDuplicateEmail) {
			return 0, ErrDuplicateEmail
		}
		return 0, errors.New("database error while creating user")
	}

	return userID, nil
}

// Login verifies user credentials and returns the user ID plus a signed JWT.
//
// Business rules:
//   - Email and password must not be empty
//   - User is looked up by email from the database
//   - If user not found, return ErrAuthFailed (do not reveal why)
//   - Password is verified using bcrypt.CompareHashAndPassword
//   - If password does not match, return ErrAuthFailed (do not reveal why)
//   - On success, generate and return a JWT containing user_id and expiration
func (s *UserService) Login(email, password string) (int, string, error) {
	if email == "" || password == "" {
		return 0, "", ErrInvalidInput
	}

	user, err := s.repo.FindByEmail(email)
	if err != nil {
		return 0, "", ErrAuthFailed
	}

	err = bcrypt.CompareHashAndPassword([]byte(user.Hash), []byte(password))
	if err != nil {
		return 0, "", ErrAuthFailed
	}

	token, err := s.generateToken(user.ID)
	if err != nil {
		return 0, "", err
	}

	return user.ID, token, nil
}
