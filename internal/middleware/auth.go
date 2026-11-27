package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// contextKey is an unexported type for context keys defined in this package.
// This prevents collisions with context keys defined in other packages.
type contextKey string

// UserIDKey is the context key under which the verified user ID is stored.
const UserIDKey contextKey = "user_id"

// JWTAuth returns middleware that verifies a Bearer JWT on every request.
// It wraps the provided handler: the wrapped handler only runs when the
// token is valid. On failure it writes HTTP 401 and never calls next.
//
// The secret must be the same value used to sign tokens during login.
func JWTAuth(secret string) func(http.HandlerFunc) http.HandlerFunc {
	secretBytes := []byte(secret)

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// 1. Read the Authorization header
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, "missing token", http.StatusUnauthorized)
				return
			}

			// 2. Expect exactly "Bearer <token>"
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
				http.Error(w, "invalid token format", http.StatusUnauthorized)
				return
			}
			tokenString := parts[1]

			// 3. Parse and verify the JWT signature and expiration
			token, err := jwt.Parse(tokenString, func(t *jwt.Token) (interface{}, error) {
				// Verify the signing method is HMAC (HS256) — reject "none" or RSA
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, errors.New("unexpected signing method")
				}
				return secretBytes, nil
			})
			if err != nil {
				if errors.Is(err, jwt.ErrTokenExpired) {
					http.Error(w, "token expired", http.StatusUnauthorized)
					return
				}
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}

			// 4. Confirm the token was actually parsed as valid
			if !token.Valid {
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}

			// 5. Extract claims and user_id
			claims, ok := token.Claims.(jwt.MapClaims)
			if !ok {
				http.Error(w, "invalid token claims", http.StatusUnauthorized)
				return
			}

			// JSON numbers decode as float64; convert to int
			userIDFloat, ok := claims["user_id"].(float64)
			if !ok {
				http.Error(w, "missing user_id claim", http.StatusUnauthorized)
				return
			}
			userID := int(userIDFloat)

			// 6. Put the verified user_id into the request context
			ctx := context.WithValue(r.Context(), UserIDKey, userID)
			r = r.WithContext(ctx)

			// 7. Call the next handler — only reached when verification succeeds
			next.ServeHTTP(w, r)
		}
	}
}

// GetUserID extracts the verified user ID from the request context.
// Returns the user ID and true when present, or 0 and false when absent.
// Handlers should use this instead of reading user_id from any request body.
func GetUserID(r *http.Request) (int, bool) {
	userID, ok := r.Context().Value(UserIDKey).(int)
	return userID, ok
}
