package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Role represents an access role for RBAC.
type Role string

const (
	RoleWarehouse Role = "warehouse_operator"
	RoleStore     Role = "store_staff"
	RoleBackoffice Role = "backoffice"
	RoleAdmin     Role = "admin"
)

type contextKey string

const claimsKey contextKey = "jwt_claims"

// Claims represents JWT token claims.
type Claims struct {
	Sub       string `json:"sub"`
	Role      string `json:"role"`
	ExpiresAt int64  `json:"exp"`
	IssuedAt  int64  `json:"iat"`
}

// Valid checks if claims are not expired.
func (c Claims) Valid() error {
	if time.Now().Unix() > c.ExpiresAt {
		return fmt.Errorf("token expired")
	}
	return nil
}

// HasRole checks if the claims contain one of the allowed roles.
func (c Claims) HasRole(allowed ...Role) bool {
	for _, r := range allowed {
		if Role(c.Role) == r {
			return true
		}
	}
	return false
}

// ClaimsFromContext extracts claims from context.
func ClaimsFromContext(ctx context.Context) (Claims, bool) {
	c, ok := ctx.Value(claimsKey).(Claims)
	return c, ok
}

// JWTAuth creates HTTP middleware that validates JWT tokens.
func JWTAuth(secret []byte, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, err := extractAndValidateToken(r.Header.Get("Authorization"), secret)
			if err != nil {
				logger.Warn("jwt auth failed", "error", err, "path", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
				return
			}

			ctx := context.WithValue(r.Context(), claimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireRoles creates HTTP middleware that checks role-based access.
func RequireRoles(roles ...Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := ClaimsFromContext(r.Context())
			if !ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
				return
			}

			if !claims.HasRole(roles...) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "forbidden"})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// GRPCAuthInterceptor returns a gRPC unary interceptor that validates JWT from metadata.
func GRPCAuthInterceptor(secret []byte, logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Skip auth for health checks.
		if strings.HasSuffix(info.FullMethod, "/Ping") ||
			strings.Contains(info.FullMethod, "grpc.health") {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		authHeaders := md.Get("authorization")
		if len(authHeaders) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization header")
		}

		claims, err := extractAndValidateToken(authHeaders[0], secret)
		if err != nil {
			logger.Warn("grpc jwt auth failed", "error", err, "method", info.FullMethod)
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}

		ctx = context.WithValue(ctx, claimsKey, claims)
		return handler(ctx, req)
	}
}

// GRPCRequireRoles returns a gRPC unary interceptor that checks RBAC roles.
func GRPCRequireRoles(roles ...Role) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if strings.HasSuffix(info.FullMethod, "/Ping") ||
			strings.Contains(info.FullMethod, "grpc.health") {
			return handler(ctx, req)
		}

		claims, ok := ClaimsFromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "no claims in context")
		}

		if !claims.HasRole(roles...) {
			return nil, status.Error(codes.PermissionDenied, "insufficient permissions")
		}

		return handler(ctx, req)
	}
}

func extractAndValidateToken(authHeader string, secret []byte) (Claims, error) {
	if authHeader == "" {
		return Claims{}, fmt.Errorf("empty authorization header")
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return Claims{}, fmt.Errorf("invalid authorization format")
	}

	token := parts[1]
	claims, err := parseHS256JWT(token, secret)
	if err != nil {
		return Claims{}, fmt.Errorf("invalid token: %w", err)
	}

	if err := claims.Valid(); err != nil {
		return Claims{}, err
	}

	return claims, nil
}

func parseHS256JWT(token string, secret []byte) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, fmt.Errorf("malformed jwt: expected 3 parts, got %d", len(parts))
	}

	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, fmt.Errorf("decode header: %w", err)
	}

	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return Claims{}, fmt.Errorf("unmarshal header: %w", err)
	}
	if header.Alg != "HS256" {
		return Claims{}, fmt.Errorf("unsupported algorithm: %s", header.Alg)
	}

	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, fmt.Errorf("decode payload: %w", err)
	}

	signatureBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Claims{}, fmt.Errorf("decode signature: %w", err)
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	expectedSig := mac.Sum(nil)
	if !hmac.Equal(signatureBytes, expectedSig) {
		return Claims{}, fmt.Errorf("signature verification failed")
	}

	var claims Claims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return Claims{}, fmt.Errorf("unmarshal claims: %w", err)
	}

	return claims, nil
}
