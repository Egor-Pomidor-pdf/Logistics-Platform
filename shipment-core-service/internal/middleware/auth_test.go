package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// JWT helper — builds HS256 tokens for tests
// ---------------------------------------------------------------------------

var testSecret = []byte("super-secret-key-for-testing")

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

func buildToken(header jwtHeader, claims Claims, secret []byte) string {
	hJSON, _ := json.Marshal(header)
	cJSON, _ := json.Marshal(claims)

	h := base64.RawURLEncoding.EncodeToString(hJSON)
	p := base64.RawURLEncoding.EncodeToString(cJSON)

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(h + "." + p))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return h + "." + p + "." + sig
}

func validClaims() Claims {
	return Claims{
		Sub:       "user-42",
		Role:      "admin",
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
		IssuedAt:  time.Now().Unix(),
	}
}

func expiredClaims() Claims {
	return Claims{
		Sub:       "user-42",
		Role:      "admin",
		ExpiresAt: time.Now().Add(-time.Hour).Unix(),
		IssuedAt:  time.Now().Add(-2 * time.Hour).Unix(),
	}
}

func hs256Header() jwtHeader {
	return jwtHeader{Alg: "HS256", Typ: "JWT"}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// ---------------------------------------------------------------------------
// parseHS256JWT tests
// ---------------------------------------------------------------------------

func TestParseHS256JWT_Valid(t *testing.T) {
	token := buildToken(hs256Header(), validClaims(), testSecret)
	claims, err := parseHS256JWT(token, testSecret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if claims.Sub != "user-42" {
		t.Fatalf("expected sub user-42, got %s", claims.Sub)
	}
	if claims.Role != "admin" {
		t.Fatalf("expected role admin, got %s", claims.Role)
	}
}

func TestParseHS256JWT_InvalidSignature(t *testing.T) {
	token := buildToken(hs256Header(), validClaims(), []byte("wrong-secret"))
	_, err := parseHS256JWT(token, testSecret)
	if err == nil {
		t.Fatal("expected signature verification error")
	}
}

func TestParseHS256JWT_ExpiredToken(t *testing.T) {
	// parseHS256JWT does NOT check expiration — it only parses.
	// Expiration is checked by Claims.Valid(). So this should succeed.
	token := buildToken(hs256Header(), expiredClaims(), testSecret)
	claims, err := parseHS256JWT(token, testSecret)
	if err != nil {
		t.Fatalf("parseHS256JWT should succeed for expired token (expiry checked later): %v", err)
	}
	if err := claims.Valid(); err == nil {
		t.Fatal("expected Valid() to return error for expired claims")
	}
}

func TestParseHS256JWT_WrongAlgorithm(t *testing.T) {
	header := jwtHeader{Alg: "RS256", Typ: "JWT"}
	token := buildToken(header, validClaims(), testSecret)
	_, err := parseHS256JWT(token, testSecret)
	if err == nil {
		t.Fatal("expected error for unsupported algorithm")
	}
}

// ---------------------------------------------------------------------------
// JWTAuth middleware tests
// ---------------------------------------------------------------------------

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})
}

func TestJWTAuth_ValidToken(t *testing.T) {
	token := buildToken(hs256Header(), validClaims(), testSecret)

	handler := JWTAuth(testSecret, testLogger())(okHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestJWTAuth_MissingToken(t *testing.T) {
	handler := JWTAuth(testSecret, testLogger())(okHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestJWTAuth_ExpiredToken(t *testing.T) {
	token := buildToken(hs256Header(), expiredClaims(), testSecret)

	handler := JWTAuth(testSecret, testLogger())(okHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// RequireRoles tests
// ---------------------------------------------------------------------------

func TestRequireRoles_MatchingRole(t *testing.T) {
	token := buildToken(hs256Header(), validClaims(), testSecret) // role=admin

	chain := JWTAuth(testSecret, testLogger())(
		RequireRoles(RoleAdmin)(okHandler()),
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	chain.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestRequireRoles_NonMatchingRole(t *testing.T) {
	token := buildToken(hs256Header(), validClaims(), testSecret) // role=admin

	chain := JWTAuth(testSecret, testLogger())(
		RequireRoles(RoleWarehouse)(okHandler()),
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	chain.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestRequireRoles_NoClaims(t *testing.T) {
	// Call RequireRoles without JWTAuth in the chain -> no claims in context.
	handler := RequireRoles(RoleAdmin)(okHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Claims.HasRole tests
// ---------------------------------------------------------------------------

func TestClaims_HasRole_Matching(t *testing.T) {
	c := Claims{Role: "admin"}
	if !c.HasRole(RoleWarehouse, RoleAdmin) {
		t.Fatal("expected HasRole to return true for admin")
	}
}

func TestClaims_HasRole_NonMatching(t *testing.T) {
	c := Claims{Role: "admin"}
	if c.HasRole(RoleWarehouse, RoleStore) {
		t.Fatal("expected HasRole to return false")
	}
}

func TestClaims_HasRole_Empty(t *testing.T) {
	c := Claims{Role: "admin"}
	if c.HasRole() {
		t.Fatal("expected HasRole with no roles to return false")
	}
}
