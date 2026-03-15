package handlers

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/dns/logistics/shipment-core-service/internal/domain"
	"github.com/dns/logistics/shipment-core-service/internal/logging"
	"github.com/dns/logistics/shipment-core-service/internal/metrics"
	"github.com/dns/logistics/shipment-core-service/internal/middleware"
	"github.com/dns/logistics/shipment-core-service/internal/repository"
	"github.com/dns/logistics/shipment-core-service/internal/service"
)

// HTTPHandler serves shipment-core read APIs.
type HTTPHandler struct {
	logger    *slog.Logger
	service   *service.CoreService
	metrics   *metrics.Collector
	jwtSecret []byte
	startedAt time.Time
}

// NewHTTPHandler creates HTTP API handler.
func NewHTTPHandler(logger *slog.Logger, svc *service.CoreService, metricCollector *metrics.Collector, jwtSecret []byte) *HTTPHandler {
	return &HTTPHandler{
		logger:    logger,
		service:   svc,
		metrics:   metricCollector,
		jwtSecret: jwtSecret,
		startedAt: time.Now().UTC(),
	}
}

// Router builds HTTP routes.
func (h *HTTPHandler) Router() http.Handler {
	r := chi.NewRouter()

	// Public endpoints — no auth required.
	r.Get("/healthz", h.handleHealth)
	r.Get("/readyz", h.handleReady)

	// Protected API — JWT auth + RBAC.
	r.Group(func(r chi.Router) {
		if len(h.jwtSecret) > 0 {
			r.Use(middleware.JWTAuth(h.jwtSecret, h.logger))
			r.Use(middleware.RequireRoles(
				middleware.RoleWarehouse,
				middleware.RoleStore,
				middleware.RoleBackoffice,
				middleware.RoleAdmin,
			))
		}
		r.Get("/api/v1/shipments/{shipment_id}", h.handleGetShipment)
		r.Get("/api/v1/shipments", h.handleListShipments)
	})

	return r
}

func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	h.metrics.ObserveHTTPRequest(r.Context(), "/healthz", http.StatusOK)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"service":    "shipment-core-service",
		"started_at": h.startedAt,
	})
}

func (h *HTTPHandler) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := h.service.IsReady(r.Context()); err != nil {
		h.metrics.ObserveHTTPRequest(r.Context(), "/readyz", http.StatusServiceUnavailable)
		h.logger.Error("readiness probe failed", append([]any{"error", err}, logging.TraceFields(r.Context())...)...)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}

	h.metrics.ObserveHTTPRequest(r.Context(), "/readyz", http.StatusOK)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *HTTPHandler) handleGetShipment(w http.ResponseWriter, r *http.Request) {
	shipmentID := chi.URLParam(r, "shipment_id")
	state, err := h.service.GetShipment(r.Context(), shipmentID)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, service.ErrInvalidShipmentID):
			status = http.StatusBadRequest
		case errors.Is(err, repository.ErrShipmentNotFound):
			status = http.StatusNotFound
		}
		h.metrics.ObserveHTTPRequest(r.Context(), "/api/v1/shipments/{shipment_id}", status)
		h.logger.Warn("get shipment failed", append([]any{"error", err, "shipment_id", shipmentID}, logging.TraceFields(r.Context())...)...)
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}

	h.metrics.ObserveHTTPRequest(r.Context(), "/api/v1/shipments/{shipment_id}", http.StatusOK)
	writeJSON(w, http.StatusOK, state)
}

func (h *HTTPHandler) handleListShipments(w http.ResponseWriter, r *http.Request) {
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 100)
	offset := parseIntOrDefault(r.URL.Query().Get("offset"), 0)
	filters := domain.ShipmentFilters{
		StatusCode:   strings.TrimSpace(r.URL.Query().Get("status_code")),
		SourceSystem: strings.TrimSpace(r.URL.Query().Get("source_system")),
		RouteID:      strings.TrimSpace(r.URL.Query().Get("route_id")),
		Limit:        limit,
		Offset:       offset,
	}

	states, err := h.service.ListShipments(r.Context(), filters)
	if err != nil {
		h.metrics.ObserveHTTPRequest(r.Context(), "/api/v1/shipments", http.StatusInternalServerError)
		h.logger.Error("list shipments failed", append([]any{"error", err}, logging.TraceFields(r.Context())...)...)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	h.metrics.ObserveHTTPRequest(r.Context(), "/api/v1/shipments", http.StatusOK)
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  states,
		"limit":  limit,
		"offset": offset,
	})
}

func parseIntOrDefault(raw string, fallback int) int {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
