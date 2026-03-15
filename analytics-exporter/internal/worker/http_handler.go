package worker

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	analyticsmetrics "github.com/dns/logistics/analytics-exporter/internal/metrics"
)

// HTTPHandler wires HTTP routes for analytics-exporter.
type HTTPHandler struct {
	router   chi.Router
	exporter *Exporter
	metrics  *analyticsmetrics.Collector
}

// NewHTTPHandler constructs HTTP router and mounts routes.
func NewHTTPHandler(exporter *Exporter, metrics *analyticsmetrics.Collector) *HTTPHandler {
	r := chi.NewRouter()

	h := &HTTPHandler{
		router:   r,
		exporter: exporter,
		metrics:  metrics,
	}

	r.Get("/healthz", h.handleHealth)
	r.Post("/jobs/export", h.handleExportNow)

	return h
}

// Router returns configured chi router.
func (h *HTTPHandler) Router() http.Handler {
	return h.router
}

func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleExportNow triggers export once (for ops).
func (h *HTTPHandler) handleExportNow(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := h.exporter.RunOnce(ctx); err != nil {
		http.Error(w, "export failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}
