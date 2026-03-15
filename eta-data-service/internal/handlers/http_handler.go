package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"

	"github.com/dns/logistics/eta-data-service/internal/cache"
	"github.com/dns/logistics/eta-data-service/internal/logging"
	"github.com/dns/logistics/eta-data-service/internal/metrics"
	"github.com/dns/logistics/eta-data-service/internal/repository"
	"github.com/dns/logistics/eta-data-service/internal/service"
	rabbittransport "github.com/dns/logistics/eta-data-service/internal/transport/rabbit"
)

// HTTPHandler wires HTTP routes for eta-data-service.
type HTTPHandler struct {
	logger    *slog.Logger
	metrics   *metrics.Collector
	router    chi.Router
	orch      *service.Orchestrator
	features  cache.FeaturesCache
	publisher *rabbittransport.Publisher
}

// NewHTTPHandler constructs HTTP router and mounts routes.
func NewHTTPHandler(
	logger *slog.Logger,
	repo repository.PostgresStore,
	metricCollector *metrics.Collector,
	tracer trace.Tracer,
	featuresCache cache.FeaturesCache,
	taskPublisher *rabbittransport.Publisher,
) *HTTPHandler {
	r := chi.NewRouter()

	orch := service.NewOrchestrator(logger, repo, metricCollector, tracer)

	h := &HTTPHandler{
		logger:    logger,
		metrics:   metricCollector,
		router:    r,
		orch:      orch,
		features:  featuresCache,
		publisher: taskPublisher,
	}

	r.Get("/healthz", h.handleHealth)
	r.Post("/jobs/train", h.handleRunTrainingBatch)
	r.Post("/jobs/score", h.handleRunScoringBatch)
	r.Get("/features/{shipment_id}", h.handleGetFeatures)

	return h
}

// Router returns configured chi router.
func (h *HTTPHandler) Router() http.Handler {
	return h.router
}

func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	h.metrics.ObserveHTTPRequest(r.Context(), "GET /healthz", http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleGetFeatures возвращает фичи по shipment_id из кеша или инициирует асинхронный пересчёт.
func (h *HTTPHandler) handleGetFeatures(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	shipmentID := chi.URLParam(r, "shipment_id")
	if shipmentID == "" {
		http.Error(w, "shipment_id is required", http.StatusBadRequest)
		h.metrics.ObserveHTTPRequest(ctx, "GET /features/{shipment_id}", http.StatusBadRequest)
		return
	}

	features, hit, err := h.features.GetShipmentFeatures(ctx, shipmentID)
	if err != nil {
		h.logger.Error("get features from cache failed",
			append([]any{"error", err, "shipment_id", shipmentID}, logging.TraceFields(ctx)...)...,
		)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.metrics.ObserveHTTPRequest(ctx, "GET /features/{shipment_id}", http.StatusInternalServerError)
		return
	}

	if hit {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(features); err != nil {
			h.logger.Error("encode features response failed",
				append([]any{"error", err, "shipment_id", shipmentID}, logging.TraceFields(ctx)...)...,
			)
		}
		h.metrics.ObserveHTTPRequest(ctx, "GET /features/{shipment_id}", http.StatusOK)
		return
	}

	// cache miss: публикуем задачу на пересчёт и возвращаем 202.
	task := rabbittransport.RecalcFeaturesTask{
		ShipmentID: shipmentID,
		Reason:     "http_cache_miss",
	}
	if err := h.publisher.PublishRecalcFeatures(ctx, task); err != nil {
		h.logger.Error("publish recalc_features task failed",
			append([]any{"error", err, "shipment_id", shipmentID}, logging.TraceFields(ctx)...)...,
		)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.metrics.ObserveHTTPRequest(ctx, "GET /features/{shipment_id}", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":      "processing",
		"shipment_id": shipmentID,
	})
	h.metrics.ObserveHTTPRequest(ctx, "GET /features/{shipment_id}", http.StatusAccepted)
}

// handleRunTrainingBatch запускает подготовку обучающего батча.
// Параметры:
//   - since, until (query, RFC3339) — окно по status_ts;
//   - limit (query, int, опционально) — макс. кол-во событий.
func (h *HTTPHandler) handleRunTrainingBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sinceStr := r.URL.Query().Get("since")
	untilStr := r.URL.Query().Get("until")
	limitStr := r.URL.Query().Get("limit")

	var (
		since time.Time
		until time.Time
		err   error
	)

	if sinceStr == "" || untilStr == "" {
		http.Error(w, "since and until are required (RFC3339)", http.StatusBadRequest)
		h.metrics.ObserveHTTPRequest(ctx, "POST /jobs/train", http.StatusBadRequest)
		return
	}

	if since, err = time.Parse(time.RFC3339, sinceStr); err != nil {
		http.Error(w, "invalid since", http.StatusBadRequest)
		h.metrics.ObserveHTTPRequest(ctx, "POST /jobs/train", http.StatusBadRequest)
		return
	}
	if until, err = time.Parse(time.RFC3339, untilStr); err != nil {
		http.Error(w, "invalid until", http.StatusBadRequest)
		h.metrics.ObserveHTTPRequest(ctx, "POST /jobs/train", http.StatusBadRequest)
		return
	}

	limit := 1000
	if limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
			limit = v
		}
	}

	features, err := h.orch.RunTrainingBatch(ctx, since, until, limit)
	if err != nil {
		h.logger.Error("training batch failed", append([]any{"error", err}, logging.TraceFields(ctx)...)...)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.metrics.ObserveHTTPRequest(ctx, "POST /jobs/train", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(features); err != nil {
		h.logger.Error("encode training batch response failed", append([]any{"error", err}, logging.TraceFields(ctx)...)...)
	}
	h.metrics.ObserveHTTPRequest(ctx, "POST /jobs/train", http.StatusOK)
}

// handleRunScoringBatch запускает подготовку батча для скоринга активных отправлений.
// Параметры:
//   - limit, offset (query) — пагинация по shipment_state.
func (h *HTTPHandler) handleRunScoringBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	limit := 500
	offset := 0

	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = v
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && v >= 0 {
		offset = v
	}

	features, err := h.orch.RunScoringBatch(ctx, limit, offset)
	if err != nil {
		h.logger.Error("scoring batch failed", append([]any{"error", err}, logging.TraceFields(ctx)...)...)
		http.Error(w, "internal error", http.StatusInternalServerError)
		h.metrics.ObserveHTTPRequest(ctx, "POST /jobs/score", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(features); err != nil {
		h.logger.Error("encode scoring batch response failed", append([]any{"error", err}, logging.TraceFields(ctx)...)...)
	}
	h.metrics.ObserveHTTPRequest(ctx, "POST /jobs/score", http.StatusOK)
}

