package handlers

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	shipmentcorev1 "github.com/dns/logistics/shipment-core-service/internal/api/shipmentcore/v1"
	"github.com/dns/logistics/shipment-core-service/internal/domain"
	"github.com/dns/logistics/shipment-core-service/internal/logging"
	"github.com/dns/logistics/shipment-core-service/internal/metrics"
	"github.com/dns/logistics/shipment-core-service/internal/repository"
	"github.com/dns/logistics/shipment-core-service/internal/service"
)

// GRPCHandler implements internal read API.
type GRPCHandler struct {
	shipmentcorev1.UnimplementedShipmentCoreServiceServer
	logger  *slog.Logger
	service *service.CoreService
	metrics *metrics.Collector
}

// NewGRPCHandler creates gRPC API implementation.
func NewGRPCHandler(logger *slog.Logger, svc *service.CoreService, metricCollector *metrics.Collector) *GRPCHandler {
	return &GRPCHandler{
		logger:  logger,
		service: svc,
		metrics: metricCollector,
	}
}

func (h *GRPCHandler) GetShipment(ctx context.Context, req *shipmentcorev1.GetShipmentRequest) (*shipmentcorev1.GetShipmentResponse, error) {
	state, err := h.service.GetShipment(ctx, req.GetShipmentId())
	if err != nil {
		grpcCode := codes.Internal
		switch {
		case errors.Is(err, service.ErrInvalidShipmentID):
			grpcCode = codes.InvalidArgument
		case errors.Is(err, repository.ErrShipmentNotFound):
			grpcCode = codes.NotFound
		}
		h.metrics.ObserveGRPCRequest(ctx, "GetShipment", grpcCode.String())
		h.logger.Warn("grpc get shipment failed", append([]any{"error", err, "shipment_id", req.GetShipmentId()}, logging.TraceFields(ctx)...)...)
		return nil, status.Error(grpcCode, err.Error())
	}

	h.metrics.ObserveGRPCRequest(ctx, "GetShipment", codes.OK.String())
	return &shipmentcorev1.GetShipmentResponse{Shipment: toProtoShipment(state)}, nil
}

func (h *GRPCHandler) ListShipments(ctx context.Context, req *shipmentcorev1.ListShipmentsRequest) (*shipmentcorev1.ListShipmentsResponse, error) {
	filters := domain.ShipmentFilters{
		StatusCode:   strings.TrimSpace(req.GetStatusCode()),
		SourceSystem: strings.TrimSpace(req.GetSourceSystem()),
		RouteID:      strings.TrimSpace(req.GetRouteId()),
		Limit:        int(req.GetLimit()),
		Offset:       int(req.GetOffset()),
	}

	items, err := h.service.ListShipments(ctx, filters)
	if err != nil {
		h.metrics.ObserveGRPCRequest(ctx, "ListShipments", codes.Internal.String())
		h.logger.Error("grpc list shipments failed", append([]any{"error", err}, logging.TraceFields(ctx)...)...)
		return nil, status.Error(codes.Internal, "internal error")
	}

	shipments := make([]*shipmentcorev1.Shipment, 0, len(items))
	for _, item := range items {
		shipments = append(shipments, toProtoShipment(item))
	}

	h.metrics.ObserveGRPCRequest(ctx, "ListShipments", codes.OK.String())
	return &shipmentcorev1.ListShipmentsResponse{
		Shipments: shipments,
		Limit:     req.GetLimit(),
		Offset:    req.GetOffset(),
	}, nil
}

func (h *GRPCHandler) Ping(ctx context.Context, _ *emptypb.Empty) (*shipmentcorev1.PingResponse, error) {
	if err := h.service.IsReady(ctx); err != nil {
		h.metrics.ObserveGRPCRequest(ctx, "Ping", codes.Unavailable.String())
		return nil, status.Error(codes.Unavailable, "not ready")
	}

	h.metrics.ObserveGRPCRequest(ctx, "Ping", codes.OK.String())
	return &shipmentcorev1.PingResponse{Status: "ok"}, nil
}

func toProtoShipment(state domain.ShipmentState) *shipmentcorev1.Shipment {
	s := &shipmentcorev1.Shipment{
		ShipmentId:   state.ShipmentID.String(),
		LastEventId:  state.LastEventID,
		StatusCode:   state.StatusCode,
		StatusTs:     timestamppb.New(state.StatusTS),
		SourceSystem: state.SourceSystem,
		LocationCode: state.LocationCode,
		PartnerId:    state.PartnerID,
		RouteId:      state.RouteID,
		Payload:      append([]byte(nil), state.Payload...),
		UpdatedAt:    timestamppb.New(state.UpdatedAt),
	}
	if state.EstimatedDelivery != nil {
		s.EstimatedDelivery = timestamppb.New(*state.EstimatedDelivery)
	}
	return s
}
