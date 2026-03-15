package domain

import (
	"encoding/json"
	"time"
)

// RawEvent is a minimal input payload consumed from raw_logistics_events.
// Partners and internal systems send estimated delivery in various formats:
// ISO date, unix timestamp, locale-specific strings — normalizer unifies them.
type RawEvent struct {
	EventID            string          `json:"event_id"`
	ShipmentID         string          `json:"shipment_id"`
	ExternalStatus     string          `json:"external_status"`
	StatusTS           string          `json:"status_ts"`
	SourceSystem       string          `json:"source_system"`
	Location           string          `json:"location,omitempty"`
	PartnerID          string          `json:"partner_id,omitempty"`
	RouteID            string          `json:"route_id,omitempty"`
	EstimatedDelivery  string          `json:"estimated_delivery,omitempty"`
	Metadata           json.RawMessage `json:"metadata,omitempty"`
}

// NormalizedEvent is a strict downstream contract for shipment-core-service.
type NormalizedEvent struct {
	EventID            string          `json:"event_id"`
	ShipmentID         string          `json:"shipment_id"`
	StatusCode         string          `json:"status_code"`
	StatusTS           time.Time       `json:"status_ts"`
	SourceSystem       string          `json:"source_system"`
	LocationCode       string          `json:"location_code,omitempty"`
	PartnerID          string          `json:"partner_id,omitempty"`
	RouteID            string          `json:"route_id,omitempty"`
	EstimatedDelivery  *time.Time      `json:"estimated_delivery,omitempty"`
	Payload            json.RawMessage `json:"payload,omitempty"`
}

// InvalidEvent describes payloads redirected to invalid/DLQ flow.
type InvalidEvent struct {
	EventID      string    `json:"event_id,omitempty"`
	ShipmentID   string    `json:"shipment_id,omitempty"`
	SourceSystem string    `json:"source_system,omitempty"`
	Reason       string    `json:"reason"`
	Error        string    `json:"error,omitempty"`
	RawPayload   string    `json:"raw_payload"`
	CreatedAt    time.Time `json:"created_at"`
}
