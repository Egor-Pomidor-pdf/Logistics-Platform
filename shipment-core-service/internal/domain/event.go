package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ShipmentEvent is an incoming event for persistence and state transition.
type ShipmentEvent struct {
	EventID           string          `json:"event_id"`
	ShipmentID        uuid.UUID       `json:"shipment_id"`
	StatusCode        string          `json:"status_code"`
	StatusTS          time.Time       `json:"status_ts"`
	SourceSystem      string          `json:"source_system"`
	LocationCode      string          `json:"location_code,omitempty"`
	PartnerID         string          `json:"partner_id,omitempty"`
	RouteID           string          `json:"route_id,omitempty"`
	EstimatedDelivery *time.Time      `json:"estimated_delivery,omitempty"`
	Payload           json.RawMessage `json:"payload,omitempty"`
}

// RecordEventResult describes write outcome.
type RecordEventResult struct {
	StateUpdated bool `json:"state_updated"`
}

// NormalizedEvent is consumed from normalized_logistics_events Kafka topic.
type NormalizedEvent struct {
	EventID           string          `json:"event_id"`
	ShipmentID        string          `json:"shipment_id"`
	StatusCode        string          `json:"status_code"`
	StatusTS          time.Time       `json:"status_ts"`
	SourceSystem      string          `json:"source_system"`
	LocationCode      string          `json:"location_code,omitempty"`
	PartnerID         string          `json:"partner_id,omitempty"`
	RouteID           string          `json:"route_id,omitempty"`
	EstimatedDelivery *time.Time      `json:"estimated_delivery,omitempty"`
	Payload           json.RawMessage `json:"payload,omitempty"`
}

// ToShipmentEvent converts topic payload to write model.
func (e NormalizedEvent) ToShipmentEvent() (ShipmentEvent, error) {
	shipmentID, err := uuid.Parse(e.ShipmentID)
	if err != nil {
		return ShipmentEvent{}, fmt.Errorf("invalid shipment_id: %w", err)
	}

	return ShipmentEvent{
		EventID:           e.EventID,
		ShipmentID:        shipmentID,
		StatusCode:        e.StatusCode,
		StatusTS:          e.StatusTS.UTC(),
		SourceSystem:      e.SourceSystem,
		LocationCode:      e.LocationCode,
		PartnerID:         e.PartnerID,
		RouteID:           e.RouteID,
		EstimatedDelivery: e.EstimatedDelivery,
		Payload:           e.Payload,
	}, nil
}
