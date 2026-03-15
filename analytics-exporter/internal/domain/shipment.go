package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ShipmentEvent mirrors shipment-core-service immutable history model.
type ShipmentEvent struct {
	EventID      string          `json:"event_id"`
	ShipmentID   uuid.UUID       `json:"shipment_id"`
	StatusCode   string          `json:"status_code"`
	StatusTS     time.Time       `json:"status_ts"`
	SourceSystem string          `json:"source_system"`
	LocationCode string          `json:"location_code,omitempty"`
	PartnerID    string          `json:"partner_id,omitempty"`
	RouteID      string          `json:"route_id,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}

// ShipmentState mirrors shipment-core-service latest state model.
type ShipmentState struct {
	ShipmentID   uuid.UUID       `json:"shipment_id"`
	LastEventID  string          `json:"last_event_id"`
	StatusCode   string          `json:"status_code"`
	StatusTS     time.Time       `json:"status_ts"`
	SourceSystem string          `json:"source_system"`
	LocationCode string          `json:"location_code,omitempty"`
	PartnerID    string          `json:"partner_id,omitempty"`
	RouteID      string          `json:"route_id,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	UpdatedAt    time.Time       `json:"updated_at"`
}
