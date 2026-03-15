package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ShipmentState stores the latest known state for shipment_id.
type ShipmentState struct {
	ShipmentID        uuid.UUID       `json:"shipment_id"`
	LastEventID       string          `json:"last_event_id"`
	StatusCode        string          `json:"status_code"`
	StatusTS          time.Time       `json:"status_ts"`
	SourceSystem      string          `json:"source_system"`
	LocationCode      string          `json:"location_code,omitempty"`
	PartnerID         string          `json:"partner_id,omitempty"`
	RouteID           string          `json:"route_id,omitempty"`
	EstimatedDelivery *time.Time      `json:"estimated_delivery,omitempty"`
	Payload           json.RawMessage `json:"payload,omitempty"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

// ShipmentFilters defines list query constraints.
type ShipmentFilters struct {
	StatusCode   string
	SourceSystem string
	RouteID      string
	Limit        int
	Offset       int
}
