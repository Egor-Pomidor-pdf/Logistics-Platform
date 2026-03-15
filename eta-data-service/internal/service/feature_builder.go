package service

import (
	"time"

	"github.com/dns/logistics/eta-data-service/internal/domain"
)

// ShipmentFeatures описывает минимальный набор ETA-фичей, построенный из истории/состояния.
type ShipmentFeatures struct {
	ShipmentID         string            `json:"shipment_id"`
	RouteID            string            `json:"route_id,omitempty"`
	SourceSystem       string            `json:"source_system,omitempty"`
	CurrentStatusCode  string            `json:"current_status_code"`
	CurrentStatusTS    time.Time         `json:"current_status_ts"`
	EventsCount        int               `json:"events_count"`
	AgeSeconds         int64             `json:"age_seconds"`
	StatusDurationsSec map[string]int64  `json:"status_durations_sec,omitempty"`
	Extra              map[string]string `json:"extra,omitempty"`
}

// BuildFeaturesForShipments группирует события по shipment_id и строит фичи.
// Здесь намеренно простая, детерминированная логика без доменных допущений.
func BuildFeaturesForShipments(
	now time.Time,
	history []domain.ShipmentEvent,
	states map[string]domain.ShipmentState,
) []ShipmentFeatures {
	byShipment := make(map[string][]domain.ShipmentEvent)
	for _, ev := range history {
		id := ev.ShipmentID.String()
		byShipment[id] = append(byShipment[id], ev)
	}

	result := make([]ShipmentFeatures, 0, len(byShipment))

	for shipmentID, events := range byShipment {
		if len(events) == 0 {
			continue
		}

		// события уже отсортированы по (shipment_id, status_ts), но для надёжности не сортируем повторно.
		first := events[0]
		last := events[len(events)-1]

		features := ShipmentFeatures{
			ShipmentID:        shipmentID,
			RouteID:           last.RouteID,
			SourceSystem:      last.SourceSystem,
			CurrentStatusCode: last.StatusCode,
			CurrentStatusTS:   last.StatusTS,
			EventsCount:       len(events),
			StatusDurationsSec: make(map[string]int64),
		}

		// возраст отправления: от первого статуса до now.
		features.AgeSeconds = int64(now.Sub(first.StatusTS).Seconds())

		// простые длительности между соседними статусами по их коду.
		for i := 0; i < len(events)-1; i++ {
			a := events[i]
			b := events[i+1]
			delta := int64(b.StatusTS.Sub(a.StatusTS).Seconds())
			if delta < 0 {
				continue
			}
			features.StatusDurationsSec[a.StatusCode] += delta
		}

		// при наличии актуального состояния можно при необходимости обогатить features.Extra.
		if st, ok := states[shipmentID]; ok {
			if features.Extra == nil {
				features.Extra = make(map[string]string)
			}
			features.Extra["location_code"] = st.LocationCode
			features.Extra["partner_id"] = st.PartnerID
		}

		result = append(result, features)
	}

	return result
}

