package rabbit

// RecalcFeaturesTask описывает задачу на пересчёт ETA-фич по отправке.
type RecalcFeaturesTask struct {
	ShipmentID string `json:"shipment_id"`
	Reason     string `json:"reason,omitempty"`
	Force      bool   `json:"force,omitempty"`
}

