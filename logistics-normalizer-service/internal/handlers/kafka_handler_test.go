package handlers

import (
	"testing"
	"time"

	"github.com/dns/logistics/logistics-normalizer-service/internal/domain"
)

// newTestHandler returns a minimal KafkaHandler suitable for calling
// validateRaw, parseStatusTS and parseEstimatedDelivery.
func newTestHandler() *KafkaHandler {
	return &KafkaHandler{
		moscowLocation: time.FixedZone("MSK", 3*60*60),
	}
}

// ---------------------------------------------------------------------------
// validateRaw
// ---------------------------------------------------------------------------

func TestValidateRaw_ValidEvent(t *testing.T) {
	h := newTestHandler()
	raw := domain.RawEvent{
		EventID:        "evt-001",
		ShipmentID:     "550e8400-e29b-41d4-a716-446655440000",
		ExternalStatus: "PICKED",
		SourceSystem:   "WMS",
		StatusTS:       "2026-03-14T12:00:00Z",
	}

	ts, reason, err := h.validateRaw(raw)
	if err != nil {
		t.Fatalf("expected no error, got %v (reason=%s)", err, reason)
	}
	if reason != "" {
		t.Fatalf("expected empty reason, got %q", reason)
	}
	if ts.IsZero() {
		t.Fatal("expected non-zero timestamp")
	}
}

func TestValidateRaw_MissingEventID(t *testing.T) {
	h := newTestHandler()
	raw := domain.RawEvent{
		ShipmentID:     "550e8400-e29b-41d4-a716-446655440000",
		ExternalStatus: "PICKED",
		SourceSystem:   "WMS",
		StatusTS:       "2026-03-14T12:00:00Z",
	}

	_, reason, err := h.validateRaw(raw)
	if err == nil {
		t.Fatal("expected error for missing event_id")
	}
	if reason != "missing_event_id" {
		t.Fatalf("expected reason missing_event_id, got %q", reason)
	}
}

func TestValidateRaw_MissingShipmentID(t *testing.T) {
	h := newTestHandler()
	raw := domain.RawEvent{
		EventID:        "evt-001",
		ExternalStatus: "PICKED",
		SourceSystem:   "WMS",
		StatusTS:       "2026-03-14T12:00:00Z",
	}

	_, reason, err := h.validateRaw(raw)
	if err == nil {
		t.Fatal("expected error for missing shipment_id")
	}
	if reason != "missing_shipment_id" {
		t.Fatalf("expected reason missing_shipment_id, got %q", reason)
	}
}

func TestValidateRaw_InvalidUUID(t *testing.T) {
	h := newTestHandler()
	raw := domain.RawEvent{
		EventID:        "evt-001",
		ShipmentID:     "not-a-uuid",
		ExternalStatus: "PICKED",
		SourceSystem:   "WMS",
		StatusTS:       "2026-03-14T12:00:00Z",
	}

	_, reason, err := h.validateRaw(raw)
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
	if reason != "invalid_shipment_uuid" {
		t.Fatalf("expected reason invalid_shipment_uuid, got %q", reason)
	}
}

func TestValidateRaw_MissingExternalStatus(t *testing.T) {
	h := newTestHandler()
	raw := domain.RawEvent{
		EventID:      "evt-001",
		ShipmentID:   "550e8400-e29b-41d4-a716-446655440000",
		SourceSystem: "WMS",
		StatusTS:     "2026-03-14T12:00:00Z",
	}

	_, reason, err := h.validateRaw(raw)
	if err == nil {
		t.Fatal("expected error for missing external_status")
	}
	if reason != "missing_external_status" {
		t.Fatalf("expected reason missing_external_status, got %q", reason)
	}
}

func TestValidateRaw_UnknownSourceSystem(t *testing.T) {
	h := newTestHandler()
	raw := domain.RawEvent{
		EventID:        "evt-001",
		ShipmentID:     "550e8400-e29b-41d4-a716-446655440000",
		ExternalStatus: "PICKED",
		SourceSystem:   "UNKNOWN_SYSTEM",
		StatusTS:       "2026-03-14T12:00:00Z",
	}

	_, reason, err := h.validateRaw(raw)
	if err == nil {
		t.Fatal("expected error for unknown source system")
	}
	if reason != "unknown_source_system" {
		t.Fatalf("expected reason unknown_source_system, got %q", reason)
	}
}

func TestValidateRaw_InvalidTimestamp(t *testing.T) {
	h := newTestHandler()
	raw := domain.RawEvent{
		EventID:        "evt-001",
		ShipmentID:     "550e8400-e29b-41d4-a716-446655440000",
		ExternalStatus: "PICKED",
		SourceSystem:   "WMS",
		StatusTS:       "not-a-timestamp",
	}

	_, reason, err := h.validateRaw(raw)
	if err == nil {
		t.Fatal("expected error for invalid timestamp")
	}
	if reason != "invalid_status_ts" {
		t.Fatalf("expected reason invalid_status_ts, got %q", reason)
	}
}

func TestValidateRaw_MissingStatusTS(t *testing.T) {
	h := newTestHandler()
	raw := domain.RawEvent{
		EventID:        "evt-001",
		ShipmentID:     "550e8400-e29b-41d4-a716-446655440000",
		ExternalStatus: "PICKED",
		SourceSystem:   "WMS",
		StatusTS:       "",
	}

	_, reason, err := h.validateRaw(raw)
	if err == nil {
		t.Fatal("expected error for missing status_ts")
	}
	if reason != "missing_status_ts" {
		t.Fatalf("expected reason missing_status_ts, got %q", reason)
	}
}

// ---------------------------------------------------------------------------
// mapExternalStatus
// ---------------------------------------------------------------------------

func TestMapExternalStatus_KnownMappings(t *testing.T) {
	tests := []struct {
		source   string
		status   string
		expected string
	}{
		{"WMS", "PICKED", "picked"},
		{"WMS", "PACKED", "packed"},
		{"WMS", "SHIPPED", "shipped"},
		{"PARTNER_X", "IN_TRANSIT", "in_transit"},
		{"PARTNER_X", "DELIVERED", "delivered"},
		{"OMS", "CREATED", "created"},
		{"OMS", "CANCELLED", "cancelled"},
	}

	for _, tt := range tests {
		t.Run(tt.source+"_"+tt.status, func(t *testing.T) {
			result, ok := mapExternalStatus(tt.source, tt.status)
			if !ok {
				t.Fatalf("expected mapping to exist for %s/%s", tt.source, tt.status)
			}
			if result != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestMapExternalStatus_UnknownStatus(t *testing.T) {
	result, ok := mapExternalStatus("WMS", "NONEXISTENT")
	if ok {
		t.Fatalf("expected mapping to not exist, got %q", result)
	}
}

func TestMapExternalStatus_UnknownSource(t *testing.T) {
	result, ok := mapExternalStatus("UNKNOWN_SOURCE", "PICKED")
	if ok {
		t.Fatalf("expected mapping to not exist, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// mapSourceSystem
// ---------------------------------------------------------------------------

func TestMapSourceSystem_Known(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"OMS", "oms"},
		{"WMS", "wms"},
		{"PARTNER_X", "partner_x"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := mapSourceSystem(tt.input)
			if result != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestMapSourceSystem_UnknownFallsBackToLower(t *testing.T) {
	result := mapSourceSystem("SOMETHING_NEW")
	if result != "something_new" {
		t.Fatalf("expected lowercase fallback, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// parseStatusTS
// ---------------------------------------------------------------------------

func TestParseStatusTS_RFC3339(t *testing.T) {
	h := newTestHandler()
	ts, err := h.parseStatusTS("2026-03-14T12:00:00Z")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	if !ts.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, ts)
	}
}

func TestParseStatusTS_RFC3339Nano(t *testing.T) {
	h := newTestHandler()
	ts, err := h.parseStatusTS("2026-03-14T12:00:00.123456789Z")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 3, 14, 12, 0, 0, 123456789, time.UTC)
	if !ts.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, ts)
	}
}

func TestParseStatusTS_RFC3339WithOffset(t *testing.T) {
	h := newTestHandler()
	ts, err := h.parseStatusTS("2026-03-14T15:00:00+03:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	if !ts.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, ts)
	}
}

func TestParseStatusTS_WithoutTimezone_MoscowFallback(t *testing.T) {
	h := newTestHandler()
	// "2026-03-14T15:00:00" without TZ should be treated as Moscow (UTC+3)
	ts, err := h.parseStatusTS("2026-03-14T15:00:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	if !ts.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, ts)
	}
}

func TestParseStatusTS_WithoutTimezone_SpaceFormat(t *testing.T) {
	h := newTestHandler()
	ts, err := h.parseStatusTS("2026-03-14 15:00:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)
	if !ts.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, ts)
	}
}

func TestParseStatusTS_InvalidFormat(t *testing.T) {
	h := newTestHandler()
	_, err := h.parseStatusTS("14/03/2026")
	if err == nil {
		t.Fatal("expected error for unsupported format")
	}
}

// ---------------------------------------------------------------------------
// parseEstimatedDelivery
// ---------------------------------------------------------------------------

func TestParseEstimatedDelivery_RFC3339(t *testing.T) {
	h := newTestHandler()
	result, err := h.parseEstimatedDelivery("2026-03-15T14:00:00+03:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	expected := time.Date(2026, 3, 15, 11, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, *result)
	}
}

func TestParseEstimatedDelivery_DateOnly_ISO(t *testing.T) {
	h := newTestHandler()
	result, err := h.parseEstimatedDelivery("2026-03-15")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	expected := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, *result)
	}
}

func TestParseEstimatedDelivery_DateOnly_DotFormat(t *testing.T) {
	h := newTestHandler()
	result, err := h.parseEstimatedDelivery("15.03.2026")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	expected := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, *result)
	}
}

func TestParseEstimatedDelivery_DatetimeWithoutTZ(t *testing.T) {
	h := newTestHandler()
	// Without timezone should be assumed Moscow (UTC+3)
	result, err := h.parseEstimatedDelivery("2026-03-15T17:00:00")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	expected := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	if !result.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, *result)
	}
}

func TestParseEstimatedDelivery_UnixTimestamp(t *testing.T) {
	h := newTestHandler()
	// 1773784800 = 2026-03-15T14:00:00Z
	result, err := h.parseEstimatedDelivery("1773784800")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	expected := time.Unix(1773784800, 0).UTC()
	if !result.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, *result)
	}
}

func TestParseEstimatedDelivery_EmptyString(t *testing.T) {
	h := newTestHandler()
	result, err := h.parseEstimatedDelivery("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for empty string, got %v", *result)
	}
}

func TestParseEstimatedDelivery_WhitespaceOnly(t *testing.T) {
	h := newTestHandler()
	result, err := h.parseEstimatedDelivery("   ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for whitespace-only string, got %v", *result)
	}
}

func TestParseEstimatedDelivery_InvalidFormat(t *testing.T) {
	h := newTestHandler()
	_, err := h.parseEstimatedDelivery("next-tuesday")
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}
