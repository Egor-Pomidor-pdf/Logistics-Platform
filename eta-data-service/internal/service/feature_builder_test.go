package service

import (
	"testing"
	"time"

	"github.com/dns/logistics/eta-data-service/internal/domain"
	"github.com/google/uuid"
)

func TestBuildFeaturesForShipments_EmptyInput(t *testing.T) {
	now := time.Now()

	result := BuildFeaturesForShipments(now, nil, nil)
	if len(result) != 0 {
		t.Fatalf("expected 0 features for nil input, got %d", len(result))
	}

	result = BuildFeaturesForShipments(now, []domain.ShipmentEvent{}, map[string]domain.ShipmentState{})
	if len(result) != 0 {
		t.Fatalf("expected 0 features for empty input, got %d", len(result))
	}
}

func TestBuildFeaturesForShipments_SingleShipmentMultipleEvents(t *testing.T) {
	shipID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	base := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	now := base.Add(2 * time.Hour) // 7200 seconds after first event

	events := []domain.ShipmentEvent{
		{
			EventID:      "e1",
			ShipmentID:   shipID,
			StatusCode:   "CREATED",
			StatusTS:     base,
			SourceSystem: "WMS",
			RouteID:      "R-100",
		},
		{
			EventID:      "e2",
			ShipmentID:   shipID,
			StatusCode:   "IN_TRANSIT",
			StatusTS:     base.Add(30 * time.Minute), // +1800s
			SourceSystem: "WMS",
			RouteID:      "R-100",
		},
		{
			EventID:      "e3",
			ShipmentID:   shipID,
			StatusCode:   "DELIVERED",
			StatusTS:     base.Add(90 * time.Minute), // +5400s from base, +3600s from IN_TRANSIT
			SourceSystem: "WMS",
			RouteID:      "R-100",
		},
	}

	result := BuildFeaturesForShipments(now, events, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 feature set, got %d", len(result))
	}

	f := result[0]

	// shipment id
	if f.ShipmentID != shipID.String() {
		t.Errorf("ShipmentID = %q, want %q", f.ShipmentID, shipID.String())
	}

	// events_count
	if f.EventsCount != 3 {
		t.Errorf("EventsCount = %d, want 3", f.EventsCount)
	}

	// current status from last event
	if f.CurrentStatusCode != "DELIVERED" {
		t.Errorf("CurrentStatusCode = %q, want DELIVERED", f.CurrentStatusCode)
	}
	if !f.CurrentStatusTS.Equal(base.Add(90 * time.Minute)) {
		t.Errorf("CurrentStatusTS = %v, want %v", f.CurrentStatusTS, base.Add(90*time.Minute))
	}

	// age: now - first event = 2h = 7200s
	if f.AgeSeconds != 7200 {
		t.Errorf("AgeSeconds = %d, want 7200", f.AgeSeconds)
	}

	// route and source from last event
	if f.RouteID != "R-100" {
		t.Errorf("RouteID = %q, want R-100", f.RouteID)
	}
	if f.SourceSystem != "WMS" {
		t.Errorf("SourceSystem = %q, want WMS", f.SourceSystem)
	}

	// status durations: CREATED->IN_TRANSIT = 1800s, IN_TRANSIT->DELIVERED = 3600s
	if f.StatusDurationsSec == nil {
		t.Fatal("StatusDurationsSec is nil")
	}
	if d := f.StatusDurationsSec["CREATED"]; d != 1800 {
		t.Errorf("StatusDurationsSec[CREATED] = %d, want 1800", d)
	}
	if d := f.StatusDurationsSec["IN_TRANSIT"]; d != 3600 {
		t.Errorf("StatusDurationsSec[IN_TRANSIT] = %d, want 3600", d)
	}
	// DELIVERED should not appear (it's the last status, no next event)
	if _, ok := f.StatusDurationsSec["DELIVERED"]; ok {
		t.Error("StatusDurationsSec should not contain DELIVERED (last status)")
	}
}

func TestBuildFeaturesForShipments_MultipleShipments(t *testing.T) {
	shipA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shipB := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	base := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	now := base.Add(1 * time.Hour)

	events := []domain.ShipmentEvent{
		{
			EventID:      "a1",
			ShipmentID:   shipA,
			StatusCode:   "CREATED",
			StatusTS:     base,
			SourceSystem: "OMS",
			RouteID:      "R-A",
		},
		{
			EventID:      "b1",
			ShipmentID:   shipB,
			StatusCode:   "CREATED",
			StatusTS:     base.Add(10 * time.Minute),
			SourceSystem: "TMS",
			RouteID:      "R-B",
		},
		{
			EventID:      "a2",
			ShipmentID:   shipA,
			StatusCode:   "IN_TRANSIT",
			StatusTS:     base.Add(20 * time.Minute),
			SourceSystem: "OMS",
			RouteID:      "R-A",
		},
	}

	result := BuildFeaturesForShipments(now, events, nil)
	if len(result) != 2 {
		t.Fatalf("expected 2 feature sets, got %d", len(result))
	}

	// Build lookup by shipment id for order-independent assertions
	byID := make(map[string]ShipmentFeatures, len(result))
	for _, f := range result {
		byID[f.ShipmentID] = f
	}

	// Shipment A: 2 events
	fA, ok := byID[shipA.String()]
	if !ok {
		t.Fatal("missing features for shipment A")
	}
	if fA.EventsCount != 2 {
		t.Errorf("shipment A EventsCount = %d, want 2", fA.EventsCount)
	}
	if fA.CurrentStatusCode != "IN_TRANSIT" {
		t.Errorf("shipment A CurrentStatusCode = %q, want IN_TRANSIT", fA.CurrentStatusCode)
	}
	if fA.SourceSystem != "OMS" {
		t.Errorf("shipment A SourceSystem = %q, want OMS", fA.SourceSystem)
	}

	// Shipment B: 1 event
	fB, ok := byID[shipB.String()]
	if !ok {
		t.Fatal("missing features for shipment B")
	}
	if fB.EventsCount != 1 {
		t.Errorf("shipment B EventsCount = %d, want 1", fB.EventsCount)
	}
	if fB.CurrentStatusCode != "CREATED" {
		t.Errorf("shipment B CurrentStatusCode = %q, want CREATED", fB.CurrentStatusCode)
	}
	if fB.SourceSystem != "TMS" {
		t.Errorf("shipment B SourceSystem = %q, want TMS", fB.SourceSystem)
	}
	// Single event => no status durations
	if len(fB.StatusDurationsSec) != 0 {
		t.Errorf("shipment B StatusDurationsSec should be empty, got %v", fB.StatusDurationsSec)
	}
}

func TestBuildFeaturesForShipments_LocationAndPartnerEnrichment(t *testing.T) {
	shipID := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")
	base := time.Date(2026, 2, 15, 10, 0, 0, 0, time.UTC)
	now := base.Add(30 * time.Minute)

	events := []domain.ShipmentEvent{
		{
			EventID:      "c1",
			ShipmentID:   shipID,
			StatusCode:   "PICKED_UP",
			StatusTS:     base,
			SourceSystem: "CARRIER",
			RouteID:      "R-C",
			LocationCode: "LOC-ORIGIN",
			PartnerID:    "P-001",
		},
	}

	states := map[string]domain.ShipmentState{
		shipID.String(): {
			ShipmentID:   shipID,
			LastEventID:  "c1",
			StatusCode:   "PICKED_UP",
			StatusTS:     base,
			SourceSystem: "CARRIER",
			LocationCode: "LOC-CURRENT",
			PartnerID:    "P-002",
			RouteID:      "R-C",
			UpdatedAt:    base.Add(1 * time.Second),
		},
	}

	result := BuildFeaturesForShipments(now, events, states)
	if len(result) != 1 {
		t.Fatalf("expected 1 feature set, got %d", len(result))
	}

	f := result[0]

	// Extra should be enriched from state
	if f.Extra == nil {
		t.Fatal("Extra is nil, expected enrichment from state")
	}
	if f.Extra["location_code"] != "LOC-CURRENT" {
		t.Errorf("Extra[location_code] = %q, want LOC-CURRENT", f.Extra["location_code"])
	}
	if f.Extra["partner_id"] != "P-002" {
		t.Errorf("Extra[partner_id] = %q, want P-002", f.Extra["partner_id"])
	}
}

func TestBuildFeaturesForShipments_NoStateNoExtra(t *testing.T) {
	shipID := uuid.MustParse("dddddddd-dddd-dddd-dddd-dddddddddddd")
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	now := base.Add(10 * time.Minute)

	events := []domain.ShipmentEvent{
		{
			EventID:      "d1",
			ShipmentID:   shipID,
			StatusCode:   "CREATED",
			StatusTS:     base,
			SourceSystem: "WMS",
		},
	}

	// No matching state entry
	result := BuildFeaturesForShipments(now, events, map[string]domain.ShipmentState{})
	if len(result) != 1 {
		t.Fatalf("expected 1 feature set, got %d", len(result))
	}

	f := result[0]
	if f.Extra != nil {
		t.Errorf("Extra should be nil when no state is provided, got %v", f.Extra)
	}
}

func TestBuildFeaturesForShipments_AgeCalculation(t *testing.T) {
	shipID := uuid.MustParse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	now := base.Add(48 * time.Hour) // 2 days = 172800 seconds

	events := []domain.ShipmentEvent{
		{
			EventID:      "e1",
			ShipmentID:   shipID,
			StatusCode:   "CREATED",
			StatusTS:     base,
			SourceSystem: "SYS",
		},
		{
			EventID:      "e2",
			ShipmentID:   shipID,
			StatusCode:   "DELIVERED",
			StatusTS:     base.Add(24 * time.Hour),
			SourceSystem: "SYS",
		},
	}

	result := BuildFeaturesForShipments(now, events, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 feature set, got %d", len(result))
	}

	// Age is from first event to `now`, not to last event
	if result[0].AgeSeconds != 172800 {
		t.Errorf("AgeSeconds = %d, want 172800 (48h)", result[0].AgeSeconds)
	}
}

func TestBuildFeaturesForShipments_StatusDurationsAccumulate(t *testing.T) {
	shipID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	now := base.Add(5 * time.Hour)

	// Shipment goes CREATED -> IN_TRANSIT -> CREATED -> IN_TRANSIT
	// so CREATED duration accumulates across two segments.
	events := []domain.ShipmentEvent{
		{EventID: "f1", ShipmentID: shipID, StatusCode: "CREATED", StatusTS: base, SourceSystem: "S"},
		{EventID: "f2", ShipmentID: shipID, StatusCode: "IN_TRANSIT", StatusTS: base.Add(1 * time.Hour), SourceSystem: "S"},
		{EventID: "f3", ShipmentID: shipID, StatusCode: "CREATED", StatusTS: base.Add(2 * time.Hour), SourceSystem: "S"},
		{EventID: "f4", ShipmentID: shipID, StatusCode: "IN_TRANSIT", StatusTS: base.Add(4 * time.Hour), SourceSystem: "S"},
	}

	result := BuildFeaturesForShipments(now, events, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 feature set, got %d", len(result))
	}

	f := result[0]

	// CREATED: 1h (f1->f2) + 2h (f3->f4) = 3h = 10800s
	if d := f.StatusDurationsSec["CREATED"]; d != 10800 {
		t.Errorf("StatusDurationsSec[CREATED] = %d, want 10800", d)
	}
	// IN_TRANSIT: 1h (f2->f3) = 3600s
	if d := f.StatusDurationsSec["IN_TRANSIT"]; d != 3600 {
		t.Errorf("StatusDurationsSec[IN_TRANSIT] = %d, want 3600", d)
	}
}
