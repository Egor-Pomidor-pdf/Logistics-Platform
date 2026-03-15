package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dns/logistics/analytics-exporter/internal/config"
	"github.com/dns/logistics/analytics-exporter/internal/domain"
)

// Client sends data to ClickHouse over HTTP interface.
type Client struct {
	baseURL      string
	database     string
	user         string
	password     string
	historyTable string
	stateTable   string
	httpClient   *http.Client
	logger       *slog.Logger
}

// NewClient creates ClickHouse HTTP client.
func NewClient(cfg config.Config, logger *slog.Logger) (*Client, error) {
	baseURL := strings.TrimRight(cfg.ClickHouseURL, "/")
	if baseURL == "" {
		return nil, fmt.Errorf("CLICKHOUSE_URL is required")
	}
	_, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse CLICKHOUSE_URL: %w", err)
	}

	client := &http.Client{
		Timeout: cfg.ClickHouseTimeout,
	}

	return &Client{
		baseURL:      baseURL,
		database:     cfg.ClickHouseDatabase,
		user:         cfg.ClickHouseUser,
		password:     cfg.ClickHousePassword,
		historyTable: cfg.ClickHouseHistory,
		stateTable:   cfg.ClickHouseState,
		httpClient:   client,
		logger:       logger,
	}, nil
}

// InsertHistory exports shipment history rows.
func (c *Client) InsertHistory(ctx context.Context, rows []domain.ShipmentEvent, exportedAt time.Time) error {
	if len(rows) == 0 {
		return nil
	}

	type historyRow struct {
		EventID      string `json:"event_id"`
		ShipmentID   string `json:"shipment_id"`
		StatusCode   string `json:"status_code"`
		StatusTS     string `json:"status_ts"`
		SourceSystem string `json:"source_system"`
		LocationCode string `json:"location_code"`
		PartnerID    string `json:"partner_id"`
		RouteID      string `json:"route_id"`
		Payload      string `json:"payload"`
		ExportedAt   string `json:"exported_at"`
	}

	buf := &bytes.Buffer{}
	for _, row := range rows {
		item := historyRow{
			EventID:      row.EventID,
			ShipmentID:   row.ShipmentID.String(),
			StatusCode:   row.StatusCode,
			StatusTS:     formatTime(row.StatusTS),
			SourceSystem: row.SourceSystem,
			LocationCode: row.LocationCode,
			PartnerID:    row.PartnerID,
			RouteID:      row.RouteID,
			Payload:      string(row.Payload),
			ExportedAt:   formatTime(exportedAt),
		}
		if err := writeJSONLine(buf, item); err != nil {
			return err
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (event_id, shipment_id, status_code, status_ts, source_system, location_code, partner_id, route_id, payload, exported_at) FORMAT JSONEachRow",
		c.database, c.historyTable,
	)
	return c.exec(ctx, query, buf)
}

// InsertStates exports shipment state rows.
func (c *Client) InsertStates(ctx context.Context, rows []domain.ShipmentState, exportedAt time.Time) error {
	if len(rows) == 0 {
		return nil
	}

	type stateRow struct {
		ShipmentID   string `json:"shipment_id"`
		LastEventID  string `json:"last_event_id"`
		StatusCode   string `json:"status_code"`
		StatusTS     string `json:"status_ts"`
		SourceSystem string `json:"source_system"`
		LocationCode string `json:"location_code"`
		PartnerID    string `json:"partner_id"`
		RouteID      string `json:"route_id"`
		Payload      string `json:"payload"`
		UpdatedAt    string `json:"updated_at"`
		ExportedAt   string `json:"exported_at"`
	}

	buf := &bytes.Buffer{}
	for _, row := range rows {
		item := stateRow{
			ShipmentID:   row.ShipmentID.String(),
			LastEventID:  row.LastEventID,
			StatusCode:   row.StatusCode,
			StatusTS:     formatTime(row.StatusTS),
			SourceSystem: row.SourceSystem,
			LocationCode: row.LocationCode,
			PartnerID:    row.PartnerID,
			RouteID:      row.RouteID,
			Payload:      string(row.Payload),
			UpdatedAt:    formatTime(row.UpdatedAt),
			ExportedAt:   formatTime(exportedAt),
		}
		if err := writeJSONLine(buf, item); err != nil {
			return err
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (shipment_id, last_event_id, status_code, status_ts, source_system, location_code, partner_id, route_id, payload, updated_at, exported_at) FORMAT JSONEachRow",
		c.database, c.stateTable,
	)
	return c.exec(ctx, query, buf)
}

func (c *Client) exec(ctx context.Context, query string, body *bytes.Buffer) error {
	endpoint := c.baseURL + "/?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return fmt.Errorf("build clickhouse request: %w", err)
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("clickhouse request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		payload, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse error: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}

	return nil
}

func writeJSONLine(buf *bytes.Buffer, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal clickhouse row: %w", err)
	}
	if _, err := buf.Write(data); err != nil {
		return fmt.Errorf("write clickhouse row: %w", err)
	}
	if err := buf.WriteByte('\n'); err != nil {
		return fmt.Errorf("write clickhouse row newline: %w", err)
	}
	return nil
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format("2006-01-02 15:04:05.000")
}
