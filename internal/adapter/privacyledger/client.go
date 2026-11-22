package privacyledger

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

type Client struct {
	URL, Token string
	HTTP       *http.Client
}

func New(url, token string) *Client {
	return &Client{URL: strings.TrimRight(url, "/"), Token: token, HTTP: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (c *Client) request(ctx context.Context, method, path, body string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.URL+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Privacy-Service-Token", c.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, errors.New("privacy ledger unavailable")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.New("privacy ledger response failed")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(raw) > 65536 {
		return nil, errors.New("privacy ledger response too large")
	}

	return raw, nil
}
func (c *Client) Read(ctx context.Context, after int64) (usecase.DeletionPage, error) {
	var result usecase.DeletionPage
	raw, err := c.request(ctx, http.MethodGet, "/internal/privacy/deletions?limit=1&watermark=0&after="+strconv.FormatInt(after, 10), "")
	if err != nil {
		return result, err
	}
	var page struct {
		Watermark int64 `json:"watermark"`
		NextAfter int64 `json:"next_after"`
		Events    []struct {
			EventID          uuid.UUID `json:"event_id"`
			EventType        string    `json:"event_type"`
			SchemaVersion    int       `json:"schema_version"`
			AggregateID      uuid.UUID `json:"aggregate_id"`
			AggregateVersion int64     `json:"aggregate_version"`
			OccurredAt       time.Time `json:"occurred_at"`
			Payload          struct {
				UserID    uuid.UUID `json:"user_id"`
				RequestID uuid.UUID `json:"request_id"`
				Tombstone bool      `json:"tombstone"`
			} `json:"payload"`
		} `json:"events"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&page) != nil || decoder.Decode(new(any)) != io.EOF || page.Events == nil || len(page.Events) > 1 || page.NextAfter < after || page.NextAfter > page.Watermark {
		return result, domain.ErrOutcomeSnapshot
	}
	result.Watermark, result.NextAfter = page.Watermark, page.NextAfter
	for _, e := range page.Events {
		if e.EventID == uuid.Nil || e.AggregateID == uuid.Nil || e.EventID != e.Payload.RequestID || e.AggregateID != e.Payload.UserID || !e.Payload.Tombstone || e.EventType != "user.deleted.v1" || e.SchemaVersion != 1 || e.AggregateVersion < 1 || e.OccurredAt.IsZero() || page.NextAfter <= after {
			return result, domain.ErrOutcomeSnapshot
		}
		result.Events = append(result.Events, domain.AccountDeletion{EventID: e.EventID, UserID: e.AggregateID, Version: e.AggregateVersion})
	}

	return result, nil
}
func (c *Client) Ack(ctx context.Context, eventID uuid.UUID) error {
	_, err := c.request(ctx, http.MethodPost, "/internal/privacy/deletions/"+eventID.String()+"/ack", `{"state":"completed"}`)

	return err
}
