package topiccatalog

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
)

// Client читает только публичную видимость canonical topic, без admin-заголовков.
type Client struct {
	base string
	http *http.Client
}

func New(base string) (*Client, error) {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base != "" {
		u, err := url.Parse(base)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, apperror.New("INVALID_CONFIG", "invalid TASKS_ENTITIES_URL", 500)
		}
	}

	return &Client{base: base, http: &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func unavailable() error {
	return apperror.New("TOPIC_CATALOG_UNAVAILABLE", "topic catalog unavailable", 503)
}

func (c *Client) ValidateTopic(ctx context.Context, id uuid.UUID) error {
	if c.base == "" {
		return unavailable()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/topics/"+id.String(), nil)
	if err != nil {
		return unavailable()
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return unavailable()
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return apperror.New("PUBLICATION_CHECK_FAILED", "active canonical topic required", 409)
	}
	if resp.StatusCode != http.StatusOK {
		return unavailable()
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(data) > 65536 {
		return unavailable()
	}
	var topic struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if json.Unmarshal(data, &topic) != nil || topic.ID != id.String() {
		return unavailable()
	}
	if topic.Status != "active" {
		return apperror.New("PUBLICATION_CHECK_FAILED", "active canonical topic required", 409)
	}

	return nil
}
