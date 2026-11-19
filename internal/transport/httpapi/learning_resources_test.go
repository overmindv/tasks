package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResourceRoutesRequireServiceToken(t *testing.T) {
	const endpoint = "/v1/internal/resources/snapshot"
	token := strings.Repeat("x", 32)
	router := http.NewServeMux()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := RegisterResources(router, nil, "", logger); err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, endpoint, nil))
	if res.Code != http.StatusNotFound {
		t.Fatal("resource endpoint enabled without credentials:", res.Code)
	}
	if err := RegisterResources(router, nil, "bad config", logger); err == nil {
		t.Fatal("invalid token accepted")
	}
	if err := RegisterResources(router, nil, token, logger); err != nil {
		t.Fatal(err)
	}
	for _, headers := range [][]string{
		nil,
		{"Bearer wrong"},
		{"Basic " + token},
		{"Bearer " + token, "Bearer " + token},
	} {
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		req.Header.Set("X-User-ID", "00000000-0000-0000-0000-000000000001")
		req.Header.Set("X-User-Roles", "admin")
		for _, header := range headers {
			req.Header.Add("Authorization", header)
		}
		res = httptest.NewRecorder()
		router.ServeHTTP(res, req)
		if res.Code != http.StatusForbidden || res.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("unauthorized resource request:", res.Code, res.Body.String())
		}
	}
}

func TestResourceQueryRejectsInvalidParameters(t *testing.T) {
	for _, raw := range []string{
		"watermark=-1", "watermark=02", "watermark=9223372036854775808",
		"limit=0", "limit=101", "limit=1&limit=2", "offset=2", "cursor=%",
		"cursor=" + strings.Repeat("x", 520),
	} {
		if _, err := parseResourceQuery(raw, "cursor"); err == nil {
			t.Error("invalid resource query accepted:", raw)
		}
	}
}

func TestTaskEstimateDistinguishesOmissionAndNull(t *testing.T) {
	for _, tc := range []struct {
		body    string
		set     bool
		has     bool
		minutes int
	}{
		{body: `{}`},
		{body: `{"estimated_minutes":null}`, set: true},
		{body: `{"estimated_minutes":12}`, set: true, has: true, minutes: 12},
	} {
		var request taskInput
		if err := json.Unmarshal([]byte(tc.body), &request); err != nil {
			t.Fatal(err)
		}
		input, err := request.domainInput()
		if err != nil || input.SetEstimatedMinutes != tc.set || (input.EstimatedMinutes != nil) != tc.has {
			t.Fatal("incorrect estimate decoding:", tc.body, err)
		}
		if tc.has && *input.EstimatedMinutes != tc.minutes {
			t.Fatal("incorrect estimate:", *input.EstimatedMinutes)
		}
	}
	var request taskInput
	if err := json.Unmarshal([]byte(`{"estimated_minutes":"12"}`), &request); err != nil {
		t.Fatal(err)
	}
	if _, err := request.domainInput(); err == nil {
		t.Fatal("estimate encoded as string was accepted")
	}
}
