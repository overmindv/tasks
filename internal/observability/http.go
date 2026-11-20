package observability

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ExecutionMetricsPath = "/internal/metrics/execution"

func (m *Execution) Handler(token string) (http.Handler, error) {
	const tokenError = "TASKS_EXECUTION_METRICS_TOKEN must contain 32 to 256 printable ASCII characters without spaces"
	if len(token) < 32 || len(token) > 256 {
		return nil, errors.New(tokenError)
	}
	for i := range len(token) {
		if token[i] < '!' || token[i] > '~' {
			return nil, errors.New(tokenError)
		}
	}

	want := sha256.Sum256([]byte(token))
	scrape := promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		auth := r.Header.Values("Authorization")
		if len(auth) == 1 {
			if value, ok := strings.CutPrefix(auth[0], "Bearer "); ok && len(value) == len(token) {
				got := sha256.Sum256([]byte(value))
				if subtle.ConstantTimeCompare(want[:], got[:]) == 1 {
					scrape.ServeHTTP(w, r)
					return
				}
			}
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}), nil
}
