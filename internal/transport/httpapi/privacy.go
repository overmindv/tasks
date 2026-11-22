package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

func RegisterPrivacy(router Router, service *usecase.PrivacyService, replayToken, privacyToken string) {
	register := func(path, token, header string, fn func(*http.Request) (any, error)) {
		if token == "" {
			return
		}
		expected := sha256.Sum256([]byte(token))
		handler := http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			value, err := fn(r)
			if err != nil {
				status := http.StatusServiceUnavailable
				if errors.Is(err, domain.ErrOutcomeSnapshot) {
					status = http.StatusConflict
				}
				writeJSON(w, status, map[string]string{"error": "invalid_or_unavailable_snapshot"})

				return
			}
			writeJSON(w, http.StatusOK, value)
		}), 10*time.Second, `{"error":"privacy_timeout"}`)
		router.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			authorized := false
			if header == "Authorization" {
				authorized = resourceAuthorized(r.Header.Values(header), expected)
			} else if values := r.Header.Values(header); len(values) == 1 && len(values[0]) <= 4096 {
				actual := sha256.Sum256([]byte(values[0]))
				authorized = subtle.ConstantTimeCompare(actual[:], expected[:]) == 1
			}
			if !authorized {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})

				return
			}
			handler.ServeHTTP(w, r)
		})
	}
	number := func(r *http.Request, key string, fallback int64) (int64, error) {
		values := r.URL.Query()[key]
		if len(values) == 0 {
			return fallback, nil
		}
		if len(values) != 1 {
			return 0, domain.ErrOutcomeSnapshot
		}
		v, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || v < 0 {
			return 0, domain.ErrOutcomeSnapshot
		}

		return v, nil
	}
	register("/v1/internal/outcomes/manifest", replayToken, "Authorization", func(r *http.Request) (any, error) {
		wm, err := number(r, "watermark", 0)
		if err != nil {
			return nil, err
		}
		epoch, err := number(r, "privacy_epoch", 0)
		if err != nil {
			return nil, err
		}

		return service.OutcomeManifest(r.Context(), wm, epoch)
	})
	register("/v1/internal/outcomes/replay", replayToken, "Authorization", func(r *http.Request) (any, error) {
		wm, err := number(r, "watermark", 0)
		if err != nil {
			return nil, err
		}
		epoch, err := number(r, "privacy_epoch", 0)
		if err != nil {
			return nil, err
		}
		after, err := number(r, "after", 0)
		if err != nil {
			return nil, err
		}
		limit, err := number(r, "limit", 100)
		if err != nil || limit < 1 || limit > 100 {
			return nil, domain.ErrOutcomeSnapshot
		}

		return service.OutcomeReplay(r.Context(), wm, epoch, after, int(limit))
	})
	register("/internal/privacy/export", privacyToken, "X-Privacy-Service-Token", func(r *http.Request) (any, error) {
		q := r.URL.Query()
		id, err := uuid.Parse(q.Get("user_id"))
		if err != nil {
			return nil, domain.ErrOutcomeSnapshot
		}
		limit, err := number(r, "limit", 100)
		if err != nil || limit < 1 || limit > 100 {
			return nil, domain.ErrOutcomeSnapshot
		}

		return service.AccountExport(r.Context(), id, q.Get("cursor"), q.Get("watermark"), int(limit))
	})
}
