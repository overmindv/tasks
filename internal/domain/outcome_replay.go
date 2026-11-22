package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrOutcomeSnapshot = errors.New("invalid or expired outcome snapshot")

type OutcomeManifest struct {
	Watermark     int64     `json:"watermark"`
	PrivacyEpoch  int64     `json:"privacy_epoch"`
	Count         int64     `json:"count"`
	SHA256        string    `json:"sha256"`
	LegacyMissing int64     `json:"legacy_missing"`
	CompleteSince time.Time `json:"complete_since"`
}
type OutcomeReplayEntry struct {
	Position int64           `json:"position"`
	SHA256   string          `json:"sha256"`
	Event    LearningOutcome `json:"event"`
}
type OutcomeReplayPage struct {
	Watermark    int64                `json:"watermark"`
	PrivacyEpoch int64                `json:"privacy_epoch"`
	Through      int64                `json:"through"`
	More         bool                 `json:"more"`
	Events       []OutcomeReplayEntry `json:"events"`
}

// OutcomeDigest канонизирует JSON: ключи сортируются, числа не преобразуются в float.
func OutcomeDigest(payload []byte) (string, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)

	return hex.EncodeToString(sum[:]), nil
}

type AccountDeletion struct {
	EventID uuid.UUID
	UserID  uuid.UUID
	Version int64
}
