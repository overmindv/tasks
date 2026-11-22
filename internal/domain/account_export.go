package domain

import (
	"time"

	"github.com/google/uuid"
)

type AccountExportRecord struct {
	UserID     uuid.UUID `json:"user_id"`
	ID         uuid.UUID `json:"id"`
	Kind       string    `json:"kind"`
	OccurredAt time.Time `json:"occurred_at"`
	ResourceID uuid.UUID `json:"resource_id"`
	Title      string    `json:"title,omitempty"`
	Text       string    `json:"text,omitempty"`
	Result     string    `json:"result,omitempty"`
}
type AccountExportPage struct {
	UserID     uuid.UUID             `json:"user_id"`
	Watermark  string                `json:"watermark"`
	NextCursor string                `json:"next_cursor"`
	Records    []AccountExportRecord `json:"records"`
}
