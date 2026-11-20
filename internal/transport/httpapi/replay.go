package httpapi

import (
	"net/http"

	"github.com/google/uuid"
)

// replayCode — внутренний admin-only recovery; произвольный DLQ payload не принимается.
func (h *Handler) replayCode(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input struct {
		ExpectedAttemptID uuid.UUID `json:"expected_attempt_id"`
	}
	if !decodeJSONLimit(w, r, &input, 1024) {
		return
	}
	result, err := h.code.Replay(r.Context(), id, input.ExpectedAttemptID, actor.UserID, actor.Admin)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusAccepted, responseCodeSubmission(result))
}
