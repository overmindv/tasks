package httpapi

import (
	"crypto/subtle"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/samber/lo"
)

type candidateImportRequest struct {
	Items []candidateImportInput `json:"items"`
}

type candidateImportInput struct {
	ExternalID        string         `json:"external_id"`
	SourceID          string         `json:"source_id"`
	SourceName        string         `json:"source_name"`
	SourceURL         string         `json:"source_url"`
	SourceHash        string         `json:"source_hash"`
	SourcePublishedAt *time.Time     `json:"source_published_at"`
	RetrievedAt       time.Time      `json:"retrieved_at"`
	CollectionJobID   string         `json:"collection_job_id"`
	Title             string         `json:"title"`
	Statement         string         `json:"statement"`
	Difficulty        string         `json:"difficulty"`
	Tags              []string       `json:"tags"`
	Examples          []exampleInput `json:"examples"`
	Constraints       []string       `json:"constraints"`
}

type candidateReviewInput struct {
	ExpectedRevision int            `json:"expected_revision"`
	TopicID          *string        `json:"topic_id"`
	Title            string         `json:"title"`
	Statement        string         `json:"statement"`
	Difficulty       string         `json:"difficulty"`
	Tags             []string       `json:"tags"`
	Examples         []exampleInput `json:"examples"`
	Constraints      []string       `json:"constraints"`
}

type candidateRejectInput struct {
	ExpectedRevision int    `json:"expected_revision"`
	Reason           string `json:"reason"`
}

type candidateResponse struct {
	ID                string         `json:"id"`
	Status            string         `json:"status"`
	Revision          int            `json:"revision"`
	ExternalID        string         `json:"external_id"`
	SourceID          string         `json:"source_id"`
	SourceName        string         `json:"source_name"`
	SourceURL         string         `json:"source_url"`
	SourcePublishedAt *time.Time     `json:"source_published_at"`
	RetrievedAt       time.Time      `json:"retrieved_at"`
	CollectionJobID   string         `json:"collection_job_id"`
	TopicID           *string        `json:"topic_id"`
	Title             string         `json:"title"`
	Statement         string         `json:"statement"`
	Difficulty        string         `json:"difficulty"`
	Tags              []string       `json:"tags"`
	Examples          []exampleInput `json:"examples"`
	Constraints       []string       `json:"constraints"`
	ApprovedTaskID    *string        `json:"approved_task_id"`
	RejectionReason   string         `json:"rejection_reason"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
}

// importCandidates принимает идемпотентный batch только от task-hunter.
func (h *Handler) importCandidates(w http.ResponseWriter, r *http.Request) {
	if !h.requireIngestToken(w, r) {
		return
	}
	var input candidateImportRequest
	if !decodeJSON(w, r, &input) {
		return
	}
	if len(input.Items) == 0 || len(input.Items) > 100 {
		writeError(w, apperror.New(apperror.ValidationError, "batch должен содержать от 1 до 100 кандидатов", http.StatusBadRequest), h.logger)

		return
	}
	items := make([]domain.CandidateImport, 0, len(input.Items))
	for _, item := range input.Items {
		jobID, err := uuid.Parse(item.CollectionJobID)
		if err != nil {
			writeError(w, apperror.New(apperror.ValidationError, "collection_job_id должен быть UUID", http.StatusBadRequest), h.logger)

			return
		}
		items = append(items, domain.CandidateImport{
			ExternalID: item.ExternalID, SourceID: item.SourceID, SourceName: item.SourceName,
			SourceURL: item.SourceURL, SourceHash: item.SourceHash, SourcePublishedAt: item.SourcePublishedAt,
			RetrievedAt: item.RetrievedAt, CollectionJobID: jobID, Title: item.Title, Statement: item.Statement,
			Difficulty: domain.Difficulty(item.Difficulty), Tags: item.Tags, Constraints: item.Constraints,
			Examples: lo.Map(item.Examples, func(example exampleInput, _ int) domain.TaskExample {
				return domain.TaskExample{Input: example.Input, Output: example.Output, Explanation: example.Explanation}
			}),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": h.candidates.ImportBatch(r.Context(), items)})
}

// listCandidates возвращает административную очередь кандидатов.
func (h *Handler) listCandidates(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	filter := domain.CandidateFilter{SourceID: strings.TrimSpace(r.URL.Query().Get("source_id")), Limit: limit, Offset: offset}
	if value := r.URL.Query().Get("status"); value != "" {
		status := domain.CandidateStatus(value)
		if status != domain.CandidateStatusPending && status != domain.CandidateStatusApproved && status != domain.CandidateStatusRejected {
			writeError(w, apperror.New(apperror.ValidationError, "неподдерживаемый status", http.StatusBadRequest), h.logger)

			return
		}
		filter.Status = &status
	}
	if value := r.URL.Query().Get("difficulty"); value != "" {
		difficulty := domain.Difficulty(value)
		if difficulty != domain.DifficultyEasy && difficulty != domain.DifficultyMedium && difficulty != domain.DifficultyHard {
			writeError(w, apperror.New(apperror.ValidationError, "неподдерживаемая difficulty", http.StatusBadRequest), h.logger)

			return
		}
		filter.Difficulty = &difficulty
	}
	filter.Limit, filter.Offset = domain.NormalizePagination(filter.Limit, filter.Offset)
	items, err := h.candidates.List(r.Context(), filter)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, listResponse[candidateResponse]{Items: lo.Map(items, func(item domain.TaskCandidate, _ int) candidateResponse { return responseCandidate(item) }), Limit: filter.Limit, Offset: filter.Offset})
}

// getCandidate возвращает один элемент очереди.
func (h *Handler) getCandidate(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	item, err := h.candidates.Get(r.Context(), id)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseCandidate(item))
}

// updateCandidate сохраняет промежуточные правки администратора.
func (h *Handler) updateCandidate(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	review, ok := decodeCandidateReview(w, r)
	if !ok {
		return
	}
	item, err := h.candidates.Update(r.Context(), id, review)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseCandidate(item))
}

// approveCandidate публикует отредактированного кандидата.
func (h *Handler) approveCandidate(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	review, ok := decodeCandidateReview(w, r)
	if !ok {
		return
	}
	item, err := h.candidates.Approve(r.Context(), id, actor.UserID, review)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseTask(item, true))
}

// rejectCandidate отклоняет pending-кандидата.
func (h *Handler) rejectCandidate(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input candidateRejectInput
	if !decodeJSON(w, r, &input) {
		return
	}
	item, err := h.candidates.Reject(r.Context(), id, actor.UserID, input.ExpectedRevision, input.Reason)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseCandidate(item))
}

// decodeCandidateReview строго преобразует административный payload.
func decodeCandidateReview(w http.ResponseWriter, r *http.Request) (domain.CandidateReview, bool) {
	var input candidateReviewInput
	if !decodeJSON(w, r, &input) {
		return domain.CandidateReview{}, false
	}
	topicID, err := optionalUUID(input.TopicID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "topic_id должен быть UUID", http.StatusBadRequest))

		return domain.CandidateReview{}, false
	}

	return domain.CandidateReview{
		ExpectedRevision: input.ExpectedRevision, TopicID: topicID, Title: input.Title, Statement: input.Statement,
		Difficulty: domain.Difficulty(input.Difficulty), Tags: input.Tags, Constraints: input.Constraints,
		Examples: lo.Map(input.Examples, func(example exampleInput, _ int) domain.TaskExample {
			return domain.TaskExample{Input: example.Input, Output: example.Output, Explanation: example.Explanation}
		}),
	}, true
}

// requireIngestToken проверяет отдельный service token без утечки timing-информации.
func (h *Handler) requireIngestToken(w http.ResponseWriter, r *http.Request) bool {
	provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if h.ingestToken == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(h.ingestToken)) != 1 {
		writeJSON(w, http.StatusUnauthorized, apperror.New(apperror.AuthenticationRequired, "неверный service token", http.StatusUnauthorized))

		return false
	}

	return true
}

// responseCandidate преобразует доменную очередь в административный DTO.
func responseCandidate(item domain.TaskCandidate) candidateResponse {
	return candidateResponse{
		ID: item.ID.String(), Status: string(item.Status), Revision: item.Revision, ExternalID: item.ExternalID,
		SourceID: item.SourceID, SourceName: item.SourceName, SourceURL: item.SourceURL,
		SourcePublishedAt: item.SourcePublishedAt, RetrievedAt: item.RetrievedAt, CollectionJobID: item.CollectionJobID.String(),
		TopicID: optionalUUIDString(item.TopicID), Title: item.Title, Statement: item.Statement,
		Difficulty: string(item.Difficulty), Tags: item.Tags, Constraints: item.Constraints,
		Examples: lo.Map(item.Examples, func(example domain.TaskExample, _ int) exampleInput {
			return exampleInput{Input: example.Input, Output: example.Output, Explanation: example.Explanation}
		}),
		ApprovedTaskID: optionalUUIDString(item.ApprovedTaskID), RejectionReason: item.RejectionReason,
		CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
	}
}
