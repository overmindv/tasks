package httpapi

import (
	"net/http"
	"time"

	"github.com/overmindv/tasks/internal/domain"
	"github.com/samber/lo"
)

type problemListResponse struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	SourceID    string    `json:"source_id"`
	SourceURL   string    `json:"source_url"`
	MemberCount int       `json:"member_count"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type problemListMemberResponse struct {
	TaskID     string `json:"task_id"`
	Title      string `json:"title"`
	Difficulty string `json:"difficulty"`
	Status     string `json:"status"`
}

type problemListDetailResponse struct {
	problemListResponse
	PendingCandidates int                         `json:"pending_candidates"`
	Members           []problemListMemberResponse `json:"members"`
}

// listProblemLists возвращает журнал списков задач с числом опубликованных членов.
func (h *Handler) listProblemLists(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	items, err := h.problemLists.List(r.Context(), limit, offset)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, listResponse[problemListResponse]{
		Items: lo.Map(items, func(item domain.ProblemListSummary, _ int) problemListResponse {
			return responseProblemList(item.ProblemList, item.MemberCount)
		}),
		Limit:  limit,
		Offset: offset,
	})
}

// getProblemList возвращает детали списка вместе с опубликованными задачами.
func (h *Handler) getProblemList(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	detail, err := h.problemLists.Get(r.Context(), id)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	response := problemListDetailResponse{
		problemListResponse: responseProblemList(detail.ProblemList, detail.MemberCount),
		PendingCandidates:   detail.PendingCandidates,
		Members: lo.Map(detail.Members, func(item domain.ProblemListMember, _ int) problemListMemberResponse {
			return problemListMemberResponse{
				TaskID: item.TaskID.String(), Title: item.Title,
				Difficulty: string(item.Difficulty), Status: string(item.Status),
			}
		}),
	}
	writeJSON(w, http.StatusOK, response)
}

// responseProblemList преобразует доменный список в административный DTO.
func responseProblemList(item domain.ProblemList, memberCount int) problemListResponse {
	return problemListResponse{
		ID: item.ID.String(), Name: item.Name, SourceID: item.SourceID, SourceURL: item.SourceURL,
		MemberCount: memberCount, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
	}
}
