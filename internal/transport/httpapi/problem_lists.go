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
	Truncated   bool      `json:"truncated"`
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
// Обычный пользователь видит только опубликованные задачи, администратор — все.
func (h *Handler) listProblemLists(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	h.writeProblemLists(w, r, actor.Admin)
}

// listAdminProblemLists — журнал списков без ограничения видимости.
func (h *Handler) listAdminProblemLists(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	h.writeProblemLists(w, r, true)
}

func (h *Handler) writeProblemLists(w http.ResponseWriter, r *http.Request, admin bool) {
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	items, err := h.problemLists.List(r.Context(), limit, offset, admin)
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
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	h.writeProblemListDetail(w, r, actor.Admin)
}

// getAdminProblemList — детали списка без ограничения видимости.
func (h *Handler) getAdminProblemList(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	h.writeProblemListDetail(w, r, true)
}

func (h *Handler) writeProblemListDetail(w http.ResponseWriter, r *http.Request, admin bool) {
	id, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	detail, err := h.problemLists.Get(r.Context(), id, admin)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	// Число ожидающих модерации кандидатов — административные метаданные.
	pendingCandidates := 0
	if admin {
		pendingCandidates = detail.PendingCandidates
	}
	response := problemListDetailResponse{
		problemListResponse: responseProblemList(detail.ProblemList, detail.MemberCount),
		PendingCandidates:   pendingCandidates,
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
		Truncated: item.Truncated, MemberCount: memberCount, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
	}
}
