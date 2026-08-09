package httpapi

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/apperror"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/samber/lo"
)

// createTask создаёт draft теста от имени администратора.
func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	var input taskInput
	if !decodeJSON(w, r, &input) {
		return
	}
	domainInput, err := input.domainInput()
	if err != nil {
		writeError(w, apperror.New(apperror.ValidationError, "topic_id должен быть UUID", http.StatusBadRequest), h.logger)

		return
	}
	result, err := h.tasks.Create(r.Context(), domainInput, actor.UserID)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusCreated, responseTask(result, true))
}

// getAdminTask возвращает текущую версию вместе с правильными вариантами.
func (h *Handler) getAdminTask(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	taskID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	result, err := h.tasks.GetAdmin(r.Context(), taskID)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseTask(result, true))
}

// listAdminTasks возвращает административный список задач.
func (h *Handler) listAdminTasks(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireAdmin(w, r); !ok {
		return
	}
	filter, err := taskFilter(r, true)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	items, err := h.tasks.ListAdmin(r.Context(), filter)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, listResponse[taskSummaryResponse]{
		Items:  lo.Map(items, func(item domain.TaskDetail, _ int) taskSummaryResponse { return responseTaskSummary(item) }),
		Limit:  filter.Limit,
		Offset: filter.Offset,
	})
}

// updateTask создаёт следующую версию теста.
func (h *Handler) updateTask(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	taskID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input taskInput
	if !decodeJSON(w, r, &input) {
		return
	}
	domainInput, err := input.domainInput()
	if err != nil {
		writeError(w, apperror.New(apperror.ValidationError, "topic_id должен быть UUID", http.StatusBadRequest), h.logger)

		return
	}
	result, err := h.tasks.Update(r.Context(), taskID, domainInput, actor.UserID)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseTask(result, true))
}

// changeTaskStatus выполняет защищённый lifecycle-переход.
func (h *Handler) changeTaskStatus(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	taskID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	var input statusInput
	if !decodeJSON(w, r, &input) {
		return
	}
	status := domain.TaskStatus(input.Status)
	if status != domain.TaskStatusDraft && status != domain.TaskStatusPublished && status != domain.TaskStatusArchived {
		writeError(w, apperror.New(apperror.ValidationError, "неподдерживаемый status", http.StatusBadRequest), h.logger)

		return
	}
	result, err := h.tasks.ChangeStatus(r.Context(), taskID, status, actor.UserID)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseTask(result, true))
}

// deleteTask выполняет soft delete теста.
func (h *Handler) deleteTask(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireAdmin(w, r)
	if !ok {
		return
	}
	taskID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	if err := h.tasks.Delete(r.Context(), taskID, actor.UserID); err != nil {
		writeError(w, err, h.logger)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// getPublishedTask возвращает тест без признаков правильного ответа.
func (h *Handler) getPublishedTask(w http.ResponseWriter, r *http.Request) {
	taskID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	result, err := h.tasks.GetPublished(r.Context(), taskID)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseTask(result, false))
}

// listPublishedTasks возвращает список опубликованных тестов.
func (h *Handler) listPublishedTasks(w http.ResponseWriter, r *http.Request) {
	filter, err := taskFilter(r, false)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	items, err := h.tasks.ListPublished(r.Context(), filter)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, listResponse[taskSummaryResponse]{
		Items:  lo.Map(items, func(item domain.TaskDetail, _ int) taskSummaryResponse { return responseTaskSummary(item) }),
		Limit:  filter.Limit,
		Offset: filter.Offset,
	})
}

// taskFilter разбирает явные фильтры списка задач.
func taskFilter(r *http.Request, allowStatus bool) (domain.TaskFilter, error) {
	limit, offset, err := pagination(r)
	if err != nil {
		return domain.TaskFilter{}, err
	}
	filter := domain.TaskFilter{
		Limit:  limit,
		Offset: offset,
	}
	if value := r.URL.Query().Get("status"); value != "" && allowStatus {
		status := domain.TaskStatus(value)
		if status != domain.TaskStatusDraft && status != domain.TaskStatusPublished && status != domain.TaskStatusArchived {
			return domain.TaskFilter{}, apperror.New(apperror.ValidationError, "неподдерживаемый status", http.StatusBadRequest)
		}
		filter.Status = &status
	}
	if value := r.URL.Query().Get("task_type"); value != "" {
		taskType := domain.TaskType(value)
		if taskType != domain.TaskTypeSingleChoice && taskType != domain.TaskTypeMultipleChoice {
			return domain.TaskFilter{}, apperror.New(apperror.ValidationError, "неподдерживаемый task_type", http.StatusBadRequest)
		}
		filter.TaskType = &taskType
	}
	if value := r.URL.Query().Get("difficulty"); value != "" {
		difficulty := domain.Difficulty(value)
		if difficulty != domain.DifficultyEasy && difficulty != domain.DifficultyMedium && difficulty != domain.DifficultyHard {
			return domain.TaskFilter{}, apperror.New(apperror.ValidationError, "неподдерживаемая difficulty", http.StatusBadRequest)
		}
		filter.Difficulty = &difficulty
	}
	if value := r.URL.Query().Get("topic_id"); value != "" {
		topicID, parseErr := uuid.Parse(value)
		if parseErr != nil {
			return domain.TaskFilter{}, apperror.New(apperror.ValidationError, "topic_id должен быть UUID", http.StatusBadRequest)
		}
		filter.TopicID = &topicID
	}
	filter.Limit, filter.Offset = domain.NormalizePagination(filter.Limit, filter.Offset)

	return filter, nil
}

// pathUUID разбирает UUID из path-параметра или пишет ошибку.
func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, fmt.Sprintf("%s должен быть UUID", name), http.StatusBadRequest))

		return uuid.Nil, false
	}

	return id, true
}
