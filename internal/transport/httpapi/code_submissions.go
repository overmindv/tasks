package httpapi

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/apperror"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/overmindv/tasks-it/internal/usecase"
	"github.com/samber/lo"
)

const maxCodeSubmissionBody = domain.MaxSourceFileSize + 64*1024

// submitCode принимает один файл и создаёт асинхронный запуск программной задачи.
func (h *Handler) submitCode(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	taskID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	input, err := parseCodeSubmission(w, r)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	result, err := h.code.Submit(r.Context(), taskID, actor.UserID, input)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusAccepted, responseCodeSubmission(result))
}

// getCodeSubmission возвращает текущее состояние запуска владельцу или администратору.
func (h *Handler) getCodeSubmission(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	submissionID, ok := pathUUID(w, r, "id")
	if !ok {
		return
	}
	result, err := h.code.Get(r.Context(), submissionID, actor.UserID, actor.Admin)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, responseCodeSubmission(result))
}

// listMyCodeSubmissions возвращает историю запусков текущего пользователя.
func (h *Handler) listMyCodeSubmissions(w http.ResponseWriter, r *http.Request) {
	actor, ok := requireUser(w, r)
	if !ok {
		return
	}
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	var taskID *uuid.UUID
	if value := r.URL.Query().Get("task_id"); value != "" {
		id, parseErr := uuid.Parse(value)
		if parseErr != nil {
			writeError(w, apperror.New(apperror.ValidationError, "task_id должен быть UUID", http.StatusBadRequest), h.logger)

			return
		}
		taskID = &id
	}
	limit, offset = domain.NormalizePagination(limit, offset)
	items, err := h.code.ListMine(r.Context(), domain.CodeSubmissionFilter{
		UserID: actor.UserID,
		TaskID: taskID,
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		writeError(w, err, h.logger)

		return
	}
	writeJSON(w, http.StatusOK, listResponse[codeSubmissionResponse]{
		Items:  lo.Map(items, func(item domain.CodeSubmission, _ int) codeSubmissionResponse { return responseCodeSubmission(item) }),
		Limit:  limit,
		Offset: offset,
	})
}

// parseCodeSubmission строго разбирает multipart-форму с одним файлом и тремя полями.
func parseCodeSubmission(w http.ResponseWriter, r *http.Request) (usecase.CodeSubmissionInput, error) {
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "multipart/form-data") {
		return usecase.CodeSubmissionInput{}, apperror.New(apperror.ValidationError, "ожидается multipart/form-data", http.StatusBadRequest)
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxCodeSubmissionBody)
	if err := r.ParseMultipartForm(domain.MaxSourceFileSize); err != nil {
		return usecase.CodeSubmissionInput{}, apperror.New(apperror.InvalidSourceFile, "multipart-форма или файл превышают допустимый размер", http.StatusBadRequest)
	}
	defer func() {
		_ = r.MultipartForm.RemoveAll()
	}()
	if err := validateMultipartFields(r.MultipartForm); err != nil {
		return usecase.CodeSubmissionInput{}, err
	}
	versionID, err := uuid.Parse(r.MultipartForm.Value["task_version_id"][0])
	if err != nil {
		return usecase.CodeSubmissionInput{}, apperror.New(apperror.ValidationError, "task_version_id должен быть UUID", http.StatusBadRequest)
	}
	idempotencyKey, err := uuid.Parse(r.MultipartForm.Value["idempotency_key"][0])
	if err != nil {
		return usecase.CodeSubmissionInput{}, apperror.New(apperror.ValidationError, "idempotency_key должен быть UUID", http.StatusBadRequest)
	}
	fileHeader := r.MultipartForm.File["file"][0]
	file, err := fileHeader.Open()
	if err != nil {
		return usecase.CodeSubmissionInput{}, fmt.Errorf("open uploaded source file: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()
	payload, err := io.ReadAll(io.LimitReader(file, domain.MaxSourceFileSize+1))
	if err != nil {
		return usecase.CodeSubmissionInput{}, fmt.Errorf("read uploaded source file: %w", err)
	}
	if len(payload) > domain.MaxSourceFileSize {
		return usecase.CodeSubmissionInput{}, apperror.New(apperror.InvalidSourceFile, "файл решения превышает 262144 байта", http.StatusBadRequest)
	}

	return usecase.CodeSubmissionInput{
		TaskVersionID:  versionID,
		IdempotencyKey: idempotencyKey,
		Language:       domain.ProgrammingLanguage(r.MultipartForm.Value["language"][0]),
		SourceFileName: fileHeader.Filename,
		SourceCode:     string(payload),
	}, nil
}

// validateMultipartFields запрещает неоднозначные, повторяющиеся и неизвестные поля формы.
func validateMultipartFields(form *multipart.Form) error {
	allowedValues := map[string]struct{}{
		"task_version_id": {},
		"idempotency_key": {},
		"language":        {},
	}
	for key, values := range form.Value {
		if _, ok := allowedValues[key]; !ok || len(values) != 1 || strings.TrimSpace(values[0]) == "" {
			return apperror.New(apperror.ValidationError, "multipart-форма содержит неизвестное, пустое или повторяющееся поле", http.StatusBadRequest)
		}
	}
	for key := range allowedValues {
		if len(form.Value[key]) != 1 {
			return apperror.New(apperror.ValidationError, "task_version_id, idempotency_key и language обязательны", http.StatusBadRequest)
		}
	}
	if len(form.File) != 1 || len(form.File["file"]) != 1 {
		return apperror.New(apperror.InvalidSourceFile, "нужно передать ровно один файл в поле file", http.StatusBadRequest)
	}

	return nil
}
