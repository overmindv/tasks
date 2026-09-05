package httpapi

import (
	"bytes"
	"errors"
	"mime/multipart"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
)

// TestParseCodeSubmission проверяет multipart contract файлового решения.
func TestParseCodeSubmission(t *testing.T) {
	t.Parallel()
	versionID := uuid.New()
	key := uuid.New()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("task_version_id", versionID.String()); err != nil {
		t.Fatalf("WriteField(task_version_id) error = %v", err)
	}
	if err := writer.WriteField("idempotency_key", key.String()); err != nil {
		t.Fatalf("WriteField(idempotency_key) error = %v", err)
	}
	if err := writer.WriteField("language", "python"); err != nil {
		t.Fatalf("WriteField(language) error = %v", err)
	}
	file, err := writer.CreateFormFile("file", "main.py")
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	if _, err := file.Write([]byte("print(input())")); err != nil {
		t.Fatalf("file.Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}
	request := httptest.NewRequest("POST", "/v1/tasks/unused/code-submissions", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	input, err := parseCodeSubmission(httptest.NewRecorder(), request)
	if err != nil {
		t.Fatalf("parseCodeSubmission() error = %v", err)
	}
	if input.TaskVersionID != versionID || input.IdempotencyKey != key || input.Language != domain.ProgrammingLanguagePython || input.SourceCode != "print(input())" {
		t.Fatalf("input = %#v", input)
	}
}

// TestParseCodeSubmissionConsole проверяет консольный вариант: текстовое поле source_code без файла.
func TestParseCodeSubmissionConsole(t *testing.T) {
	t.Parallel()
	versionID := uuid.New()
	key := uuid.New()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("task_version_id", versionID.String())
	_ = writer.WriteField("idempotency_key", key.String())
	_ = writer.WriteField("language", "python")
	_ = writer.WriteField("source_code", "print(input())")
	_ = writer.Close()
	request := httptest.NewRequest("POST", "/v1/tasks/unused/code-submissions", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	input, err := parseCodeSubmission(httptest.NewRecorder(), request)
	if err != nil {
		t.Fatalf("parseCodeSubmission() error = %v", err)
	}
	if input.TaskVersionID != versionID || input.IdempotencyKey != key {
		t.Fatalf("input = %#v", input)
	}
	if input.Language != domain.ProgrammingLanguagePython || input.SourceCode != "print(input())" {
		t.Fatalf("input = %#v", input)
	}
	// Консольный вариант осознанно не задаёт имя файла: его выводит usecase из языка.
	if input.SourceFileName != "" {
		t.Fatalf("консольный вариант не должен задавать имя файла, получено %q", input.SourceFileName)
	}
}

// TestParseCodeSubmissionRejectsBothSources проверяет отклонение при одновременно файле и коде в консоли.
func TestParseCodeSubmissionRejectsBothSources(t *testing.T) {
	t.Parallel()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("task_version_id", uuid.NewString())
	_ = writer.WriteField("idempotency_key", uuid.NewString())
	_ = writer.WriteField("language", "go")
	_ = writer.WriteField("source_code", "package main")
	file, err := writer.CreateFormFile("file", "main.go")
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	_, _ = file.Write([]byte("package main"))
	_ = writer.Close()
	request := httptest.NewRequest("POST", "/v1/tasks/unused/code-submissions", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	_, err = parseCodeSubmission(httptest.NewRecorder(), request)
	if err == nil {
		t.Fatal("parseCodeSubmission() должен отклонить оба источника кода")
	}
	var public *apperror.Error
	if !errors.As(err, &public) || public.Code != apperror.AmbiguousCodeSource {
		t.Fatalf("ожидался AMBIGUOUS_CODE_SOURCE, получено: %v", err)
	}
}

// TestParseCodeSubmissionRejectsNoSource проверяет отклонение, когда нет ни файла, ни кода в консоли.
func TestParseCodeSubmissionRejectsNoSource(t *testing.T) {
	t.Parallel()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("task_version_id", uuid.NewString())
	_ = writer.WriteField("idempotency_key", uuid.NewString())
	_ = writer.WriteField("language", "python")
	_ = writer.Close()
	request := httptest.NewRequest("POST", "/v1/tasks/unused/code-submissions", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	_, err := parseCodeSubmission(httptest.NewRecorder(), request)
	if err == nil {
		t.Fatal("parseCodeSubmission() должен отклонить отсутствие источника кода")
	}
	var public *apperror.Error
	if !errors.As(err, &public) || public.Code != apperror.InvalidSourceFile {
		t.Fatalf("ожидался INVALID_SOURCE_FILE, получено: %v", err)
	}
}

// TestParseCodeSubmissionRejectsUnknownField проверяет строгий allowlist multipart-полей.
func TestParseCodeSubmissionRejectsUnknownField(t *testing.T) {
	t.Parallel()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("task_version_id", uuid.NewString())
	_ = writer.WriteField("idempotency_key", uuid.NewString())
	_ = writer.WriteField("language", "go")
	_ = writer.WriteField("unexpected", "value")
	file, err := writer.CreateFormFile("file", "main.go")
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	_, _ = file.Write([]byte("package main"))
	_ = writer.Close()
	request := httptest.NewRequest("POST", "/v1/tasks/unused/code-submissions", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	if _, err := parseCodeSubmission(httptest.NewRecorder(), request); err == nil {
		t.Fatal("parseCodeSubmission() должен отклонить unknown field")
	}
}
