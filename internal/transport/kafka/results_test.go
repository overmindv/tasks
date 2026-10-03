package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/usecase"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeResultHandler имитирует контракт resultHandler без Kafka и PostgreSQL.
type fakeResultHandler struct {
	handleErr error
	rejectErr error

	handled      int
	handleMeta   usecase.ExecutionMessageMetadata
	handleResult execution.RunResult
	rejected     []rejectCall
}

// rejectCall фиксирует один вызов RejectResult.
type rejectCall struct {
	metadata  usecase.ExecutionMessageMetadata
	eventID   *uuid.UUID
	errorCode string
}

// HandleRunResult запоминает вызов или возвращает настроенную ошибку.
func (f *fakeResultHandler) HandleRunResult(_ context.Context, metadata usecase.ExecutionMessageMetadata, result execution.RunResult) error {
	f.handled++
	f.handleMeta = metadata
	f.handleResult = result

	return f.handleErr
}

// RejectResult запоминает запись об отклонении или возвращает настроенную ошибку.
func (f *fakeResultHandler) RejectResult(_ context.Context, metadata usecase.ExecutionMessageMetadata, eventID *uuid.UUID, errorCode string) error {
	f.rejected = append(f.rejected, rejectCall{metadata: metadata, eventID: eventID, errorCode: errorCode})

	return f.rejectErr
}

// discardLogger гасит логи, чтобы тесты не выводили шум в stdout.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestProcessResultHandlesValidEvent проверяет commit-путь: валидный результат
// передаётся в HandleRunResult с корректными метаданными, ошибки нет.
func TestProcessResultHandlesValidEvent(t *testing.T) {
	t.Parallel()
	attemptID := uuid.New()
	submissionID := uuid.New()
	handler := &fakeResultHandler{}
	record := &kgo.Record{
		Topic:     "code-execution.results.v1",
		Partition: 2,
		Offset:    11,
		Value:     validResultPayload(submissionID.String(), attemptID.String()),
	}
	if err := processResult(context.Background(), record, handler, discardLogger()); err != nil {
		t.Fatalf("processResult() error = %v", err)
	}
	if handler.handled != 1 || len(handler.rejected) != 0 {
		t.Fatalf("handled=%d, rejected=%d, want handled=1 rejected=0", handler.handled, len(handler.rejected))
	}
	if handler.handleMeta.Topic != record.Topic || handler.handleMeta.Partition != record.Partition ||
		handler.handleMeta.Offset != record.Offset || handler.handleMeta.PayloadSHA256 == "" {
		t.Fatalf("metadata = %#v", handler.handleMeta)
	}
	if handler.handleResult.AttemptID != attemptID.String() || handler.handleResult.SubmissionID != submissionID.String() ||
		len(handler.handleResult.Cases) != 1 {
		t.Fatalf("result = %#v", handler.handleResult)
	}
}

// TestProcessResultRejectsUndecodableEvent проверяет commit-путь при decode-ошибке:
// RejectResult вызывается с INVALID_EVENT либо UNSUPPORTED_EVENT, ошибки нет.
func TestProcessResultRejectsUndecodableEvent(t *testing.T) {
	t.Parallel()
	attemptID := uuid.New()
	tests := []struct {
		name     string
		payload  []byte
		wantCode string
		wantID   *uuid.UUID
	}{
		{
			name:     "invalid json",
			payload:  []byte(`{not json`),
			wantCode: execution.InboxErrorInvalidEvent,
			wantID:   nil,
		},
		{
			name:     "valid json without attempt",
			payload:  []byte(`{"schema_version":"sandbox.run.result.v9"}`),
			wantCode: execution.InboxErrorUnsupportedEvent,
			wantID:   nil,
		},
		{
			name:     "unsupported schema version",
			payload:  []byte(`{"schema_version":"sandbox.run.result.v9","attempt_id":"` + attemptID.String() + `"}`),
			wantCode: execution.InboxErrorUnsupportedEvent,
			wantID:   &attemptID,
		},
		{
			name:     "decodes but fails validation",
			payload:  []byte(`{"schema_version":"sandbox.run.result.v1","attempt_id":"` + attemptID.String() + `"}`),
			wantCode: execution.InboxErrorInvalidEvent,
			wantID:   &attemptID,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler := &fakeResultHandler{}
			record := &kgo.Record{Topic: "results", Partition: 3, Offset: 7, Value: tc.payload}
			if err := processResult(context.Background(), record, handler, discardLogger()); err != nil {
				t.Fatalf("processResult() error = %v", err)
			}
			if handler.handled != 0 || len(handler.rejected) != 1 {
				t.Fatalf("handled=%d, rejected=%d, want handled=0 rejected=1", handler.handled, len(handler.rejected))
			}
			reject := handler.rejected[0]
			if reject.errorCode != tc.wantCode {
				t.Fatalf("error_code = %q, want %q", reject.errorCode, tc.wantCode)
			}
			if (reject.eventID == nil) != (tc.wantID == nil) || (reject.eventID != nil && *reject.eventID != *tc.wantID) {
				t.Fatalf("eventID = %v, want %v", reject.eventID, tc.wantID)
			}
			if reject.metadata.Topic != record.Topic || reject.metadata.Offset != record.Offset {
				t.Fatalf("reject metadata = %#v", reject.metadata)
			}
		})
	}
}

// TestProcessResultRejectsPermanentMismatch проверяет permanent-путь: mismatch
// (ExecutionResultMismatch) завершается записью rejected и возвратом nil,
// чтобы consumer подтвердил offset, а не ретраил poison-сообщение.
func TestProcessResultRejectsPermanentMismatch(t *testing.T) {
	t.Parallel()
	attemptID := uuid.New()
	submissionID := uuid.New()
	handler := &fakeResultHandler{
		handleErr: apperror.New(apperror.ExecutionResultMismatch, "запуск завершён конкурирующим обработчиком", 409),
	}
	record := &kgo.Record{
		Topic:     "code-execution.results.v1",
		Partition: 5,
		Offset:    21,
		Value:     validResultPayload(submissionID.String(), attemptID.String()),
	}
	if err := processResult(context.Background(), record, handler, discardLogger()); err != nil {
		t.Fatalf("processResult() error = %v", err)
	}
	if handler.handled != 1 || len(handler.rejected) != 1 {
		t.Fatalf("handled=%d, rejected=%d, want handled=1 rejected=1", handler.handled, len(handler.rejected))
	}
	reject := handler.rejected[0]
	if reject.errorCode != execution.InboxErrorInvalidEvent {
		t.Fatalf("error_code = %q, want %q", reject.errorCode, execution.InboxErrorInvalidEvent)
	}
	if reject.eventID == nil || *reject.eventID != attemptID {
		t.Fatalf("eventID = %v, want attempt %v", reject.eventID, attemptID)
	}
}

// TestProcessResultReturnsTransientError проверяет retry-путь: транзиентная ошибка
// сервиса возвращается наружу, RejectResult не вызывается.
func TestProcessResultReturnsTransientError(t *testing.T) {
	t.Parallel()
	handler := &fakeResultHandler{handleErr: errors.New("db unavailable")}
	record := &kgo.Record{
		Topic:     "results",
		Partition: 1,
		Offset:    3,
		Value:     validResultPayload(uuid.NewString(), uuid.NewString()),
	}
	err := processResult(context.Background(), record, handler, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "handle execution result") {
		t.Fatalf("processResult() error = %v, want wrapped handle execution result", err)
	}
	if len(handler.rejected) != 0 {
		t.Fatalf("RejectResult не должен вызываться при транзиентной ошибке, rejected = %#v", handler.rejected)
	}
}

// TestProcessResultRejectFailureReturnsError проверяет, что сбой самой rejected-записи
// тоже возвращается наружу (событие не теряется, offset не подтверждается).
func TestProcessResultRejectFailureReturnsError(t *testing.T) {
	t.Parallel()
	handler := &fakeResultHandler{rejectErr: errors.New("db unavailable")}
	record := &kgo.Record{Topic: "results", Partition: 4, Offset: 9, Value: []byte(`{not json`)}
	err := processResult(context.Background(), record, handler, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "persist rejected execution result") {
		t.Fatalf("processResult() error = %v, want wrapped persist rejected error", err)
	}
	if len(handler.rejected) != 1 {
		t.Fatalf("rejected = %d, want 1", len(handler.rejected))
	}
}

// validResultPayload строит валидный JSON result event с заданными идентификаторами.
func validResultPayload(submissionID, attemptID string) []byte {
	return []byte(fmt.Sprintf(`{
        "schema_version":"sandbox.run.result.v1",
        "submission_id":"%s",
        "attempt_id":"%s",
        "user_id":"`+uuid.NewString()+`",
        "task_id":"`+uuid.NewString()+`",
        "language":{"id":"python","version":"3.12"},
        "status":"ok",
        "summary":{"cases_total":1,"cases_with_error":0,"tests_total":0,"tests_passed":0,"tests_failed":0},
        "timing":{"queue_ms":1,"prepare_ms":2,"run_ms":3,"total_ms":6},
        "resources":{"cpu_ms":3,"memory_peak_bytes":2048,"exit_code":0},
        "logs":{"stdout":"","stderr":"","truncated":false},
        "cases":[{"index":0,"stdout":"4\n","stderr":"","truncated":false,"exit_code":0,"status":"ok","cpu_ms":3,"memory_peak_bytes":2048,"duration_ms":3}],
        "error":null,
        "trace_id":"trace-1",
        "created_at":"2026-08-31T10:00:00Z"
    }`, submissionID, attemptID))
}
