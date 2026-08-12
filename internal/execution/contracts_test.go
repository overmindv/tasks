package execution

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/domain"
)

// TestRequestContractRoundTrip проверяет обязательные поля JSON-запроса sandbox.
func TestRequestContractRoundTrip(t *testing.T) {
	t.Parallel()
	event := validRequestEvent()
	payload, err := EncodeRequest(event)
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	for _, field := range [][]byte{[]byte(`"schema_version":1`), []byte(`"language":"python"`), []byte(`"visibility":"open"`)} {
		if !bytes.Contains(payload, field) {
			t.Fatalf("payload %s does not contain %s", payload, field)
		}
	}
}

// TestDecodeResultRejectsUnknownField проверяет строгую эволюцию result contract.
func TestDecodeResultRejectsUnknownField(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
        "event_id":"` + uuid.NewString() + `",
        "event_type":"code_execution.completed",
        "schema_version":1,
        "occurred_at":"2026-08-13T10:00:00Z",
        "correlation_id":"` + uuid.NewString() + `",
        "submission_id":"` + uuid.NewString() + `",
        "execution_id":"` + uuid.NewString() + `",
        "task_id":"` + uuid.NewString() + `",
        "task_version_id":"` + uuid.NewString() + `",
        "verdict":"accepted",
        "tests":[],
        "unexpected":true
    }`)
	if _, err := DecodeResult(payload); err == nil {
		t.Fatal("DecodeResult() должен отклонить неизвестное поле")
	}
}

// validRequestEvent создаёт минимальный валидный request event.
func validRequestEvent() RequestEvent {
	return RequestEvent{
		EventID:       uuid.New(),
		EventType:     RequestEventType,
		SchemaVersion: SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: uuid.New(),
		SubmissionID:  uuid.New(),
		ExecutionID:   uuid.New(),
		TaskID:        uuid.New(),
		TaskVersionID: uuid.New(),
		Language:      domain.ProgrammingLanguagePython,
		Source: SourceFile{
			Name:    "main.py",
			Content: "print(input())",
		},
		Tests: []TestCase{
			{ID: "open-1", Visibility: TestVisibilityOpen, Input: "1", ExpectedOutput: "1"},
		},
		Limits: ResourceLimits{
			TimeMS:      1000,
			MemoryBytes: 64 * 1024 * 1024,
		},
	}
}
