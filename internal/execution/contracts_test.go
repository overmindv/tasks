package execution

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestRunRequestContractRoundTrip проверяет обязательные поля JSON-запроса sandbox.
func TestRunRequestContractRoundTrip(t *testing.T) {
	t.Parallel()
	request := validRunRequest()
	payload, err := EncodeRunRequest(request)
	if err != nil {
		t.Fatalf("EncodeRunRequest() error = %v", err)
	}
	for _, field := range [][]byte{
		[]byte(`"schema_version":"sandbox.run.request.v1"`),
		[]byte(`"mode":"execution"`),
		[]byte(`"language":{"id":"python"`),
	} {
		if !bytes.Contains(payload, field) {
			t.Fatalf("payload %s does not contain %s", payload, field)
		}
	}
}

// TestDecodeRunResultRejectsUnknownField проверяет строгую эволюцию result contract.
func TestDecodeRunResultRejectsUnknownField(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
        "schema_version":"sandbox.run.result.v1",
        "submission_id":"` + uuid.NewString() + `",
        "attempt_id":"` + uuid.NewString() + `",
        "status":"ok",
        "summary":{"cases_total":0,"cases_with_error":0},
        "resources":{"cpu_ms":0,"memory_peak_bytes":0},
        "cases":[],
        "created_at":"2026-08-31T10:00:00Z",
        "unexpected":true
    }`)
	if _, err := DecodeRunResult(payload); err == nil {
		t.Fatal("DecodeRunResult() должен отклонить неизвестное поле")
	}
}

// TestDecodeRunResultRoundTrip валидирует корректный результат с кейсами.
func TestDecodeRunResultRoundTrip(t *testing.T) {
	t.Parallel()
	exit := 0
	result := RunResult{
		SchemaVersion: ResultSchemaVersion,
		SubmissionID:  uuid.NewString(),
		AttemptID:     uuid.NewString(),
		Status:        ResultStatusOK,
		Summary:       ResultSummary{CasesTotal: 1},
		Resources:     ResultResources{CPUms: 12, MemoryPeakBytes: 1024, ExitCode: &exit},
		Cases: []CaseRunResult{
			{Index: 0, Stdout: "hello\n", Stderr: "", Status: CaseStatusOK, CPUms: 12, MemoryPeakBytes: 1024},
		},
		CreatedAt: time.Now().UTC(),
	}
	encoded, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		t.Fatalf("marshal result: %v", marshalErr)
	}
	decoded, decodeErr := DecodeRunResult(encoded)
	if decodeErr != nil {
		t.Fatalf("DecodeRunResult() error = %v", decodeErr)
	}
	if len(decoded.Cases) != 1 || decoded.Cases[0].Stdout != "hello\n" {
		t.Fatalf("DecodeRunResult() = %#v", decoded)
	}
}

// TestDecodeRunResultAcceptsSandboxEnvelope проверяет, что строгий декод принимает
// полный envelope sandbox (с timing/logs/tests), которые tasks не использует,
// но которые песочница всегда включает в результат.
func TestDecodeRunResultAcceptsSandboxEnvelope(t *testing.T) {
	t.Parallel()
	payload := []byte(`{
        "schema_version":"sandbox.run.result.v1",
        "submission_id":"` + uuid.NewString() + `",
        "attempt_id":"` + uuid.NewString() + `",
        "user_id":"` + uuid.NewString() + `",
        "task_id":"` + uuid.NewString() + `",
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
    }`)
	result, err := DecodeRunResult(payload)
	if err != nil {
		t.Fatalf("DecodeRunResult() должен принять полный sandbox envelope: %v", err)
	}
	if len(result.Cases) != 1 || result.Cases[0].Stdout != "4\n" {
		t.Fatalf("DecodeRunResult() = %#v", result)
	}
}

// validRunRequest создаёт минимальный валидный execution request.
func validRunRequest() RunRequest {
	return RunRequest{
		SchemaVersion: RequestSchemaVersion,
		SubmissionID:  uuid.NewString(),
		AttemptID:     uuid.NewString(),
		UserID:        uuid.NewString(),
		TaskID:        uuid.NewString(),
		Language:      Language{ID: "python", Version: "3.12"},
		Code: Code{
			Entrypoint: "main.py",
			Files:      []SourceFile{{Path: "main.py", ContentB64: "cHJpbnQoaW5wdXQoKSkK"}},
		},
		Execution: &ExecutionSpec{Mode: ExecutionMode, Inputs: []string{"1", "2"}},
		Limits: Limits{
			CPUms:          1000,
			Wallms:         1000,
			MemoryMB:       64,
			PIDs:           32,
			StdoutBytes:    32768,
			StderrBytes:    32768,
			WorkspaceBytes: 1048576,
		},
		TraceID:   "trace-001",
		CreatedAt: time.Now().UTC(),
	}
}
