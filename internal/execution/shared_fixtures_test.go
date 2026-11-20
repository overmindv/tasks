package execution

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Общие payload хранятся как snapshot sandbox и проверяются также JSON Schema.
func TestSharedExecutionFixtures(t *testing.T) {
	t.Parallel()
	root := "../../tests/fixtures/execution-contract"
	for _, mode := range []string{"execution", "execution-go", "pytest", "go-test"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			payload, err := os.ReadFile(filepath.Join(root, mode+".result.json")) //nolint:gosec // mode ограничен фиксированным набором fixtures
			if err != nil {
				t.Fatal(err)
			}
			result, err := DecodeRunResult(payload)
			if err != nil {
				t.Fatal(err)
			}
			if result.SubmissionID != "11111111-1111-4111-8111-111111111111" || result.AttemptID != "22222222-2222-4222-8222-222222222222" {
				t.Fatal("result identity lost")
			}
			if mode != "execution" && mode != "execution-go" {
				return
			} // tasks пока публикует только stdin-прогоны.
			payload, err = os.ReadFile(filepath.Join(root, mode+".request.json")) //nolint:gosec // mode ограничен фиксированным набором fixtures
			if err != nil {
				t.Fatal(err)
			}
			var request RunRequest
			if decodeErr := json.Unmarshal(payload, &request); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if result.EvaluationContext == nil || result.EvaluationContext.TaskVersionID != request.Metadata["task_version_id"] || result.EvaluationContext.SuiteSHA256 != InputSuiteSHA256(request.Execution.Inputs) {
				t.Fatal("fixture evaluation context does not match inputs/version")
			}
			encoded, err := EncodeRunRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			var before, after any
			if err := json.Unmarshal(payload, &before); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("request round-trip changed wire payload")
			}
		})
	}
}
