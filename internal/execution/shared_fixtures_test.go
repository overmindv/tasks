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
	for _, mode := range []string{"execution", "pytest", "go-test"} {
		t.Run(mode, func(t *testing.T) {
			payload, err := os.ReadFile(filepath.Join(root, mode+".result.json"))
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
			if mode != "execution" {
				return
			} // tasks пока публикует только stdin-прогоны.
			payload, err = os.ReadFile(filepath.Join(root, mode+".request.json"))
			if err != nil {
				t.Fatal(err)
			}
			var request RunRequest
			if err := json.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
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
