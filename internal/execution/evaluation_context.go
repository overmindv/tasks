package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// EvaluationContext связывает результат с неизменяемой версией и входами запуска.
type EvaluationContext struct {
	TaskVersionID string `json:"task_version_id"`
	SuiteSHA256   string `json:"suite_sha256"`
}

// InputSuiteSHA256 хеширует упорядоченный JSON-массив stdin (Go json.Marshal).
// Эталоны остаются в tasks; их идентичность задаётся TaskVersionID.
func InputSuiteSHA256(inputs []string) string {
	payload, _ := json.Marshal(inputs)
	sum := sha256.Sum256(payload)

	return hex.EncodeToString(sum[:])
}
