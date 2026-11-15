package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"unicode/utf8"

	"github.com/google/uuid"
)

// SuiteDefinition — полный закрытый snapshot; никогда не включается в public Task DTO.
type SuiteDefinition struct {
	Language        ProgrammingLanguage `json:"language"`
	LanguageVersion string              `json:"language_version"`
	Checker         string              `json:"checker"`
	Limits          SuiteLimits         `json:"limits"`
	Cases           []SuiteCase         `json:"cases"`
}
type SuiteLimits struct {
	CPUms          int `json:"cpu_ms"`
	Wallms         int `json:"wall_ms"`
	MemoryMB       int `json:"memory_mb"`
	PIDs           int `json:"pids"`
	StdoutBytes    int `json:"stdout_bytes"`
	StderrBytes    int `json:"stderr_bytes"`
	WorkspaceBytes int `json:"workspace_bytes"`
}
type SuiteCase struct {
	ID     string `json:"id"`
	Input  string `json:"input"`
	Output string `json:"output"`
	Hidden bool   `json:"hidden"`
}
type SuiteReference struct {
	ID             uuid.UUID `json:"suite_id"`
	Version        string    `json:"version"`
	ChecksumSHA256 string    `json:"checksum_sha256"`
}
type PublishedSuite struct {
	Reference     SuiteReference  `json:"reference"`
	TaskID        uuid.UUID       `json:"task_id"`
	TaskVersionID uuid.UUID       `json:"task_version_id"`
	Definition    SuiteDefinition `json:"definition"`
}
type SuiteDraft struct {
	ID                 uuid.UUID       `json:"id"`
	TaskID             uuid.UUID       `json:"task_id"`
	BaseVersionID      uuid.UUID       `json:"base_version_id"`
	Revision           int             `json:"revision"`
	Definition         SuiteDefinition `json:"definition"`
	PublishedVersionID *uuid.UUID      `json:"published_version_id,omitempty"`
}

// ValidateSuite ограничивает данные до записи и до публикации, без значений в ошибке.
func ValidateSuite(s SuiteDefinition, requireHidden bool) error {
	if (s.Language != ProgrammingLanguagePython && s.Language != ProgrammingLanguageGo) || !regexp.MustCompile(`^[0-9]+\.[0-9]+(\.[0-9]+)?$`).MatchString(s.LanguageVersion) {
		return errors.New("invalid suite language")
	}
	if s.Checker != "exact" && s.Checker != "trim_newlines" {
		return errors.New("unsupported suite checker")
	}
	l := s.Limits
	if l.CPUms < 100 || l.CPUms > 30000 || l.Wallms < l.CPUms || l.Wallms > 60000 || l.MemoryMB < 16 || l.MemoryMB > 1024 || l.PIDs < 1 || l.PIDs > 128 || l.StdoutBytes < 1 || l.StdoutBytes > 65536 || l.StderrBytes < 1 || l.StderrBytes > 65536 || l.WorkspaceBytes < 1048576 || l.WorkspaceBytes > 10485760 {
		return errors.New("invalid suite limits")
	}
	if len(s.Cases) < 1 || len(s.Cases) > 20 {
		return errors.New("invalid suite case count")
	}
	seen := map[string]bool{}
	hidden := false
	size := 0
	for _, c := range s.Cases {
		if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(c.ID) || seen[c.ID] {
			return errors.New("invalid suite case identity")
		}
		seen[c.ID] = true
		hidden = hidden || c.Hidden
		size += len(c.Input) + len(c.Output)
		if !utf8.ValidString(c.Input) || !utf8.ValidString(c.Output) || len(c.Input) > 65536 || len(c.Output) > 65536 {
			return errors.New("invalid suite case size")
		}
	}
	if size > 1048576 {
		return errors.New("suite too large")
	}
	if requireHidden && !hidden {
		return errors.New("graded suite requires hidden cases")
	}
	return nil
}
func (s SuiteDefinition) Checksum() string {
	payload, _ := json.Marshal(s)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// InputManifest содержит только stdin; эталоны и checker остаются в tasks.
type InputManifest struct {
	SchemaVersion string   `json:"schema_version"`
	SuiteID       string   `json:"suite_id"`
	Version       string   `json:"version"`
	Inputs        []string `json:"inputs"`
}

func (s SuiteDefinition) Manifest(id uuid.UUID, version string) ([]byte, string) {
	inputs := make([]string, len(s.Cases))
	for i, c := range s.Cases {
		inputs[i] = c.Input
	}
	payload, _ := json.Marshal(InputManifest{SchemaVersion: "sandbox.inputs.v1", SuiteID: id.String(), Version: version, Inputs: inputs})
	sum := sha256.Sum256(payload)
	return payload, hex.EncodeToString(sum[:])
}
