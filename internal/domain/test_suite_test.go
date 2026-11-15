package domain

import (
	"strings"
	"testing"
)

func validSuite() SuiteDefinition {
	return SuiteDefinition{Language: ProgrammingLanguagePython, LanguageVersion: "3.12", Checker: "trim_newlines", Limits: SuiteLimits{CPUms: 1000, Wallms: 2000, MemoryMB: 64, PIDs: 32, StdoutBytes: 32768, StderrBytes: 32768, WorkspaceBytes: 1048576}, Cases: []SuiteCase{{ID: "sample", Input: "1", Output: "1"}, {ID: "secret", Input: "2", Output: "2", Hidden: true}}}
}
func TestSuiteInvariants(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*SuiteDefinition)
	}{
		{"duplicate", func(s *SuiteDefinition) { s.Cases[1].ID = s.Cases[0].ID }},
		{"no hidden", func(s *SuiteDefinition) { s.Cases[1].Hidden = false }},
		{"unknown checker", func(s *SuiteDefinition) { s.Checker = "exec" }},
		{"language", func(s *SuiteDefinition) { s.Language = "bash" }},
		{"unbounded", func(s *SuiteDefinition) { s.Limits.CPUms = 0 }},
		{"input size", func(s *SuiteDefinition) { s.Cases[0].Input = strings.Repeat("x", 65537) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := validSuite()
			tt.change(&s)
			if ValidateSuite(s, true) == nil {
				t.Fatal("invalid suite accepted")
			}
		})
	}
	s := validSuite()
	if err := ValidateSuite(s, true); err != nil {
		t.Fatal(err)
	}
	original := s.Checksum()
	s.Limits.CPUms++
	if original == s.Checksum() {
		t.Fatal("limits excluded from checksum")
	}
	s = validSuite()
	s.Cases[1].Output = "different"
	if original == s.Checksum() {
		t.Fatal("expected output excluded from checksum")
	}
}
