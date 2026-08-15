package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestValidateCandidateImport фиксирует обязательную provenance и programming payload.
func TestValidateCandidateImport(t *testing.T) {
	t.Parallel()
	input := NormalizeCandidateImport(CandidateImport{
		ExternalID:      "4/A",
		SourceID:        "codeforces",
		SourceURL:       "https://codeforces.com/problemset/problem/4/A",
		SourceHash:      "hash",
		RetrievedAt:     time.Now().UTC(),
		CollectionJobID: uuid.New(),
		Title:           "Watermelon",
		Statement:       "Определите, можно ли разделить арбуз.",
		Difficulty:      DifficultyEasy,
	})
	if err := ValidateCandidateImport(input); err != nil {
		t.Fatalf("valid candidate rejected: %v", err)
	}
	input.SourceURL = ""
	if err := ValidateCandidateImport(input); err == nil {
		t.Fatal("expected immutable provenance validation error")
	}
}

// TestValidateCandidateReview требует optimistic revision.
func TestValidateCandidateReview(t *testing.T) {
	t.Parallel()
	input := CandidateReview{
		ExpectedRevision: 1,
		Title:            "Задача",
		Statement:        "Полное условие задачи.",
		Difficulty:       DifficultyEasy,
	}
	if err := ValidateCandidateReview(input); err != nil {
		t.Fatalf("valid review rejected: %v", err)
	}
	input.ExpectedRevision = 0
	if err := ValidateCandidateReview(input); err == nil {
		t.Fatal("expected revision validation error")
	}
}
