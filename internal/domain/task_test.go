package domain

import "testing"

// TestValidateTaskInput проверяет основные инварианты обоих типов тестов.
func TestValidateTaskInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   TaskInput
		wantErr bool
	}{
		{
			name: "single choice valid",
			input: TaskInput{
				Title:      "HTTP status",
				Statement:  "Какой статус означает успех?",
				TaskType:   TaskTypeSingleChoice,
				Difficulty: DifficultyEasy,
				Options: []OptionInput{
					{Text: "200", IsCorrect: true},
					{Text: "500", IsCorrect: false},
				},
			},
		},
		{
			name: "single choice has two correct",
			input: TaskInput{
				Title:      "HTTP status",
				Statement:  "Выберите ответ",
				TaskType:   TaskTypeSingleChoice,
				Difficulty: DifficultyEasy,
				Options: []OptionInput{
					{Text: "200", IsCorrect: true},
					{Text: "201", IsCorrect: true},
				},
			},
			wantErr: true,
		},
		{
			name: "multiple choice valid",
			input: TaskInput{
				Title:      "Go primitives",
				Statement:  "Выберите встроенные типы",
				TaskType:   TaskTypeMultipleChoice,
				Difficulty: DifficultyMedium,
				Options: []OptionInput{
					{Text: "string", IsCorrect: true},
					{Text: "int", IsCorrect: true},
					{Text: "decimal", IsCorrect: false},
				},
			},
		},
		{
			name: "duplicate option text",
			input: TaskInput{
				Title:      "Duplicates",
				Statement:  "Выберите ответ",
				TaskType:   TaskTypeMultipleChoice,
				Difficulty: DifficultyHard,
				Options: []OptionInput{
					{Text: "Go", IsCorrect: true},
					{Text: " go ", IsCorrect: false},
				},
			},
			wantErr: true,
		},
		{
			name: "programming valid without options",
			input: TaskInput{
				Title:       "Два указателя",
				Statement:   "Найдите пару с заданной суммой.",
				TaskType:    TaskTypeProgramming,
				Difficulty:  DifficultyMedium,
				Tags:        []string{"arrays"},
				Examples:    []TaskExample{{Input: "1 2 3", Output: "1 3"}},
				Constraints: []string{"1 <= n <= 1000"},
			},
		},
		{
			name: "programming rejects choice options",
			input: TaskInput{
				Title:      "Два указателя",
				Statement:  "Найдите пару с заданной суммой.",
				TaskType:   TaskTypeProgramming,
				Difficulty: DifficultyMedium,
				Options:    []OptionInput{{Text: "вариант", IsCorrect: true}},
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := NormalizeTaskInput(test.input)
			err := ValidateTaskInput(input)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateTaskInput() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

// TestCanTransition фиксирует разрешённый lifecycle теста.
func TestCanTransition(t *testing.T) {
	t.Parallel()
	if !CanTransition(TaskStatusDraft, TaskStatusPublished) {
		t.Fatal("draft -> published должен быть разрешён")
	}
	if CanTransition(TaskStatusPublished, TaskStatusDraft) {
		t.Fatal("published -> draft не должен быть разрешён")
	}
	if !CanTransition(TaskStatusArchived, TaskStatusDraft) {
		t.Fatal("archived -> draft должен быть разрешён")
	}
}

// TestNormalizePagination проверяет безопасные границы pagination.
func TestNormalizePagination(t *testing.T) {
	t.Parallel()
	limit, offset := NormalizePagination(1000, -5)
	if limit != 100 || offset != 0 {
		t.Fatalf("NormalizePagination() = %d, %d", limit, offset)
	}
}
