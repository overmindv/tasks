package checker

import (
	"testing"

	"github.com/google/uuid"
)

// TestChoice проверяет set-сравнение ответов без учёта порядка.
func TestChoice(t *testing.T) {
	t.Parallel()
	first := uuid.New()
	second := uuid.New()
	third := uuid.New()
	tests := []struct {
		name     string
		selected []uuid.UUID
		correct  []uuid.UUID
		want     bool
	}{
		{name: "same single", selected: []uuid.UUID{first}, correct: []uuid.UUID{first}, want: true},
		{name: "same set different order", selected: []uuid.UUID{second, first}, correct: []uuid.UUID{first, second}, want: true},
		{name: "missing option", selected: []uuid.UUID{first}, correct: []uuid.UUID{first, second}, want: false},
		{name: "unexpected option", selected: []uuid.UUID{first, third}, correct: []uuid.UUID{first, second}, want: false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := Choice(test.selected, test.correct); got != test.want {
				t.Fatalf("Choice() = %v, want %v", got, test.want)
			}
		})
	}
}
