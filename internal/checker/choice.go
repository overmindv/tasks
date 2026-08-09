package checker

import (
	"github.com/google/uuid"
	"github.com/samber/lo"
)

// Choice сравнивает выбранные и правильные варианты как множества UUID.
func Choice(selected, correct []uuid.UUID) bool {
	selected = lo.Uniq(selected)
	correct = lo.Uniq(correct)
	if len(selected) != len(correct) {
		return false
	}
	correctSet := lo.SliceToMap(correct, func(id uuid.UUID) (uuid.UUID, struct{}) {
		return id, struct{}{}
	})

	return lo.EveryBy(selected, func(id uuid.UUID) bool {
		_, ok := correctSet[id]

		return ok
	})
}
