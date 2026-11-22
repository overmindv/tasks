//go:build component

package component

import (
	"github.com/google/uuid"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"testing"
)

// seedLegacyPublication моделирует уже опубликованные до OVM-252 данные.
// Проверки истории, replay и каталога не должны создавать редакционные запуски
// в своих фикстурах. Новый publish проверяется в TestEditorialPublicationPostgres.
func seedLegacyPublication(t testing.TB, id, actor uuid.UUID) domain.TaskDetail {
	t.Helper()
	repo := postgresadapter.New(openComponentDB(t))
	if err := repo.SetTaskStatus(t.Context(), id, domain.TaskStatusPublished, actor); err != nil {
		t.Fatal(err)
	}
	detail, err := repo.GetTaskDetail(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return detail
}
