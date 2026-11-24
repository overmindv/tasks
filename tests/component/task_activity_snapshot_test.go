//go:build component

package component

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

// Журнал публикаций пишется триггером в транзакции смены статуса и не зависит
// от Kafka retention и очистки outbox.
func TestTaskActivitySnapshotRecordsPublication(t *testing.T) {
	ctx := context.Background()
	pool, store := activityStore(t, ctx)

	taskID, versionID := insertActivityTask(t, ctx, pool, "Публикация в журнале")
	publishActivityTask(t, ctx, pool, taskID)

	changes := readActivityChanges(t, ctx, store, 0, store.Watermark(t, ctx))
	published := findActivityChange(t, changes, taskID, domain.TaskActivityPublished)
	if published.TaskVersionID == nil || *published.TaskVersionID != versionID {
		t.Fatalf("версия публикации не сохранена: %+v", published)
	}
	if published.Title == nil || *published.Title != "Публикация в журнале" || published.TaskType == nil || published.Difficulty == nil {
		t.Fatalf("метаданные публикации не сохранены: %+v", published)
	}

	archiveActivityTask(t, ctx, pool, taskID)
	changes = readActivityChanges(t, ctx, store, 0, store.Watermark(t, ctx))
	unpublished := findActivityChange(t, changes, taskID, domain.TaskActivityUnpublished)
	if unpublished.TaskVersionID == nil || *unpublished.TaskVersionID != versionID {
		t.Fatalf("версия снятия с публикации не сохранена: %+v", unpublished)
	}

	beforeRepeat := len(changes)
	if _, err := pool.Exec(ctx, `UPDATE tasks SET status='draft' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	if after := readActivityChanges(t, ctx, store, 0, store.Watermark(t, ctx)); len(after) != beforeRepeat {
		t.Fatalf("переход archived→draft создал лишнюю запись: было %d, стало %d", beforeRepeat, len(after))
	}

	deletedID, _ := insertActivityTask(t, ctx, pool, "Удалённая задача")
	publishActivityTask(t, ctx, pool, deletedID)
	if _, err := pool.Exec(ctx, `UPDATE tasks SET deleted_at=now() WHERE id=$1`, deletedID); err != nil {
		t.Fatal(err)
	}
	changes = readActivityChanges(t, ctx, store, 0, store.Watermark(t, ctx))
	findActivityChange(t, changes, deletedID, domain.TaskActivityUnpublished)
}

// Параллельные публикации не теряются и не сдвигают уже закреплённый watermark.
func TestTaskActivitySnapshotPaginatesConcurrentPublications(t *testing.T) {
	ctx := context.Background()
	pool, store := activityStore(t, ctx)

	const total = 15
	before := store.Watermark(t, ctx)
	tasks := make([]uuid.UUID, total)
	for index := range tasks {
		tasks[index], _ = insertActivityTask(t, ctx, pool, "Параллельная публикация")
	}
	var wait sync.WaitGroup
	for _, taskID := range tasks {
		wait.Go(func() {
			if _, err := pool.Exec(ctx, `UPDATE tasks SET status='published' WHERE id=$1`, taskID); err != nil {
				t.Error(err)
			}
		})
	}
	wait.Wait()

	watermark := store.Watermark(t, ctx)
	service := usecase.NewTaskActivityService(store)
	seen := make(map[uuid.UUID]int, total)
	after := before
	for {
		page, err := service.Changes(ctx, &watermark, after, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, change := range page.Changes {
			if change.Revision > watermark {
				t.Fatalf("страница вышла за watermark: %d > %d", change.Revision, watermark)
			}
			seen[change.TaskID]++
		}
		if !page.More {
			break
		}
		after = page.Through
	}
	for _, taskID := range tasks {
		if seen[taskID] != 1 {
			t.Fatalf("задача %s встречается %d раз", taskID, seen[taskID])
		}
	}
}

func TestTaskActivitySnapshotPinsWatermark(t *testing.T) {
	ctx := context.Background()
	pool, store := activityStore(t, ctx)

	before := store.Watermark(t, ctx)
	first, _ := insertActivityTask(t, ctx, pool, "До watermark")
	publishActivityTask(t, ctx, pool, first)
	watermark := store.Watermark(t, ctx)

	second, _ := insertActivityTask(t, ctx, pool, "После watermark")
	publishActivityTask(t, ctx, pool, second)

	service := usecase.NewTaskActivityService(store)
	seen := make(map[uuid.UUID]bool, 2)
	after := before
	for {
		page, err := service.Changes(ctx, &watermark, after, 4)
		if err != nil {
			t.Fatal(err)
		}
		if page.Watermark != watermark {
			t.Fatalf("watermark не закреплён: %d != %d", page.Watermark, watermark)
		}
		for _, change := range page.Changes {
			seen[change.TaskID] = true
		}
		if !page.More {
			break
		}
		after = page.Through
	}
	if !seen[first] || seen[second] {
		t.Fatal("страница на закреплённом watermark содержит не те события")
	}
}

func TestTaskActivitySnapshotSurvivesOutboxCleanup(t *testing.T) {
	ctx := context.Background()
	pool, store := activityStore(t, ctx)

	taskID, _ := insertActivityTask(t, ctx, pool, "Переживает outbox")
	publishActivityTask(t, ctx, pool, taskID)
	messageID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO outbox (id, topic, message_key, message_value, status) VALUES ($1,$2,$3,$4,'sent')`,
		messageID, domain.TaskActivityTopic, taskID.String(), []byte(`{"event":"task.published"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM outbox WHERE id=$1`, messageID); err != nil {
		t.Fatal(err)
	}

	changes := readActivityChanges(t, ctx, store, 0, store.Watermark(t, ctx))
	findActivityChange(t, changes, taskID, domain.TaskActivityPublished)
}

type activityStoreFixture struct {
	*postgresadapter.Postgres
	pool *pgxpool.Pool
}

func (f activityStoreFixture) Watermark(t *testing.T, ctx context.Context) int64 {
	t.Helper()
	watermark, err := f.Postgres.TaskActivityWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}

	return watermark
}

func activityStore(t *testing.T, ctx context.Context) (*pgxpool.Pool, activityStoreFixture) {
	t.Helper()
	pool, err := pgxpool.New(ctx, os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool, activityStoreFixture{Postgres: postgresadapter.New(pool), pool: pool}
}

func insertActivityTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, title string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	taskID, versionID, actor := uuid.New(), uuid.New(), uuid.New()
	// current_version_id ссылается на task_versions, поэтому версия создаётся
	// первой, а указатель проставляется отдельным UPDATE.
	if _, err := pool.Exec(ctx, `INSERT INTO tasks (id, status, created_by, updated_by) VALUES ($1,'draft',$2,$2)`, taskID, actor); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO task_versions (id, task_id, version_number, title, statement, task_type, difficulty, created_by) VALUES ($1,$2,1,$3,'statement','single_choice','easy',$4)`,
		versionID, taskID, title, actor); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tasks SET current_version_id=$2 WHERE id=$1`, taskID, versionID); err != nil {
		t.Fatal(err)
	}

	return taskID, versionID
}

func publishActivityTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID uuid.UUID) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE tasks SET status='published' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
}

func archiveActivityTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, taskID uuid.UUID) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE tasks SET status='archived' WHERE id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
}

func readActivityChanges(t *testing.T, ctx context.Context, store activityStoreFixture, after, watermark int64) []domain.TaskActivityChange {
	t.Helper()
	var all []domain.TaskActivityChange
	for {
		page, err := store.TaskActivityChanges(ctx, watermark, after, usecase.MaxTaskActivityPage)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page...)
		if len(page) < usecase.MaxTaskActivityPage {
			return all
		}
		after = page[len(page)-1].Revision
	}
}

func findActivityChange(t *testing.T, changes []domain.TaskActivityChange, taskID uuid.UUID, eventType string) domain.TaskActivityChange {
	t.Helper()
	for _, change := range changes {
		if change.TaskID == taskID && change.EventType == eventType {
			return change
		}
	}
	t.Fatalf("запись %s для задачи %s не найдена", eventType, taskID)

	return domain.TaskActivityChange{}
}
