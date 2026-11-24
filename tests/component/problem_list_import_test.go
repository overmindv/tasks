//go:build component

package component

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/usecase"
)

// Повторный импорт того же списка не дублирует список и кандидатов,
// а частичный сбор помечает список и снимается полным повтором.
func TestProblemListImportIsIdempotentAndTracksTruncation(t *testing.T) {
	ctx := context.Background()
	pool, store := problemListStore(t, ctx)
	candidates := usecase.NewCandidateService(store)
	lists := usecase.NewProblemListService(store)

	url := "https://leetcode.com/problem-list/" + uuid.NewString()
	jobID := uuid.New()
	items := problemListItems(url, jobID)

	if results := candidates.ImportBatch(ctx, items); !allStatuses(results, "imported") {
		t.Fatalf("первый импорт: %#v", results)
	}
	if results := candidates.ImportBatch(ctx, items); !allStatuses(results, "duplicate") {
		t.Fatalf("повторный импорт должен быть идемпотентным: %#v", results)
	}
	listID, truncated := problemListRow(t, ctx, pool, url)
	if !truncated {
		t.Fatal("частичный сбор не отметил список")
	}
	if count := candidateCount(t, ctx, pool, listID); count != len(items) {
		t.Fatalf("кандидатов в списке %d, ожидалось %d", count, len(items))
	}

	// Полный повторный сбор снимает отметку о частичности.
	items[0].ProblemList.Truncated = false
	candidates.ImportBatch(ctx, items)
	if _, truncated := problemListRow(t, ctx, pool, url); truncated {
		t.Fatal("полный сбор не снял отметку о частичности")
	}

	detail, err := lists.Get(ctx, listID, true)
	if err != nil {
		t.Fatal(err)
	}
	if detail.PendingCandidates != len(items) || detail.MemberCount != 0 {
		t.Fatalf("детали до модерации: %+v", detail)
	}
}

// Одобрение переносит порядок сбора в члены списка, а обычный пользователь
// видит только опубликованные задачи.
func TestProblemListMembersFollowCollectionOrderAndVisibility(t *testing.T) {
	ctx := context.Background()
	pool, store := problemListStore(t, ctx)
	candidates := usecase.NewCandidateService(store)
	lists := usecase.NewProblemListService(store)

	url := "https://leetcode.com/problem-list/" + uuid.NewString()
	items := problemListItems(url, uuid.New())
	// Второй элемент приходит с явной позицией: источник сохраняет свой порядок.
	position := 5
	items[1].ProblemListPosition = &position
	if results := candidates.ImportBatch(ctx, items); !allStatuses(results, "imported") {
		t.Fatalf("импорт списка: %#v", results)
	}
	listID, _ := problemListRow(t, ctx, pool, url)

	actorID := uuid.New()
	taskIDs := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		candidateID, revision := candidateRef(t, ctx, pool, item.ExternalID)
		review := domain.CandidateReview{
			ExpectedRevision: revision, Title: item.Title,
			Statement: item.Statement, Difficulty: item.Difficulty,
		}
		detail, err := candidates.Approve(ctx, candidateID, actorID, review)
		if err != nil {
			t.Fatalf("одобрение %s: %v", item.ExternalID, err)
		}
		taskIDs = append(taskIDs, detail.Task.ID)
	}

	// Одобренные задачи — черновики: публичная выдача их скрывает.
	public, err := lists.Get(ctx, listID, false)
	if err != nil {
		t.Fatal(err)
	}
	if public.MemberCount != 0 || len(public.Members) != 0 {
		t.Fatalf("публичный список раскрывает черновики: %+v", public)
	}
	admin, err := lists.Get(ctx, listID, true)
	if err != nil {
		t.Fatal(err)
	}
	if admin.MemberCount != len(items) {
		t.Fatalf("администратор не видит черновики списка: %+v", admin)
	}
	if positions := memberPositions(t, ctx, pool, listID); len(positions) != 2 || positions[0] != 0 || positions[1] != 5 {
		t.Fatalf("порядок сбора не сохранён: %v", positions)
	}

	if _, err := pool.Exec(ctx, `UPDATE tasks SET status='published' WHERE id = ANY($1)`, taskIDs); err != nil {
		t.Fatal(err)
	}
	public, err = lists.Get(ctx, listID, false)
	if err != nil {
		t.Fatal(err)
	}
	if public.MemberCount != len(items) || len(public.Members) != len(items) {
		t.Fatalf("публичный список не показывает опубликованные задачи: %+v", public)
	}
	if public.Members[0].TaskID != taskIDs[0] {
		t.Fatalf("нарушен порядок списка: %+v", public.Members)
	}
}

func problemListStore(t *testing.T, ctx context.Context) (*pgxpool.Pool, *postgresadapter.Postgres) {
	t.Helper()
	pool, err := pgxpool.New(ctx, os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool, postgresadapter.New(pool)
}

// problemListItems собирает два кандидата одного частично собранного списка.
func candidateRef(t *testing.T, ctx context.Context, pool *pgxpool.Pool, externalID string) (uuid.UUID, int) {
	t.Helper()
	var id uuid.UUID
	var revision int
	if err := pool.QueryRow(ctx, `SELECT id, revision FROM task_candidates WHERE source_id='leetcode' AND external_id=$1`, externalID).Scan(&id, &revision); err != nil {
		t.Fatal(err)
	}

	return id, revision
}

func problemListItems(url string, jobID uuid.UUID) []domain.CandidateImport {
	list := &domain.ProblemListImport{Name: "Component list", SourceID: "leetcode", SourceURL: url, Truncated: true}
	return []domain.CandidateImport{
		{
			ExternalID: "list:" + url, SourceID: "leetcode", SourceName: "LeetCode",
			SourceURL: "https://leetcode.com/problems/component-one-" + url, SourceHash: "hash:" + url,
			RetrievedAt: time.Now().UTC(), CollectionJobID: jobID, Title: "Component one",
			Statement: "Первая задача списка", Difficulty: domain.DifficultyEasy, ProblemList: list,
		},
		{
			ExternalID: "list2:" + url, SourceID: "leetcode", SourceName: "LeetCode",
			SourceURL: "https://leetcode.com/problems/component-two-" + url, SourceHash: "hash2:" + url,
			RetrievedAt: time.Now().UTC(), CollectionJobID: jobID, Title: "Component two",
			Statement: "Вторая задача списка", Difficulty: domain.DifficultyMedium, ProblemList: list,
		},
	}
}

func allStatuses(results []usecase.CandidateImportResult, status string) bool {
	for _, result := range results {
		if result.Status != status {
			return false
		}
	}

	return len(results) > 0
}

func problemListRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, url string) (uuid.UUID, bool) {
	t.Helper()
	var id uuid.UUID
	var truncated bool
	if err := pool.QueryRow(ctx, `SELECT id, truncated FROM problem_lists WHERE source_url=$1`, url).Scan(&id, &truncated); err != nil {
		t.Fatal(err)
	}

	return id, truncated
}

func candidateCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, listID uuid.UUID) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_candidates WHERE problem_list_id=$1`, listID).Scan(&count); err != nil {
		t.Fatal(err)
	}

	return count
}

func memberPositions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, listID uuid.UUID) []int {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT position FROM problem_list_members WHERE problem_list_id=$1 ORDER BY position`, listID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	positions := make([]int, 0, 2)
	for rows.Next() {
		var position int
		if err := rows.Scan(&position); err != nil {
			t.Fatal(err)
		}
		positions = append(positions, position)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	return positions
}
