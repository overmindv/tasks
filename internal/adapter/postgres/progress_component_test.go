//go:build component

package postgresadapter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/tasks/internal/usecase"
)

func TestPracticeProgressPostgres(t *testing.T) {
	dsn := os.Getenv("PROGRESS_TEST_DSN")
	if dsn == "" || (!strings.Contains(dsn, "test") && !strings.Contains(dsn, "component")) {
		t.Fatal("PROGRESS_TEST_DSN must name a dedicated test/component database")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	repo := New(pool)
	owner, privateUser, actor := uuid.New(), uuid.New(), uuid.New()
	taskA, taskB, versionA, versionA2, versionB, topicA, topicB := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, task := range []uuid.UUID{taskA, taskB} {
		if _, err := pool.Exec(ctx, `INSERT INTO tasks (id,status,created_by,updated_by) VALUES ($1,'published',$2,$2)`, task, actor); err != nil {
			t.Fatal(err)
		}
	}
	for _, version := range []struct {
		id, task, topic uuid.UUID
		number          int
		difficulty      string
	}{
		{versionA, taskA, topicA, 1, "easy"}, {versionA2, taskA, topicA, 2, "hard"}, {versionB, taskB, topicB, 1, "medium"},
	} {
		_, err := pool.Exec(ctx, `INSERT INTO task_versions (id,task_id,version_number,topic_id,title,statement,task_type,difficulty,created_by)
			VALUES ($1,$2,$3,$4,'Test task','Statement','single_choice',$5,$6)`, version.id, version.task, version.number, version.topic, version.difficulty, actor)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM code_submissions WHERE user_id IN ($1,$2)`, owner, privateUser)
		_, _ = pool.Exec(context.Background(), `DELETE FROM submissions WHERE user_id IN ($1,$2)`, owner, privateUser)
		_, _ = pool.Exec(context.Background(), `UPDATE tasks SET current_version_id = NULL WHERE id IN ($1,$2)`, taskA, taskB)
		_, _ = pool.Exec(context.Background(), `DELETE FROM task_versions WHERE id IN ($1,$2,$3)`, versionA, versionA2, versionB)
		_, _ = pool.Exec(context.Background(), `DELETE FROM tasks WHERE id IN ($1,$2)`, taskA, taskB)
		_, _ = pool.Exec(context.Background(), `DELETE FROM practice_progress_visibility WHERE user_id IN ($1,$2)`, owner, privateUser)
	})
	if _, err := pool.Exec(ctx, `UPDATE tasks SET current_version_id = CASE id WHEN $1 THEN $2::uuid ELSE $3::uuid END WHERE id IN ($1,$4)`, taskA, versionA2, versionB, taskB); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-2 * time.Hour)
	answer := func(user, task, version uuid.UUID, verdict string, at time.Time) {
		t.Helper()
		_, err := pool.Exec(ctx, `INSERT INTO submissions (id,user_id,task_id,task_version_id,idempotency_key,request_hash,verdict,created_at)
			VALUES ($1,$2,$3,$4,$5,'hash',$6,$7)`, uuid.New(), user, task, version, uuid.New(), verdict, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	code := func(user, task, version uuid.UUID, verdict string, at time.Time) {
		t.Helper()
		_, err := pool.Exec(ctx, `INSERT INTO code_submissions (id,user_id,task_id,task_version_id,execution_id,correlation_id,idempotency_key,request_hash,language,source_file_name,source_code,status,verdict,completed_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'hash','go','main.go','PRIVATE_SOURCE_CANARY','completed',$8,$9)`, uuid.New(), user, task, version, uuid.New(), uuid.New(), uuid.New(), verdict, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	answer(owner, taskA, versionA, "wrong_answer", base)
	answer(owner, taskA, versionA, "accepted", base.Add(time.Minute))
	code(owner, taskA, versionA2, "accepted", base.Add(2*time.Minute))
	code(owner, taskB, versionB, "accepted", base.Add(3*time.Minute))
	code(owner, taskB, versionB, "infrastructure_error", base.Add(4*time.Minute))
	answer(privateUser, taskB, versionB, "accepted", base)
	service := usecase.NewProgressService(repo)
	result, err := service.Get(ctx, owner, owner)
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 4 || result.AcceptedAttempts != 3 || result.SolvedTasks != 2 || result.Score != 30 || result.AccuracyPercent != 75 {
		t.Fatalf("incorrect full-history summary: %+v", result)
	}
	if _, err := service.Get(ctx, actor, owner); err != usecase.ErrProgressPrivate {
		t.Fatalf("private summary leaked: %v", err)
	}
	if err := service.SetVisibility(ctx, owner, true); err != nil {
		t.Fatal(err)
	}
	public, err := service.Get(ctx, actor, owner)
	if err != nil || !public.Public || public.Score != 30 {
		t.Fatalf("public summary=%+v err=%v", public, err)
	}
	global, err := service.Candidates(ctx, nil, 50, 0)
	if err != nil || len(global) != 1 || global[0].UserID != owner.String() || global[0].Score != 30 {
		t.Fatalf("global rating=%+v err=%v", global, err)
	}
	topics := []uuid.UUID{topicA}
	course, err := service.Candidates(ctx, &topics, 50, 0)
	if err != nil || len(course) != 1 || course[0].Score != 10 {
		t.Fatalf("course rating=%+v err=%v", course, err)
	}
	topics = []uuid.UUID{}
	empty, err := service.Candidates(ctx, &topics, 50, 0)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty course=%+v err=%v", empty, err)
	}
	if err := service.SetVisibility(ctx, owner, false); err != nil {
		t.Fatal(err)
	}
	global, err = service.Candidates(ctx, nil, 50, 0)
	if err != nil || len(global) != 0 {
		t.Fatalf("revoked user in rating=%+v err=%v", global, err)
	}
}
