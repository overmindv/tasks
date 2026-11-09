// Контрактный стенд поднимает реальные HTTP-обработчики прогресса с выделенной БД.
// Запускается только тестовым скриптом, без Kafka и других фоновых процессов.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/transport/httpapi"
	"github.com/overmindv/tasks/internal/usecase"
)

func main() {
	dsn := os.Getenv("PROGRESS_TEST_DSN")
	if dsn == "" || (!strings.Contains(dsn, "test") && !strings.Contains(dsn, "component")) {
		panic("PROGRESS_TEST_DSN must refer to a dedicated test/component database")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	if err := seed(pool); err != nil {
		panic(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := http.NewServeMux()
	httpapi.RegisterProgress(router, usecase.NewProgressService(postgresadapter.New(pool)), logger)
	router.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	router.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	address := os.Getenv("PROGRESS_FIXTURE_ADDRESS")
	if address == "" {
		panic("PROGRESS_FIXTURE_ADDRESS is required")
	}
	if err := http.ListenAndServe(address, router); err != nil {
		panic(fmt.Errorf("serve progress fixture: %w", err))
	}
}

func seed(pool *pgxpool.Pool) error {
	ctx := context.Background()
	// Стенд удаляет только собственные фиксированные UUID в специально выделенной БД.
	_, err := pool.Exec(ctx, `
		DELETE FROM submissions WHERE id IN ('30000000-0000-4000-8000-000000000001', '30000000-0000-4000-8000-000000000002', '30000000-0000-4000-8000-000000000003', '30000000-0000-4000-8000-000000000004');
		DELETE FROM practice_progress_visibility WHERE user_id IN ('00000000-0000-4000-8000-000000000002', '00000000-0000-4000-8000-000000000003', '00000000-0000-4000-8000-000000000004');
		INSERT INTO tasks (id, status, created_by, updated_by) VALUES
		 ('10000000-0000-4000-8000-000000000001','published','00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000002'),
		 ('10000000-0000-4000-8000-000000000002','published','00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000002')
		 ON CONFLICT (id) DO NOTHING;
		INSERT INTO task_versions (id, task_id, version_number, topic_id, title, statement, task_type, difficulty, created_by) VALUES
		 ('20000000-0000-4000-8000-000000000001','10000000-0000-4000-8000-000000000001',1,'00000000-0000-4000-8000-000000000011','First','Statement','single_choice','easy','00000000-0000-4000-8000-000000000002'),
		 ('20000000-0000-4000-8000-000000000002','10000000-0000-4000-8000-000000000002',1,'00000000-0000-4000-8000-000000000012','Second','Statement','single_choice','medium','00000000-0000-4000-8000-000000000002')
		 ON CONFLICT (id) DO NOTHING;
		INSERT INTO submissions (id, user_id, task_id, task_version_id, idempotency_key, request_hash, verdict) VALUES
		 ('30000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-000000000002','10000000-0000-4000-8000-000000000001','20000000-0000-4000-8000-000000000001','40000000-0000-4000-8000-000000000001','hash','accepted'),
		 ('30000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000002','10000000-0000-4000-8000-000000000002','20000000-0000-4000-8000-000000000002','40000000-0000-4000-8000-000000000002','hash','accepted'),
		 ('30000000-0000-4000-8000-000000000003','00000000-0000-4000-8000-000000000003','10000000-0000-4000-8000-000000000002','20000000-0000-4000-8000-000000000002','40000000-0000-4000-8000-000000000003','hash','accepted'),
		 ('30000000-0000-4000-8000-000000000004','00000000-0000-4000-8000-000000000004','10000000-0000-4000-8000-000000000001','20000000-0000-4000-8000-000000000001','40000000-0000-4000-8000-000000000004','hash','accepted');
		INSERT INTO practice_progress_visibility (user_id, is_public) VALUES ('00000000-0000-4000-8000-000000000004', true);
	`)
	return err
}
