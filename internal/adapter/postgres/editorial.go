package postgresadapter

import (
	"context"

	"github.com/google/uuid"
)

// EditorialCheckPassed учитывает последний запуск именно неизменяемой версии.
// Task row lock у вызывающего сериализует публикацию с постановкой нового запуска.
func (r *Postgres) EditorialCheckPassed(ctx context.Context, taskID, versionID uuid.UUID) (bool, error) {
	var passed bool
	err := r.query.QueryRow(ctx, `SELECT COALESCE((SELECT status = 'completed' AND verdict = 'accepted'
 FROM code_submissions WHERE task_id=$1 AND task_version_id=$2 AND run_mode='editorial'
 ORDER BY created_at DESC, id DESC LIMIT 1), false)`, taskID, versionID).Scan(&passed)

	return passed, err
}
