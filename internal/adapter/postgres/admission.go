package postgresadapter

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// CodeAdmissionUsage требует транзакцию. Глобальный lock берётся до task row lock;
// отдельный SELECT после ожидания видит уже закоммиченные admission других реплик.
// Ключ 7480248 зарезервирован для admission программных решений tasks.
func (r *Postgres) CodeAdmissionUsage(ctx context.Context, userID uuid.UUID) (own, total int, err error) {
	if r.pool != nil {
		return 0, 0, fmt.Errorf("submission admission requires a transaction")
	}
	if _, err = r.query.Exec(ctx, `SELECT pg_advisory_xact_lock(7480248)`); err != nil {
		return 0, 0, err
	}
	err = r.query.QueryRow(ctx, `SELECT count(*) FILTER (WHERE user_id=$1),count(*) FROM code_submissions WHERE status='queued'`, userID).Scan(&own, &total)

	return
}
