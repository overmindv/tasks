package postgresadapter

import (
	"context"
	"fmt"
)

func (r *Postgres) ExecutionQueueAges(ctx context.Context) (outbox, result float64, err error) {
	err = r.query.QueryRow(ctx, `
		SELECT GREATEST(0, EXTRACT(EPOCH FROM now() - (
			SELECT created_at FROM code_submission_outbox
			WHERE published_at IS NULL ORDER BY created_at LIMIT 1
		)))::double precision,
		GREATEST(0, EXTRACT(EPOCH FROM now() - (
			SELECT created_at FROM code_submissions
			WHERE status = 'queued' ORDER BY created_at LIMIT 1
		)))::double precision
	`).Scan(&outbox, &result)
	if err != nil {
		return 0, 0, fmt.Errorf("read execution queue ages: %w", err)
	}

	return outbox, result, nil
}
