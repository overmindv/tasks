package postgresadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ReplayCodeSubmission выполняется в транзакции вместе с admission и новым outbox.
func (r *Postgres) ReplayCodeSubmission(ctx context.Context, id, expected, next, actor uuid.UUID, at, deadline time.Time) (bool, error) {
	if r.pool != nil {
		return false, fmt.Errorf("replay requires transaction")
	}
	tag, err := r.query.Exec(ctx, `WITH previous AS MATERIALIZED (
 SELECT id,execution_id,attempts FROM code_submissions WHERE id=$1 AND execution_id=$2
 AND ((status='completed' AND verdict IN ('infrastructure_error','checker_error')) OR (status='queued' AND next_attempt_at<=$5)) FOR UPDATE
 ), changed AS (
 UPDATE code_submissions s SET execution_id=$3,status='queued',verdict=NULL,attempts=1,next_attempt_at=$6,compilation_result=NULL,execution_result=NULL,test_results='[]'::jsonb,error_code=NULL,error_message=NULL,completed_at=NULL,updated_at=$5
 FROM previous p WHERE s.id=p.id AND $3<>$2 RETURNING s.id,p.execution_id,p.attempts
 ) INSERT INTO code_submission_replay_audit(submission_id,previous_execution_id,next_execution_id,actor_id,previous_attempts,created_at)
 SELECT id,execution_id,$3,$4,attempts,$5 FROM changed`, id, expected, next, actor, at, deadline)
	if err != nil {
		return false, err
	}

	return tag.RowsAffected() == 1, nil
}
