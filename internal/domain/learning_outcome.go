package domain

import (
	"time"

	"github.com/google/uuid"
)

const LearningOutcomeTopic = "learning.outcome.v1"

type LearningOutcome struct {
	EventID          uuid.UUID              `json:"event_id"`
	EventType        string                 `json:"event_type"`
	SchemaVersion    int                    `json:"schema_version"`
	AggregateID      uuid.UUID              `json:"aggregate_id"`
	AggregateVersion int                    `json:"aggregate_version"`
	OccurredAt       time.Time              `json:"occurred_at"`
	CorrelationID    *uuid.UUID             `json:"correlation_id,omitempty"`
	Payload          LearningOutcomePayload `json:"payload"`
}

type LearningOutcomePayload struct {
	SubmissionID            uuid.UUID        `json:"submission_id"`
	LearnerID               uuid.UUID        `json:"learner_id"`
	TaskID                  uuid.UUID        `json:"task_id"`
	TaskVersionID           uuid.UUID        `json:"task_version_id"`
	TopicIDs                []uuid.UUID      `json:"topic_ids"`
	Verdict                 ExecutionVerdict `json:"verdict"`
	GradedAt                time.Time        `json:"graded_at"`
	FirstIndependentAttempt bool             `json:"first_independent_attempt"`
	CorrectionVersion       int              `json:"correction_version"`
}

func (v ExecutionVerdict) IsLearningOutcome() bool {
	switch v {
	case ExecutionVerdictAccepted, ExecutionVerdictWrongAnswer, ExecutionVerdictCompilationError,
		ExecutionVerdictRuntimeError, ExecutionVerdictTimeLimitExceeded, ExecutionVerdictMemoryLimitExceeded,
		ExecutionVerdictOutputLimitExceeded:
		return true
	default:
		return false
	}
}
