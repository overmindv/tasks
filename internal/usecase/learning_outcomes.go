package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

func recordLearningOutcome(ctx context.Context, tx repository.Repository, version domain.TaskVersion, learner, submission uuid.UUID,
	verdict domain.ExecutionVerdict, correlation *uuid.UUID,
) error {
	first, err := tx.FirstLearningOutcome(ctx, learner, version.TaskID, submission)
	if err != nil {
		return fmt.Errorf("resolve first learning outcome: %w", err)
	}
	topics := make([]uuid.UUID, 0, 1)
	if version.TopicID != nil && *version.TopicID != uuid.Nil {
		topics = append(topics, *version.TopicID)
	}
	at := time.Now().UTC()

	return tx.InsertLearningOutcome(ctx, domain.LearningOutcome{
		EventID: uuid.New(), EventType: domain.LearningOutcomeTopic, SchemaVersion: 1,
		AggregateID: submission, AggregateVersion: 1, OccurredAt: at, CorrelationID: correlation,
		Payload: domain.LearningOutcomePayload{
			SubmissionID: submission, LearnerID: learner, TaskID: version.TaskID, TaskVersionID: version.ID, TopicIDs: topics,
			Verdict: verdict, GradedAt: at, FirstIndependentAttempt: first, CorrectionVersion: 1,
		},
	})
}
