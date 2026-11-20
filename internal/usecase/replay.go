package usecase

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/execution"
	"github.com/overmindv/tasks/internal/observability"
	"github.com/overmindv/tasks/internal/repository"
)

// Replay создаёт новую попытку инфраструктурного восстановления и атомарный audit.
func (s *CodeSubmissionService) Replay(ctx context.Context, id, expected, actor uuid.UUID, admin bool) (domain.CodeSubmission, error) {
	if !admin || actor == uuid.Nil {
		return domain.CodeSubmission{}, apperror.New(apperror.PermissionDenied, "administrator required", http.StatusForbidden)
	}
	if expected == uuid.Nil {
		return domain.CodeSubmission{}, apperror.New(apperror.ValidationError, "expected attempt required", http.StatusBadRequest)
	}
	snapshot, err := s.repository.GetCodeSubmission(ctx, id)
	if err != nil {
		return domain.CodeSubmission{}, err
	}
	var next domain.CodeSubmission
	err = s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		own, total, usageErr := tx.CodeAdmissionUsage(ctx, snapshot.UserID)
		if usageErr != nil {
			return usageErr
		}
		current, currentErr := tx.GetCodeSubmissionForUpdate(ctx, id)
		if currentErr != nil {
			return currentErr
		}
		now := time.Now().UTC()
		eligible := current.Status == domain.CodeSubmissionStatusQueued && !now.Before(current.NextAttemptAt)
		if current.Status == domain.CodeSubmissionStatusCompleted && current.Verdict != nil {
			eligible = *current.Verdict == domain.ExecutionVerdictInfrastructureError || *current.Verdict == domain.ExecutionVerdictCheckerError
		}
		if current.ExecutionID != expected || !eligible {
			return apperror.New("REPLAY_CONFLICT", "attempt is not replayable", http.StatusConflict)
		}
		if current.Status != domain.CodeSubmissionStatusQueued && (own >= s.policy.MaxPendingPerUser || total >= s.policy.MaxPendingTotal) {
			return apperror.New(apperror.RateLimited, "submission capacity unavailable", http.StatusTooManyRequests)
		}
		task, taskErr := tx.GetTask(ctx, current.TaskID, true)
		if taskErr != nil {
			return taskErr
		}
		if task.Status != domain.TaskStatusPublished {
			return apperror.New(apperror.TaskNotAvailable, "task not available", http.StatusConflict)
		}
		version, versionErr := tx.GetTaskVersion(ctx, current.TaskID, current.TaskVersionID)
		if versionErr != nil {
			return versionErr
		}
		suite, suiteErr := executionSuite(ctx, tx, current.TaskID, current.TaskVersionID, current.RunMode, current.Language)
		if suiteErr != nil {
			return suiteErr
		}
		next = current
		next.ExecutionID = uuid.New()
		next.Attempts = 1
		next.NextAttemptAt = now.Add(s.policy.RetryGrace)
		next.Status = domain.CodeSubmissionStatusQueued
		next.Verdict = nil
		next.Failure = nil
		next.Tests = nil
		next.CompletedAt = nil
		next.Compilation = nil
		next.Execution = nil
		next.UpdatedAt = now
		changed, replayErr := tx.ReplayCodeSubmission(ctx, id, expected, next.ExecutionID, actor, now, next.NextAttemptAt)
		if replayErr != nil {
			return replayErr
		}
		if !changed {
			return apperror.New("REPLAY_CONFLICT", "attempt changed", http.StatusConflict)
		}
		request := s.buildRunRequest(next, openExamples(version.Examples), now)
		applyExecutionSuite(&request, suite)
		payload, encodeErr := execution.EncodeRunRequest(request)
		if encodeErr != nil {
			return encodeErr
		}

		return tx.InsertOutboxMessage(ctx, domain.OutboxMessage{ID: uuid.New(), AggregateID: id, Topic: s.policy.RequestsTopic, MessageKey: id.String(), Payload: payload, AvailableAt: now, CreatedAt: now})
	})
	if err == nil {
		s.metrics.Attempt(observability.ReplayAttempt)
	}

	return next, err
}
