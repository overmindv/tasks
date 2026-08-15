package repository

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

// Repository задаёт операции хранения задач и пользовательских решений.
type Repository interface {
	Ping(ctx context.Context) error
	WithinTransaction(ctx context.Context, fn func(Repository) error) error
	InsertTask(ctx context.Context, task domain.Task) error
	InsertTaskVersion(ctx context.Context, version domain.TaskVersion) error
	InsertTaskOptions(ctx context.Context, options []domain.TaskOption) error
	InsertTaskContent(ctx context.Context, version domain.TaskVersion) error
	GetTask(ctx context.Context, id uuid.UUID, lock bool) (domain.Task, error)
	GetCurrentTaskVersion(ctx context.Context, taskID uuid.UUID) (domain.TaskVersion, error)
	GetTaskVersion(ctx context.Context, taskID, versionID uuid.UUID) (domain.TaskVersion, error)
	GetTaskDetail(ctx context.Context, taskID uuid.UUID) (domain.TaskDetail, error)
	ListTaskDetails(ctx context.Context, filter domain.TaskFilter) ([]domain.TaskDetail, error)
	SetCurrentTaskVersion(ctx context.Context, taskID, versionID, actorID uuid.UUID) error
	SetTaskStatus(ctx context.Context, taskID uuid.UUID, status domain.TaskStatus, actorID uuid.UUID) error
	SoftDeleteTask(ctx context.Context, taskID, actorID uuid.UUID) error
	FindSubmissionByIdempotency(ctx context.Context, userID, key uuid.UUID) (*domain.Submission, error)
	InsertSubmission(ctx context.Context, submission domain.Submission) error
	InsertSubmissionAnswers(ctx context.Context, submissionID, versionID uuid.UUID, optionIDs []uuid.UUID) error
	GetSubmission(ctx context.Context, id uuid.UUID) (domain.Submission, error)
	ListSubmissions(ctx context.Context, filter domain.SubmissionFilter) ([]domain.Submission, error)
	FindCodeSubmissionByIdempotency(ctx context.Context, userID, key uuid.UUID) (*domain.CodeSubmission, error)
	InsertCodeSubmission(ctx context.Context, submission domain.CodeSubmission) error
	GetCodeSubmission(ctx context.Context, id uuid.UUID) (domain.CodeSubmission, error)
	ListCodeSubmissions(ctx context.Context, filter domain.CodeSubmissionFilter) ([]domain.CodeSubmission, error)
	InsertOutboxMessage(ctx context.Context, message domain.OutboxMessage) error
	ClaimOutboxMessages(ctx context.Context, limit int, claimToken uuid.UUID, claimedUntil time.Time) ([]domain.OutboxMessage, error)
	MarkOutboxPublished(ctx context.Context, id, claimToken uuid.UUID) error
	ReleaseOutboxMessage(ctx context.Context, id, claimToken uuid.UUID, availableAt time.Time, lastError string) error
	InsertExecutionInbox(ctx context.Context, record domain.ExecutionInboxRecord) (bool, error)
	CompleteCodeSubmission(ctx context.Context, result domain.CodeSubmission) (bool, error)
	InsertCandidate(ctx context.Context, candidate domain.TaskCandidate) (bool, error)
	GetCandidate(ctx context.Context, id uuid.UUID, lock bool) (domain.TaskCandidate, error)
	ListCandidates(ctx context.Context, filter domain.CandidateFilter) ([]domain.TaskCandidate, error)
	UpdateCandidate(ctx context.Context, candidate domain.TaskCandidate, expectedRevision int) error
	MarkCandidateApproved(ctx context.Context, candidateID, taskID, actorID uuid.UUID, expectedRevision int) error
	MarkCandidateRejected(ctx context.Context, candidateID, actorID uuid.UUID, expectedRevision int, reason string) error
}
