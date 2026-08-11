package postgresadapter

import "github.com/go-jet/jet/v2/postgres"

var (
	tasks             = newTasksTable("")
	taskVersions      = newTaskVersionsTable("")
	taskOptions       = newTaskOptionsTable("")
	submissions       = newSubmissionsTable("")
	submissionAnswers = newSubmissionAnswersTable("")
	taskVersionTags   = postgres.NewTable("public", "task_version_tags", "",
		postgres.StringColumn("task_version_id"), postgres.StringColumn("tag"), postgres.IntegerColumn("position"))
	taskVersionExamples = postgres.NewTable("public", "task_version_examples", "",
		postgres.StringColumn("id"), postgres.StringColumn("task_version_id"), postgres.StringColumn("input"),
		postgres.StringColumn("output"), postgres.StringColumn("explanation"), postgres.IntegerColumn("position"))
	taskVersionConstraints = postgres.NewTable("public", "task_version_constraints", "",
		postgres.StringColumn("task_version_id"), postgres.StringColumn("value"), postgres.IntegerColumn("position"))
	taskVersionSources = postgres.NewTable("public", "task_version_sources", "",
		postgres.StringColumn("task_version_id"), postgres.StringColumn("source_id"), postgres.StringColumn("source_name"),
		postgres.StringColumn("source_url"), postgres.TimestampzColumn("published_at"))
	taskCandidates    = newTaskCandidatesTable("")
	taskCandidateTags = postgres.NewTable("public", "task_candidate_tags", "",
		postgres.StringColumn("candidate_id"), postgres.StringColumn("tag"), postgres.IntegerColumn("position"))
	taskCandidateExamples = postgres.NewTable("public", "task_candidate_examples", "",
		postgres.StringColumn("id"), postgres.StringColumn("candidate_id"), postgres.StringColumn("input"),
		postgres.StringColumn("output"), postgres.StringColumn("explanation"), postgres.IntegerColumn("position"))
	taskCandidateConstraints = postgres.NewTable("public", "task_candidate_constraints", "",
		postgres.StringColumn("candidate_id"), postgres.StringColumn("value"), postgres.IntegerColumn("position"))
)

type taskCandidatesTable struct {
	postgres.Table
	ID                postgres.ColumnString
	Status            postgres.ColumnString
	Revision          postgres.ColumnInteger
	ExternalID        postgres.ColumnString
	SourceID          postgres.ColumnString
	SourceName        postgres.ColumnString
	SourceURL         postgres.ColumnString
	SourceHash        postgres.ColumnString
	SourcePublishedAt postgres.ColumnTimestampz
	RetrievedAt       postgres.ColumnTimestampz
	CollectionJobID   postgres.ColumnString
	TopicID           postgres.ColumnString
	Title             postgres.ColumnString
	Statement         postgres.ColumnString
	Difficulty        postgres.ColumnString
	ApprovedTaskID    postgres.ColumnString
	ReviewedBy        postgres.ColumnString
	ReviewedAt        postgres.ColumnTimestampz
	RejectionReason   postgres.ColumnString
	CreatedAt         postgres.ColumnTimestampz
	UpdatedAt         postgres.ColumnTimestampz
}

// newTaskCandidatesTable создаёт типизированное описание очереди кандидатов.
func newTaskCandidatesTable(alias string) taskCandidatesTable {
	id := postgres.StringColumn("id")
	status := postgres.StringColumn("status")
	revision := postgres.IntegerColumn("revision")
	externalID := postgres.StringColumn("external_id")
	sourceID := postgres.StringColumn("source_id")
	sourceName := postgres.StringColumn("source_name")
	sourceURL := postgres.StringColumn("source_url")
	sourceHash := postgres.StringColumn("source_hash")
	sourcePublishedAt := postgres.TimestampzColumn("source_published_at")
	retrievedAt := postgres.TimestampzColumn("retrieved_at")
	collectionJobID := postgres.StringColumn("collection_job_id")
	topicID := postgres.StringColumn("topic_id")
	title := postgres.StringColumn("title")
	statement := postgres.StringColumn("statement")
	difficulty := postgres.StringColumn("difficulty")
	approvedTaskID := postgres.StringColumn("approved_task_id")
	reviewedBy := postgres.StringColumn("reviewed_by")
	reviewedAt := postgres.TimestampzColumn("reviewed_at")
	rejectionReason := postgres.StringColumn("rejection_reason")
	createdAt := postgres.TimestampzColumn("created_at")
	updatedAt := postgres.TimestampzColumn("updated_at")

	return taskCandidatesTable{
		Table: postgres.NewTable("public", "task_candidates", alias, id, status, revision, externalID, sourceID, sourceName, sourceURL, sourceHash, sourcePublishedAt, retrievedAt, collectionJobID, topicID, title, statement, difficulty, approvedTaskID, reviewedBy, reviewedAt, rejectionReason, createdAt, updatedAt),
		ID:    id, Status: status, Revision: revision, ExternalID: externalID, SourceID: sourceID,
		SourceName: sourceName, SourceURL: sourceURL, SourceHash: sourceHash,
		SourcePublishedAt: sourcePublishedAt, RetrievedAt: retrievedAt, CollectionJobID: collectionJobID,
		TopicID: topicID, Title: title, Statement: statement, Difficulty: difficulty,
		ApprovedTaskID: approvedTaskID, ReviewedBy: reviewedBy, ReviewedAt: reviewedAt,
		RejectionReason: rejectionReason, CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
}

type tasksTable struct {
	postgres.Table
	ID               postgres.ColumnString
	CurrentVersionID postgres.ColumnString
	Status           postgres.ColumnString
	CreatedBy        postgres.ColumnString
	UpdatedBy        postgres.ColumnString
	CreatedAt        postgres.ColumnTimestampz
	UpdatedAt        postgres.ColumnTimestampz
	DeletedAt        postgres.ColumnTimestampz
}

// newTasksTable создаёт типизированное описание таблицы tasks.
func newTasksTable(alias string) tasksTable {
	id := postgres.StringColumn("id")
	currentVersionID := postgres.StringColumn("current_version_id")
	status := postgres.StringColumn("status")
	createdBy := postgres.StringColumn("created_by")
	updatedBy := postgres.StringColumn("updated_by")
	createdAt := postgres.TimestampzColumn("created_at")
	updatedAt := postgres.TimestampzColumn("updated_at")
	deletedAt := postgres.TimestampzColumn("deleted_at")

	return tasksTable{
		Table:            postgres.NewTable("public", "tasks", alias, id, currentVersionID, status, createdBy, updatedBy, createdAt, updatedAt, deletedAt),
		ID:               id,
		CurrentVersionID: currentVersionID,
		Status:           status,
		CreatedBy:        createdBy,
		UpdatedBy:        updatedBy,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
		DeletedAt:        deletedAt,
	}
}

type taskVersionsTable struct {
	postgres.Table
	ID            postgres.ColumnString
	TaskID        postgres.ColumnString
	VersionNumber postgres.ColumnInteger
	TopicID       postgres.ColumnString
	Title         postgres.ColumnString
	Statement     postgres.ColumnString
	TaskType      postgres.ColumnString
	Difficulty    postgres.ColumnString
	CreatedBy     postgres.ColumnString
	CreatedAt     postgres.ColumnTimestampz
	UpdatedAt     postgres.ColumnTimestampz
}

// newTaskVersionsTable создаёт типизированное описание таблицы task_versions.
func newTaskVersionsTable(alias string) taskVersionsTable {
	id := postgres.StringColumn("id")
	taskID := postgres.StringColumn("task_id")
	versionNumber := postgres.IntegerColumn("version_number")
	topicID := postgres.StringColumn("topic_id")
	title := postgres.StringColumn("title")
	statement := postgres.StringColumn("statement")
	taskType := postgres.StringColumn("task_type")
	difficulty := postgres.StringColumn("difficulty")
	createdBy := postgres.StringColumn("created_by")
	createdAt := postgres.TimestampzColumn("created_at")
	updatedAt := postgres.TimestampzColumn("updated_at")

	return taskVersionsTable{
		Table:         postgres.NewTable("public", "task_versions", alias, id, taskID, versionNumber, topicID, title, statement, taskType, difficulty, createdBy, createdAt, updatedAt),
		ID:            id,
		TaskID:        taskID,
		VersionNumber: versionNumber,
		TopicID:       topicID,
		Title:         title,
		Statement:     statement,
		TaskType:      taskType,
		Difficulty:    difficulty,
		CreatedBy:     createdBy,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}
}

type taskOptionsTable struct {
	postgres.Table
	ID            postgres.ColumnString
	TaskVersionID postgres.ColumnString
	Text          postgres.ColumnString
	IsCorrect     postgres.ColumnBool
	Position      postgres.ColumnInteger
	CreatedAt     postgres.ColumnTimestampz
	UpdatedAt     postgres.ColumnTimestampz
}

// newTaskOptionsTable создаёт типизированное описание таблицы task_options.
func newTaskOptionsTable(alias string) taskOptionsTable {
	id := postgres.StringColumn("id")
	taskVersionID := postgres.StringColumn("task_version_id")
	text := postgres.StringColumn("text")
	isCorrect := postgres.BoolColumn("is_correct")
	position := postgres.IntegerColumn("position")
	createdAt := postgres.TimestampzColumn("created_at")
	updatedAt := postgres.TimestampzColumn("updated_at")

	return taskOptionsTable{
		Table:         postgres.NewTable("public", "task_options", alias, id, taskVersionID, text, isCorrect, position, createdAt, updatedAt),
		ID:            id,
		TaskVersionID: taskVersionID,
		Text:          text,
		IsCorrect:     isCorrect,
		Position:      position,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}
}

type submissionsTable struct {
	postgres.Table
	ID             postgres.ColumnString
	UserID         postgres.ColumnString
	TaskID         postgres.ColumnString
	TaskVersionID  postgres.ColumnString
	IdempotencyKey postgres.ColumnString
	RequestHash    postgres.ColumnString
	Verdict        postgres.ColumnString
	CreatedAt      postgres.ColumnTimestampz
	UpdatedAt      postgres.ColumnTimestampz
}

// newSubmissionsTable создаёт типизированное описание таблицы submissions.
func newSubmissionsTable(alias string) submissionsTable {
	id := postgres.StringColumn("id")
	userID := postgres.StringColumn("user_id")
	taskID := postgres.StringColumn("task_id")
	taskVersionID := postgres.StringColumn("task_version_id")
	idempotencyKey := postgres.StringColumn("idempotency_key")
	requestHash := postgres.StringColumn("request_hash")
	verdict := postgres.StringColumn("verdict")
	createdAt := postgres.TimestampzColumn("created_at")
	updatedAt := postgres.TimestampzColumn("updated_at")

	return submissionsTable{
		Table:          postgres.NewTable("public", "submissions", alias, id, userID, taskID, taskVersionID, idempotencyKey, requestHash, verdict, createdAt, updatedAt),
		ID:             id,
		UserID:         userID,
		TaskID:         taskID,
		TaskVersionID:  taskVersionID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    requestHash,
		Verdict:        verdict,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}
}

type submissionAnswersTable struct {
	postgres.Table
	ID            postgres.ColumnString
	SubmissionID  postgres.ColumnString
	TaskVersionID postgres.ColumnString
	OptionID      postgres.ColumnString
	CreatedAt     postgres.ColumnTimestampz
	UpdatedAt     postgres.ColumnTimestampz
}

// newSubmissionAnswersTable создаёт типизированное описание таблицы submission_answers.
func newSubmissionAnswersTable(alias string) submissionAnswersTable {
	id := postgres.StringColumn("id")
	submissionID := postgres.StringColumn("submission_id")
	taskVersionID := postgres.StringColumn("task_version_id")
	optionID := postgres.StringColumn("option_id")
	createdAt := postgres.TimestampzColumn("created_at")
	updatedAt := postgres.TimestampzColumn("updated_at")

	return submissionAnswersTable{
		Table:         postgres.NewTable("public", "submission_answers", alias, id, submissionID, taskVersionID, optionID, createdAt, updatedAt),
		ID:            id,
		SubmissionID:  submissionID,
		TaskVersionID: taskVersionID,
		OptionID:      optionID,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}
}
