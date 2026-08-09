package postgresadapter

import (
	"context"
	"fmt"

	jet "github.com/go-jet/jet/v2/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks-it/internal/apperror"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/samber/lo"
)

// InsertTask сохраняет новый агрегат задачи без текущей версии.
func (r *Postgres) InsertTask(ctx context.Context, task domain.Task) error {
	statement := tasks.INSERT(tasks.ID, tasks.Status, tasks.CreatedBy, tasks.UpdatedBy).
		VALUES(task.ID.String(), string(task.Status), task.CreatedBy.String(), task.UpdatedBy.String())
	query, args := statementSQL(statement)
	if _, err := r.query.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("execute insert task: %w", err)
	}

	return nil
}

// InsertTaskVersion сохраняет неизменяемое содержимое версии.
func (r *Postgres) InsertTaskVersion(ctx context.Context, version domain.TaskVersion) error {
	var topicID any
	if version.TopicID != nil {
		topicID = version.TopicID.String()
	}
	statement := taskVersions.INSERT(
		taskVersions.ID,
		taskVersions.TaskID,
		taskVersions.VersionNumber,
		taskVersions.TopicID,
		taskVersions.Title,
		taskVersions.Statement,
		taskVersions.TaskType,
		taskVersions.Difficulty,
		taskVersions.CreatedBy,
	).VALUES(
		version.ID.String(),
		version.TaskID.String(),
		version.VersionNumber,
		topicID,
		version.Title,
		version.Statement,
		string(version.TaskType),
		string(version.Difficulty),
		version.CreatedBy.String(),
	)
	query, args := statementSQL(statement)
	if _, err := r.query.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("execute insert task version: %w", err)
	}

	return nil
}

// InsertTaskOptions сохраняет варианты ответа одним Jet INSERT.
func (r *Postgres) InsertTaskOptions(ctx context.Context, options []domain.TaskOption) error {
	statement := taskOptions.INSERT(
		taskOptions.ID,
		taskOptions.TaskVersionID,
		taskOptions.Text,
		taskOptions.IsCorrect,
		taskOptions.Position,
	)
	for _, option := range options {
		statement = statement.VALUES(
			option.ID.String(),
			option.TaskVersionID.String(),
			option.Text,
			option.IsCorrect,
			option.Position,
		)
	}
	query, args := statementSQL(statement)
	if _, err := r.query.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("execute insert task options: %w", err)
	}

	return nil
}

// GetTask возвращает активный агрегат и при необходимости блокирует строку.
func (r *Postgres) GetTask(ctx context.Context, id uuid.UUID, lock bool) (domain.Task, error) {
	statement := jet.SELECT(
		tasks.ID,
		tasks.CurrentVersionID,
		tasks.Status,
		tasks.CreatedBy,
		tasks.UpdatedBy,
		tasks.CreatedAt,
		tasks.UpdatedAt,
		tasks.DeletedAt,
	).FROM(tasks.Table).WHERE(tasks.ID.EQ(jet.UUID(id)).AND(tasks.DeletedAt.IS_NULL()))
	if lock {
		statement = statement.FOR(jet.UPDATE())
	}
	query, args := statementSQL(statement)
	task, err := scanTask(r.query.QueryRow(ctx, query, args...))
	if err != nil {
		return domain.Task{}, notFound(err, apperror.TaskNotFound, "тест не найден")
	}

	return task, nil
}

// GetCurrentTaskVersion возвращает текущую версию активной задачи.
func (r *Postgres) GetCurrentTaskVersion(ctx context.Context, taskID uuid.UUID) (domain.TaskVersion, error) {
	task, err := r.GetTask(ctx, taskID, false)
	if err != nil {
		return domain.TaskVersion{}, err
	}
	if task.CurrentVersionID == nil {
		return domain.TaskVersion{}, fmt.Errorf("task %s has no current version", taskID)
	}

	return r.GetTaskVersion(ctx, taskID, *task.CurrentVersionID)
}

// GetTaskVersion возвращает версию и все её варианты ответа.
func (r *Postgres) GetTaskVersion(ctx context.Context, taskID, versionID uuid.UUID) (domain.TaskVersion, error) {
	statement := jet.SELECT(
		taskVersions.ID,
		taskVersions.TaskID,
		taskVersions.VersionNumber,
		taskVersions.TopicID,
		taskVersions.Title,
		taskVersions.Statement,
		taskVersions.TaskType,
		taskVersions.Difficulty,
		taskVersions.CreatedBy,
		taskVersions.CreatedAt,
		taskVersions.UpdatedAt,
	).FROM(taskVersions.Table).WHERE(
		taskVersions.ID.EQ(jet.UUID(versionID)).
			AND(taskVersions.TaskID.EQ(jet.UUID(taskID))),
	)
	query, args := statementSQL(statement)
	version, err := scanTaskVersion(r.query.QueryRow(ctx, query, args...))
	if err != nil {
		return domain.TaskVersion{}, notFound(err, apperror.TaskVersionNotFound, "версия теста не найдена")
	}
	options, err := r.listOptions(ctx, []uuid.UUID{version.ID})
	if err != nil {
		return domain.TaskVersion{}, fmt.Errorf("list version options: %w", err)
	}
	version.Options = options[version.ID]

	return version, nil
}

// GetTaskDetail возвращает активную задачу с текущей версией.
func (r *Postgres) GetTaskDetail(ctx context.Context, taskID uuid.UUID) (domain.TaskDetail, error) {
	task, err := r.GetTask(ctx, taskID, false)
	if err != nil {
		return domain.TaskDetail{}, err
	}
	if task.CurrentVersionID == nil {
		return domain.TaskDetail{}, fmt.Errorf("task %s has no current version", taskID)
	}
	version, err := r.GetTaskVersion(ctx, taskID, *task.CurrentVersionID)
	if err != nil {
		return domain.TaskDetail{}, fmt.Errorf("get current version: %w", err)
	}

	return domain.TaskDetail{
		Task:    task,
		Version: version,
	}, nil
}

// ListTaskDetails возвращает текущие версии задач с фильтрами и стабильным порядком.
func (r *Postgres) ListTaskDetails(ctx context.Context, filter domain.TaskFilter) ([]domain.TaskDetail, error) {
	condition := tasks.DeletedAt.IS_NULL()
	if filter.Status != nil {
		condition = condition.AND(tasks.Status.EQ(jet.String(string(*filter.Status))))
	}
	if filter.TaskType != nil {
		condition = condition.AND(taskVersions.TaskType.EQ(jet.String(string(*filter.TaskType))))
	}
	if filter.Difficulty != nil {
		condition = condition.AND(taskVersions.Difficulty.EQ(jet.String(string(*filter.Difficulty))))
	}
	if filter.TopicID != nil {
		condition = condition.AND(taskVersions.TopicID.EQ(jet.UUID(*filter.TopicID)))
	}
	join := tasks.INNER_JOIN(taskVersions.Table, tasks.CurrentVersionID.EQ(taskVersions.ID))
	statement := jet.SELECT(
		tasks.ID,
		tasks.CurrentVersionID,
		tasks.Status,
		tasks.CreatedBy,
		tasks.UpdatedBy,
		tasks.CreatedAt,
		tasks.UpdatedAt,
		tasks.DeletedAt,
		taskVersions.ID,
		taskVersions.TaskID,
		taskVersions.VersionNumber,
		taskVersions.TopicID,
		taskVersions.Title,
		taskVersions.Statement,
		taskVersions.TaskType,
		taskVersions.Difficulty,
		taskVersions.CreatedBy,
		taskVersions.CreatedAt,
		taskVersions.UpdatedAt,
	).FROM(join).WHERE(condition).
		ORDER_BY(tasks.CreatedAt.DESC(), tasks.ID.DESC()).
		LIMIT(int64(filter.Limit)).OFFSET(int64(filter.Offset))
	query, args := statementSQL(statement)
	rows, err := r.query.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query task list: %w", err)
	}
	defer rows.Close()
	items := make([]domain.TaskDetail, 0)
	for rows.Next() {
		task, version, scanErr := scanTaskDetail(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan task list row: %w", scanErr)
		}
		items = append(items, domain.TaskDetail{Task: task, Version: version})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task list: %w", err)
	}
	versionIDs := lo.Map(items, func(item domain.TaskDetail, _ int) uuid.UUID { return item.Version.ID })
	options, err := r.listOptions(ctx, versionIDs)
	if err != nil {
		return nil, fmt.Errorf("list task options: %w", err)
	}
	for index := range items {
		items[index].Version.Options = options[items[index].Version.ID]
	}

	return items, nil
}

// SetCurrentTaskVersion переключает текущую версию и обновляет аудит.
func (r *Postgres) SetCurrentTaskVersion(ctx context.Context, taskID, versionID, actorID uuid.UUID) error {
	statement := tasks.UPDATE(tasks.CurrentVersionID, tasks.UpdatedBy, tasks.UpdatedAt).
		SET(versionID.String(), actorID.String(), jet.NOW()).
		WHERE(tasks.ID.EQ(jet.UUID(taskID)).AND(tasks.DeletedAt.IS_NULL()))
	query, args := statementSQL(statement)
	tag, err := r.query.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute set current version: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.TaskNotFound, "тест не найден", 404)
	}

	return nil
}

// SetTaskStatus меняет lifecycle-статус и аудит задачи.
func (r *Postgres) SetTaskStatus(ctx context.Context, taskID uuid.UUID, status domain.TaskStatus, actorID uuid.UUID) error {
	statement := tasks.UPDATE(tasks.Status, tasks.UpdatedBy, tasks.UpdatedAt).
		SET(string(status), actorID.String(), jet.NOW()).
		WHERE(tasks.ID.EQ(jet.UUID(taskID)).AND(tasks.DeletedAt.IS_NULL()))
	query, args := statementSQL(statement)
	tag, err := r.query.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute set task status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.TaskNotFound, "тест не найден", 404)
	}

	return nil
}

// SoftDeleteTask помечает задачу удалённой без очистки истории.
func (r *Postgres) SoftDeleteTask(ctx context.Context, taskID, actorID uuid.UUID) error {
	statement := tasks.UPDATE(tasks.DeletedAt, tasks.UpdatedAt, tasks.UpdatedBy).
		SET(jet.NOW(), jet.NOW(), actorID.String()).
		WHERE(tasks.ID.EQ(jet.UUID(taskID)).AND(tasks.DeletedAt.IS_NULL()))
	query, args := statementSQL(statement)
	tag, err := r.query.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute soft delete task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.TaskNotFound, "тест не найден", 404)
	}

	return nil
}

// listOptions загружает варианты нескольких версий одним запросом.
func (r *Postgres) listOptions(ctx context.Context, versionIDs []uuid.UUID) (map[uuid.UUID][]domain.TaskOption, error) {
	result := make(map[uuid.UUID][]domain.TaskOption, len(versionIDs))
	if len(versionIDs) == 0 {
		return result, nil
	}
	statement := jet.SELECT(
		taskOptions.ID,
		taskOptions.TaskVersionID,
		taskOptions.Text,
		taskOptions.IsCorrect,
		taskOptions.Position,
		taskOptions.CreatedAt,
		taskOptions.UpdatedAt,
	).FROM(taskOptions.Table).
		WHERE(taskOptions.TaskVersionID.IN(uuidExpressions(versionIDs)...)).
		ORDER_BY(taskOptions.TaskVersionID.ASC(), taskOptions.Position.ASC())
	query, args := statementSQL(statement)
	rows, err := r.query.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query task options: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		option, scanErr := scanTaskOption(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan task option: %w", scanErr)
		}
		result[option.TaskVersionID] = append(result[option.TaskVersionID], option)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task options: %w", err)
	}

	return result, nil
}

// scanTask считывает одну строку tasks в доменную модель.
func scanTask(row pgx.Row) (domain.Task, error) {
	var task domain.Task
	var status string
	err := row.Scan(&task.ID, &task.CurrentVersionID, &status, &task.CreatedBy, &task.UpdatedBy, &task.CreatedAt, &task.UpdatedAt, &task.DeletedAt)
	task.Status = domain.TaskStatus(status)

	return task, err
}

// scanTaskVersion считывает одну строку task_versions в доменную модель.
func scanTaskVersion(row pgx.Row) (domain.TaskVersion, error) {
	var version domain.TaskVersion
	var taskType string
	var difficulty string
	err := row.Scan(&version.ID, &version.TaskID, &version.VersionNumber, &version.TopicID, &version.Title, &version.Statement, &taskType, &difficulty, &version.CreatedBy, &version.CreatedAt, &version.UpdatedAt)
	version.TaskType = domain.TaskType(taskType)
	version.Difficulty = domain.Difficulty(difficulty)

	return version, err
}

// scanTaskDetail считывает joined строку tasks и task_versions.
func scanTaskDetail(row pgx.Row) (domain.Task, domain.TaskVersion, error) {
	var task domain.Task
	var version domain.TaskVersion
	var status string
	var taskType string
	var difficulty string
	err := row.Scan(
		&task.ID,
		&task.CurrentVersionID,
		&status,
		&task.CreatedBy,
		&task.UpdatedBy,
		&task.CreatedAt,
		&task.UpdatedAt,
		&task.DeletedAt,
		&version.ID,
		&version.TaskID,
		&version.VersionNumber,
		&version.TopicID,
		&version.Title,
		&version.Statement,
		&taskType,
		&difficulty,
		&version.CreatedBy,
		&version.CreatedAt,
		&version.UpdatedAt,
	)
	task.Status = domain.TaskStatus(status)
	version.TaskType = domain.TaskType(taskType)
	version.Difficulty = domain.Difficulty(difficulty)

	return task, version, err
}

// scanTaskOption считывает строку task_options в доменную модель.
func scanTaskOption(row pgx.Row) (domain.TaskOption, error) {
	var option domain.TaskOption
	err := row.Scan(&option.ID, &option.TaskVersionID, &option.Text, &option.IsCorrect, &option.Position, &option.CreatedAt, &option.UpdatedAt)

	return option, err
}
