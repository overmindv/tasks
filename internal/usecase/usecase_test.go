package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
	"github.com/samber/lo"
)

type taskMemoryRepository struct {
	repository.Repository
	task     domain.Task
	versions map[uuid.UUID]domain.TaskVersion
}

// WithinTransaction выполняет callback над тем же in-memory repository.
func (r *taskMemoryRepository) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	return fn(r)
}

// InsertTask запоминает агрегат задачи.
func (r *taskMemoryRepository) InsertTask(_ context.Context, task domain.Task) error {
	r.task = task

	return nil
}

// InsertTaskVersion запоминает новую версию.
func (r *taskMemoryRepository) InsertTaskVersion(_ context.Context, version domain.TaskVersion) error {
	if r.versions == nil {
		r.versions = make(map[uuid.UUID]domain.TaskVersion)
	}
	r.versions[version.ID] = version

	return nil
}

// InsertTaskOptions добавляет варианты к сохранённой версии.
func (r *taskMemoryRepository) InsertTaskOptions(_ context.Context, options []domain.TaskOption) error {
	if len(options) == 0 {
		return errors.New("options are required")
	}
	version := r.versions[options[0].TaskVersionID]
	version.Options = options
	r.versions[version.ID] = version

	return nil
}

// SetCurrentTaskVersion переключает текущую версию in-memory задачи.
func (r *taskMemoryRepository) SetCurrentTaskVersion(_ context.Context, _ uuid.UUID, versionID, actorID uuid.UUID) error {
	r.task.CurrentVersionID = &versionID
	r.task.UpdatedBy = actorID

	return nil
}

// GetTask возвращает in-memory агрегат.
func (r *taskMemoryRepository) GetTask(_ context.Context, _ uuid.UUID, _ bool) (domain.Task, error) {
	return r.task, nil
}

// GetCurrentTaskVersion возвращает текущую in-memory версию.
func (r *taskMemoryRepository) GetCurrentTaskVersion(_ context.Context, _ uuid.UUID) (domain.TaskVersion, error) {
	return r.versions[*r.task.CurrentVersionID], nil
}

// GetTaskDetail возвращает агрегат с текущей версией.
func (r *taskMemoryRepository) GetTaskDetail(_ context.Context, _ uuid.UUID) (domain.TaskDetail, error) {
	return domain.TaskDetail{
		Task:    r.task,
		Version: r.versions[*r.task.CurrentVersionID],
	}, nil
}

// TestTaskServiceCreatesAndVersionsTask проверяет создание и последующее версионирование.
func TestTaskServiceCreatesAndVersionsTask(t *testing.T) {
	t.Parallel()
	repo := &taskMemoryRepository{}
	service := NewTaskService(repo)
	actorID := uuid.New()
	input := domain.TaskInput{
		Title:      "HTTP status",
		Statement:  "Какой статус означает успех?",
		TaskType:   domain.TaskTypeSingleChoice,
		Difficulty: domain.DifficultyEasy,
		Options: []domain.OptionInput{
			{Text: "200", IsCorrect: true},
			{Text: "500", IsCorrect: false},
		},
	}
	created, err := service.Create(context.Background(), input, actorID)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.Version.VersionNumber != 1 || created.Task.Status != domain.TaskStatusDraft {
		t.Fatalf("created task = %#v", created)
	}
	firstVersionID := created.Version.ID
	input.Statement = "Какой HTTP status означает успешный запрос?"
	updated, err := service.Update(context.Background(), created.Task.ID, input, actorID)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.Version.VersionNumber != 2 || updated.Version.ID == firstVersionID {
		t.Fatalf("updated version = %#v", updated.Version)
	}
	if _, ok := repo.versions[firstVersionID]; !ok {
		t.Fatal("первая версия должна оставаться в repository")
	}
}

type submissionMemoryRepository struct {
	repository.Repository
	task        domain.Task
	versions    map[uuid.UUID]domain.TaskVersion
	submissions map[uuid.UUID]domain.Submission
	answers     map[uuid.UUID][]uuid.UUID
}

// WithinTransaction выполняет submission callback над in-memory repository.
func (r *submissionMemoryRepository) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	return fn(r)
}

// GetTask возвращает опубликованную in-memory задачу.
func (r *submissionMemoryRepository) GetTask(_ context.Context, _ uuid.UUID, _ bool) (domain.Task, error) {
	return r.task, nil
}

// GetTaskVersion возвращает запрошенную историческую версию.
func (r *submissionMemoryRepository) GetTaskVersion(_ context.Context, _, versionID uuid.UUID) (domain.TaskVersion, error) {
	version, ok := r.versions[versionID]
	if !ok {
		return domain.TaskVersion{}, errors.New("version not found")
	}

	return version, nil
}

// FindSubmissionByIdempotency ищет отправку по in-memory ключу.
func (r *submissionMemoryRepository) FindSubmissionByIdempotency(_ context.Context, userID, key uuid.UUID) (*domain.Submission, error) {
	for _, submission := range r.submissions {
		if submission.UserID == userID && submission.IdempotencyKey == key {
			result := r.hydrate(submission)

			return &result, nil
		}
	}

	return nil, nil
}

// InsertSubmission сохраняет in-memory результат.
func (r *submissionMemoryRepository) InsertSubmission(_ context.Context, submission domain.Submission) error {
	if r.submissions == nil {
		r.submissions = make(map[uuid.UUID]domain.Submission)
	}
	r.submissions[submission.ID] = submission

	return nil
}

// InsertSubmissionAnswers сохраняет выбранные варианты.
func (r *submissionMemoryRepository) InsertSubmissionAnswers(_ context.Context, submissionID, _ uuid.UUID, optionIDs []uuid.UUID) error {
	if r.answers == nil {
		r.answers = make(map[uuid.UUID][]uuid.UUID)
	}
	r.answers[submissionID] = optionIDs

	return nil
}

// GetSubmission возвращает hydrated in-memory результат.
func (r *submissionMemoryRepository) GetSubmission(_ context.Context, id uuid.UUID) (domain.Submission, error) {
	return r.hydrate(r.submissions[id]), nil
}

// hydrate дополняет результат выбранными, правильными и текущими версиями.
func (r *submissionMemoryRepository) hydrate(submission domain.Submission) domain.Submission {
	version := r.versions[submission.TaskVersionID]
	latest := r.versions[*r.task.CurrentVersionID]
	submission.TaskVersionNumber = version.VersionNumber
	submission.SelectedOptionIDs = r.answers[submission.ID]
	submission.CorrectOptionIDs = lo.FilterMap(version.Options, func(option domain.TaskOption, _ int) (uuid.UUID, bool) {
		return option.ID, option.IsCorrect
	})
	submission.LatestTaskVersionID = latest.ID
	submission.LatestVersionNumber = latest.VersionNumber
	submission.TaskUpdated = version.ID != latest.ID

	return submission
}

// TestSubmissionServiceKeepsOldVersionAndIdempotency проверяет старую версию и conflict ключа.
func TestSubmissionServiceKeepsOldVersionAndIdempotency(t *testing.T) {
	t.Parallel()
	taskID := uuid.New()
	userID := uuid.New()
	oldVersionID := uuid.New()
	latestVersionID := uuid.New()
	correctOptionID := uuid.New()
	wrongOptionID := uuid.New()
	repo := &submissionMemoryRepository{
		task: domain.Task{
			ID:               taskID,
			CurrentVersionID: &latestVersionID,
			Status:           domain.TaskStatusPublished,
		},
		versions: map[uuid.UUID]domain.TaskVersion{
			oldVersionID: {
				ID:            oldVersionID,
				TaskID:        taskID,
				VersionNumber: 1,
				TaskType:      domain.TaskTypeSingleChoice,
				Options: []domain.TaskOption{
					{ID: correctOptionID, TaskVersionID: oldVersionID, IsCorrect: true},
					{ID: wrongOptionID, TaskVersionID: oldVersionID, IsCorrect: false},
				},
			},
			latestVersionID: {
				ID:            latestVersionID,
				TaskID:        taskID,
				VersionNumber: 2,
			},
		},
	}
	service := NewSubmissionService(repo)
	key := uuid.New()
	input := SubmitInput{
		TaskVersionID:     oldVersionID,
		IdempotencyKey:    key,
		SelectedOptionIDs: []uuid.UUID{correctOptionID},
	}
	result, err := service.Submit(context.Background(), taskID, userID, input)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if !result.IsCorrect() || !result.TaskUpdated || result.LatestTaskVersionID != latestVersionID {
		t.Fatalf("submission = %#v", result)
	}
	repeated, err := service.Submit(context.Background(), taskID, userID, input)
	if err != nil || repeated.ID != result.ID {
		t.Fatalf("repeated Submit() = %#v, %v", repeated, err)
	}
	input.SelectedOptionIDs = []uuid.UUID{wrongOptionID}
	_, err = service.Submit(context.Background(), taskID, userID, input)
	var public *apperror.Error
	if !errors.As(err, &public) || public.Code != apperror.IdempotencyKeyConflict {
		t.Fatalf("conflicting Submit() error = %v", err)
	}
}
