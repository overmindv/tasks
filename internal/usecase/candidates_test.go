package usecase

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks-it/internal/domain"
	"github.com/overmindv/tasks-it/internal/repository"
)

type candidateMemoryRepository struct {
	repository.Repository
	candidate domain.TaskCandidate
	task      domain.Task
	version   domain.TaskVersion
}

// WithinTransaction выполняет callback над одной in-memory транзакцией.
func (r *candidateMemoryRepository) WithinTransaction(_ context.Context, fn func(repository.Repository) error) error {
	return fn(r)
}

// GetCandidate возвращает текущего кандидата.
func (r *candidateMemoryRepository) GetCandidate(_ context.Context, _ uuid.UUID, _ bool) (domain.TaskCandidate, error) {
	return r.candidate, nil
}

// UpdateCandidate сохраняет итоговый payload при совпавшей revision.
func (r *candidateMemoryRepository) UpdateCandidate(_ context.Context, candidate domain.TaskCandidate, expectedRevision int) error {
	if r.candidate.Revision != expectedRevision || r.candidate.Status != domain.CandidateStatusPending {
		return fmt.Errorf("revision conflict")
	}
	candidate.Revision++
	r.candidate = candidate

	return nil
}

// InsertTask сохраняет опубликованный агрегат.
func (r *candidateMemoryRepository) InsertTask(_ context.Context, task domain.Task) error {
	r.task = task

	return nil
}

// InsertTaskVersion сохраняет первую immutable version.
func (r *candidateMemoryRepository) InsertTaskVersion(_ context.Context, version domain.TaskVersion) error {
	r.version = version

	return nil
}

// InsertTaskContent подтверждает сохранение расширенного programming-содержимого.
func (r *candidateMemoryRepository) InsertTaskContent(_ context.Context, version domain.TaskVersion) error {
	r.version = version

	return nil
}

// SetCurrentTaskVersion связывает агрегат с опубликованной версией.
func (r *candidateMemoryRepository) SetCurrentTaskVersion(_ context.Context, _ uuid.UUID, versionID, _ uuid.UUID) error {
	r.task.CurrentVersionID = &versionID

	return nil
}

// MarkCandidateApproved завершает кандидата при совпавшей новой revision.
func (r *candidateMemoryRepository) MarkCandidateApproved(_ context.Context, _ uuid.UUID, taskID, actorID uuid.UUID, expectedRevision int) error {
	if r.candidate.Revision != expectedRevision {
		return fmt.Errorf("revision conflict")
	}
	r.candidate.Status = domain.CandidateStatusApproved
	r.candidate.Revision++
	r.candidate.ApprovedTaskID = &taskID
	r.candidate.ReviewedBy = &actorID

	return nil
}

// GetTaskDetail возвращает опубликованную задачу после commit.
func (r *candidateMemoryRepository) GetTaskDetail(_ context.Context, _ uuid.UUID) (domain.TaskDetail, error) {
	return domain.TaskDetail{Task: r.task, Version: r.version}, nil
}

// TestCandidateApproveIsAtomicAndIdempotent проверяет финальный payload и повторный approve.
func TestCandidateApproveIsAtomicAndIdempotent(t *testing.T) {
	t.Parallel()

	candidateID := uuid.New()
	repo := &candidateMemoryRepository{candidate: domain.TaskCandidate{
		ID: candidateID, Status: domain.CandidateStatusPending, Revision: 3,
		ExternalID: "coderun:calculator", SourceID: "coderun", SourceName: "CodeRun",
		SourceURL: "https://coderun.yandex.ru/problem/calculator", SourceHash: "hash",
		RetrievedAt: time.Now().UTC(), CollectionJobID: uuid.New(), Title: "Черновик",
		Statement: "Старое условие", Difficulty: domain.DifficultyEasy,
	}}
	service := NewCandidateService(repo)
	actorID := uuid.New()
	review := domain.CandidateReview{
		ExpectedRevision: 3,
		Title:            "Калькулятор",
		Statement:        "Вычислите выражение",
		Difficulty:       domain.DifficultyMedium,
		Tags:             []string{"math"},
		Examples:         []domain.TaskExample{{Input: "2 + 2", Output: "4"}},
		Constraints:      []string{"Ввод корректен"},
	}
	approved, err := service.Approve(context.Background(), candidateID, actorID, review)
	if err != nil {
		t.Fatal(err)
	}
	if approved.Task.Status != domain.TaskStatusPublished || approved.Version.TaskType != domain.TaskTypeProgramming {
		t.Fatalf("unexpected approved task: %+v", approved)
	}
	if approved.Version.Title != review.Title || len(approved.Version.Tags) != 1 || approved.Version.Source == nil {
		t.Fatalf("final payload was not transferred: %+v", approved.Version)
	}
	if repo.candidate.Status != domain.CandidateStatusApproved || repo.candidate.Title != review.Title || repo.candidate.Revision != 5 {
		t.Fatalf("candidate was not finalized: %+v", repo.candidate)
	}

	repeated, err := service.Approve(context.Background(), candidateID, actorID, review)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.Task.ID != approved.Task.ID {
		t.Fatalf("repeated approve created another task: %s != %s", repeated.Task.ID, approved.Task.ID)
	}
}
