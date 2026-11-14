package usecase

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

type candidateMemoryRepository struct {
	repository.Repository
	candidate     domain.TaskCandidate
	task          domain.Task
	version       domain.TaskVersion
	created       bool
	problemListID uuid.UUID
	memberAdded   bool
}

// InsertCandidate записывает кандидата как новую запись входной очереди.
func (r *candidateMemoryRepository) InsertCandidate(_ context.Context, candidate domain.TaskCandidate) (bool, error) {
	r.candidate = candidate
	r.created = true

	return true, nil
}

// GetOrCreateProblemList находит или создаёт список по source_url.
func (r *candidateMemoryRepository) GetOrCreateProblemList(_ context.Context, input domain.ProblemListImport) (uuid.UUID, error) {
	if r.problemListID == uuid.Nil {
		r.problemListID = uuid.New()
	}
	_ = input

	return r.problemListID, nil
}

// InsertProblemListMember фиксирует членство опубликованной задачи в списке.
func (r *candidateMemoryRepository) InsertProblemListMember(_ context.Context, _ uuid.UUID, taskID uuid.UUID, _ int) error {
	if r.task.ID != taskID {
		return fmt.Errorf("member task mismatch")
	}
	r.memberAdded = true

	return nil
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

// InsertTaskActivityOutbox фиксирует business-событие публикации задачи.
func (r *candidateMemoryRepository) InsertTaskActivityOutbox(_ context.Context, _ domain.TaskActivityOutboxMessage) error {
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

// TestCandidateImportAssociatesProblemList проверяет привязку кандидатов
// к problem-list при импорте и членство опубликованной задачи при approve.
func TestCandidateImportAssociatesProblemList(t *testing.T) {
	repo := &candidateMemoryRepository{}
	service := NewCandidateService(repo)

	results := service.ImportBatch(context.Background(), []domain.CandidateImport{
		{
			ExternalID: "leetcode:two-sum", SourceID: "leetcode", SourceName: "LeetCode",
			SourceURL: "https://leetcode.com/problems/two-sum", SourceHash: "hash1",
			RetrievedAt: time.Now().UTC(), CollectionJobID: uuid.New(), Title: "Two Sum",
			Statement: "Найдите сумму двух чисел", Difficulty: domain.DifficultyEasy,
			ProblemList: &domain.ProblemListImport{
				Name: "oy4y273c", SourceID: "leetcode", SourceURL: "https://leetcode.com/problem-list/oy4y273c",
			},
		},
	})
	if len(results) != 1 || results[0].Status != "imported" {
		t.Fatalf("unexpected import results: %#v", results)
	}
	if repo.candidate.ProblemListID == nil || *repo.candidate.ProblemListID != repo.problemListID {
		t.Fatalf("candidate was not linked to problem list: %#v", repo.candidate.ProblemListID)
	}

	// Одобряем кандидата списка — задача должна попасть в состав списка.
	actorID := uuid.New()
	review := domain.CandidateReview{
		ExpectedRevision: 1, Title: "Two Sum", Statement: "Найдите сумму двух чисел", Difficulty: domain.DifficultyEasy,
	}
	if _, err := service.Approve(context.Background(), repo.candidate.ID, actorID, review); err != nil {
		t.Fatalf("approve list candidate: %v", err)
	}
	if !repo.memberAdded {
		t.Fatal("published task was not added to the problem list")
	}
}

// TestCandidateImportSkipsProblemListWithoutRef проверяет импорт без ссылки на список.
func TestCandidateImportSkipsProblemListWithoutRef(t *testing.T) {
	repo := &candidateMemoryRepository{}
	service := NewCandidateService(repo)

	results := service.ImportBatch(context.Background(), []domain.CandidateImport{
		{
			ExternalID: "leetcode:two-sum", SourceID: "leetcode", SourceName: "LeetCode",
			SourceURL: "https://leetcode.com/problems/two-sum", SourceHash: "hash1",
			RetrievedAt: time.Now().UTC(), CollectionJobID: uuid.New(), Title: "Two Sum",
			Statement: "Найдите сумму двух чисел", Difficulty: domain.DifficultyEasy,
		},
	})
	if len(results) != 1 || results[0].Status != "imported" {
		t.Fatalf("unexpected import results: %#v", results)
	}
	if repo.candidate.ProblemListID != nil || repo.problemListID != uuid.Nil {
		t.Fatal("problem list should not be created without a ref")
	}
}
