package usecase

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

type publicationRepository struct {
	taskMemoryRepository
	emitted bool
	suite   *domain.PublishedSuite
	passed  bool
}

type topicValidatorFunc func(context.Context, uuid.UUID) error

func (f topicValidatorFunc) ValidateTopic(ctx context.Context, id uuid.UUID) error { return f(ctx, id) }
func (r *publicationRepository) GetPublishedSuite(context.Context, uuid.UUID, uuid.UUID) (*domain.PublishedSuite, error) {
	return r.suite, nil
}
func (r *publicationRepository) EditorialCheckPassed(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return r.passed, nil
}

func (r *publicationRepository) WithinTransaction(_ context.Context, fn func(repository.Repository) error) error {
	return fn(r)
}

func TestPublicationGate(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"accepted", "no attribution", "invalid URL", "no suite", "unsafe limits", "unsupported runtime", "failed or pending", "topic unavailable", "version race"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			id, v, topic := uuid.New(), uuid.New(), uuid.New()
			r := &publicationRepository{taskMemoryRepository: taskMemoryRepository{task: domain.Task{ID: id, Status: domain.TaskStatusDraft, CurrentVersionID: &v}, versions: map[uuid.UUID]domain.TaskVersion{v: {ID: v, TaskID: id, TopicID: &topic, TaskType: domain.TaskTypeProgramming, Source: &domain.TaskSource{SourceID: "author", SourceName: "Author", SourceURL: "https://example.org/task"}}}}, passed: true, suite: &domain.PublishedSuite{Definition: domain.SuiteDefinition{Language: "python", LanguageVersion: "3.12", Checker: "exact", Limits: domain.SuiteLimits{CPUms: 1000, Wallms: 2000, MemoryMB: 64, PIDs: 32, StdoutBytes: 32768, StderrBytes: 32768, WorkspaceBytes: 1048576}, Cases: []domain.SuiteCase{{ID: "hidden", Input: "1", Output: "1", Hidden: true}}}}}
			validator := topicValidatorFunc(func(context.Context, uuid.UUID) error { return nil })
			switch name {
			case "no attribution":
				version := r.versions[v]
				version.Source = nil
				r.versions[v] = version
			case "invalid URL":
				r.versions[v].Source.SourceURL = "javascript:alert(1)"
			case "no suite":
				r.suite = nil
			case "unsafe limits":
				r.suite.Definition.Limits.MemoryMB = 9999
			case "unsupported runtime":
				r.suite.Definition.LanguageVersion = "99.0"
			case "failed or pending":
				r.passed = false
			case "topic unavailable":
				validator = func(context.Context, uuid.UUID) error { return publicationError("topic unavailable") }
			case "version race":
				validator = func(context.Context, uuid.UUID) error {
					next := uuid.New()
					r.task.CurrentVersionID = &next

					return nil
				}
			}
			_, err := NewTaskService(r).WithPublicationChecks(validator, map[domain.ProgrammingLanguage]string{"python": "3.12"}).ChangeStatus(context.Background(), id, domain.TaskStatusPublished, uuid.New())
			if (err == nil) != (name == string(domain.ExecutionVerdictAccepted)) {
				t.Fatalf("unexpected publication: %v", err)
			}
			if name != string(domain.ExecutionVerdictAccepted) && (r.emitted || r.task.Status == domain.TaskStatusPublished) {
				t.Fatal("rejected publication changed state")
			}
		})
	}
}
func (r *publicationRepository) SetTaskStatus(_ context.Context, _ uuid.UUID, status domain.TaskStatus, _ uuid.UUID) error {
	r.task.Status = status

	return nil
}
func (r *publicationRepository) InsertTaskActivityOutbox(_ context.Context, _ domain.TaskActivityOutboxMessage) error {
	r.emitted = true

	return nil
}

func TestPublicationRequiresTopic(t *testing.T) {
	t.Parallel()
	id, version := uuid.New(), uuid.New()
	r := &publicationRepository{taskMemoryRepository: taskMemoryRepository{task: domain.Task{ID: id, Status: domain.TaskStatusDraft, CurrentVersionID: &version}, versions: map[uuid.UUID]domain.TaskVersion{version: {ID: version, TaskID: id, TaskType: domain.TaskTypeProgramming}}}}
	_, err := NewTaskService(r).ChangeStatus(context.Background(), id, domain.TaskStatusPublished, uuid.New())
	if err == nil {
		t.Fatal("published without canonical topic and editorial check")
	}
	if r.emitted {
		t.Fatal("emitted publication event on rejection")
	}
}
