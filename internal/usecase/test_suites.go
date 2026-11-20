package usecase

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

// SuiteObjectStore публикует объект без возможности перезаписи другим содержимым.
type SuiteObjectStore interface {
	PutImmutable(context.Context, string, []byte) error
}
type SuiteService struct {
	repository repository.Repository
	objects    SuiteObjectStore
	versions   map[domain.ProgrammingLanguage]string
}

func NewSuiteService(r repository.Repository, o SuiteObjectStore, configured ...map[domain.ProgrammingLanguage]string) *SuiteService {
	versions := map[domain.ProgrammingLanguage]string{domain.ProgrammingLanguagePython: "3.12", domain.ProgrammingLanguageGo: "1.26"}
	if len(configured) > 0 {
		versions = make(map[domain.ProgrammingLanguage]string)
		for k, v := range configured[0] {
			versions[k] = v
		}
	}
	return &SuiteService{repository: r, objects: o, versions: versions}
}
func suiteConflict() error {
	return apperror.New("SUITE_CONFLICT", "suite revision or task version changed", http.StatusConflict)
}
func (s *SuiteService) Create(ctx context.Context, taskID, baseID uuid.UUID, definition domain.SuiteDefinition) (domain.SuiteDraft, error) {
	if err := domain.ValidateSuite(definition, false); err != nil {
		return domain.SuiteDraft{}, validation(err)
	}
	draft := domain.SuiteDraft{ID: uuid.New(), TaskID: taskID, BaseVersionID: baseID, Revision: 1, Definition: definition}
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		task, err := tx.GetTask(ctx, taskID, true)
		if err != nil {
			return err
		}
		if task.Status == domain.TaskStatusArchived || task.CurrentVersionID == nil || *task.CurrentVersionID != baseID {
			return suiteConflict()
		}
		version, err := tx.GetTaskVersion(ctx, taskID, baseID)
		if err != nil {
			return err
		}
		if version.TaskType != domain.TaskTypeProgramming {
			return apperror.New(apperror.TaskTypeNotSubmittable, "programming task required", 409)
		}
		return tx.InsertSuiteDraft(ctx, draft)
	})
	return draft, err
}
func (s *SuiteService) Get(ctx context.Context, id uuid.UUID) (domain.SuiteDraft, error) {
	return s.repository.GetSuiteDraft(ctx, id, false)
}
func (s *SuiteService) Update(ctx context.Context, id uuid.UUID, revision int, definition domain.SuiteDefinition) (domain.SuiteDraft, error) {
	if err := domain.ValidateSuite(definition, false); err != nil {
		return domain.SuiteDraft{}, validation(err)
	}
	var result domain.SuiteDraft
	err := s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		d, err := tx.GetSuiteDraft(ctx, id, true)
		if err != nil {
			return err
		}
		if d.Revision != revision || d.PublishedVersionID != nil {
			return suiteConflict()
		}
		d.Definition = definition
		ok, err := tx.UpdateSuiteDraft(ctx, d, revision)
		if err != nil {
			return err
		}
		if !ok {
			return suiteConflict()
		}
		d.Revision++
		result = d
		return nil
	})
	return result, err
}
func (s *SuiteService) Delete(ctx context.Context, id uuid.UUID, revision int) error {
	ok, err := s.repository.DeleteSuiteDraft(ctx, id, revision)
	if err != nil {
		return err
	}
	if !ok {
		return suiteConflict()
	}
	return nil
}
func (s *SuiteService) Publish(ctx context.Context, id uuid.UUID, revision int, actor uuid.UUID) (domain.PublishedSuite, error) {
	d, err := s.repository.GetSuiteDraft(ctx, id, false)
	if err != nil {
		return domain.PublishedSuite{}, err
	}
	if d.Revision != revision {
		return domain.PublishedSuite{}, suiteConflict()
	}
	if d.PublishedVersionID != nil {
		return s.published(ctx, d.TaskID, *d.PublishedVersionID)
	}
	if err := domain.ValidateSuite(d.Definition, true); err != nil {
		return domain.PublishedSuite{}, validation(err)
	}
	if s.versions[d.Definition.Language] != d.Definition.LanguageVersion {
		return domain.PublishedSuite{}, apperror.New(apperror.ValidationError, "suite runtime is not configured", 400)
	}
	if s.objects == nil {
		return domain.PublishedSuite{}, apperror.New("SUITE_STORAGE_UNAVAILABLE", "suite storage unavailable", http.StatusServiceUnavailable)
	}
	version := strconv.Itoa(d.Revision)
	payload, hash := d.Definition.Manifest(d.ID, version)
	// Загрузка выполняется вне транзакции; CAS ниже оставляет возможный orphan для отдельного cleanup.
	if err := s.objects.PutImmutable(ctx, d.ID.String()+"/"+version+"/suite.json", payload); err != nil {
		return domain.PublishedSuite{}, apperror.New("SUITE_STORAGE_UNAVAILABLE", "suite storage unavailable", http.StatusServiceUnavailable)
	}
	var published domain.PublishedSuite
	err = s.repository.WithinTransaction(ctx, func(tx repository.Repository) error {
		current, err := tx.GetSuiteDraft(ctx, id, true)
		if err != nil {
			return err
		}
		if current.Revision != revision {
			return suiteConflict()
		}
		if current.PublishedVersionID != nil {
			existing, err := tx.GetPublishedSuite(ctx, d.TaskID, *current.PublishedVersionID)
			if err != nil {
				return err
			}
			if existing == nil {
				return fmt.Errorf("published suite missing")
			}
			published = *existing
			return nil
		}
		task, err := tx.GetTask(ctx, d.TaskID, true)
		if err != nil {
			return err
		}
		if task.Status == domain.TaskStatusArchived || task.CurrentVersionID == nil || *task.CurrentVersionID != d.BaseVersionID {
			return suiteConflict()
		}
		old, err := tx.GetTaskVersion(ctx, d.TaskID, d.BaseVersionID)
		if err != nil {
			return err
		}
		input := domain.TaskInput{TopicID: old.TopicID, Title: old.Title, Statement: old.Statement, TaskType: old.TaskType, Difficulty: old.Difficulty, EstimatedMinutes: old.EstimatedMinutes, Tags: old.Tags, Examples: old.Examples, Constraints: old.Constraints, Source: old.Source}
		next := buildVersion(d.TaskID, uuid.New(), old.VersionNumber+1, input, actor)
		if err := tx.InsertTaskVersion(ctx, next); err != nil {
			return err
		}
		if hasExtendedContent(next) {
			if err := tx.InsertTaskContent(ctx, next); err != nil {
				return err
			}
		}
		published = domain.PublishedSuite{TaskID: d.TaskID, TaskVersionID: next.ID, Reference: domain.SuiteReference{ID: d.ID, Version: version, ChecksumSHA256: hash}, Definition: d.Definition}
		if err := tx.InsertPublishedSuite(ctx, published); err != nil {
			return err
		}
		if err := tx.SetCurrentTaskVersion(ctx, d.TaskID, next.ID, actor); err != nil {
			return err
		}
		ok, err := tx.MarkSuitePublished(ctx, d.ID, revision, next.ID)
		if err != nil {
			return err
		}
		if !ok {
			return suiteConflict()
		}
		return nil
	})
	return published, err
}
func (s *SuiteService) published(ctx context.Context, taskID, versionID uuid.UUID) (domain.PublishedSuite, error) {
	p, err := s.repository.GetPublishedSuite(ctx, taskID, versionID)
	if err != nil {
		return domain.PublishedSuite{}, err
	}
	if p == nil {
		return domain.PublishedSuite{}, fmt.Errorf("published suite missing")
	}
	return *p, nil
}
