package usecase

import (
	"context"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

// TopicValidator проверяет публичную доступность canonical topic через entities API.
type TopicValidator interface {
	ValidateTopic(context.Context, uuid.UUID) error
}

func publicationError(message string) error {
	return apperror.New("PUBLICATION_CHECK_FAILED", message, 409)
}

// publicationSnapshot проверяет внешний каталог вне транзакции; версия сверяется под lock.
func (s *TaskService) publicationSnapshot(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	v, err := s.repository.GetCurrentTaskVersion(ctx, id)
	if err != nil {
		return uuid.Nil, err
	}
	if v.TopicID == nil || *v.TopicID == uuid.Nil {
		return uuid.Nil, publicationError("canonical topic required")
	}
	if s.topics == nil {
		return uuid.Nil, apperror.New("TOPIC_CATALOG_UNAVAILABLE", "topic catalog unavailable", 503)
	}
	if err := s.topics.ValidateTopic(ctx, *v.TopicID); err != nil {
		return uuid.Nil, err
	}

	return v.ID, nil
}

func (s *TaskService) validatePublication(ctx context.Context, tx repository.Repository, v domain.TaskVersion) error {
	if v.TaskType != domain.TaskTypeProgramming {
		return nil
	}
	if v.Source == nil || strings.TrimSpace(v.Source.SourceID) == "" || strings.TrimSpace(v.Source.SourceName) == "" {
		return publicationError("source attribution required")
	}
	u, err := url.Parse(v.Source.SourceURL)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return publicationError("valid source attribution URL required")
	}
	suite, err := tx.GetPublishedSuite(ctx, v.TaskID, v.ID)
	if err != nil {
		return err
	}
	if suite == nil {
		return publicationError("published suite required")
	}
	if validationErr := domain.ValidateSuite(suite.Definition, true); validationErr != nil {
		return publicationError("valid graded suite required")
	}
	if s.versions[suite.Definition.Language] != suite.Definition.LanguageVersion {
		return publicationError("suite runtime not configured")
	}
	passed, err := tx.EditorialCheckPassed(ctx, v.TaskID, v.ID)
	if err != nil {
		return err
	}
	if !passed {
		return publicationError("accepted editorial run required for current version")
	}

	return nil
}
