package usecase

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/google/uuid"
	"github.com/overmindv/tasks/internal/domain"
)

const MaxResourcePage = 100

var ErrInvalidResourcePage = errors.New("invalid resource page")

type ResourceStore interface {
	ResourceWatermark(context.Context) (int64, error)
	ResourceSnapshot(ctx context.Context, watermark int64, after uuid.UUID, limit int) ([]domain.ResourceChange, error)
	ResourceChanges(ctx context.Context, watermark, after int64, limit int) ([]domain.ResourceChange, error)
	AvailableResourceVersions(context.Context, []domain.ResourceKey) ([]domain.ResourceAvailability, error)
}

type ResourceService struct {
	store    ResourceStore
	runtimes map[domain.ProgrammingLanguage]string
}

func NewResourceService(store ResourceStore, runtimes map[domain.ProgrammingLanguage]string) *ResourceService {
	return &ResourceService{store: store, runtimes: maps.Clone(runtimes)}
}

type ResourceSnapshotPage struct {
	Watermark  int64
	NextCursor *uuid.UUID
	Records    []domain.ResourceChange
}

type ResourceChangePage struct {
	Watermark int64
	Through   int64
	More      bool
	Changes   []domain.ResourceChange
}

func (s *ResourceService) Snapshot(ctx context.Context, requested *int64, after uuid.UUID, limit int) (ResourceSnapshotPage, error) {
	if after != uuid.Nil && requested == nil {
		return ResourceSnapshotPage{}, ErrInvalidResourcePage
	}
	watermark, err := s.watermark(ctx, requested, 0, limit)
	if err != nil {
		return ResourceSnapshotPage{}, err
	}
	records, err := s.store.ResourceSnapshot(ctx, watermark, after, limit+1)
	if err != nil {
		return ResourceSnapshotPage{}, fmt.Errorf("load resource snapshot: %w", err)
	}
	page := ResourceSnapshotPage{Watermark: watermark, Records: records}
	if len(records) > limit {
		page.Records = records[:limit]
		id := records[limit-1].TaskID
		page.NextCursor = &id
	}

	return page, nil
}

func (s *ResourceService) Changes(ctx context.Context, requested *int64, after int64, limit int) (ResourceChangePage, error) {
	watermark, err := s.watermark(ctx, requested, after, limit)
	if err != nil {
		return ResourceChangePage{}, err
	}
	changes, err := s.store.ResourceChanges(ctx, watermark, after, limit+1)
	if err != nil {
		return ResourceChangePage{}, fmt.Errorf("load resource changes: %w", err)
	}
	page := ResourceChangePage{Watermark: watermark, Through: watermark, Changes: changes}
	if len(changes) > limit {
		page.More = true
		page.Through = changes[limit-1].Revision
		page.Changes = changes[:limit]
	}

	return page, nil
}

func (s *ResourceService) Available(ctx context.Context, keys []domain.ResourceKey) ([]domain.ResourceKey, error) {
	if len(keys) < 1 || len(keys) > MaxResourcePage {
		return nil, ErrInvalidResourcePage
	}
	available := make(map[domain.ResourceKey]bool, len(keys))
	for _, key := range keys {
		if _, duplicate := available[key]; key.TaskID == uuid.Nil || key.VersionID == uuid.Nil || duplicate {
			return nil, ErrInvalidResourcePage
		}
		available[key] = false
	}
	current, err := s.store.AvailableResourceVersions(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("load available resource versions: %w", err)
	}
	for _, item := range current {
		if item.Runtime == nil {
			available[item.Key] = true
		} else if version, ok := s.runtimes[item.Runtime.Language]; ok && version == item.Runtime.Version {
			available[item.Key] = true
		}
	}
	result := make([]domain.ResourceKey, 0, len(current))
	for _, key := range keys {
		if available[key] {
			result = append(result, key)
		}
	}

	return result, nil
}

func (s *ResourceService) watermark(ctx context.Context, requested *int64, after int64, limit int) (int64, error) {
	if after < 0 || limit < 1 || limit > MaxResourcePage || (requested != nil && *requested < after) {
		return 0, ErrInvalidResourcePage
	}
	current, err := s.store.ResourceWatermark(ctx)
	if err != nil {
		return 0, fmt.Errorf("locate resource watermark: %w", err)
	}
	if after > current || (requested != nil && *requested > current) {
		return 0, ErrInvalidResourcePage
	}
	if requested != nil {
		return *requested, nil
	}

	return current, nil
}
