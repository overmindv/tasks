package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/overmindv/tasks/internal/apperror"
	"github.com/overmindv/tasks/internal/domain"
)

const forUpdateClause = " FOR UPDATE"

func (r *Postgres) GetPublishedSuite(ctx context.Context, taskID, versionID uuid.UUID) (*domain.PublishedSuite, error) {
	s := domain.PublishedSuite{TaskID: taskID, TaskVersionID: versionID}
	var raw []byte
	var checksum string
	err := r.query.QueryRow(ctx, `SELECT suite_id,suite_version,manifest_sha256,definition,definition_sha256 FROM task_version_suites WHERE task_id=$1 AND task_version_id=$2`, taskID, versionID).Scan(&s.Reference.ID, &s.Reference.Version, &s.Reference.ChecksumSHA256, &raw, &checksum)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read published suite: %w", err)
	}
	if err = json.Unmarshal(raw, &s.Definition); err != nil {
		return nil, fmt.Errorf("decode suite: %w", err)
	}
	if err = domain.ValidateSuite(s.Definition, true); err != nil {
		return nil, err
	}
	_, manifestHash := s.Definition.Manifest(s.Reference.ID, s.Reference.Version)
	if s.Definition.Checksum() != checksum || manifestHash != s.Reference.ChecksumSHA256 {
		return nil, errors.New("suite checksum mismatch")
	}

	return &s, nil
}
func (r *Postgres) InsertPublishedSuite(ctx context.Context, s domain.PublishedSuite) error {
	if err := domain.ValidateSuite(s.Definition, true); err != nil {
		return err
	}
	raw, err := json.Marshal(s.Definition)
	if err != nil {
		return err
	}
	_, hash := s.Definition.Manifest(s.Reference.ID, s.Reference.Version)
	if hash != s.Reference.ChecksumSHA256 {
		return errors.New("suite manifest checksum mismatch")
	}
	_, err = r.query.Exec(ctx, `INSERT INTO task_version_suites(task_version_id,task_id,suite_id,suite_version,manifest_sha256,definition_sha256,definition) VALUES($1,$2,$3,$4,$5,$6,$7)`, s.TaskVersionID, s.TaskID, s.Reference.ID, s.Reference.Version, s.Reference.ChecksumSHA256, s.Definition.Checksum(), raw)

	return err
}
func (r *Postgres) GetSuiteDraft(ctx context.Context, id uuid.UUID, lock bool) (domain.SuiteDraft, error) {
	var d domain.SuiteDraft
	var raw []byte
	q := `SELECT id,task_id,base_version_id,revision,definition,published_version_id FROM test_suite_drafts WHERE id=$1`
	if lock {
		q += forUpdateClause
	}
	err := r.query.QueryRow(ctx, q, id).Scan(&d.ID, &d.TaskID, &d.BaseVersionID, &d.Revision, &raw, &d.PublishedVersionID)
	if err != nil {
		return d, notFound(err, apperror.TaskNotFound, "suite draft not found")
	}
	err = json.Unmarshal(raw, &d.Definition)

	return d, err
}
func (r *Postgres) InsertSuiteDraft(ctx context.Context, d domain.SuiteDraft) error {
	raw, err := json.Marshal(d.Definition)
	if err != nil {
		return err
	}
	_, err = r.query.Exec(ctx, `INSERT INTO test_suite_drafts(id,task_id,base_version_id,revision,definition) VALUES($1,$2,$3,$4,$5)`, d.ID, d.TaskID, d.BaseVersionID, d.Revision, raw)

	return err
}
func (r *Postgres) UpdateSuiteDraft(ctx context.Context, d domain.SuiteDraft, revision int) (bool, error) {
	raw, err := json.Marshal(d.Definition)
	if err != nil {
		return false, err
	}
	tag, err := r.query.Exec(ctx, `UPDATE test_suite_drafts SET definition=$3,revision=revision+1 WHERE id=$1 AND revision=$2 AND published_version_id IS NULL`, d.ID, revision, raw)

	return tag.RowsAffected() == 1, err
}
func (r *Postgres) DeleteSuiteDraft(ctx context.Context, id uuid.UUID, revision int) (bool, error) {
	tag, err := r.query.Exec(ctx, `DELETE FROM test_suite_drafts WHERE id=$1 AND revision=$2 AND published_version_id IS NULL`, id, revision)

	return tag.RowsAffected() == 1, err
}
func (r *Postgres) MarkSuitePublished(ctx context.Context, id uuid.UUID, revision int, versionID uuid.UUID) (bool, error) {
	tag, err := r.query.Exec(ctx, `UPDATE test_suite_drafts SET published_version_id=$3 WHERE id=$1 AND revision=$2 AND published_version_id IS NULL`, id, revision, versionID)

	return tag.RowsAffected() == 1, err
}
