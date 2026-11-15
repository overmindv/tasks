-- +goose Up
CREATE TABLE test_suite_drafts (
 id uuid PRIMARY KEY,
 task_id uuid NOT NULL REFERENCES tasks(id),
 base_version_id uuid NOT NULL,
 revision integer NOT NULL CHECK (revision > 0),
 definition jsonb NOT NULL CHECK (jsonb_typeof(definition)='object'),
 published_version_id uuid,
 FOREIGN KEY (task_id,base_version_id) REFERENCES task_versions(task_id,id),
 FOREIGN KEY (task_id,published_version_id) REFERENCES task_versions(task_id,id)
);
CREATE TABLE task_version_suites (
 task_version_id uuid PRIMARY KEY,
 task_id uuid NOT NULL,
 suite_id uuid NOT NULL,
 suite_version text NOT NULL,
 manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
 definition_sha256 text NOT NULL CHECK (definition_sha256 ~ '^[a-f0-9]{64}$'),
 definition jsonb NOT NULL CHECK (jsonb_typeof(definition)='object'),
 FOREIGN KEY (task_id,task_version_id) REFERENCES task_versions(task_id,id),
 UNIQUE(suite_id,suite_version)
);
-- +goose StatementBegin
CREATE FUNCTION prevent_suite_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'published suites are immutable';
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER immutable_suite BEFORE UPDATE ON task_version_suites FOR EACH ROW EXECUTE FUNCTION prevent_suite_update();
-- +goose Down
DROP TABLE task_version_suites;
DROP FUNCTION prevent_suite_update();
DROP TABLE test_suite_drafts;
