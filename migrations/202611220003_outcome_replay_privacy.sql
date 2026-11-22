-- +goose Up
CREATE TABLE task_privacy_tombstones (
 user_id uuid PRIMARY KEY, event_id uuid NOT NULL, version bigint NOT NULL,
 completed boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE task_privacy_sync(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), position bigint NOT NULL DEFAULT 0);
INSERT INTO task_privacy_sync(singleton) VALUES(true);
CREATE TABLE task_export_snapshots(id uuid PRIMARY KEY,user_id uuid NOT NULL,privacy_epoch bigint NOT NULL,expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '1 hour');
CREATE INDEX task_export_subject ON task_export_snapshots(user_id,id);
CREATE TABLE task_export_records(snapshot_id uuid NOT NULL REFERENCES task_export_snapshots(id) ON DELETE CASCADE,position bigint NOT NULL,record jsonb NOT NULL,PRIMARY KEY(snapshot_id,position));
CREATE TABLE outcome_journal_clock (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), position bigint NOT NULL DEFAULT 0,
 privacy_epoch bigint NOT NULL DEFAULT 1, legacy_missing bigint NOT NULL DEFAULT 0,
 complete_since timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO outcome_journal_clock(singleton,legacy_missing)
 SELECT true,count(*) FROM learning_outcome_outbox WHERE payload IS NULL;
CREATE TABLE outcome_journal (
 position bigint PRIMARY KEY, event_id uuid NOT NULL UNIQUE, aggregate_id uuid NOT NULL,
 learner_id uuid NOT NULL, correction_version integer NOT NULL, original_graded_at timestamptz NOT NULL,
 payload jsonb NOT NULL, UNIQUE(aggregate_id,correction_version)
);
CREATE INDEX outcome_journal_learner ON outcome_journal(learner_id,position);
CREATE INDEX outcome_journal_expiry ON outcome_journal(original_graded_at,position);
CREATE TABLE outcome_retired_aggregates(aggregate_id uuid PRIMARY KEY,retired_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE INDEX tasks_privacy_author ON tasks(created_by,id);
CREATE INDEX tasks_privacy_editor ON tasks(updated_by,id);
CREATE INDEX task_versions_privacy_author ON task_versions(created_by,id);
CREATE INDEX task_candidates_privacy_reviewer ON task_candidates(reviewed_by,id) WHERE reviewed_by IS NOT NULL;
CREATE INDEX execution_audit_privacy_actor ON code_submission_replay_audit(actor_id,id);
CREATE INDEX outcome_outbox_privacy_learner ON learning_outcome_outbox((payload->'payload'->>'learner_id'),id) WHERE payload IS NOT NULL;
INSERT INTO outcome_journal(position,event_id,aggregate_id,learner_id,correction_version,original_graded_at,payload)
 SELECT row_number() OVER(ORDER BY created_at,id),id,aggregate_id,(payload->'payload'->>'learner_id')::uuid,
 correction_version,(payload->'payload'->>'graded_at')::timestamptz,payload FROM learning_outcome_outbox WHERE payload IS NOT NULL;
UPDATE outcome_journal_clock SET position=(SELECT count(*) FROM outcome_journal);

-- +goose StatementBegin
CREATE FUNCTION tasks_guard_deleted_actor() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE actor uuid;
BEGIN
 actor:=(to_jsonb(NEW)->>TG_ARGV[0])::uuid;
 IF actor IS NULL THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('tasks-user:'||actor::text,0));
 IF EXISTS(SELECT 1 FROM task_privacy_tombstones WHERE user_id=actor) THEN
  RAISE EXCEPTION 'deleted actor' USING ERRCODE='42501';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER submissions_privacy BEFORE INSERT ON submissions FOR EACH ROW EXECUTE FUNCTION tasks_guard_deleted_actor('user_id');
CREATE TRIGGER code_submissions_privacy BEFORE INSERT ON code_submissions FOR EACH ROW EXECUTE FUNCTION tasks_guard_deleted_actor('user_id');
CREATE TRIGGER tasks_privacy BEFORE INSERT ON tasks FOR EACH ROW EXECUTE FUNCTION tasks_guard_deleted_actor('created_by');
CREATE TRIGGER task_versions_privacy BEFORE INSERT ON task_versions FOR EACH ROW EXECUTE FUNCTION tasks_guard_deleted_actor('created_by');
CREATE TRIGGER progress_privacy BEFORE INSERT ON practice_progress_visibility FOR EACH ROW EXECUTE FUNCTION tasks_guard_deleted_actor('user_id');
CREATE TRIGGER tasks_editor_privacy BEFORE INSERT OR UPDATE OF updated_by ON tasks FOR EACH ROW EXECUTE FUNCTION tasks_guard_deleted_actor('updated_by');
CREATE TRIGGER candidates_privacy BEFORE INSERT OR UPDATE OF reviewed_by ON task_candidates FOR EACH ROW EXECUTE FUNCTION tasks_guard_deleted_actor('reviewed_by');
CREATE TRIGGER audit_privacy BEFORE INSERT ON code_submission_replay_audit FOR EACH ROW EXECUTE FUNCTION tasks_guard_deleted_actor('actor_id');

-- +goose StatementBegin
CREATE FUNCTION tasks_retain_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE learner uuid; next_position bigint; graded timestamptz;
BEGIN
 learner:=(NEW.payload->'payload'->>'learner_id')::uuid;
 PERFORM pg_advisory_xact_lock(hashtextextended('tasks-user:'||learner::text,0));
 UPDATE outcome_journal_clock SET position=position+1 WHERE singleton RETURNING position INTO next_position;
 IF EXISTS(SELECT 1 FROM task_privacy_tombstones WHERE user_id=learner)
 OR EXISTS(SELECT 1 FROM outcome_retired_aggregates WHERE aggregate_id=NEW.aggregate_id) THEN
  RAISE EXCEPTION 'retired learning outcome' USING ERRCODE='42501';
 END IF;
 SELECT min(original_graded_at) INTO graded FROM outcome_journal WHERE aggregate_id=NEW.aggregate_id;
 graded:=COALESCE(graded,(NEW.payload->'payload'->>'graded_at')::timestamptz);
 INSERT INTO outcome_journal(position,event_id,aggregate_id,learner_id,correction_version,original_graded_at,payload)
 VALUES(next_position,NEW.id,NEW.aggregate_id,learner,NEW.correction_version,graded,NEW.payload);
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER retain_learning_outcome BEFORE INSERT ON learning_outcome_outbox FOR EACH ROW EXECUTE FUNCTION tasks_retain_outcome();

-- +goose Down
DROP INDEX outcome_outbox_privacy_learner;
DROP INDEX execution_audit_privacy_actor;
DROP INDEX task_candidates_privacy_reviewer;
DROP INDEX task_versions_privacy_author;
DROP INDEX tasks_privacy_author;
DROP INDEX tasks_privacy_editor;
DROP TRIGGER tasks_editor_privacy ON tasks;
DROP TRIGGER candidates_privacy ON task_candidates;
DROP TRIGGER audit_privacy ON code_submission_replay_audit;
DROP TRIGGER retain_learning_outcome ON learning_outcome_outbox;
DROP FUNCTION tasks_retain_outcome();
DROP TRIGGER progress_privacy ON practice_progress_visibility;
DROP TRIGGER task_versions_privacy ON task_versions;
DROP TRIGGER tasks_privacy ON tasks;
DROP TRIGGER code_submissions_privacy ON code_submissions;
DROP TRIGGER submissions_privacy ON submissions;
DROP FUNCTION tasks_guard_deleted_actor();
DROP TABLE outcome_retired_aggregates;
DROP TABLE outcome_journal;
DROP TABLE outcome_journal_clock;
DROP TABLE task_privacy_sync;
DROP TABLE task_export_records;
DROP TABLE task_export_snapshots;
DROP TABLE task_privacy_tombstones;
