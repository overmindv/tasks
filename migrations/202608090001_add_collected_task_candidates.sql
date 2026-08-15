-- +goose Up
ALTER TABLE task_versions
    DROP CONSTRAINT task_versions_type_check,
    ADD CONSTRAINT task_versions_type_check
        CHECK (task_type IN ('single_choice', 'multiple_choice', 'programming'));

CREATE TABLE task_version_tags (
    task_version_id UUID NOT NULL REFERENCES task_versions(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    position INTEGER NOT NULL,
    CONSTRAINT task_version_tags_position_check CHECK (position >= 0),
    CONSTRAINT task_version_tags_length_check CHECK (char_length(tag) BETWEEN 1 AND 100),
    CONSTRAINT task_version_tags_version_position_key UNIQUE (task_version_id, position),
    CONSTRAINT task_version_tags_version_tag_key UNIQUE (task_version_id, tag)
);

CREATE TABLE task_version_examples (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_version_id UUID NOT NULL REFERENCES task_versions(id) ON DELETE CASCADE,
    input TEXT NOT NULL DEFAULT '',
    output TEXT NOT NULL DEFAULT '',
    explanation TEXT NOT NULL DEFAULT '',
    position INTEGER NOT NULL,
    CONSTRAINT task_version_examples_position_check CHECK (position >= 0),
    CONSTRAINT task_version_examples_content_check CHECK (input <> '' OR output <> ''),
    CONSTRAINT task_version_examples_version_position_key UNIQUE (task_version_id, position)
);

CREATE TABLE task_version_constraints (
    task_version_id UUID NOT NULL REFERENCES task_versions(id) ON DELETE CASCADE,
    value TEXT NOT NULL,
    position INTEGER NOT NULL,
    CONSTRAINT task_version_constraints_position_check CHECK (position >= 0),
    CONSTRAINT task_version_constraints_value_check CHECK (char_length(value) > 0),
    CONSTRAINT task_version_constraints_version_position_key UNIQUE (task_version_id, position)
);

CREATE TABLE task_version_sources (
    task_version_id UUID PRIMARY KEY REFERENCES task_versions(id) ON DELETE CASCADE,
    source_id TEXT NOT NULL,
    source_name TEXT NOT NULL DEFAULT '',
    source_url TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT task_version_sources_url_check CHECK (char_length(source_url) > 0)
);

CREATE TABLE task_candidates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status TEXT NOT NULL DEFAULT 'pending',
    revision INTEGER NOT NULL DEFAULT 1,
    external_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_name TEXT NOT NULL DEFAULT '',
    source_url TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    source_published_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ NOT NULL,
    collection_job_id UUID NOT NULL,
    topic_id UUID,
    title TEXT NOT NULL,
    statement TEXT NOT NULL,
    difficulty TEXT NOT NULL,
    approved_task_id UUID REFERENCES tasks(id),
    reviewed_by UUID,
    reviewed_at TIMESTAMPTZ,
    rejection_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT task_candidates_status_check CHECK (status IN ('pending', 'approved', 'rejected')),
    CONSTRAINT task_candidates_revision_check CHECK (revision > 0),
    CONSTRAINT task_candidates_title_check CHECK (char_length(title) BETWEEN 1 AND 200),
    CONSTRAINT task_candidates_statement_check CHECK (char_length(statement) BETWEEN 1 AND 50000),
    CONSTRAINT task_candidates_difficulty_check CHECK (difficulty IN ('easy', 'medium', 'hard')),
    CONSTRAINT task_candidates_source_external_key UNIQUE (source_id, external_id),
    CONSTRAINT task_candidates_source_hash_key UNIQUE (source_hash)
);

CREATE TABLE task_candidate_tags (
    candidate_id UUID NOT NULL REFERENCES task_candidates(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    position INTEGER NOT NULL,
    CONSTRAINT task_candidate_tags_position_check CHECK (position >= 0),
    CONSTRAINT task_candidate_tags_length_check CHECK (char_length(tag) BETWEEN 1 AND 100),
    CONSTRAINT task_candidate_tags_candidate_position_key UNIQUE (candidate_id, position),
    CONSTRAINT task_candidate_tags_candidate_tag_key UNIQUE (candidate_id, tag)
);

CREATE TABLE task_candidate_examples (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_id UUID NOT NULL REFERENCES task_candidates(id) ON DELETE CASCADE,
    input TEXT NOT NULL DEFAULT '',
    output TEXT NOT NULL DEFAULT '',
    explanation TEXT NOT NULL DEFAULT '',
    position INTEGER NOT NULL,
    CONSTRAINT task_candidate_examples_position_check CHECK (position >= 0),
    CONSTRAINT task_candidate_examples_content_check CHECK (input <> '' OR output <> ''),
    CONSTRAINT task_candidate_examples_candidate_position_key UNIQUE (candidate_id, position)
);

CREATE TABLE task_candidate_constraints (
    candidate_id UUID NOT NULL REFERENCES task_candidates(id) ON DELETE CASCADE,
    value TEXT NOT NULL,
    position INTEGER NOT NULL,
    CONSTRAINT task_candidate_constraints_position_check CHECK (position >= 0),
    CONSTRAINT task_candidate_constraints_value_check CHECK (char_length(value) > 0),
    CONSTRAINT task_candidate_constraints_candidate_position_key UNIQUE (candidate_id, position)
);

CREATE INDEX task_candidates_queue_idx
    ON task_candidates (status, created_at DESC, id DESC);
CREATE INDEX task_candidates_source_idx
    ON task_candidates (source_id, created_at DESC, id DESC);
CREATE INDEX task_candidates_job_idx
    ON task_candidates (collection_job_id);

-- +goose Down
DROP TABLE IF EXISTS task_candidate_constraints;
DROP TABLE IF EXISTS task_candidate_examples;
DROP TABLE IF EXISTS task_candidate_tags;
DROP TABLE IF EXISTS task_candidates;
DROP TABLE IF EXISTS task_version_sources;
DROP TABLE IF EXISTS task_version_constraints;
DROP TABLE IF EXISTS task_version_examples;
DROP TABLE IF EXISTS task_version_tags;

ALTER TABLE task_versions
    DROP CONSTRAINT task_versions_type_check,
    ADD CONSTRAINT task_versions_type_check
        CHECK (task_type IN ('single_choice', 'multiple_choice'));
