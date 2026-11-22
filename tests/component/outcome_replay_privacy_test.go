//go:build component

package component

import (
	"testing"
	"time"

	"github.com/google/uuid"
	postgresadapter "github.com/overmindv/tasks/internal/adapter/postgres"
	"github.com/overmindv/tasks/internal/domain"
	"github.com/overmindv/tasks/internal/repository"
)

func TestOutcomeReplaySurvivesDeliveryAndDeletion(t *testing.T) {
	ctx := t.Context()
	pool := openComponentDB(t)
	store := postgresadapter.New(pool)
	start, err := store.OutcomeManifest(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	learner, submission := uuid.New(), uuid.New()
	now := time.Now().UTC()
	event := domain.LearningOutcome{EventID: uuid.New(), EventType: domain.LearningOutcomeTopic, SchemaVersion: 1, AggregateID: submission, AggregateVersion: 1, OccurredAt: now, Payload: domain.LearningOutcomePayload{SubmissionID: submission, LearnerID: learner, TaskID: uuid.New(), TaskVersionID: uuid.New(), TopicIDs: []uuid.UUID{uuid.New()}, Verdict: domain.ExecutionVerdictAccepted, GradedAt: now, CorrectionVersion: 1}}
	insert := func(e domain.LearningOutcome) error {
		return store.WithinTransaction(ctx, func(tx repository.Repository) error { return tx.InsertLearningOutcome(ctx, e) })
	}
	if err = insert(event); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE learning_outcome_outbox SET payload=NULL,published_at=now() WHERE id=$1`, event.EventID); err != nil {
		t.Fatal(err)
	}
	next := event
	next.EventID = uuid.New()
	next.AggregateVersion = 2
	next.Payload.CorrectionVersion = 2
	next.Payload.Verdict = domain.ExecutionVerdictWrongAnswer
	if err = insert(next); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.OutcomeManifest(ctx, 0, 0)
	if err != nil || manifest.Count != start.Count+2 {
		t.Fatalf("manifest %+v %v", manifest, err)
	}
	page, err := store.OutcomeReplay(ctx, manifest.Watermark, manifest.PrivacyEpoch, start.Watermark, 1)
	if err != nil || len(page.Events) != 1 || !page.More || page.Events[0].Event.EventID != event.EventID {
		t.Fatalf("page %+v %v", page, err)
	}
	page, err = store.OutcomeReplay(ctx, manifest.Watermark, manifest.PrivacyEpoch, page.Through, 1)
	if err != nil || len(page.Events) != 1 || page.More || page.Events[0].Event.EventID != next.EventID {
		t.Fatalf("continuation %+v %v", page, err)
	}
	deletion := domain.AccountDeletion{EventID: uuid.New(), UserID: learner, Version: 1}
	for range 10 {
		done, err := store.ApplyAccountDeletion(ctx, deletion, 100)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if _, err = store.OutcomeReplay(ctx, manifest.Watermark, manifest.PrivacyEpoch, 0, 100); err == nil {
		t.Fatal("stale privacy epoch accepted")
	}
	next.EventID = uuid.New()
	next.AggregateVersion = 3
	next.Payload.CorrectionVersion = 3
	if err = insert(next); err == nil {
		t.Fatal("deleted learner resurrected")
	}
	done, err := store.ApplyAccountDeletion(ctx, deletion, 100)
	if err != nil || !done {
		t.Fatal("duplicate deletion", err)
	}
}

func TestExpiredOutcomeFencesConcurrentCorrection(t *testing.T) {
	pool := openComponentDB(t)
	ctx := t.Context()
	store := postgresadapter.New(pool)
	aggregate, learner := uuid.New(), uuid.New()
	now := time.Now().UTC()
	event := domain.LearningOutcome{EventID: uuid.New(), EventType: domain.LearningOutcomeTopic, SchemaVersion: 1, AggregateID: aggregate, AggregateVersion: 1, OccurredAt: now, Payload: domain.LearningOutcomePayload{SubmissionID: aggregate, LearnerID: learner, TaskID: uuid.New(), TaskVersionID: uuid.New(), TopicIDs: []uuid.UUID{}, Verdict: domain.ExecutionVerdictAccepted, GradedAt: now, CorrectionVersion: 1}}
	expiry, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = expiry.Rollback(ctx) }()
	if _, err = expiry.Exec(ctx, `SELECT position FROM outcome_journal_clock WHERE singleton FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	pidCh := make(chan uint32, 1)
	done := make(chan error, 1)
	go func() {
		done <- store.WithinTransaction(ctx, func(tx repository.Repository) error {
			// Активный backend определяется по уникальному advisory lock ниже в pg_stat_activity.
			pidCh <- 1
			return tx.InsertLearningOutcome(ctx, event)
		})
	}()
	<-pidCh
	blocked := false
	for range 100 {
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%INSERT INTO learning_outcome_outbox%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("writer did not reach clock fence")
	}
	if _, err = expiry.Exec(ctx, `INSERT INTO outcome_retired_aggregates(aggregate_id) VALUES($1)`, aggregate); err != nil {
		t.Fatal(err)
	}
	if err = expiry.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("expired correction resurrected after waiting for clock")
	}
}
