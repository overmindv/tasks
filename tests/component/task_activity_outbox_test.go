//go:build component

package component

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/parker"
)

// Activity outbox задачи публикуется стандартным PgOutbox. Тест проверяет схему
// после owner-миграций 202611240001/0002: claim/lease, ack по token, retry
// до exhausted, replay с audit и отказ смешанного rollout со старым worker.
func TestTaskActivityOutboxLeasePostgres(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	outbox := parker.NewPgOutbox(pool)

	ackID := insertActivityEvent(t, ctx, pool, 0)
	defer cleanupActivityEvents(t, ctx, pool, ackID)

	claimed := claimActivityEvent(t, ctx, outbox, ackID)
	if claimed.ClaimToken == "" || claimed.LeaseUntil.IsZero() {
		t.Fatal("claim без token или lease")
	}
	// Старый worker без claim token не должен подтверждать закреплённую запись.
	if err := outbox.MarkSent(ctx, []string{ackID}); err == nil {
		t.Fatal("tokenless ack закреплённой записи принят")
	}
	if err := outbox.MarkClaimSent(ctx, parker.OutboxRecord{ID: ackID, ClaimToken: uuid.NewString()}); err == nil {
		t.Fatal("ack с чужим claim token принят")
	}
	if err := outbox.MarkClaimSent(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM outbox WHERE id=$1`, ackID).Scan(&status); err != nil || status != "sent" {
		t.Fatalf("после ack status=%q err=%v", status, err)
	}
}

func TestTaskActivityOutboxRetryAndReplayPostgres(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	outbox := parker.NewPgOutbox(pool)

	id := insertActivityEvent(t, ctx, pool, 7)
	defer cleanupActivityEvents(t, ctx, pool, id)

	claimed := claimActivityEvent(t, ctx, outbox, id)
	if err := outbox.MarkClaimFailed(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT status, attempts FROM outbox WHERE id=$1`, id).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "exhausted" || attempts != 8 {
		t.Fatalf("после восьмой ошибки status=%q attempts=%d", status, attempts)
	}
	actor := uuid.NewString()
	if err := outbox.ReplayExhausted(ctx, id, 0, actor, false); err == nil {
		t.Fatal("replay без администратора принят")
	}
	if err := outbox.ReplayExhausted(ctx, id, 1, actor, true); err == nil {
		t.Fatal("replay с чужим generation принят")
	}
	if err := outbox.ReplayExhausted(ctx, id, 0, actor, true); err != nil {
		t.Fatal(err)
	}
	var generation, audited int
	if err := pool.QueryRow(ctx, `SELECT replay_generation FROM outbox WHERE id=$1`, id).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_replay_audit WHERE outbox_id=$1 AND generation=1 AND actor_id=$2`, id, actor).Scan(&audited); err != nil {
		t.Fatal(err)
	}
	if generation != 1 || audited != 1 {
		t.Fatalf("generation=%d audit=%d", generation, audited)
	}
}

// Истёкшая lease возвращается другому worker, а старый token больше не действует.
func TestTaskActivityOutboxExpiredLeasePostgres(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("COMPONENT_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	outbox := parker.NewPgOutbox(pool)

	id := insertActivityEvent(t, ctx, pool, 0)
	defer cleanupActivityEvents(t, ctx, pool, id)

	stale := claimActivityEvent(t, ctx, outbox, id)
	if _, err := pool.Exec(ctx, `UPDATE outbox SET lease_until=now()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	next := claimActivityEvent(t, ctx, outbox, id)
	if next.ClaimToken == stale.ClaimToken {
		t.Fatal("истёкшая lease не перешла другому worker")
	}
	if err := outbox.MarkClaimSent(ctx, stale); err == nil {
		t.Fatal("stale claim подтвердил новую lease")
	}
	if err := outbox.MarkClaimSent(ctx, next); err != nil {
		t.Fatal(err)
	}
}

func insertActivityEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempts int) string {
	t.Helper()
	id := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO outbox (id, topic, message_key, message_value, attempts) VALUES ($1,'task.activity.v1',$1,$2,$3)`, id, []byte(`{"event":"task.published"}`), attempts); err != nil {
		t.Fatal(err)
	}

	return id
}

func cleanupActivityEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `DELETE FROM outbox_replay_audit WHERE outbox_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM outbox WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}

func claimActivityEvent(t *testing.T, ctx context.Context, outbox *parker.PgOutbox, id string) parker.OutboxRecord {
	t.Helper()
	for range 10 {
		records, err := outbox.FetchPending(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			if record.ID == id {
				return record
			}
		}
	}
	t.Fatalf("запись %s не закреплена", id)

	return parker.OutboxRecord{}
}
