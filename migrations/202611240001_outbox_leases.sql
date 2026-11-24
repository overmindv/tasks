-- +goose Up
-- Durable lease для activity-outbox (стандартная parker-таблица outbox).
-- Выполнить до переключения worker на Parker v0.3.0; старые worker'ы продолжают
-- работать на этих колонках, потому что они nullable.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS claim_token UUID;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS outbox_lease_idx ON outbox(status, lease_until);

-- +goose Down
-- Столбцы остаются: перед запуском старых worker'ов остановить новые и вернуть
-- publishing в pending с очисткой claim_token/lease_until (см. release v0.2.0).
SELECT 1;
