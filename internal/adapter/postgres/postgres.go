package postgresadapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	jet "github.com/go-jet/jet/v2/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/tasks-it/internal/apperror"
	"github.com/overmindv/tasks-it/internal/repository"
)

type querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Postgres реализует repository contract через Jet и pgx.
type Postgres struct {
	pool  *pgxpool.Pool
	query querier
}

// New создаёт PostgreSQL repository с готовым пулом соединений.
func New(pool *pgxpool.Pool) *Postgres {
	return &Postgres{
		pool:  pool,
		query: pool,
	}
}

// Ping проверяет доступность PostgreSQL.
func (r *Postgres) Ping(ctx context.Context) error {
	statement := jet.SELECT(jet.RawInt("1"))
	query, args := statementSQL(statement)
	if err := r.query.QueryRow(ctx, query, args...).Scan(new(int)); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	return nil
}

// WithinTransaction выполняет callback в короткой PostgreSQL-транзакции.
func (r *Postgres) WithinTransaction(ctx context.Context, fn func(repository.Repository) error) error {
	if r.pool == nil {
		return fmt.Errorf("nested transactions are not supported")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	txRepository := &Postgres{
		query: tx,
	}
	if err := fn(txRepository); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			return fmt.Errorf("rollback transaction after %v: %w", err, rollbackErr)
		}

		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// notFound преобразует отсутствие строки в безопасную доменную ошибку.
func notFound(err error, code, message string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return apperror.New(code, message, http.StatusNotFound)
	}

	return err
}

// statementSQL сериализует Jet statement в параметризованный SQL.
func statementSQL(statement interface {
	Sql() (string, []interface{})
}) (string, []any) {
	query, args := statement.Sql()

	return query, args
}

// uuidExpressions преобразует UUID в Jet-выражения для IN.
func uuidExpressions(ids []uuid.UUID) []jet.Expression {
	result := make([]jet.Expression, 0, len(ids))
	for _, id := range ids {
		result = append(result, jet.UUID(id))
	}

	return result
}
