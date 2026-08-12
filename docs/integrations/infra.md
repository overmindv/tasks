# Интеграция с infra

`infra` должен добавить отдельную PostgreSQL database `tasks_it`, deployment и internal service `tasks-it:8080`. База не должна разделяться таблицами с другими сервисами.

Перед запуском приложения init container или отдельный migration job выполняет:

```bash
goose -dir /app/migrations postgres "$DATABASE_URL" up
```

Deployment передаёт `SERVICE_NAME`, `HTTP_ADDR`, `DATABASE_URL`, `LOG_LEVEL`, `ENV`, `READ_TIMEOUT`, `WRITE_TIMEOUT`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_REQUESTS_TOPIC`, `KAFKA_RESULTS_TOPIC`, `KAFKA_RESULTS_CONSUMER_GROUP`, `KAFKA_OUTBOX_POLL_INTERVAL`, `CODE_EXECUTION_TIME_LIMIT` и `CODE_EXECUTION_MEMORY_LIMIT_BYTES`. Secret хранит только `DATABASE_URL` и ingestion token; Kafka broker и topics не являются секретами.

Kafka должна содержать topics `code-execution.requests.v1` и `code-execution.results.v1`. Внутри compose/Kubernetes `KAFKA_BOOTSTRAP_SERVERS` указывает на internal listener, например `kafka:9092`; локальный запуск с host использует advertised host listener, например `localhost:29092`. Request topic читается `sandbox`, result topic — consumer group `tasks-it-code-results-v1`.

Readiness probe использует `/ready` и требует доступности PostgreSQL и Kafka, liveness probe — `/health`. `tasks-it` доступен только `api-gateway`, не ingress.
