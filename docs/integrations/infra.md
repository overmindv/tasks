# Интеграция с infra

`infra` должен добавить отдельную PostgreSQL database `tasks_it`, deployment и internal service `tasks-it:8080`. База не должна разделяться таблицами с другими сервисами.

Перед запуском приложения init container или отдельный migration job выполняет:

```bash
goose -dir /app/migrations postgres "$DATABASE_URL" up
```

Deployment передаёт `SERVICE_NAME`, `HTTP_ADDR`, `DATABASE_URL`, `LOG_LEVEL`, `ENV`, `READ_TIMEOUT`, `WRITE_TIMEOUT`. Secret хранит только `DATABASE_URL`; значения не фиксируются в manifest.

Readiness probe использует `/ready`, liveness probe — `/health`. `tasks-it` доступен только `api-gateway`, не ingress.
