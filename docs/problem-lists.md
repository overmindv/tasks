# Списки задач (problem lists)

`tasks` владеет списками задач, собранными из внешних `problem-list`-страниц
(LeetCode и аналогичных источников). Списки связывают внешний порядок с
опубликованными задачами платформы и не являются отдельным источником контента:
задача появляется в списке только после модерации кандидата.

## Модель

- `problem_lists` — уникальный список, ключ идемпотентности — `source_url`.
  `truncated` показывает, что источник отдал набор не полностью.
- `task_candidates.problem_list_id` и `problem_list_position` — атрибуция
  кандидата и место в порядке сбора.
- `problem_list_members` — состав списка (`problem_list_id`, `task_id`, `position`),
  первичный ключ `(problem_list_id, task_id)` исключает дубли.

## Импорт

`task-hunter` присылает batch в `POST /v1/internal/task-candidates/batch`.
Для каждого элемента допустимы поля `problem_list {name, source_id, source_url, truncated}`
и `problem_list_position`.

- Список создаётся или переиспользуется по `source_url`: повторный импорт не
  дублирует ни список, ни кандидатов (уникальность `source_id + external_id`,
  `source_hash`).
- Если `problem_list_position` не задан, позиция присваивается по порядку batch.
  Явная позиция источника сохраняется как есть.
- `truncated=true` помечает список как частично собранный; повторный полный сбор
  (`truncated=false`) снимает отметку.
- Отрицательная позиция отклоняется валидацией.

## Видимость

Одобрение кандидата создаёт черновик задачи и сразу добавляет его в состав списка
с сохранённой позицией. Порядок списка не зависит от порядка модерации.

- `GET /v1/problem-lists`, `GET /v1/problem-lists/{id}` — пользовательская выдача:
  только опубликованные и не удалённые задачи; `pending_candidates` скрыт.
- `GET /v1/admin/problem-lists`, `GET /v1/admin/problem-lists/{id}` — полная выдача:
  черновики и архив, число ожидающих кандидатов.

Публичного доступа к неопубликованным задачам нет: кандидат не становится
публичным до модерации.

## Проверки

- Unit: `internal/usecase/problem_lists_test.go`, `internal/transport/httpapi/problem_lists_test.go`.
- Component: `tests/component/problem_list_import_test.go` — повтор импорта,
  отметка частичности, порядок сбора и роли.
