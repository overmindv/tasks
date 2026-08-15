# Интеграция с api-gateway

В следующем изменении `api-gateway` должен получить `TASKS_URL`, отдельный HTTP-клиент и GraphQL adapters без бизнес-логики.

Нужно добавить GraphQL types `ITTask`, `ITTaskOption`, `ITSubmission`, enums типов, сложности, status и verdict. Queries должны покрыть опубликованный список, деталь теста, административный список, административную деталь, результат и историю текущего пользователя. Mutations должны покрыть create, update, status change, delete и submit.

JWT context преобразуется в `X-User-ID` и `X-User-Roles` для каждого защищённого вызова. `X-Request-ID` также передаётся дальше. Ответы и error codes `tasks` маппятся в GraphQL errors без раскрытия upstream body или внутренних адресов.

Для формы создания теста `api-gateway` отдельно запрашивает темы у `entities`. `tasks` не проксирует этот список.
