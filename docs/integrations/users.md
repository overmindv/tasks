# Интеграция с users

`users` владеет пользователями, JWT и глобальными ролями. `tasks-it` не вызывает `users` и не читает его database.

`api-gateway` проверяет JWT и передаёт во внутренний запрос `X-User-ID` и `X-User-Roles`. `tasks-it` сохраняет user ID в submission как opaque UUID. Admin-доступ разрешён ролям `admin` и `superuser`.

Сервис нельзя публиковать напрямую: без защищённой internal boundary клиент мог бы подделать trusted headers.
