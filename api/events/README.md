# Контракты исполнения

Действующий wire-контракт Kafka — `sandbox.run.request.v1.json` и
`sandbox.run.result.v1.json`. Это проверяемый snapshot схем владельца sandbox
(`api/schemas`); изменения вносятся там и синхронизируются сюда.

`code-execution-requested.v1.schema.json` и `code-execution-completed.v1.schema.json`
— исторические проекты, не payload действующих topics. Они не публикуются текущим
producer и не определяют формат учебных outcome events (OVM-249).

Протокол, retry, compatibility и границы профиля tasks:
[execution-contract](../../docs/execution-contract.md).

Проверка локальной копии: установить `scripts/requirements-contract.txt` в venv,
запустить `python scripts/check_execution_contract.py` из корня сервиса.
Сверка двух checkout: `python scripts/check_execution_contract.py --peer ../sandbox`.
Fixtures синтетические; содержат только открытые примеры.

Учебные результаты используют отдельную схему [learning.outcome.v1](learning.outcome.v1.json).
Поля, настройки доступа и порядок доставки описаны в [учебном потоке](../../docs/learning-outcomes.md).
