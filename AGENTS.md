# AGENTS.md

## Repository identity

The repository name is `tasks`.

## Service purpose

`tasks` owns programming and other IT-related tasks in the Overmindv
platform.

Examples include:

* algorithms and data structures;
* backend development;
* databases and SQL;
* infrastructure and DevOps;
* computer networks;
* operating systems;
* testing and quality assurance;
* security;
* programming-language exercises;
* code-reading and debugging tasks;
* architecture and system-design exercises;
* multiple-choice and short-answer IT tests.

The service is responsible for the lifecycle of IT tasks and, where supported,
the lifecycle of attempts and submissions for those tasks.

A task may contain:

* title;
* statement;
* task type;
* difficulty;
* constraints;
* input and output specification;
* examples;
* hints;
* tags;
* supported programming languages;
* time and memory limits;
* visible tests;
* hidden tests;
* checker configuration;
* solution templates;
* reference solutions;
* publication state;
* version;
* links to canonical topics.

## Platform boundaries

`tasks` is the source of truth for IT task definitions and task-specific
validation configuration.

It does not own:

* users, credentials, sessions or global roles;
* universities, programs, courses or canonical topics;
* general educational articles and theory materials;
* binary media storage;
* frontend presentation;
* global infrastructure configuration.

Related Overmindv services:

* `users`: users, authentication, identities and global roles;
* `entities`: universities, programs, courses and canonical topics;
* `content`: educational materials and articles;
* `media`: files, images and other media;
* `api-gateway`: GraphQL API gateway;
* `frontend`: web frontend;
* `infra`: Docker, Kubernetes and shared infrastructure.

Store external entity identifiers as opaque IDs.

Do not create cross-service database foreign keys.

Do not copy complete user, course, topic or media records into the service
database. Store only identifiers and task-owned snapshot fields when a
documented consistency requirement requires them.

## Ownership model

`tasks` owns:

* task definitions;
* task versions;
* task publication state;
* task statements and constraints;
* language-specific starter code;
* task examples;
* visible and hidden tests;
* checker configuration;
* task-specific limits;
* task-to-topic references;
* attempts and submissions, if they are part of this repository;
* task-specific evaluation results received from the execution subsystem.

An execution runner or judge owns:

* isolated process execution;
* container lifecycle;
* compilation and runtime sandboxing;
* operating-system resource enforcement;
* untrusted process termination.

If runner functionality currently resides in this repository, keep it behind an
explicit interface so that it can be extracted into a separate service later.

## Required reading

Before changing service boundaries, read:

* `README.md`;
* `docs/architecture.md`, if present;
* relevant files under `docs/decisions/`, if present;
* API schemas related to the requested change;
* database migrations related to the affected entities.

Do not infer current architecture only from directory names. Verify it against
the implementation and tests.

## Repository navigation

Use the existing repository structure as the source of truth.

The preferred package responsibilities are:

* `cmd/`: process entrypoints and dependency composition;
* `internal/domain/`: entities, value objects, invariants and domain errors;
* `internal/usecase/`: application workflows and transaction orchestration;
* `internal/repository/`: persistence interfaces;
* `internal/adapter/postgres/`: PostgreSQL implementations;
* `internal/transport/`: HTTP, gRPC, GraphQL-facing or message adapters;
* `internal/checker/`: answer and checker abstractions;
* `internal/execution/`: interfaces to runner or judge infrastructure;
* `internal/config/`: configuration loading and validation;
* `internal/observability/`: logs, metrics and tracing;
* `api/`: protobuf, GraphQL, OpenAPI or event schemas;
* `migrations/`: append-only database migrations;
* `tests/`: integration, contract and end-to-end tests.

Do not create these directories merely to match this document. Preserve the
existing structure unless the current task requires an architectural change.

Do not put domain decisions in:

* `cmd`;
* transport handlers;
* generated GraphQL resolvers;
* PostgreSQL adapters;
* message-consumer plumbing.

## Code-review-graph workflow

Use `code-review-graph` for non-trivial investigation, implementation and
review tasks.

Before changing multiple files:

1. Check graph health:

   `code-review-graph status`

2. If the graph is not maintained by hooks or daemon, update it:

   `code-review-graph update --brief`

3. Query the graph through MCP to determine:

   * relevant symbols;
   * callers and callees;
   * direct and transitive dependencies;
   * affected tests;
   * architectural communities;
   * the blast radius of the planned change.

4. Read the actual source files and tests identified by the graph.

5. Verify graph conclusions using Go compiler information, repository search,
   API schemas and database constraints.

After implementation:

1. Refresh the graph when necessary:

   `code-review-graph update --brief`

2. Inspect affected symbols and tests again.

3. Run the required compiler, test and lint checks.

The graph is an indexing and impact-analysis tool, not a source of truth.

Go compiler results, tests, schemas, migrations and explicit domain rules take
priority over inferred graph edges.

Do not force graph analysis for a trivial isolated change when reading the
changed file directly is cheaper and clearer.

## Domain model rules

Model important concepts explicitly.

Typical concepts include:

* `Task`;
* `TaskVersion`;
* `TaskType`;
* `Difficulty`;
* `PublicationStatus`;
* `ProgrammingLanguage`;
* `TestCase`;
* `Checker`;
* `ResourceLimits`;
* `Attempt`;
* `Submission`;
* `Verdict`.

Use names that match the actual repository model. Do not introduce duplicate
types merely because they are listed here.

Critical task invariants must be enforced in the domain or use-case layer, not
only in transport validation.

Examples of invariants:

* a published task must have a valid statement;
* a programming task must define supported languages;
* execution limits must be positive and bounded;
* hidden tests must never be exposed through public APIs;
* a task cannot reference the same external topic more than once;
* immutable historical task versions must not change silently;
* a submission must reference the exact task version used for evaluation;
* a checker type must be compatible with its configuration;
* an archived or deleted task cannot accept new attempts unless explicitly
  supported by the product requirements.

Use explicit domain errors for expected business failures.

Do not represent business failures only as arbitrary strings.

## Task types

Do not assume that every task is a code-execution task.

The model should be able to distinguish task types such as:

* programming;
* SQL;
* multiple choice;
* multiple select;
* short answer;
* code review;
* debugging;
* ordering or matching;
* system design;
* infrastructure configuration.

Task-type-specific data should have explicit validation.

Avoid a single unvalidated JSON field that accumulates unrelated schemas.

When flexible payloads are required, define:

* a versioned schema;
* strict decoding;
* validation;
* backward-compatibility rules;
* migration behavior.

## Task versioning

Published task content should be treated as versioned educational content.

A change that can affect whether an existing submission is correct should
normally create a new task version.

This includes changes to:

* statement semantics;
* constraints;
* tests;
* checker behavior;
* language versions;
* time limits;
* memory limits;
* starter code;
* reference answers.

Existing submissions must remain associated with the version against which they
were evaluated.

Do not overwrite historical evaluation context silently.

Purely cosmetic changes may update the current representation only when they
cannot affect interpretation or evaluation.

## Task publication lifecycle

Use an explicit lifecycle rather than independent boolean flags.

A typical lifecycle is:

`draft -> review -> published -> archived`

Use the lifecycle that already exists in the repository.

Publication must validate all mandatory task-type-specific requirements.

Only authorized actors may:

* create tasks;
* change protected task content;
* manage hidden tests;
* publish tasks;
* archive tasks;
* restore archived tasks.

Do not trust role information supplied directly by the client.

Authorization context must come from the trusted identity or gateway boundary.

## Tests and checker data

Visible examples and hidden evaluation tests are different security classes.

Never expose through public APIs:

* hidden test input;
* hidden expected output;
* reference solutions;
* checker secrets;
* infrastructure credentials;
* internal anti-cheating metadata.

Avoid writing hidden tests into ordinary application logs.

Checker implementations must be deterministic for identical inputs unless a
non-deterministic checker is an explicit, documented requirement.

Supported checker strategies may include:

* exact text comparison;
* whitespace-normalized comparison;
* token comparison;
* numeric comparison with tolerance;
* unordered collection comparison;
* custom checker execution.

Custom checkers are untrusted executable code and must use the same sandbox
principles as user submissions.

Validate numeric tolerances and comparison settings.

Do not silently accept malformed checker configuration.

## Submission and execution flow

A typical programming submission flow is:

1. validate actor and task availability;
2. resolve the exact task version;
3. validate the selected language;
4. create an immutable submission record;
5. send an execution request with an idempotency key;
6. receive execution progress or a final result;
7. validate event ordering and ownership;
8. persist the final verdict transactionally;
9. expose a sanitized result to the client.

Do not execute untrusted source code in the API process.

Do not run user code through `os/exec` without a hardened sandbox boundary.

Do not mount:

* the Docker socket;
* host credentials;
* source repositories;
* unrestricted host directories;
* cloud metadata endpoints;
* service databases.

Execution requests and result events must carry stable identifiers:

* submission ID;
* task version ID;
* attempt number when applicable;
* execution ID;
* idempotency key;
* trace or correlation ID.

Duplicate result delivery must not create duplicate attempts or conflicting
final states.

## Verdict model

Use an explicit finite set of verdicts.

Typical verdicts include:

* `pending`;
* `queued`;
* `running`;
* `accepted`;
* `wrong_answer`;
* `compilation_error`;
* `runtime_error`;
* `time_limit_exceeded`;
* `memory_limit_exceeded`;
* `output_limit_exceeded`;
* `presentation_error`;
* `checker_error`;
* `infrastructure_error`;
* `cancelled`.

Keep infrastructure failures distinct from wrong user answers.

Do not mark a submission as wrong merely because the runner or checker failed.

Final verdict transitions must be idempotent and validated.

## Security for untrusted code

Treat all submitted source code, archives, SQL and custom checker data as
untrusted.

Execution isolation must enforce:

* CPU limits;
* memory limits;
* process limits;
* wall-clock timeout;
* output-size limits;
* filesystem quotas;
* read-only base filesystem where possible;
* a dedicated temporary working directory;
* blocked outbound network by default;
* restricted system calls;
* non-root execution;
* cleanup after termination.

Do not interpolate user-controlled values into shell commands.

Pass arguments as an explicit argument array.

Validate language identifiers using an allowlist.

Pin compiler, runtime and container-image versions.

Do not accept arbitrary container images from clients.

Do not return raw internal runner errors to public clients.

## SQL task safety

SQL tasks require a disposable and isolated database environment.

Never execute a learner's SQL against:

* production databases;
* shared service databases;
* migration databases;
* databases containing other users' data.

Apply:

* statement timeout;
* transaction timeout;
* row and result-size limits;
* isolated schemas or disposable databases;
* restricted database roles;
* explicit extension and command allowlists.

Prevent access to filesystem, network and privileged database functions.

Reset the environment between submissions.

## API and contract rules

Transport DTOs are not domain entities.

Do not expose PostgreSQL models directly through GraphQL, gRPC or HTTP APIs.

Before changing a contract:

1. locate its producers and consumers;
2. inspect the impact through code-review-graph and repository search;
3. check compatibility with `api-gateway`;
4. check compatibility with `frontend` when relevant;
5. update generated code;
6. update contract tests;
7. document coordinated deployment requirements.

Preserve backward compatibility unless the current task explicitly permits a
breaking change.

Use pagination for list endpoints.

Pagination must have deterministic ordering and a stable tie-breaker.

Never return hidden tests or internal checker configuration through generic
task-detail endpoints.

## Events and asynchronous processing

Use durable messaging for operations that may outlive an HTTP or GraphQL
request.

Event schemas must include:

* event ID;
* event type;
* schema version;
* aggregate or entity ID;
* occurrence timestamp;
* correlation ID when available.

Consumers must be idempotent.

Do not assume exactly-once delivery from the message broker.

Use inbox or equivalent deduplication for side-effecting consumers.

Use transactional outbox or an equivalent reliable-publishing mechanism when a
database state change and event publication must be atomic.

Do not publish events before the owning transaction commits.

## Go conventions

Use the Go version declared in `go.mod`.

Format changed Go files with:

`gofmt`

Prefer:

* small cohesive packages;
* explicit dependencies;
* explicit constructors;
* concrete internal implementations;
* interfaces at real architectural boundaries;
* table-driven tests;
* error wrapping with `%w`;
* `errors.Is` and `errors.As`;
* standard-library types unless a dependency provides clear value.

Do not:

* create interfaces only for hypothetical future implementations;
* store `context.Context` in structs;
* pass a nil context;
* use mutable global service state;
* use `init` for application wiring;
* use `panic` for expected runtime failures;
* log and return the same error at every layer;
* introduce a dependency without explaining its necessity;
* use reflection when ordinary typed code is sufficient.

Pass `context.Context` as the first argument to I/O-bound operations.

Keep public APIs minimal.

Use domain-specific types when they prevent invalid combinations of primitive
values.

## Error handling

Separate:

* domain errors;
* validation errors;
* authorization errors;
* persistence errors;
* external dependency errors;
* execution infrastructure errors.

Add operation context when wrapping an unexpected error.

Preserve the original error using `%w`.

Map internal errors to transport errors in the transport boundary.

Do not expose:

* SQL details;
* filesystem paths;
* container internals;
* stack traces;
* internal hostnames;
* secrets.

Retries must be limited to transient and idempotent operations.

Use bounded retry count, timeout and backoff.

## Database rules

Use PostgreSQL-compatible SQL.

Always use parameterized queries.

Do not concatenate user input into SQL.

Keep transactions short.

Transaction boundaries belong in use-case orchestration when several repository
operations must be atomic.

Repository methods must not start hidden nested transactions unless the
repository architecture explicitly supports them.

Use deterministic ordering for pagination.

Avoid N+1 queries.

Use database constraints for critical integrity rules where practical.

Indexes must correspond to concrete access patterns.

For performance-sensitive changes, inspect the query plan against realistic
data.

## Migration rules

Migrations are append-only.

Never modify a migration that may have already been applied outside the local
environment.

Use expand-and-contract migrations for backward-incompatible schema changes:

1. add the new schema;
2. deploy compatible code;
3. backfill;
4. switch reads and writes;
5. remove old schema in a later release.

A destructive migration must account for:

* rollback behavior;
* lock duration;
* table size;
* running old application instances;
* queued events and delayed consumers;
* historical submissions and task versions.

Do not delete task-version or submission history merely because the current
task version was archived.

## Concurrency and idempotency

Assume that:

* requests may be retried;
* messages may be delivered more than once;
* multiple workers may process related submissions;
* execution results may arrive out of order;
* clients may submit duplicate commands.

Use:

* idempotency keys;
* unique constraints;
* guarded state transitions;
* compare-and-set updates;
* row locking only when justified;
* inbox deduplication for messages.

Avoid holding a database transaction while calling external services.

## Testing strategy

Every behavior change requires tests at the lowest meaningful level.

Use:

* unit tests for domain invariants;
* table-driven tests for validators and checkers;
* use-case tests for orchestration and state transitions;
* PostgreSQL integration tests for repositories and constraints;
* contract tests for APIs and events;
* end-to-end tests for critical submission flows;
* security tests for hidden-test leakage;
* concurrency and duplicate-delivery tests for asynchronous workflows.

A fixed bug requires a regression test that fails before the fix.

Tests must be:

* deterministic;
* independent;
* parallel-safe where marked parallel;
* explicit about clocks and randomness;
* free from dependency on execution order.

Use fake clocks or injected time sources for time-dependent domain behavior.

Use deterministic IDs or controlled generators in tests where useful.

Do not weaken assertions or delete tests merely to obtain a passing build.

## Required checks

For a focused change, first run tests for the affected packages:

`go test ./path/to/affected/package/...`

Before completing a normal code change, run:

`go test ./...`

Also run:

`go vet ./...`

If the repository provides a standard lint command, use it.

Examples:

`make lint`

or:

`golangci-lint run`

For concurrency-sensitive changes run:

`go test -race ./...`

For database changes run repository integration tests against a real supported
PostgreSQL version.

For API or event changes run code generation and contract tests.

Prefer repository-provided Makefile or Taskfile commands over inventing new
commands.

Report commands that could not be executed and the concrete reason.

## Generated code

Do not manually edit generated files.

Generated files may include:

* `*.pb.go`;
* `*.pb.gw.go`;
* generated GraphQL code;
* generated OpenAPI clients;
* generated mocks;
* generated enum stringers.

Change the source schema or generator configuration and regenerate.

Inspect generated diffs before completion.

Do not include unrelated regeneration output.

## Logging

Use structured logging.

Use stable field names such as:

* `operation`;
* `request_id`;
* `trace_id`;
* `task_id`;
* `task_version_id`;
* `submission_id`;
* `execution_id`;
* `user_id`, when permitted.

Do not log:

* source code by default;
* hidden tests;
* expected hidden outputs;
* reference solutions;
* authentication tokens;
* cookies;
* authorization headers;
* secrets;
* full personal data;
* arbitrary request bodies.

Log an error once at the layer where enough context exists to act on it.

Do not treat expected validation or wrong-answer results as internal server
errors.

## Metrics and tracing

Prefer low-cardinality metric labels.

Good metric dimensions include:

* task type;
* language;
* checker type;
* verdict category;
* operation status.

Do not use IDs as metric labels.

Do not use as labels:

* `task_id`;
* `submission_id`;
* `user_id`;
* `execution_id`;
* raw error message.

Useful metrics may include:

* task operation latency;
* submission queue latency;
* execution duration;
* checker duration;
* verdict counts;
* runner infrastructure failures;
* duplicate event counts;
* publication validation failures.

Propagate trace context through synchronous calls and message metadata.

## Configuration

Configuration must come from explicit configuration sources such as environment
variables or configuration files supported by the repository.

Validate mandatory configuration at startup.

Fail fast for invalid security-critical configuration.

Do not silently use unsafe defaults for:

* execution network access;
* resource limits;
* runner endpoints;
* authentication;
* database TLS;
* broker security.

Do not commit secrets.

Provide non-secret local defaults only where they are safe.

## Dependency changes

Before adding a dependency:

1. verify that the standard library or an existing dependency is insufficient;
2. check maintenance and license compatibility;
3. assess transitive dependency impact;
4. avoid adding infrastructure for a speculative future use case;
5. document the reason in the change summary.

Do not replace an established library or framework as an unrelated refactor.

## Change discipline

Keep the diff scoped to the requested task.

Do not:

* perform unrelated refactoring;
* rename public APIs without necessity;
* reformat unrelated packages;
* change infrastructure unrelated to the feature;
* introduce speculative abstractions;
* silently change task semantics;
* silently expose previously hidden data;
* modify deployment manifests unless required;
* preserve the old `optimus-prime` name in newly created artifacts.

Before finishing, inspect:

`git status --short`

`git diff --stat`

`git diff`

`git diff --check`

Remove:

* debug prints;
* temporary files;
* commented-out experiments;
* accidental generated files;
* credentials;
* unrelated formatting changes.

## Documentation

Update documentation when changing:

* API contracts;
* event contracts;
* task schemas;
* task-version semantics;
* checker configuration;
* supported languages;
* execution limits;
* publication lifecycle;
* deployment requirements;
* security assumptions.

Record major architectural decisions as ADRs when the repository uses ADRs.

Do not place detailed operational or architectural documentation only in
`AGENTS.md`. Keep this file actionable and link to canonical documentation.

## Completion criteria

A task is complete only when:

* the requested behavior is implemented;
* architectural boundaries are preserved;
* hidden test data is not exposed;
* task-version compatibility is considered;
* errors are mapped correctly;
* relevant tests exist;
* required checks have been run;
* generated code is current;
* migrations are safe;
* the final diff contains no unrelated changes.

## Final response format

The final implementation report must include:

1. summary of the implemented behavior;
2. important architectural decisions;
3. files changed;
4. database, API or event changes;
5. tests and checks executed;
6. checks that could not be executed;
7. remaining risks or follow-up work.

Do not claim that a command passed unless it was actually executed.
