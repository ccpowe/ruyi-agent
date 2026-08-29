# Gateway Liveness and Readiness Probes

Date: 2026-08-29

Status: Accepted

## Context

The Gateway previously had no stable endpoint for an orchestrator or load
balancer to distinguish process liveness from readiness to receive business
traffic. Polling an authenticated Task endpoint is not an adequate substitute:
it requires distributing the global Gateway credential to probe configuration,
couples availability to Task data, and cannot represent the startup and
shutdown boundaries cleanly.

Liveness and readiness have different operational effects. A liveness failure
can cause an orchestrator to restart a container, while a readiness failure
should remove an instance from traffic without killing it. Making liveness
depend on SQLite, a model provider, MCP, a remote agent, or an execution backend
would turn a downstream incident into repeated Gateway restarts. Conversely,
the Gateway must not claim readiness before its runtime and Task Module are
installed or while those resources are being closed.

## Decision

### Public HTTP contract

The FastAPI adapter exposes exactly two unauthenticated `GET` operations:

- `GET /health` returns `200 {"status":"ok"}` whenever the ASGI application can
  execute the handler.
- `GET /ready` returns `200 {"status":"ready"}` only when the readiness gate is
  true and the request can resolve the installed Gateway Task Module. Otherwise
  it returns `503 {"status":"not_ready"}` with `Retry-After: 1`.

Both responses set `Cache-Control: no-store`. They contain no configuration,
dependency names, exception messages, credentials, agent names, or other
diagnostic details. Bearer and team-console session authentication do not apply
to these endpoints so standard orchestrators can call them without holding the
Gateway credential. Other HTTP methods are not part of the contract.

### Liveness scope

The liveness handler is constant-time application code. It does not resolve the
Gateway Task Module and does not perform filesystem, SQLite, model, MCP, remote
agent, network, or backend operations. A wedged event loop naturally causes the
HTTP request to time out; a recoverable downstream failure does not request a
process restart.

### Readiness scope

The standard bootstrapped app initializes its readiness gate to false. Inside
the FastAPI lifespan it opens the complete application runtime, installs the
Gateway Task Module, and only then changes the gate to true. A `finally` block
changes the gate back to false before `bootstrap_application()` begins closing
runtime resources, including stores and background tasks.

This is deliberately a lifecycle and installation check, not a periodic deep
dependency check. Opening the required SQLite stores, checkpointer, configuration,
and Task Module is already part of bootstrap and prevents startup on failure.
Model providers, MCP servers, remote refs, and execution backends are request
targets or degradable capabilities; probing them would incorrectly make the
whole Gateway unready when only one capability is unavailable. The probe also
does not promise to discover later disk exhaustion, SQLite corruption, or a
dependency failure that only a real operation can expose.

`create_gateway_app(service=...)` treats the caller-provided Task Module as
installed and therefore ready immediately. That factory does not own the
service's external resource lifecycle. Embedders that need a dynamic lifecycle
gate can call `attach_gateway_routes` with a synchronous, side-effect-free
readiness getter. The adapter additionally resolves the service only after that
getter succeeds and translates ordinary getter failures into the same generic
503 response.

## Consequences

- Kubernetes and load balancers can independently restart dead instances and
  drain instances that are starting or stopping.
- Probe configuration does not require or leak the global Gateway Bearer token.
- Frequent probes remain cheap and cannot invoke an LLM, mutate Task state, or
  amplify a downstream outage.
- A successful readiness response means the Gateway runtime is installed; it
  is not a guarantee that every possible Task operation or remote capability
  will succeed.
- Operators still need metrics, logs, and dependency-specific monitoring for
  detailed diagnosis.

## Rejected alternatives

- Reusing an authenticated Task or Agent endpoint conflates business data with
  process state and requires credentials in probe configuration.
- Using one endpoint for both signals makes transient dependency problems able
  to trigger restart loops or lets traffic reach a runtime that is not installed.
- Running `SELECT 1`, model calls, MCP discovery, remote Agent requests, or
  backend recovery on every readiness request adds load and couples unrelated
  capability failures to whole-instance routing.
- Returning dependency details helps reconnaissance and exposes unstable
  implementation internals without replacing proper observability.

## References

- [Kubernetes: Liveness, Readiness, and Startup Probes](https://kubernetes.io/docs/concepts/workloads/pods/probes/)
