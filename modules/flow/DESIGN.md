# Flow module: design contract

A Camunda/n8n-like process engine on top of the actor layer. A flow is a serializable DAG of nodes. This file is the
contract between the parts (model, engine, HTTP API, UI). The model code is in `modules/flow/model` and
`modules/flow/nodes-core` and is the source of truth for the JSON shapes; read it before relying on this summary.

## Effect type rule (decided)

Every module of the flow stack (`engine`, `nodes-*`, `http`, and the server's wiring) is written over a generic `F[_]`
(tagless final): capabilities are asked for as constraints (`Async[F]`, `Temporal[F]`, ...) or as parameters. The concrete
`IO` appears only in tests and in `AppIO`, the entry point of `flow/server` (an `IOApp`), which instantiates the whole
application with `F = IO`. The http4s server, its routes (`HttpRoutes[F]`) and the SSE streams are therefore built under
`F[_]` too; the Ember (or other) server is started from `AppIO`.

## Cron node `core.cron` — IMPLEMENTED

One kind of node, two modes decided by the graph (no `mode` field):

- **Launcher** (no incoming edge): the flow is started by the schedule. `CronScheduler` (engine) looks at the clock every second
  (`Settings.pollEvery`) and, for each schedule, starts an instance with `Origin.Cron("<flowId>/<nodeId>")` and the node's
  `inputs` (static JSON in its params, **the inputs of the flow, by name of the input**; they reach the state through the flow's
  `inputs` declarations). The instance id is deterministic: **`<flowId>:<nodeId>:<tick in epoch ms>`**, so a time dealt with twice
  (restart, two servers sharing the journal) starts nothing the second time. The node itself, when it runs inside that instance
  (it is an entry node: `NodeInput.entry`), does nothing and lets the execution go on. A launcher is **armed only while the flow is
  enabled** (`Deployments`, see the API below): saving a draft does not start launching it.
- **Timer** (with incoming edges): the node receives the execution and **waits until the next time the expression matches**,
  then leaves through `out`. The wait is durable: the answer `NodeOutput.suspendUntil = Some(epochMs)` becomes the event
  `NodeSuspended` (node stays `running`, `InstanceStatus.nodes.<id>.fireAt` says when), the audit has `node-waiting` with
  `decision: {reason: "scheduled", fireAt}`, and when the time comes the engine finishes the node with what it answered, **without
  running it again**. After a restart the actor reads the journal and sets the timer again (a time that already passed goes on at
  once). A time that is not in the future is no wait. `retry` of an instance that waits is refused (409: it has not ended), so the
  wait is kept.

Params: `expression` (5 fields `m h dom mon dow`, or 6 with seconds first; lists, ranges, steps, month/day names, `@yearly|@annually|
@monthly|@weekly|@daily|@midnight|@hourly`; checked when the flow is saved by `CronExpression.parse` in the model, pure and
cross-platform), `timezone` (IANA, default `UTC`; checked when the node runs and when a schedule is made), `misfire`
(`fire-once` | `skip`, default `fire-once`: launcher, what to do with times missed while the server was down), `maxWait` (`90s`,
`30m`, `2h`, `1d`: timer, fails through `error` if the next time is farther), `inputs` (launcher).

Calendar rules (`CronSchedule`, `java.time`, engine): the next time is strictly after the given one; when both day-of-month and
day-of-week are restricted either matches (as cron does); a local time that does not exist (clock goes forward) runs at the first
instant after the jump; a local time that happens twice (clock goes back) **runs once, the first time**, so an every-hour expression
runs once less that day.

Misfire: a time that is more than `grace` (5 s) late is a miss. `fire-once` starts one instance for them (the latest); `skip` none.
A schedule seen for the first time counts from now (it does not run the past). A schedule that goes away (flow disabled, trigger
deleted) is forgotten, so enabling it later does not make up for the time it was off. The last time dealt with is kept per schedule in
`Checkpoints` (`checkpoints.json`).

Simulation: the wait does not happen. The simulator's made-up time starts at the real time; a node that waits is woken at once and
the time jumps to when it would have gone on, so the next node asks for the time it would have seen. The audit has `node-waiting`
(`scheduled`, with the `fireAt` it would have waited for) then `node-finished`. `SideEffects.Pure`.

Contract changes: `NodeOutput.suspendUntil`, `NodeInput.now` (ms; nodes ask the time here, not from the clock) and `NodeInput.entry`,
`InstanceEvent.NodeSuspended`, `Decision.Scheduled`, `Origin.Cron` (label `cron:<flowId>/<nodeId>`), `InstanceStatus.Node.fireAt`,
`AuditEntry.NodeFinished.info` (what a node says about its run, e.g. `fireAt`, `expression`, the destination of a published message),
`CoreNodes.registry(runner, nextFire)`. The cron node asks its calendar from a function it is given (`CronNode.NextFire`), so
`flow-nodes-core` stays cross JVM/JS.

## Stream processing nodes — IMPLEMENTED (`flow-model` + `flow-engine` + `modules/flow/nodes-stream`, package `xplat.flow.stream`)

Stream operators are ordinary nodes of the same graph. The graph has two kinds of edge, told apart by the ports they join:

- **Token edge** (as before): hands the execution over once.
- **Stream edge**: carries a sequence of elements. A port of a node can carry a stream.

### JSON (backward compatible: a flow without streams is written exactly as before)

- `Port` gets an optional `stream` (a `DataType`, the type of the elements): `{"name":"out","stream":{"type":"any"}}`. It is left out
  when the port is a token one.
- `Edge` gets an optional `input`, the input of the target it enters: `{"from":"a","port":"out","to":"j","input":"left"}`. It is left out
  when it is `in`, the default (every node has an input `in`; `stream.join` and `stream.zip` have `left` and `right`).
- `NodeDescriptor` has `inputs` (ports too, with their `stream`). The catalog entries (`GET /api/catalog`) and `POST /api/catalog/{kind}/describe`
  now carry `"inputs": [Port]` and `"role"`: `node` (not part of a stream), `source`, `operator` or `sink`. The node that stands for a segment
  (`stream.segment`) is made by the engine and is **not** in the catalog.
- What the editor draws: a **source** has an input `in` (token) and an output `out` with `stream`; an **operator** has input(s) and an
  output `out` that both have `stream`; a **sink** has an input `in` with `stream` and an output `out` (token) that its `OutputDecl`s write
  to. Edges join ports of the same kind: stream with stream (the element types must be compatible, `conformsTo`), token with token. The `error`
  port is always a token one. A port with several edges broadcasts and the slowest consumer sets the pace.
- Validation errors (`FlowError`, all with `node`): `stream-token-mix` (stream port joined to a token one), `stream-type-mismatch`,
  `unknown-input`, `stream-without-source`, `stream-without-sink` (the last two carry `nodes: [ids]`). Cycles are `cycle` as always. `stream.segment` written by hand is `unknown-kind`.

### What it is for the instance: one step

The engine condenses each connected group of stream nodes (a *segment*) into one node of kind `stream.segment`, whose id is the **first source**
(its *anchor*, sorted by id) and whose `params` carry the nodes inside it. The ordinary edges that enter the sources and leave the sinks are
attached to it (edges that enter a source are marked with it, `Edge.input`). So an `If` before a stream that decides which of two sources
starts works: only the sources whose edge fired give elements.

- The segment runs while its streams run. It ends when every source has ended (`list`, `range`, `cron` with `maxTicks`, a flow of messages
  that stops...) and then leaves through `out`, writing the `OutputDecl`s of its sinks to the state. Inside the step the outputs are answered by node, as `<node>.<name>` (the audit shows `rows.result`), so two sinks may
  both call their output `result`. A source that never ends (a queue, a cron
  without `maxTicks`) keeps the instance **running until it is cancelled**. The segment has no time limit (`NodeType.unbounded`).
- It fails (leaves through `error`) if a source or an operator throws or a connection cannot be opened. An element that a node cannot handle is
  **not** a failure: it is a *dead letter* and the stream goes on.
- `RunPolicy` and `retry` apply to the whole segment, with the policy of its anchor node: with `skip` a retried instance does not run the segment
  again and its outputs are written again.
- `POST /api/instances/{id}/cancel` (new, 202 `{instanceId}`; 404 if unknown; nothing happens to an instance that has ended) cancels what runs
  (sources are closed, connections released), marks the running nodes `failed` with the error `cancelled` and the ones that had not started `dead`, and
  ends the instance as `failed`. `FlowEngine.cancel(id)`.

### Inside a segment

Every edge is a small bounded queue and every node is a stream that runs at the same time: a slow sink holds back the nodes before it and a queue
source stops reading from its broker (**backpressure**). If one node fails they all stop. Delivery is **at least once**: an element carries an
acknowledgement (`Elem.ack`) that the broker consumer waits for, and it is called when the end of the segment is done with it (the sink handled it, or it was
dropped or failed). A queue source delivers one message at a time per source. Windows, aggregates, joins and the deduplication hold their state in
memory: they acknowledge elements as they take them in, so what they keep is lost if the server stops (a tumbling window is acknowledged when it is
passed on). Nothing is audited per element.

### Nodes (v1). Parameters are JSON; `*` is required

Sources (category `Stream sources`, pure unless noted): `stream.source.list` {`path`*: state path of an array}; `stream.source.range` {`from`=0,
`to`*, `step`=1}; `stream.source.cron` {`expression`*, `timezone`=UTC, `maxTicks`} (elements `{"at": ms}`); `stream.source.queue` (reads) {`connection`*,
`source`*, `group`, `destinationType`, `format`=json|text, `envelope`=false}.

Operators (category `Stream operators`): `stream.map` {`set`: {field: expr}, `remove`: [path], `replace`: expr}; `stream.filter` {`where`*: expr};
`stream.flatMap` {`path`*, `merge`=false}; `stream.window` {`kind`=tumbling|sliding, `by`=count|time, `size`, `duration`, `every`}: elements
`{"items":[...],"count":n,"start":ms,"end":ms}`; `stream.aggregate` {`aggregations`*: {name: {`op`: count|sum|avg|min|max|first|last|collect, `path`}}, `key`,
`over`=stream|items, `emit`=end|each, `limit`=1000} (rows `{"key":k, name: value...}`); `stream.dedupe` {`key`, `max`=10000}; `stream.throttle` {`elements`=1,
`per`="1 second"}; `stream.merge`; `stream.zip` (inputs `left`/`right`, elements `{"left","right"}`); `stream.join` (inputs `left`/`right`) {`leftKey`*, `rightKey`*, `maxBuffer`}.
There is no `broadcast` node: a stream port with several edges broadcasts.

Sinks (category `Stream sinks`; the first `OutputDecl` of the node receives the value; default output name `result`): `stream.sink.collect` {`limit`=1000, `path`} (a list);
`stream.sink.reduce` {`op`=count|sum|avg|min|max|any|all, `path`} (a number; `any`/`all` give true or false of a boolean field, which is how a stream feeds an `If`); `stream.sink.flow` (writes) {`flowId`*, `revision`, `inputs`: {childInput: path in the element or `$`},
`wait`=true, `parallel`=1} (number of flows that ended well; the origin of each child is the sink node); `stream.sink.publish` (writes) {`connection`*,
`destination`*, `destinationType`, `key`, `format`, `headers`} (number published; header `x-idempotency-key` = `<instance>:<node>:<n>`); `stream.sink.http` (writes) {`url`*,
`method`=POST, `headers`, `timeoutSeconds`=30} (number accepted; any non-2xx is a dead letter).

**Expressions** (`Expr`, in `map` and `filter`; now `xplat.flow.model.Expr`, shared with the entities, which also has list operators,
`matches` and `round`: see Entities): JSON, not code. Literals; `{"path":"a.b","default":x}`; `{"const":v}`; `eq neq gt gte lt lte in`; `and or not`;
`add sub mul div concat len exists if`. Checked when the flow is saved (`invalid-params`).

### What the status and the audit show (new fields)

- `GET /api/instances/{id}`: every node of a segment appears in `nodes` with the status, `port` and `error` **of the segment** (they are one step), and its own
  `metrics`: `{"in","out","dropped","failed","latencyMs","sampleIn":[...],"sampleOut":[...]}` (`latencyMs` is the mean time since an element entered the segment;
  samples are the first 5). While the segment runs the metrics are live (reported every second; kept in the process, not recovered after a restart); when it
  ended they are the final ones. New top-level `deadLetters`: `[{"node","error","element","at"}]` (the first 100 of each segment; more are counted in `failed`).
- Audit: the entries of the segment are those of its **anchor node** (`node-started`, `node-finished`, `edge-*`, `state-changed`): `node-started.nodeKind` is
  `stream.segment` and its `params` have `members` (the nodes inside, secrets hidden), `edges`, `sources`, `sinks`. `node-finished.info` has `metrics` (by node id) and `deadLetters`.
  A UI that draws the original flow maps the anchor's entries to the whole segment (`params.members`).
- Simulation (`POST /api/flows/{id}/test`, or `engine.simulate`): a **mock for a source** is `{"type":"output","values":{"elements":[...]}}` (or `{"type":"fail","error":"..."}`),
  by the id of the source node. Pure nodes run for real; a source that reads outside (queue) with no mock gives nothing; sinks that write (publish, http, flow) are not done
  and answer the default of their output. Time does not wait (throttle, cron ticks, time windows). The trace step of the segment is `{nodeId: anchor, kind: "stream.segment",
  outcome: "simulated", info: {metrics, deadLetters}}`; `metrics[node].sampleIn` of a write sink is what it would have sent.

### Limits (v1)

Windows, aggregates, joins and deduplication are in memory (lost on restart). No state shared between servers; one message in flight per queue source. `stream.sink.flow` is not
checked against the called flow when the flow is saved. A segment is one step: it has one `RunPolicy` (its anchor's), and a sink writes one output (its first). The samples and dead
letters are bounded. The nodes of a segment show the status of the step, not their own (a node whose input ended before the others still shows `running`).

## Semantics (decided)

- **No cycles** (DAG). Validated on save (`FlowValidator`).
- **Global state** is one JSON object shared by the instance. A node receives `(params, state, memory)`; `memory` is the
  node's own, per instance. Nodes do not mutate the state: they return values by the name of their declared
  `OutputDecl`s, and the engine writes each one at its `target` path after checking its type. The state is event-sourced.
- **Ports**: a node leaves through one named port (`out` by default; `If` has `then`/`else`; every node has `error`).
  Every edge leaving the port that fired is followed, so a port with several edges **broadcasts** (parallel branches).
  An edge that will never fire (branch not taken) counts as arrived for a join, so joins do not wait forever.
- **Join** (`NodeDef.joinPolicy`): `all` waits for every incoming edge, `any` runs on the first one.
- **RunPolicy** (`whenSucceeded`/`whenFailed`: `run`|`skip`): applies when an instance is run again or retried. `skip`
  does not run the node: the execution continues with the stored result (same port, outputs re-applied to the state).
  A node that never ran always runs. A stored result is only reused if the hash of its `params` and of what it read is
  unchanged.
- **If** is an ordinary node (`core.if`): reads a boolean from the state (or its own memory), goes through `then`/`else`,
  fails through `error` if the variable is missing or not boolean.
- **Subflows** (`core.subflow`): runs another flow; inputs filled from the caller's state, outputs taken into it; `wait`
  true/false; revision pinned or latest. Flows do not call each other in a circle.
- **Test mode** (`Simulate`): `SideEffects.Pure` nodes run for real; `Write` nodes are always simulated; `Read` nodes are
  configurable. Per-node mocks (fixed output / error). In-memory journal, virtual time. Returns a full trace (nodes
  visited, ports taken, state after each step). A test is a JSON spec: `{flowId, inputs, mocks, expect}`.
- **Triggers** are separate from flows: `{id, flowId, kind: http|cron|queue|manual, ...}`; an HTTP trigger gives a
  method+path served under `/hooks/**`; it can answer async (202 + instanceId) or sync (a `core.respond` node ends the flow
  and defines the response; on timeout answer 202).
- **Secrets** never travel to the UI. Global configuration is a set of named *connections* (`claude`, `kafka-prod`,
  `postgres-main`...) with a `ConfigSchema`; nodes refer to them by name in their params.

## Audit (decided)

Every execution leaves an audit trail that explains what happened and why, readable after the fact (and live).
It is built from the instance's own event-sourced events, enriched so no step needs guessing:

- **Instance**: `instanceId`, `flowId`, `revision` (and the definition hash), who/what started it (`origin`: trigger id +
  request id, parent instance for subflows, manual user, retry of instance X), the initial inputs, start/end time, final
  status, and every retry/re-run with its reason.
- **Per node attempt**: node id and kind, attempt number, start/end timestamps and duration, `params` hash, the values it
  read from the state (path, value), its memory before/after, the outputs it wrote (path, previous value, new value),
  the port it left through, the error with its message if it failed, and the decision of the engine when it did not run
  it: `skipped-by-run-policy` (with the stored result it reused), `skipped-dead-branch` (which branch was not taken and
  why: the `If` that decided, with the value it read), `waiting-join` (which edges were pending).
- **Routing decisions**: which edges fired from a port, which were marked dead.
- **Subflows**: the audit of the child is linked by `childInstanceId`.
- **Simulation** produces the same record, marked `simulated`, with the mock that answered for each node.
- Values of `secret` fields and connection credentials are **redacted** before anything is written to the audit.
- The audit is append-only. Its retention is configurable (default: keep everything).

Queries (REST, see below): the timeline of one instance, and a search of instances by flow, status, origin and time range.
The UI shows it as a timeline with the state diff at each step.

## Engine interface (Scala, `modules/flow/engine`, package `xplat.flow.engine`) — IMPLEMENTED

Everything is generic in `F[_]` (tagless final): `cats.effect.IO` appears only in tests and in `flow-server`. The engine
asks for `Async[F]`, an `ActorSystem[F]` and a `Journal[F]`; the instances are persistent actors (`IOPersistence`, which is
generic in `Async[F]`) that write to that journal. Use `InMemoryJournal.make[F]` without a database.

```scala
trait FlowStore[F[_]] {                       // definitions, versioned by revision; InMemoryFlowStore provided
  def save(flow: FlowDef): F[Either[List[FlowError], FlowDef]]   // validates; assigns next revision
  def get(id: String, revision: Option[Int]): F[Option[FlowDef]] // None revision = latest
  def list: F[List[FlowSummary]]                                  // FlowSummary(id, name, revision)
  def revisions(id: String): F[List[FlowDef]]
  def delete(id: String): F[Unit]
  def resolver: F[FlowResolver]
}

trait FlowEngine[F[_]] {
  def start(flowId: String, revision: Option[Int], inputs: Json, instanceId: Option[String],
            origin: Origin = Origin.Manual()): F[InstanceId]      // inputs: object by input name; existing instanceId = idempotent
  def status(id: InstanceId): F[Option[InstanceStatus]]
  def retry(id: InstanceId, useLatest: Boolean = false, by: Option[String] = None): F[Unit]
  def events(id: InstanceId): fs2.Stream[F, AuditEntry]            // replay + live, ends at instance-finished (SSE)
  def audit(id: InstanceId): F[Option[AuditTrail]]                 // AuditTrail(instance: Header, entries)
  def search(query: InstanceQuery): F[Page[InstanceSummary]]       // newest first
  def simulate(flow: FlowDef, inputs: Json, mocks: Map[String, Mock], realReads: Boolean = false): F[Trace]
  def catalog: NodeCatalog
  def store: FlowStore[F]
}

ActorFlowEngine.make[F](system, journal, extraNodes = Nil, config = Config(nodeTimeout, secretPaths), store = None, index = None)
```

Notes that the HTTP layer needs:
- `events` now emits the **same `AuditEntry`** that `audit` returns (one stream for the live view and the timeline).
- **`retry` re-runs the same instance** (it must: `RunPolicy` reuses that instance's previous results). The instance id does
  not change, so `POST /api/instances/{id}/retry` answers 202 `{instanceId: <same id>}`. A retry is recorded as a
  `retry-requested` entry whose origin is `retry:<id>`.
- `useLatest` takes the latest revision of the flow, which is how an edited flow is picked up; nodes whose params or declared
  reads changed run again even if their policy says skip.
- Errors (`FlowEngine.Error`): `FlowNotFound`, `InvalidInputs`, `InstanceNotFound`, `NotFinished`.
- `InstanceQuery(flowId, status, origin, from, to, limit = 50, after)`: `origin` is a **prefix of the origin label**;
  `after` is the **id of the last instance of the previous page**; `Page(items, next)` where `next` is that id (HTTP can
  return `items` as a flat array).
- Audit JSON (see `AuditEntry`): `kind` is one of `instance-started`, `node-started`, `node-finished`, `node-skipped`,
  `node-waiting`, `edge-fired`, `edge-dead`, `state-changed`, `retry-requested`, `instance-finished`. `origin` is the label
  (`manual`, `trigger:<id>`, `retry:<id>`, `subflow:<id>`) and `originDetail` the structured form (user, request id...).
  Skips and waits carry `decision: {reason: skipped-by-run-policy | skipped-dead-branch | waiting-join | scheduled, ...}` (`scheduled` has
  `fireAt`: a timer that waits). A secret is
  written `{"$redacted": true}`. `childInstanceId` is on the `node-finished` of a node that runs a flow.
- Simulation: `Trace(status, steps, state, audit)`; steps have `mocked` and `simulated` booleans; every audit entry has
  `simulated: true` and `node-finished` has `simulation: {outcome, mock?}`.
- Model additions: `FlowDef.layout: {nodeId: {x, y}}` (optional, ignored by the validator); `FlowError.Cycle` has `nodes` and
  `FlowCycle` has `chain` in the JSON. `NodeInput` carries `instanceId`/`nodeId`, `NodeOutput` has `info`, `FlowRunner.run`
  takes a `Caller`.
- Known limits: the instance index and the in-memory hub live in the process (the journal is the source of truth: an
  instance that is touched after a restart is rebuilt, but `search` only knows instances this process has run or brought up);
  finished instances keep their actor up (no eviction yet); retention of the audit is "keep everything".

## REST API (`modules/flow/http`, http4s, JSON via circe, base path `/api`)

| Method + path | Purpose |
|---|---|
| `GET /api/catalog` | Node kinds: `[{kind, category, label, config: ConfigSchema, sideEffects, ports, defaultOutputs}]` (ports/outputs for empty params) |
| `POST /api/catalog/{kind}/describe` body `params` | `{ports, defaultOutputs, reads}` for those params (ports can depend on params) |
| `GET /api/flows` | `[{id, name, revision, enabled, draft}]`; a flow that only has a draft has `revision: 0` |
| `GET /api/flows/{id}?revision=n` | the `FlowDef` (latest if no revision) |
| `GET /api/flows/{id}/schedule` | `{enabled, schedules: [{node, expression, timezone, misfire, nextFireAt}]}`: the cron launchers of the latest revision |
| `POST /api/flows/{id}/enable` · `POST /api/flows/{id}/disable` | switch the cron launchers of the flow on or off (same answer as `schedule`); a flow that is deleted is switched off |
| `PUT /api/flows/{id}` body `FlowDef` | validate + save new revision. 200 `FlowDef`; 422 `{errors:[FlowError]}` |
| `DELETE /api/flows/{id}` | delete (the revisions and the draft) |
| `PUT /api/flows/{id}/draft` body `FlowDef` | save the draft **without validating**; 200 `{flow, errors}` (what stops it from being published) |
| `GET /api/flows/{id}/draft` · `DELETE /api/flows/{id}/draft` | the draft `{flow, errors}` (404 if none) · discard it |
| `POST /api/flows/{id}/publish` | validate the draft and save it as a new revision, removing the draft. 200 `FlowDef`; 422 `{errors}` (the draft stays) |
| `POST /api/flows/validate` body `FlowDef` | `{errors:[FlowError]}` (always 200) |
| `POST /api/flows/{id}/instances` body `{inputs?, revision?, instanceId?}` | 202 `{instanceId}` |
| `GET /api/instances/{id}` | `{id, flowId, revision, status, state, nodes:{nodeId:{status, port?, error?}}}`; `status`: `running|succeeded|failed` |
| `POST /api/instances/{id}/retry` | 202 `{instanceId}` (same instance, re-run; `?latest=true` takes the newest revision of the flow) |
| `POST /api/instances/{id}/cancel` | 202 `{instanceId}`: stops what runs (a stream that does not end too) and ends the instance as `failed`; 404 if unknown; nothing if it has ended |
| `GET /api/instances?flowId=&status=&origin=&from=&to=&limit=&after=` | search instances: flat array `[{id, flowId, revision, status, origin, startedAt, endedAt}]`, `origin` is a label prefix, `after` = id of the last row |
| `GET /api/instances/{id}/audit` | the audit trail: `{instance: {...}, entries: [AuditEntry]}` ordered by sequence; each entry has `seq`, `at`, `kind` (`instance-started`, `node-started`, `node-finished`, `node-skipped`, `edge-fired`, `edge-dead`, `state-changed`, `retry-requested`, `instance-finished`...), `node?`, `attempt?` and the fields described in the Audit section |
| `GET /api/instances/{id}/events` | SSE: one JSON `InstanceEvent` per event (replay then live) |
| `POST /api/flows/{id}/test` body `{flow?, inputs, mocks}` | simulation `Trace`; if `flow` is given, simulate the unsaved draft |
| `GET/PUT/DELETE /api/triggers[/{id}]` | trigger specs |
| `GET/PUT/DELETE /api/connections[/{name}]` | connections; secret fields are write-only, never returned |
| `ANY /hooks/**` | dispatches HTTP triggers |
| `GET /` and static files | the UI (`index.html` fallback for the SPA router) |

JSON shapes of `FlowDef`, `NodeDef`, `Edge`, `OutputDecl`, `DataType`, `ConfigSchema`, `FlowError`, `NodeOutput`
come from the codecs in `modules/flow/model` (`DataType` is `{"type":"bool"|"num"|"str"|"any"|"list"|"obj",...}`).

## Modules

```
flow/model        cross JVM/JS   DONE   spec, validator, codecs, `Expr`, `Types`
flow/entity       cross JVM/JS   DONE   entities of the business model: `EntityDef`, checks of values, DSL
flow/nodes-core   cross JVM/JS   DONE   NodeType, registry, If, Subflow
flow/engine       JVM                   actors + event sourcing, simulate, FlowStore
flow/nodes-queue  JVM            DONE   `queue.publish` + `queue` triggers (Kafka, ActiveMQ/Artemis, Redis Streams)
flow/nodes-stream JVM            DONE   stream sources, operators, sinks and the node that runs a segment
flow/nodes-script JVM            DONE   `script.js` (JavaScript written in the editor) and `stream.script`, GraalJS sandbox
flow/nodes-db     JVM            DONE   `db.query/execute/insert/update/delete` over JDBC + doobie (postgres, mariadb, sqlite)
flow/nodes-mongo  JVM            DONE   `mongo.find/aggregate/insert/update/delete` + change streams for `queue` triggers
flow/nodes-http   JVM            DONE   `http.request` over the JDK HTTP client, with `http` connections
flow/nodes-ai     JVM            DONE   `ai.agent` (Claude, OpenAI-compatible, Codex) over the http4s client
flow/http         JVM            DONE   http4s routes over F[_] (API, hooks, static, SSE), triggers, connections, secrets
flow/server       JVM            DONE   `AppIO` (the only IO), config, fat jar with the UI
flow/ui           npm/Vite       DONE   React Flow editor (tested against its own mock only)
```

## Queue nodes and triggers — IMPLEMENTED (`modules/flow/nodes-queue`, package `xplat.flow.queue`)

An optional JVM module on the connectors (`coreConnectors`, Kafka, ActiveMQ/Artemis and Redis Streams). `flow-server` includes it; the
fat jar is ~95 MB with it. Everything is over `F[_]`.

- **Connections**: kinds `kafka` (`bootstrapServers`, `securityProtocol`, `saslMechanism`, `username`, secret `password`, `properties`),
  `activemq` (`url`, `flavor` artemis|classic, `username`, secret `password`) and `redis` (`uri`, secret `password`). They are passed to
  `ConnectionStore.make` (the server does it) and listed by `GET /api/connection-kinds`. Nodes and triggers find a connection by
  name with the interface `Connections[F]` (`flow-nodes-core`: `resolve(name)` → kind, params and secrets), which
  `ConnectionStore.provider(store, secrets)` implements. Secrets never reach the audit or the UI.
- **`queue.publish`** (`PublishNode`, `SideEffects.Write`, category "Messaging"): params `connection`, `destination` (topic / queue /
  stream), `payload` (state path), optional `key` (state path; messages with the same key keep their order), `format` (`json` default |
  `text`), `headers` (fixed, text), `destinationType` (`queue`|`topic`, ActiveMQ only). Every message carries the header
  **`x-idempotency-key = <instanceId>:<nodeId>`** (ActiveMQ: `x_idempotency_key`, JMS names must be identifiers), so a consumer can drop the
  duplicate of a retry; with `RunPolicy(whenSucceeded = skip)` a retried instance does not publish again. `reads` declares `payload` and
  `key`, so the validator checks they exist. Publishers are opened once per connection and destination (`PublisherPool`), reopened if the
  connection is edited, and closed when the application stops. A broker that fails is a failed node (error port), not an exception.
  **Simulation** does not connect: `NodeType.simulated(input)` answers with the message it would have sent in the `info` of the
  `node-finished` entry (`destination`, `key`, `headers`, `payload`).
- **`queue` triggers**: served by plugins as `TriggerSource`s and run by the server's `TriggerSources` (see "Everything is a
  plugin"). The plugin `queue` reads them from the brokers (`QueueTriggers.source`), the plugin `mongo` from the changes of a
  collection. For every trigger of kind `queue` a consumer (`ConsumerFactory`, with the connector's retry and ack)
  starts an instance of the flow per message. The message becomes a request `{source, key, position, headers, body}` (body: JSON if it is
  JSON, otherwise the text) and `inputs` of the trigger map it like for HTTP (`$body.x`, `$key`, `$headers.x`, `$source`, `$position`; no
  `inputs` = the body if it is an object). The instance id is `<trigger>-<id>` with the id the message carries (`x-idempotency-key`,
  `idempotency-key` or `message-id` header) or else `<source>-<position>`, so a message delivered again does not start a second instance;
  the message is acknowledged once the instance has started. A message that cannot become inputs (`InvalidInputs`) is logged and acknowledged
  (it would never work); any other failure is retried by the connector's `RetryPolicy`, and a consumer that gives up is started again after 5 s.
  Triggers and connections are compared with the running consumers every 2 s: added, edited, deleted triggers and changed connections
  start, restart or stop consumers; a trigger whose connection does not exist waits for it. Origin: `trigger:<id>`.
- Verified against a real Redis (`redis:7-alpine`): a `XADD` started the instance, and `queue.publish` wrote the message to another stream
  with the idempotency header. Kafka and ActiveMQ are covered by the connectors' own contract tests (containers) and by the fakes of
  this module; they were not run against a broker from here (the Kafka image was not available locally).

## Script, database and AI nodes — IMPLEMENTED

Three optional modules; `AppIO` (through `FlowServer`) registers them as `extraNodes` (and `stream.script` through the new `extra`
parameter of `StreamNodes`), and adds their connection kinds to `GET /api/connection-kinds`. **Contract change:** `NodeInput` has a new
optional `outputs: List[OutputDecl]` (what the node declared). The engine accepts exactly the declared names in a node's answer, so a node
that can answer several values (`db.query`: `rows`, `count`, `first`) answers only the ones that were declared. Empty when run on its own.

### `nodes-script` (package `xplat.flow.script`)

- **`script.js`** (category "Scripting", `SideEffects.Pure`, so it runs for real in test mode). Config: `code`\* (the body of a function of
  `(state, memory, params, log)`; the editor should offer a code editor: the field description ends with `editor:javascript`), `ports`
  (names, default `["out"]`; `error` is always there), `args` (a JSON value the script sees as `params`), `timeoutMs` (never longer than the
  server's 5 s). The script returns nothing/`null` or `{values, port, memory}`: `values` by the names of the declared outputs (the engine checks
  their types), `port` one of `ports` or `error`, `memory` the new memory of the node. `log(...)` lines go to `info.logs` of the audit.
  `files` (`[{name, code}]`, names such as `helpers.js`) are more files of the script: they run before the code, in their order, in the same
  context, so what they declare at their top level the code uses; their errors say `<file> line N`. A new node starts with a template
  (`ui.template`) that writes its default outputs `total: num` and `big: bool`, so it runs as it is.
- **`stream.script`** (stream operator): same sandbox, code is the body of a function of `(element, params, log)`; `mode` is `map` (returns the
  new element; nothing = dead letter), `filter` (truthy keeps) or `flatMap` (returns a list). The code is compiled once per segment.
- **Sandbox** (GraalJS 24.1 `org.graalvm.polyglot:polyglot` + `js`): host access, class lookup, IO, threads, native code, process creation, environment,
  `Polyglot`, `load`, `print`, `console` and the `java`/`Packages` globals are off; `require`, `process`, `fetch` do not exist. `state` and `memory`
  are deep-frozen copies and the code is strict, so assigning to them throws. One context per run (nothing shared between runs or instances).
  Limits: time (default 5 s; the context is cancelled), statements (20 M), result size (1 MB), 50 log lines. **No memory limit** exists on a
  JDK that is not GraalVM Enterprise: only the timeout stops a script that fills the heap. Not a defense against a malicious operator: restrict who
  can edit flows. On a normal JDK the engine runs in interpreter mode: slow to start (first call ~1 s) and several times slower than on GraalVM.
- **Validation on save:** the code is compiled; a syntax error is `invalid-params` with the line (`code: line 2: ...`). A runtime error goes through
  `error` with `line N: Error: message`.
- The fat jar must keep `META-INF/graalvm/**` and `META-INF/resources/**` (Truffle's version files); the assembly merge strategy does.

### `nodes-db` (package `xplat.flow.db`)

- Connection kinds: `postgres` and `mariadb` (`host`\*, `port`, `database`\*, `username`, `password` **secret**, `ssl`, `maxPoolSize`), `sqlite` (`file`\*).
  The JDBC url never contains the password. One Hikari pool per connection name, opened on first use, reopened when the connection is edited,
  closed with the server. SQLite uses one connection. Messages of the driver are cleaned of the connection's secrets.
- Nodes (category "Database"): `db.query` (`SideEffects.Read`; outputs `rows` = `list[{}]`, `count`, `first`), `db.execute` (a raw parameterised
  write; output `affected`), and `db.insert`, `db.update`, `db.delete` (structured; output `affected`). All `Write` except query.
- **Parameters, never concatenation:** `sql` has `?` and `params` is a list with one entry for each: `{"path": "state.path"}`, `{"value": 1}`,
  `{"context": "key"}` (`instanceId`, `nodeId` or `key` = `<instanceId>:<nodeId>`) or a plain literal. `??` is a literal question mark. Table and column
  names of the structured nodes must be plain names (`[A-Za-z_][A-Za-z0-9_$]*` with one dot at most). `where` is column = value (a null value is
  `IS NULL`); `update`/`delete` without `where` are refused unless `allowAll`. One statement only (a `;` in the middle is refused).
- `db.query` is read only: it must start like a read (SELECT/WITH/SHOW/EXPLAIN/VALUES) and runs on a read-only connection (`setReadOnly`, `PRAGMA
  query_only` for SQLite), so a `WITH ... DELETE` is stopped by the database. `maxRows` (default 1000; `info.truncated` says if there were more),
  `timeoutSeconds` (30). Column types: booleans and numbers as such (decimals keep their digits), text as text, dates/times as ISO text, binary as
  base64, `json`/`jsonb` parsed, anything else as text; a repeated column name keeps the last.
- Each node run is one transaction. **Retries/idempotence:** a node that writes runs again if the instance is run again, unless its `RunPolicy` is
  `whenSucceeded: skip`; even then a crash between the write and its record can repeat it. For a write that must not repeat use `db.execute`
  with `{"context": "key"}` in a unique column and `ON CONFLICT DO NOTHING`. In test mode writes and queries do not connect: the audit shows the
  statement and parameters it would have run (`info.simulated`, `info.params`).
- Not supported: the generated key of an insert (`INSERT ... RETURNING` is not a read, so `db.query`
  refuses it: insert with `db.execute`/`db.insert` and read the row back with `db.query`).

### `nodes-mongo` (package `xplat.flow.mongo`)
- Connection kind `mongodb`: `uri` (without the password; `mongodb://` or `mongodb+srv://`) or `host`/`port`, plus `database`, `username`,
  `password` (secret, given to the driver as a credential with `authSource`, default `admin`), `tls`, `maxPoolSize`. One client per named
  connection (`MongoPool`), opened on first use and replaced when the connection changes.
- Nodes (category Database): `mongo.find` (`filter`, `projection`, `sort`, `skip`, `limit`), `mongo.aggregate` (`pipeline`; `$out`/`$merge`
  refused, so it only reads), `mongo.insert` (`document` or `documents`), `mongo.update` (`filter`, `update` as operators or a pipeline, `many`,
  `upsert`), `mongo.delete` (`filter`, `many`). An empty filter in update/delete is refused unless `allowAll`.
- Documents and pipelines can be JSON or text in the syntax of the MongoDB shell (what the editor sends): keys without quotes, single
  quotes, `ObjectId('...')`, `ISODate('...')`; `[{$match: {parentId: null}}]` works. Text that does not parse says which field and why.
- Values of the run go into the documents with markers, anywhere: `{"$path": "state.path"}` and `{"$context": "key"}` (`instanceId`, `nodeId`,
  `key`). Everything else is MongoDB Extended JSON, and documents come out in its relaxed form (`{"$oid": ...}`, `{"$date": ...}`), so an
  `_id` that was read can go back into a filter as it is.
- Outputs: reads answer `documents`, `count`, `first`; insert `affected`, `insertedIds`, `insertedId`; update `matched`, `affected` (modified),
  `upsertedId`; delete `affected`. Writes are simulated in test mode (`info.operation`, `info.simulated`).
- **Change streams:** a `queue` trigger whose connection is `mongodb` reads the changes of the collection named in `queue` (insert, update,
  replace, delete; MongoDB must be a replica set). The body is `{operationType, documentKey, fullDocument, updateDescription, clusterTime}`
  (`fullDocument` looked up for updates, `null` for deletes), the key the `_id`, the header `operation-type`, and the position the resume token,
  so a change delivered again starts no second instance. The resume token of each consumer group is kept in `_flow_resume` of the same database.

### `nodes-http` (package `xplat.flow.rest`)
- `http.request` (category HTTP): `method` (GET, POST, PUT, PATCH, DELETE; default GET), `url`*, `connection`, `query`, `headers`, `body`,
  `failOnStatus` (true), `timeoutSeconds` (30). `{{state.path}}` in the url puts in that value of the state, URL-encoded; `query`, `headers` and
  `body` take values with the markers `{"$path": ...}` / `{"$context": ...}` (`xplat.flow.nodes.Template`, shared with the Mongo nodes). A null
  query value is left out; a GET has no body.
- With a `connection` of kind `http` the url can be relative to its `baseUrl`, and its `token` goes as `Authorization: Bearer` (shown as `***`
  in the audit, cleaned from error messages).
- Outputs: `body` (parsed JSON, or text, or `null`), `status`, `headers` (lower-case names, first value). Default output `body` → `response`.
  A status that is not 2xx goes out by `error` with the first 300 characters of the answer, unless `failOnStatus` is false. Side effects
  `write`: in test mode it does not call, and the audit shows the request (`info.request`, `info.simulated`).

### `nodes-ai` (package `xplat.flow.ai`)

- Connection kinds `claude` (Anthropic Messages API), `openai` and `codex` (OpenAI Chat Completions, so `baseUrl` can be any compatible service):
  `baseUrl`, `apiKey` **secret**\*, `model` (default per kind when empty: `claude-sonnet-5-5`, `gpt-4.1`, `codex-mini-latest`), and `anthropicVersion` for claude.
- **`ai.agent`** (category "AI", `SideEffects.Read`). Config: `connection`\*, `prompt`\* and `system` (text with `{{path}}` where state values go: strings as they
  are, others as JSON; a missing path fails before calling), `model` (overrides the connection's), `maxTokens` (1024), `temperature`, `timeoutSeconds` (60),
  `retries` (2: 429, 5xx, timeout and network errors, exponential backoff 0.5 s doubling to 8 s, or `Retry-After`), `json` (ask for JSON; the output is `json`),
  `schema` (a `DataType` the JSON must fit), `maxTotalTokens` (a clearly larger prompt is not sent, the answer is shortened to what is left, and a run
  that used more fails), `simulatedResponse`. Outputs it can answer, only if declared: `text` (str), `json` (when `json: true`), `usage`
  (`{inputTokens, outputTokens}`). The audit `info` has `usage`, `model`, `attempts`, `stop`, `durationMs`.
- **Secrets:** the API key is only in the request header; messages and the audit never contain it (also hidden if the service repeats it).
- **Test mode:** it does not call the service. It answers `simulatedResponse`, or a value of the right type (`"[simulated answer]"`; for `json`, a default of
  the schema), and shows the `prompt`/`system` it would have sent. **Record/replay:** `--ai-record <dir>` keeps every successful answer in a file named after
  a hash of the url and body (never the key); `--ai-replay <dir>` answers from those files and never goes to the network (an unknown request fails the node).
- Not supported yet: tool use, streaming, images.

## HTTP layer and server — IMPLEMENTED

### Running it

```
sbt flowServer/assembly                       # builds the UI with npm (skipped with a warning if npm is missing) and the fat jar
java -jar modules/flow/server/target/scala-2.13/flow-server.jar [--port 8080] [--data-dir ./flow-data] [--db sqlite|mongodb|memory]
sbt flowServer/run                            # same, from sbt (settings as arguments or FLOW_* variables)
sbt ~flowServer/reStart                       # restarts on every change of the Scala code or of the UI
```

Settings (argument wins over variable): `--ai-record <dir>`/`FLOW_AI_RECORD` and `--ai-replay <dir>`/`FLOW_AI_REPLAY` (see AI nodes), `--host`/`FLOW_HOST` (0.0.0.0), `--port`/`FLOW_PORT` (8080), `--data-dir`/`FLOW_DATA_DIR`
(`./flow-data`), `--db`/`FLOW_DB` (`sqlite`; `memory` keeps nothing, not even flows; `mongodb` keeps everything in MongoDB, see below), `--node-timeout-seconds`/`FLOW_NODE_TIMEOUT_SECONDS`
(300), `FLOW_LOG_LEVEL` (INFO). The data directory holds `flow.db` (instances), `flows.json` (every revision), `triggers.json`,
`connections.json` and `secrets.json` (mode 600, **values in clear**: protect the directory). `-Dflow.ui.skip=true` or
`FLOW_UI_SKIP=true` at build time skips the npm build; `/` then answers 503 and the API still works. The UI build is cached by file times,
so it only runs when `modules/flow/ui` changed.

**Examples** (`--examples`/`FLOW_EXAMPLES`, default true): on a start with no flows and no triggers the server installs, from
`server/src/main/resources/examples`, two flows with their HTTP trigger. They call `jsonplaceholder.typicode.com` (a fake JSON API), so they
need internet: `GET /hooks/examples/posts?user=2` (http.request → script.js → core.respond with JSON) and `GET /hooks/examples/user?id=3`
(two requests in parallel joined by a script into an HTML page answered with `Content-Type: text/html`).

With `--db mongodb` (`--mongo-uri`/`FLOW_MONGO_URI`, default `mongodb://localhost:27017`; `--mongo-database`/`FLOW_MONGO_DATABASE`, default
`flow`) nothing goes to the data directory: the journal of the instances is `flow_events`/`flow_snapshots` (the `mongodb` persistence module),
and flows, triggers, connections, secrets (**values in clear**), deployments and checkpoints are one document each in `flow_state`
(`{_id, json, updatedAt}`, drafts included; the value is JSON text because flows have keys MongoDB does not allow as field names). Every store goes through a
`JsonSlot` (a file or a document), so others can be added the same way.

### What the HTTP layer does (`xplat.flow.http`, all over `F[_]`; `FlowHttp.routes[F]` gives an `HttpRoutes[F]`)

- Every route of the REST table above. `PUT /api/flows/{id}` creates the flow if it does not exist and takes the id from the path.
  There is no authentication on `/api` (put a proxy in front, or add it before exposing the server).
- **Drafts** (`FlowDrafts`, one per flow id, in the slot `drafts`): what is being edited, kept without validating so work that does not
  validate yet is not lost. Only published revisions run, are scheduled or are called as subflows. Publishing validates on the server; a
  `PUT /api/flows/{id}` that saves a revision also removes the draft. The editor opens the draft when there is one, saves to it ("Guardar
  borrador") and only enables "Publicar" when the validator has no errors.
- **Environment variables in connections**: any text of the parameters and any secret value can be `${NAME}` or `${NAME:-default}`
  (`$${` is a literal). A number or boolean field can be a whole reference (`"port": "${DB_PORT}"`) and gets that type. They are resolved
  each time a node asks for the connection (`ConnectionStore.provider`), never stored resolved, so a secret can live only in the environment
  (`"password": "${DB_PASSWORD}"`). The API shows `env` (the names referred to) and `envMissing` (not set and without default); a node that
  uses a connection with a missing variable fails saying which.
- **Node secrets**: `GET` of a flow removes the `secret` fields of the `params` of its nodes; `PUT` keeps the ones a node does not bring
  (matched by node id and kind). The definitions in the store do keep them.
- **Connections** have kinds (`GET /api/connection-kinds`: `secret` and `http` for now; pass more to `ConnectionStore.make`). `PUT
  /api/connections/{name}` takes `{kind, params, secrets}`; a secret that is missing or empty keeps its value. Secret values live in the
  `SecretStore` under `<connection>.<field>`; the API only says which are set (`secretsSet`).
- **Triggers** are flat JSON: `{id, flowId, kind: http|cron|queue|manual, method?, path?, cron?, queue?, inputs?, auth?, response?, timeoutMs?}`.
  `http`, `cron` and `queue` triggers run (see "Queue nodes and triggers" and the cron section; `manual` is only data).
  A `cron` trigger has `cron` (expression) and optional `timezone`; its `inputs` are literal values (no request to read from) and it is
  on as soon as it exists (it does not need the flow to be enabled). A `queue` trigger has `connection`, `queue`, optional `group`
  and `destinationType`.
- **Hooks** (`/hooks/<path>`): found by method and path in a registry that changes at runtime. `inputs` of the trigger map the request into the
  inputs of the flow: a string `"$body.order.id"`, `"$query.x"`, `"$headers.x-user"`, `"$method"`, `"$path"` is a reference, anything else is a
  literal; with no `inputs` the JSON body is the inputs. `auth`: `{scheme: apiKey|bearer|hmac, secret: "<connection>.<field>", header?}`
  (`hmac` = `sha256=<hex>` of the body in `X-Signature`); a secret that is not set rejects everybody. `Idempotency-Key` makes the instance id
  `<trigger>-<key>` (characters outside `[A-Za-z0-9_.-]` become `_`), so a repeated request starts nothing and gets the same answer.
  `response: "sync"` waits (`timeoutMs`, 10 s) for the first `core.respond` node to finish and answers `status`, `headers` and the value at
  `body` of the state; flow ended without one: 200 `{instanceId,status}` (500 if failed); timeout: 202. Async (default): 202 `{instanceId}`.
- `core.respond` (nodes-core): `{status = 200, body?: <state path>, headers?: {}}`. Category `Triggers`, pure.
- SSE (`GET /api/instances/{id}/events`) is the audit stream: `data:` is the `AuditEntry` JSON with `type` = `kind`, `id:` is `seq`, and a
  `keep-alive` comment every 15 s. It ends when the instance ends. There is no separate audit stream.
- `POST /api/flows/{id}/test` takes `{flow?, inputs?, mocks?, realReads?}`; a mock is `{values?, port?, error?}` (what the UI writes) or
  `{type: "output"|"fail", ...}` (what the engine writes).
- Search: `from`/`to` are epoch millis or ISO instants; the response is the flat array, and the header `X-Next-After` has the cursor.
- Errors are JSON: `{errors: [{code, node: null, message}]}` (422 for a flow, a trigger, a connection or inputs that do not fit; 404; 400;
  409 for retrying an instance that has not ended).

### Known gaps (backend side is the contract)

- Times in the audit and instance summaries are **numbers (epoch ms)**; `new Date(n)` handles them.
- A node that will not run shows `skipped` in `GET /api/instances/{id}` (the engine calls it `dead`).
- `retry` answers the **same** `instanceId`.
- No `GET /api/flows/{id}/revisions` yet; the API has no trigger status; `describe` returns `problems` that the UI does not show.
- `ConfigSchema` does not mark which fields are connection fields.
- `/api` has no authentication (put a proxy in front); secrets are stored in clear in `secrets.json` (mode 600).
- In-memory only: stream state (windows, aggregates, live metrics) and the search index (lost on restart).
- Not tested against real services: Postgres, MariaDB, Kafka, ActiveMQ, Anthropic, OpenAI (SQLite, Redis and MongoDB are).
- `script.js` has no memory limit (only the 5 s timeout); AI nodes have no tool use, streaming or images.
- Known flaky test: `ScheduleTest` ("the audit and status show a timer that waits").

### UI specification (`UiSpec`, `xplat.flow.model`)

A `ConfigSchema.Field` may carry `ui`: `{widget?, language?, rows?, placeholder?, help?, options?: [{value,label}], visibleWhen?: {field, equals}, group?, order?, template?, filesField?}`.
Widgets: `text`, `textarea`, `code` (with `language`: javascript, scala, sql, json), `password`, `number`, `checkbox`, `select`, `json`, `files`.
`template` is the value a new node starts with. `filesField` (on a `code`) names the sibling field with more files (`widget: files`, not drawn on
its own): javascript and scala are edited with Monaco, one tab per file (create, rename, delete), and a «Plantilla» button puts the template back.
JavaScript gets a `.d.ts` built from the flow (the state from the inputs and the outputs of the nodes, entities as interfaces, `params` from
`args`, the node's outputs and ports); Scala gets highlighting and suggestions of `state.<path>` as typed circe reads, `out(...)` with the
node's outputs, and the names the other files define. Monaco is loaded the first time a code node is opened. Without `ui` the
editor infers the widget from the `DataType`, `secret` and `oneOf`. The editor has one interpreter (`SpecForm`) for every node and connection kind,
so a new kind needs no editor code. `script.js`, `stream.script`, `db.query`/`db.execute` and `ai.agent` already declare it.

## Entities (business model) — IMPLEMENTED (`modules/flow/entity`, package `xplat.flow.entity`; nodes in `nodes-core`)

The business model is defined once, as entities, and flows use it as types. An entity is data (`EntityDef`, JSON), edited in the UI
(`#/entities`) or written in Scala with the DSL (`xplat.flow.entity.dsl`), which builds the same `EntityDef`. Only the model is kept:
the server stores no records of an entity (they live in the user's databases, reached with the `db.*`/`mongo.*` nodes).

### Format

```json
{"name": "Order", "label": "Pedido", "kind": "root", "key": "id",
 "fields": [
   {"name": "id", "type": {"type": "str"}, "constraints": {"pattern": "^O-[0-9]+$"}},
   {"name": "customer", "type": {"type": "ref", "entity": "Customer"}},
   {"name": "lines", "type": {"type": "list", "element": {"type": "entity", "name": "OrderLine"}}, "constraints": {"minItems": 1}},
   {"name": "total", "type": {"type": "num"}, "computed": {"sum": ["lines", {"path": "subtotal"}]}},
   {"name": "status", "type": {"type": "str"}},
   {"name": "notes", "type": {"type": "str"}, "required": false}],
 "rules": [{"name": "shipped-tracking", "when": {"eq": [{"path": "status"}, "shipped"]}, "check": {"exists": "trackingId"}, "message": "..."}],
 "states": {"field": "status", "initial": "draft", "states": ["draft", "approved", "shipped"],
            "transitions": [{"name": "approve", "from": ["draft"], "to": "approved", "guard": {"gt": [{"path": "total"}, 0]}, "message": "..."}]},
 "revision": 3, "version": 1}
```

- `kind`: `root` has an identity (`key`, a required `str`/`num` field that is not computed) and can be referenced; `value` has none and only
  lives inside others (an address, a line of an order).
- A field: `name`, `type` (any `DataType`), `required` (true), `default`, `constraints`, `computed` (an `Expr`), `label`, `description`.
  Constraints apply to one type: `min`/`max` (num), `minLength`/`maxLength`/`pattern` (str; the pattern is found anywhere unless anchored
  with `^...$`; Java syntax on the JVM, JavaScript in Scala.js), `minItems`/`maxItems` (list), `oneOf` (any).
- Nested values: `{"type": "entity", "name": "OrderLine"}` holds a whole value of another entity (with its rules), alone or in a `list`.
  Relations: `{"type": "ref", "entity": "Customer"}` is the value of the key of a root (it is not checked that it exists).
- `rules`: `check` must be true when `when` is (no `when`: always). `states`: the text field that holds the state, its states, the
  initial one, and the transitions with an optional `guard`.
- Expressions are the `Expr` of the stream nodes, now in `xplat.flow.model.Expr`, with more operators: `sum count min max all any` over
  the items of a list (`{"sum": ["lines", <expression on each item>]}`; without expression, the items themselves; a list that is not there
  is empty), `matches` (`[text, "regex"]`, the pattern a fixed text) and `round` (`[number, decimals]`, half up). Inside a list operator
  the paths are those of the item (the root cannot be read from there).
- `EntityValidator` checks a definition before it is saved (`EntityError`: `code`, `entity`, `field`, `message`): names, the key, that
  the entities named exist and a `ref` points to a root (`unknown-entity`, `ref-to-value`), that no value would hold itself forever
  (`embedding-cycle`: a required, non-list chain back to itself), constraints and defaults that fit their type, expressions that only read
  fields that exist (also inside the items of a list) and computed fields that do not read each other in a circle (`computed-cycle`), and
  consistent states (`invalid-states`).

### A value of an entity (`EntityCatalog.check` / `conform`)

In order: the defaults of the fields it lacks (null is lacking) and the initial state; each field as its type makes it (values of other
entities inside it first, so their computed fields exist); the computed fields, those that others read first (a value that brings one has
it replaced); then the constraints, the state (one of `states`) and the rules. Fields the entity does not declare are kept (a row of a
database may have more columns). Every problem is reported at once as `{path, rule, message}` (`lines.1.qty`, `min`, `must be at least 1`);
`rule` is `type`, `required`, a constraint name, `computed`, `state`, `transition` or the name of a rule. A computed field or a rule that
cannot be evaluated because of another problem says nothing.

`transition(entity, value, name)`: the value must be valid, its state one the transition starts from, the guard must hold, and the
value in the new state must still be valid (rules can depend on the state: "a shipped order needs a tracking id").

### Entities in flows

- **Types.** `DataType` has two new cases, `entity` and `ref`. What an entity is comes from a `Types` (model; `EntityCatalog`
  implements it), passed to `accepts`, `at`, `conformsTo` and the new `conform`. `FlowValidator.validate(flow, catalog, flows, types)`
  follows paths through entity fields (`order.customer`, `order.lines`), and reports `unknown-entity` (with `entity`) for a type that names
  one that does not exist (in inputs, outputs, node outputs or what a node reads). An entity is only itself, or an object whose fields it
  has; an `obj` is never an entity (nothing says it keeps the rules). A `ref` and the type of the key of its entity are interchangeable.
- **Contract.** A value that enters the state with an entity type is made by it: the inputs of an instance (and of a simulation) and the
  outputs of nodes. The made value (defaults, computed fields) is what the event stores, so a replay does not depend on the entity as it is
  later. An input that breaks it is refused (`422`: `input 'order' is not a valid Order: lines.0.qty: must be at least 1`); a node output
  that breaks it makes the node fail.
- **Ports that write some outputs.** `Port.writes` (optional) names the outputs a node writes when it leaves through that port; the engine
  requires only those. The entity nodes use it: through `invalid` they write the problems, not the value.
- **Nodes** (category `Entities`, pure, so they run in simulations; each run uses the entities as they are then):
  - `entity.build` `{entity*, values*}`: a value from `values`, an object whose texts can be references (`"${state.order.id}"`). `out`
    (`value`) or `invalid` (`errors`).
  - `entity.validate` `{entity*, path*}`: the value at `path` (any type) checked. `valid` (`value`, made) or `invalid` (`errors`).
  - `entity.transition` `{entity*, path*, transition*}`: reads `path` as `entity X` (so the value must come typed: an input, a node output
    or `entity.validate`). `out` (`value`) or `rejected` (`errors`).
  - Default outputs: `value` at the path (or at the decapitalised entity name for `build`) typed `entity X`, and `errors` next to it
    (`<name>Errors`, `list[{path, rule, message}]`). An entity that does not exist fails the node. The config field `entity` has
    `ui.widget = "entity"` (the editor offers the entities).
- **Simulation**: a made-up answer of an entity type has a value for each field (an entity inside itself is left empty); if the rules
  refuse it the node fails, and it needs a mock.

### Where entities come from

- `EntityStore` (http): the edited ones, one JSON value (`entities.json`, or the `entities` document in Mongo) with every revision; flows
  use the latest one. Entities of code are read-only: `FlowServer.resource(config, entities = List(...))` and plugins
  (`FlowPlugin.entities`, `FlowPlugin.ApiVersion` 1.1.0; a `process` plugin answers `entities` in `initialize`). A plugin that brings an
  entity another plugin brings fails to load (two majors of one plugin keep the newest one's). When an edited entity and one of code have
  the same name, the one of code is used and the start says so.
- API (`EntityRoutes`):

| Method + path | Purpose |
|---|---|
| `GET /api/entities` | `[{name, kind, label, revision, source, fields}]`; `source`: `store`, `code`, `plugin:<name>`. `?full=true`: the whole entities, each with `source` |
| `GET /api/entities/{name}` · `GET /api/entities/{name}/revisions` | the entity with its `source` · every revision |
| `PUT /api/entities/{name}` | saves the next revision. 200 `{entity, affected: [{flowId, errors}]}` (`affected`: the flows that validated before and do not now; a warning, not a block); 422 `{errors: [EntityError]}` (also for a definition that does not decode, `invalid-entity`); 409 if it is of code |
| `DELETE /api/entities/{name}` | 204; 409 if it is of code, or another entity or a flow uses it (says which); 404 |
| `POST /api/entities/validate` | `{errors}` |
| `POST /api/entities/{name}/check` body `{value, transition?, entity?}` | `{ok, value, problems}`; `entity` is a definition not saved yet, to try it |

### DSL (Scala 2.13 and 3, no macros)

```scala
import xplat.flow.entity.dsl._

val OrderLine = valueObject("OrderLine")(
  field("sku", str).pattern("^[A-Z0-9-]+$"),
  field("qty", num).min(1),
  field("price", num).min(0),
  computed("subtotal", num)(path("qty") * path("price"))
)
val Order = entity("Order", key = "id")(
  field("id", str),
  field("customer", ref("Customer")),
  field("lines", listOf(OrderLine)).minItems(1),
  computed("total", num)(sum("lines")(path("subtotal"))),
  field("status", str),
  field("trackingId", str).optional
).ruleWhen("shipped-tracking", "A shipped order needs a tracking id")(path("status") === "shipped")(exists("trackingId"))
 .lifecycle("status", initial = "draft")(
   transition("approve", from = "draft", to = "approved").guard(path("total") > 0).message("The total must be positive"),
   transition("ship", from = "approved", to = "shipped"))
```

Types `str num bool any listOf obj embed ref`; field options `optional default min max minLength maxLength pattern minItems maxItems oneOf
label description`; entity options `rule ruleWhen lifecycle labelled described`; expressions `path pathOr lit exists not len round ifElse
all oneHolds sum count countWhere minOf maxOf forAll forAny` and the operators `=== =!= > >= < <= && || ! + - * / ++ in matches implies`
(values on the right are numbers, texts, booleans, JSON or expressions). `Order.asJson` is the JSON the editor writes.

### Limits (v1)

No records are kept; a `ref` is not checked to exist; flows always use the latest revision of an entity (a change can break published
flows: saving says which); fields an entity does not declare are accepted; inside a list operator the root of the value cannot be read.

## Planned (designed, NOT implemented)

Agreed in design discussions; nothing of this exists in the code yet. Transactions across nodes were discussed and left out.

### Unified reference syntax — IMPLEMENTED (`xplat.flow.model.Refs`; `args.` comes with connection arguments)

Used by `Template` (Mongo nodes, `http.request` query/headers/body/url), AI prompts, `EnvRefs` (connections) and
`HookRequest` (trigger inputs). The older forms are still read. The editor suggests the references a field can use when
`${` is typed (`state.<path>`, `context.*`); `script.js` and `script.scala` code get the typed suggestions of Monaco instead.

One syntax everywhere: `${origin.path}`, the origin always explicit, the path dotted.

| origin | what | allowed in |
|---|---|---|
| `state.` | flow state: inputs and node outputs (`${state.getProd.documents}`) | node params |
| `context.` | the run (`instanceId`, `flowId`, ...) | node params |
| `args.` | arguments a connection receives from the node that uses it | connection params and secrets |
| `env.` | server environment variables | connections and triggers, never flows (a flow must not read server secrets) |
| `request.` | `request.query.x`, `request.body.x`, `request.headers.x`, `request.path`, `request.method` | http trigger inputs |
| `message.` | `message.body`, `message.key`, `message.headers.x`, `message.source`, `message.position` | queue trigger inputs |

Rules:
- A value that is only the reference (`"${state.qty}"`) is replaced by the value with its type (number, object, list); a
  reference inside text (`"ipa-${args.domain}.internal"`) is rendered as text and interpolated.
- Default: `${env.PORT:-27017}`. Escape: `$${` is a literal `${`.
- Inside JSON/Mongo documents: `{ urn: "${state.urn}" }` (no clash with `$match`, `$in`: those have no braces).
- Validated on save: an origin not allowed in that place is an error; `state.` paths must exist (as `$path` today).
- Not interpreted inside code (`script.js`): there `${}` is JavaScript.
- No transformation language (`split`, `at`...): a value that needs computing comes from a node that provides it.

Replaces: `${NAME}` in connections (read as `${env.NAME}` for compatibility), `{ $path: "x" }` in Mongo nodes,
`{{x}}` in `http.request`, `$query.x` / `$body.x` in trigger inputs.

### Connection arguments

A connection's arguments are the `${args.*}` its params and secrets use (deduced, not declared):

```
connection ipa (mongodb)
  host:       ipa-${args.domain}.internal
  authSource: ${args.authDb:-admin}
  password:   ${env.IPA_PASSWORD}
```

- The view adds `args: [{name, required, default?}]` next to `env` / `envMissing`. A connection rejects `${state.*}`.
- Every node that takes `connection` also takes `args` (templates: `{ domain: "${state.domain}" }`). The UI shows the
  connection's arguments as fields once it is chosen.
- Flow validation: missing required argument, unknown argument, unknown connection, bad `state.` path → errors (draft).
- Saving a connection that adds a required argument warns with the flows that do not pass it (not a block); those
  flows fail at run time with the same message.
- At run time: resolve the node's `args` → resolve the connection (`args.`, `env.`) → ask the pool with that config.
  The test-mode trace shows the resolved connection without secrets.
- Safety: an argument interpolated into text accepts only `[A-Za-z0-9._-]` by default; a connection can set
  `argPatterns: { domain: "^(fline|mobile)$" }` (also an allow-list of reachable targets).

### Connection pool — IMPLEMENTED (`xplat.flow.nodes.ClientPool`)

`MongoPool`, `DbPool` and `PublisherPool` are now kinds of one `ClientPool` that the server creates (`--pool-idle-seconds`,
`--pool-max`, `--pool-acquire-seconds`, `--pool-shutdown-seconds`); nodes borrow with `use`, and a stream sink keeps its
publisher for the whole stream. `ConnectionStore.inSlot(onChange = ...)` retires a connection's clients when it is saved or
deleted. API: `GET /api/connection-clients`, `POST /api/connections/{name}/close`. An edited connection's old client is no
longer closed at once: it is retired, or (if the key changed without a save, e.g. an environment variable) closes when idle.

A server resource (`Resource[F, ConnectionPool[F]]`), created empty at start, closed at shutdown (waits for running
operations up to a grace time). It manages clients; each client keeps its own socket pool (`maxPoolSize`).

- Key: kind + resolved config (params and secrets after `args.` and `env.`). `ipa` with `domain=fline` and with
  `domain=mobile` are two clients; runs with the same key share one.
- Lazy creation, once per key even under concurrent requests. A creation error fails the node and keeps nothing
  (optional short backoff).
- A node borrows the client for the operation; a borrowed client is never closed.
- Closed when idle for `--pool-idle` (10m); above `--pool-max` (50) the least recently used idle client is closed; when
  all are borrowed a request waits up to `--pool-acquire-timeout` (30s). Shutdown grace `--pool-shutdown-grace` (15s).
- Editing or deleting a connection retires its clients: no new borrows, closed when the last borrow returns. A changed
  environment variable gives a new key, so the old client just goes idle.
- Generic: each connection kind registers how to open and close its client; `MongoPool` becomes the Mongo part.
- The connections view shows open clients per connection (`ipa: fline (3 in use), mobile (idle 4 min)`) and an action
  to close them.

### Plugins

**Status:** IMPLEMENTED:
- the `jvm` transport: `flow-nodes-api` (`modules/flow/nodes-api`: `FlowPlugin`, `PluginManifest`, `JavaFlowPlugin` for
  Java/Kotlin, and `NodeType`, `Connections`, `ClientPool`, `ConnectionKind` moved there), the loader
  `modules/flow/plugins` (`PluginLoader`, `PluginClassLoader`), and an example with a Scala and a Java node
  (`modules/flow/plugin-example`);
- the `process` transport (`ProcessPlugin`, `RpcProcess`): JSON-RPC 2.0 over stdio, one message per line; `initialize`
  answers the descriptors as data (static `ports`, `inputs`, `outputs`), `run` gets the resolved `NodeInput` and, when the
  params name a `connection` of one of the plugin's own kinds, that connection with its secrets; `$/cancel` when a node
  times out; `shutdown` on stop; stderr goes to the server log; a process that dies is started again on the next call.
  `simulate` is not sent (the engine answers with default values). Python SDK: `modules/flow/plugin-sdk/python/xflow_plugin.py`
  (one file, standard library only), example in `modules/flow/plugin-example-python`;
- `--plugins-dir` / `--plugins-strict`, `GET /api/plugins` and the Plugins view of the UI; the editor marks nodes on an old
  major (`major viejo`) and the palette only offers the newest one.

- the testkit `flow-nodes-testkit`: `PluginHarness` loads a plugin as the server does (`jvm(jar, manifest)` or
  `directory(dir)` for any transport) and `PluginContract` is the suite a plugin extends (kinds named
  `<plugin>.<kind>@<major>`, names once each, descriptor functions total, pure nodes never throw, samples answer through
  their ports with declared outputs). The Scala/Java example and the Python one pass it.

Pending: starter templates, `simulate` for process plugins.

**`script.scala`** (`modules/flow/nodes-scala`, IMPLEMENTED): Scala written in the editor, the body of
`(state, memory, params, log) => Json` with circe, `cats.syntax.all._` and `out(...)` / `via(port, ...)` imported; it
returns what `script.js` returns. Compiled with the compiler of the server's own Scala version (2.13 or 3) the first time
it runs, cached by text; errors carry the user's line numbers. Its `files` (`helpers.scala`) are compiled in the same class,
before `apply`, so their `def`, `val`, `case class`, `object` and `import` are seen by the code in 2.13 and 3; errors say `<file> line N`. It is NOT sandboxed; the server offers it by default and
`--scala-nodes false` (`FLOW_SCALA_NODES`) turns it off; a timeout stops waiting but cannot stop the thread.

Like n8n community nodes or Terraform providers: node kinds (and connection kinds) that are not part of the server,
loaded at start. A plugin can be written in any language; only the transport changes.

**The contract is data.** A node is `describe` → `NodeDescriptor` and `run(NodeInput)` → `NodeOutput`, plus optional
`simulate(NodeInput)` → `NodeOutput`. All three already have JSON codecs (the UI and the audit use them), so the JSON
shapes are the plugin API, versioned on their own (`apiVersion`, semver: a server accepts plugins of its major version).

- `flow-nodes-api` (new, small, cross JVM): the contract types and their codecs, `NodeType[F]`, `ConfigSchema`,
  connection kinds and the `Connections` view. The server provides it (and cats-effect, circe) as `Provided`.
- Manifest `flow-plugin.json` in every plugin: `name`, `version`, `apiVersion`, `transport` (`jvm` | `process`), and
  for `process` the command to start it.
- Kind names: `<plugin>.<node>@<major>` (`ipa.lookup@1`). The prefix means two plugins never collide; a kind
  registered twice stops the start with an error naming both plugins. Only the major version goes in the kind:
  minor and patch releases must stay compatible (same params, same outputs) and replace the old one in place.
  A breaking change is a new major, and two majors of a plugin can be installed side by side
  (`plugins/ipa@1/`, `plugins/ipa@2/`), so published flows keep running on `@1` until they are migrated.
  The editor offers the newest major for new nodes and marks nodes on an older one.

**Transports**

1. `jvm` — a jar (Scala, Java, Kotlin) with `ServiceLoader` entries for `FlowPlugin`. In process, in its own
   child-first classloader whose parent exposes only the API. The only transport with direct access to the pools of
   connections. Java/Kotlin get an adapter without `F[_]`: `run(input) → CompletableFuture<NodeOutput>`.
2. `process` — any executable (Python, Go, Node, Rust…) speaking JSON-RPC 2.0 over stdin/stdout, one JSON per line
   (methods `describe`, `run`, `simulate`, `shutdown`; `$/cancel` for a run the engine stops). The server starts it,
   asks `describe` and registers a bridge `NodeType` per kind. Calls are concurrent (ids). A process that dies fails
   the runs in flight, is restarted with backoff and reported in the admin view. The same protocol over HTTP is the
   remote variant (a plugin deployed as its own service), added only if needed.
3. Later, optional: WASM (Chicory, pure JVM) for code from untrusted authors: in process but really sandboxed; I/O
   only through functions the host exposes.

GraalJS (`nodes-script`): the code of a script node is part of the flow, not a plugin; the script node kinds themselves
come from the built-in plugin `script` (see "Everything is a plugin").

**What crosses the boundary**

- Params: every plugin node receives its params already resolved (`${state.x}`, `${context.key}` replaced by their
  values; done by the loader with `Template` for `jvm`, by the bridge for `process`). A reference that cannot be
  resolved fails the node without running the plugin's code, and the validator sees the paths the references read.
- Connections: a `jvm` plugin borrows clients from the pool. A `process` plugin receives, in `run`, everything
  already resolved: the node's params (references to the state, `args.` and `env.` substituted) and the config of
  the connections its descriptor declares, secrets included. It keeps its own clients. Only the connections the
  node declares are sent, never the rest.
- Simulation: `simulate` is optional; without it the engine answers with default outputs, as for any node.
- Stream nodes: not in the first version of the protocol (request/response only).
- Timeouts and cancellation: the engine's node timeout applies; on cancel the bridge sends `$/cancel`.
- No sandbox for `jvm` and `process`: a plugin runs with the server's permissions. Only install trusted plugins.

**Deploy**

- Plugins live in `--plugins-dir` (`FLOW_PLUGINS_DIR`, default `<data-dir>/plugins`), one directory per plugin with
  its manifest. Adding or updating one needs a restart. `GET /api/plugins` and the admin view list the loaded plugins,
  their kinds and their state (loaded, failed with the reason, process restarting).
- A plugin that fails to load does not stop the server unless `--plugins-strict true`; flows that use its kinds fail
  validation with "unknown kind" as today.
- Docker: the server image plus the plugins (copied into the image, or a volume mounted on the plugins dir); the image
  of a `process` plugin must contain its runtime (python, node…). Kubernetes: the same image for every replica, so
  all servers have the same plugins (needed in Mongo mode, where any server may run any instance).
- A published revision records the exact plugin version (`1.4.2`) its nodes ran with, for the audit; starting a server whose plugin lacks a kind that a
  published flow uses logs a warning per flow.

**Testing**

- `flow-nodes-testkit` (JVM): runs a `NodeType` without engine or server (`run`/`simulate` with an input built by
  hand, fake `Connections`), and a contract suite every plugin can run: the descriptor decodes, the declared outputs
  match what `run` answers, `simulate` does not touch the outside.
- Process plugins: the same contract suite drives the executable over stdio (`ProcessPluginContract("python main.py")`),
  so a Python plugin is checked by the same tests as a jar. Each SDK (Python, TypeScript) also has its own unit tests
  of the protocol.
- In the server: tests with an example plugin of each transport under `plugins/` (loading, prefixing, collisions,
  a process that dies and restarts, cancellation), and an end-to-end flow test that runs a plugin node.
- Starter templates (sbt for JVM, Python, TypeScript) come with an example node and its test using the testkit.

**Later:** REST nodes defined only in JSON (method, URL, mappings), no compiling; in the UI spec, fields shown
depending on another field's value.

### Everything is a plugin

**Status:** IMPLEMENTED.

The server is an engine that knows no kind of node: every kind of node comes from a plugin. Nine plugins come with the server,
`core`, `entity`, `script`, `db`, `mongo`, `http`, `queue`, `stream` and `ai`; they implement the same `FlowPlugin` as the
external ones and `GET /api/plugins` lists them first, with `source: "builtin"` (`"directory"` for the others). The engine still
brings the core nodes itself when it is used as a library (`ActorFlowEngine.make(..., coreNodes = true)`, the default, for
tests); the server says `coreNodes = false` and gets them from the plugin `core`.

**Two kinds of service**

- *Services of the server*, lent to every plugin in `FlowPlugin.Context`: `connections`, `clients` (the pool),
  `runner` (starts other flows: `core.subflow`, `stream.sink.flow`), `metrics` (how stream segments go: the status of the
  instances shows it) and `entities` (the catalog of the business model as it is now: `entity.*`). The runner and the entities
  work once the engine and the entity store exist; nodes only use them when they run. `Context.basic(connections, clients)`
  gives one without them, for tests and tools.
- *Services of a plugin*: what it builds in `install` (the JDBC pool of `db`, the Mongo pool of `mongo`, the publishers of
  `queue` and `stream`, the HTTP client of `ai`). Its nodes see them by closure; no other plugin does. There is no registry of
  services.

**API** (`FlowPlugin.ApiVersion` 1.2.0)

```scala
trait FlowPlugin {
  def install[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, FlowPlugin.Installed[F]] // = nodes(context).map(Installed(_))
  def nodes[F[_]: Async](context: FlowPlugin.Context[F]): Resource[F, List[NodeType[F]]] = Resource.pure(Nil) // API 1.0/1.1
  def connectionKinds: List[ConnectionKind] = Nil
  def entities: List[EntityDef] = Nil
}
final case class Installed[F[_]](nodes: List[NodeType[F]], triggers: List[TriggerSource[F]] = Nil)

trait TriggerSource[F[_]] {              // xplat.flow.nodes
  def kind: String                       // the kind of trigger it serves: "queue"
  def accepts(connectionKind: String): Boolean
  def run(trigger: Json, connection: Connections.Resolved, start: TriggerSource.Start[F]): F[Unit] // until cancelled
}
// start(request, key, position): F[Accepted | Rejected(reason)]
```

A minor version: jars built for 1.x keep loading (they implement `nodes`, which `install` calls). Recompiling the source of a
1.x plugin needs `override` on `nodes`, or moving to `install`. `FlowRunner` and `SegmentMetrics` moved to `flow-nodes-api`
(same packages) so the context can carry them.

**Built-in plugins** (`PluginLoader.Builtin(name, plugin)`, given by `FlowServer` in this order, before `--plugins-dir`)

| Plugin   | Class                              | Kinds                                   | Its own services            |
|----------|------------------------------------|-----------------------------------------|-----------------------------|
| `core`   | `xplat.flow.engine.CorePlugin`     | `core.if`, `subflow`, `respond`, `cron` | (runner of the context)     |
| `entity` | `xplat.flow.nodes.EntityPlugin`    | `entity.build`, `validate`, `transition`| (entities of the context)   |
| `script` | `xplat.flow.script.ScriptPlugin`   | `script.js`, `script.scala` (if `--scala-nodes`) | —                  |
| `db`     | `xplat.flow.db.DbPlugin`           | `db.*` + JDBC connection kinds          | `DbPool`                    |
| `mongo`  | `xplat.flow.mongo.MongoPlugin`     | `mongo.*` + `mongodb`, `queue` triggers on change streams | `MongoPool` |
| `http`   | `xplat.flow.rest.HttpPlugin`       | `http.request`                          | —                           |
| `queue`  | `xplat.flow.queue.QueuePlugin`     | `queue.publish` + broker kinds, `queue` triggers on them | `PublisherPool`, consumers |
| `stream` | `xplat.flow.stream.StreamPlugin`   | `stream.*`                              | `PublisherPool`, consumers  |
| `ai`     | `xplat.flow.ai.AiPlugin`           | `ai.agent` + AI kinds                   | HTTP client, cassettes      |

- Their settings are given when they are made (`new AiPlugin(cassettes)`, `new ScriptPlugin(scala = ...)`), not through the
  context.
- Their kinds keep the names they always had (`db.query`, not `db.query@1`): they are versioned with the server, so stored flows
  need no migration. A built-in kind must start with `<plugin>.`; a kind declared twice, a kind of connection that already
  exists or entities in conflict stop the start (a built-in plugin never fails quietly).
- They resolve their own `${...}` references (the loader does it only for external plugins).
- An external plugin cannot take the name of a built-in one, nor declare one of its kinds of connection.

**Between plugins.** No plugin reaches another's services. `stream.script` is made by `script` code but registered by
`stream`: the server makes `new StreamPlugin(extra = ScriptNodes.streamExtra)`. `queue` and `stream` each open their
publishers on the shared client pool of the server, so a connection still has one client.

**Triggers.** The server keeps the triggers (`/api/triggers`, `TriggerRegistry`); plugins bring the *sources* that read them.
`TriggerSources.resource` (`flow-http`) runs, for each trigger whose kind a source serves and whose connection is of a kind it
accepts, that source (the first installed that accepts it), and does what is common to all: it polls the triggers and the
connections every 2 s and starts, restarts or stops the sources; it starts a source again 5 s after it stops; `start` maps the
request to the inputs with the trigger's `inputs`, names the instance `<trigger>-<key>` (so a key delivered again starts nothing)
with origin `trigger:<id>`, and turns `InvalidInputs` into `Rejected` (logged; the source acknowledges and drops it). A trigger
whose connection no source reads does not run (logged at debug). `GET /api/plugins` lists the kinds of trigger of each plugin
(`triggers`). External `jvm` plugins can bring sources too; `process` plugins cannot yet.

**What stays in the server:** `http` and `manual` triggers (they are its HTTP layer), `cron` triggers (they share the scheduler,
its checkpoints and its misfire rules with the cron launchers of flows), the stores and the kinds of connection
`ConnectionKind.defaults` (`http`, `secret`).

### Bundles: moving flows between servers

**Status:** IMPLEMENTED: export, import with plan, the directory format, the CLI and the UI ("Mover").

One server per environment (dev, staging, prod): they share nothing, not even secrets. What moves between them is a *bundle*, a
JSON document stable enough to keep in git (keys sorted, two spaces, no revisions), so promotion is a pull request plus a CI
job that imports it.

```json
{ "format": "x-flow-bundle", "version": 1,
  "flows":       [{ "flow": FlowDef (no revision, no secret params), "enabled": true }],
  "triggers":    [TriggerDef],
  "entities":    [EntityDef (no revision)],              // only the ones edited in the server, not those of code or plugins
  "connections": [{ "name", "kind", "params" }],          // never secrets
  "requires":    { "kinds": ["db.query", ...], "connectionKinds": ["postgres", ...] } }
```

- `GET /api/bundle?flows=a,b` exports those flows (all if not given): their latest revision, whether they are enabled, the
  triggers that start them, the connections their nodes (`params.connection`) and triggers name, every entity edited in the
  server, and what the target needs to have.
- `POST /api/bundle?dryRun=true` (body: a bundle) answers the plan without changing anything:
  `{items: [{type: flow|trigger|entity|connection, id, action: create|update|same|skip|draft}], problems: [...], warnings: [...]}`.
  Problems stop the import: a kind of node or of connection the server does not have, a flow or an entity that does not
  validate, a format it does not know. Warnings do not: a connection created without its secrets, a secret field of a node
  that the target does not have yet.
- `POST /api/bundle` imports it if the plan has no problems (otherwise `422` with the plan), in order: entities, connections,
  flows (a new revision only if they changed), triggers, enabled. 200 with the plan as it was applied.
- **Connections belong to the environment**: an import creates the ones that are missing (without secrets: they are set in the
  target) and never changes one that exists (`skip`, with a warning if it differs).
- **Secrets** never travel: secret params of nodes are left out on export and kept from the flow of the target on import.
  A flow that still lacks some (it is new in the target, or a node got a new secret field) goes in as a **draft** (`draft`,
  with a warning naming `node.field`): it is completed and published in the target, and it is not enabled until then. The
  plan validates it with stand-ins for those secrets, so every other mistake is still a problem.
- Nothing is deleted: a flow that is not in the bundle stays.
- Not atomic: the plan is checked first; a failure while applying (the store is down) answers what was done.
- **Directory** (`BundleFiles`): `bundle.json` (format, version, requires) and one file per item in `flows/`, `triggers/`,
  `entities/`, `connections/` (`<id>.json`; ids that are not safe file names are refused). An export replaces the `.json` files
  of those folders, so a deleted item is a deleted file.
- **CLI** (`flow-server.jar bundle ...`, `BundleCli`): `export --server URL [--flows a,b] (--dir D | --file F)`,
  `plan` and `import` with the same `--server` and `--dir`/`--file`. `plan` and `import` print the changes, problems and
  warnings and end with code 1 when there are problems, for CI.
- **UI**: the view "Mover" (`#/bundle`) downloads a bundle of the chosen flows and imports one after showing its plan.

### Metrics, traces and health

**Status:** metrics, health and traces IMPLEMENTED; the `meter` of plugins and consumer lag PLANNED.

- **Metrics** (`GET /metrics`, Prometheus text, no library): `FlowMetrics` (engine) is a `FlowObserver[F]`
  (`ActorFlowEngine.make(observer = ...)`), which the `EventHub` calls with each entry of the audit as it is published (not the
  ones read back from the journal), so no node measures itself:
  - `flow_instances_started_total{flow, origin}` (the kind of origin), `flow_instances_finished_total{flow, status}`,
    `flow_instance_duration_seconds{flow}` (histogram), `flow_instances_running{flow}`;
  - `flow_node_duration_seconds{kind}` (histogram), `flow_node_failures_total{flow, node, kind}` (a failure the flow handles
    through the `error` port counts too), `flow_retries_total{flow}`;
  - `flow_trigger_source{trigger, state=running|waiting|unserved}` (`TriggerSources.View`), `flow_client_pool{connection,
    state=in_use|idle}`.
  - An instance that started before this server did is counted under `flow="unknown"` when it ends. Each server measures what
    it runs: Prometheus adds the replicas.
- **Health** (`GET /api/health`, view "Salud" `#/health`, refreshed every 10 s): per flow, the instances of the last 24 h on
  this server (up to 1000 per flow): running, succeeded, failed, p50 and p95 of the duration, the latest failures with the node
  and error that failed them (linking to their audit); and the state of each trigger source.
- **Traces** (OpenTelemetry, OTLP/HTTP; on with `--otel-endpoint http://collector:4318` / `OTEL_EXPORTER_OTLP_ENDPOINT`, service
  `--otel-service` / `OTEL_SERVICE_NAME`, default `x-flow`). `FlowTracing` (engine, no OpenTelemetry in it) is another
  `FlowObserver` (`FlowObserver.all`): it makes the spans from the audit, with the times the audit recorded, and hands finished
  ones to `OtlpTraces` (server), which turns them into the SDK's `SpanData` and sends them in batches (512, or every 2 s; the
  rest when the server stops; a collector that fails loses spans, never instances).
  - A span `flow <id>` (kind server) per instance, with `flow.id`, `flow.revision`, `flow.instance`, `flow.origin`,
    `flow.status`, and error status with the node and message that failed it; a span `<kind> <node>` per run of a node under
    it, with `flow.node`, `flow.node.kind`, `flow.node.attempt`, `flow.node.port`, `flow.child_instance`.
  - **In**: an HTTP trigger with a `traceparent` header, and a message whose headers carry one (`TriggerSources`), start an
    instance on that trace (`FlowTracing.link` before the start; a hook without `idempotency-key` gets its instance id then).
    A subflow goes under the span of the node that called it.
  - **Out**: `Traces[F]` is a service of the server in `FlowPlugin.Context` (`traces`): the `traceparent` of a running node,
    known before it runs because the engine publishes the start of a node before launching it. `http.request`,
    `queue.publish`, `stream.sink.http` and `stream.sink.publish` send it as the `traceparent` header, unless the node's own
    headers set one.
  - Not traced: what runs after a retry of an instance that had ended (its spans are already sent), and instances that
    started before the server did.
- **Plugins**: the `Context` gets a `meter` for a plugin's own metrics, named `plugin_<name>_*`.
- **Alerts** are Prometheus/Grafana rules, not the server's; documented examples: failure rate of a flow, lag of a queue
  trigger, a cron flow that did not run.
- **UI, "Salud" view**: per flow, success rate and p95 duration over the last hour/day, latest failures linking to their audit,
  and the state of each trigger source (running, waiting for its connection).

### Resources of plugins (their UI as data)

**Status:** PLANNED.

A plugin can bring kinds of *resource*: records it owns, which the admin edits in the UI (a git repository of code, a model,
a mapping table). Like kinds of connection, they are declared as data and the UI draws them; a plugin never ships JavaScript.
That keeps one version of the UI, keeps foreign code out of the admin's browser and works for `process` plugins too (it is
all JSON).

```scala
// FlowPlugin
def resources: List[ResourceKind] = Nil

final case class ResourceKind(
  kind: String,                      // "library": its records are `<plugin>.<kind>` ("library.library")
  label: String,                     // "Librerías de código"
  config: ConfigSchema,              // the form: fields, secrets, `UiSpec` widgets (as for connections)
  actions: List[ResourceKind.Action] = Nil,  // buttons: Action("sync", "Sincronizar")
  status: Boolean = false            // whether each record has a status the plugin reports
)

// FlowPlugin.Context: the records of the plugin's own kinds, with their secrets
resources: Resources[F]   // list(kind), get(kind, id), changes: fs2.Stream[F, Resources.Change]

// What the plugin implements (in `Installed`)
final case class ResourceHandler[F[_]](
  kind: String,
  action: (String, Resources.Record) => F[Either[String, Json]], // (action, record) → message or problem
  status: Resources.Record => F[ResourceStatus]
)
final case class ResourceStatus(state: String /* ok | working | failed */, summary: String, details: Json, problems: List[Problem])
final case class Problem(message: String, file: Option[String], line: Option[Int])
```

- **Storage**: the server keeps the records like connections (a JSON slot or Mongo, secrets in the secret store, redacted in
  answers). A plugin only sees the records of its own kinds.
- **API**: `GET/PUT/DELETE /api/plugins/{plugin}/resources/{kind}[/{id}]`, `POST …/{id}/actions/{action}`,
  `GET …/{id}/status`. `GET /api/plugins` lists each plugin's kinds of resource with their schema.
- **UI**: the Plugins view gets a page per kind of resource: a table of records with their status badge, the form generated
  from `config` (`ConfigForm`), the action buttons and a status panel (summary, problems with file and line). Nothing of it
  is specific to a plugin.
- **`process` plugins**: `initialize` answers `resources`; the methods `resource/action` and `resource/status`.
- **Validation**: the server checks a record against `config` before saving; the plugin can refuse it in its status.

### Shared code from git (`library` plugin)

**Status:** PLANNED (after "Resources of plugins").

Code shared by script nodes lives in a git repository and is edited in a real IDE, with reviews, tests and versions; the
platform does not grow an IDE. The built-in plugin `library` loads it; `script` uses it.

- **A library** is a resource of `library`: `{name, url, ref, path = "/", language: scala | js, token? (secret)}`. Any git host;
  `token` for private repositories. An allow-list of hosts/organisations (`--libraries-allow`) limits where code can come from,
  since `script.scala` is not sandboxed.
- **Sync** (action `sync`, and on start): fetch `ref`, resolve it to a commit SHA, compile the files of `path`:
  - Scala: the `.scala` files compiled with the server's compiler against the classpath of `script.scala` (Scala library, circe,
    cats; no Maven dependencies in v1) into classes cached by SHA in `<data-dir>/libraries/<name>/<sha>/`.
  - JS: the `.js` files concatenated into one source, loaded in the sandbox before the node's code.
  - The status shows the SHA, the time, and the compile errors with file and line. A failed sync keeps the last good SHA.
- **Use from a node**: `script.scala` and `script.js` get a field `libraries: ["pricing@v1.4.0"]` (`name@ref`; `name` alone
  is the ref of the resource). The code imports what the library defines (`import pricing._`). The editor completes the names
  the library exports, as it does for `files`.
- **Versions and replay**: a ref that is a tag or a commit is fixed. A branch is resolved to its SHA when the flow is published,
  and the SHA goes in the published revision and in the audit of the instance, so a push never changes what a published flow
  runs and a replay compiles the same code.
- **Replicas** (Mongo mode): the resolved SHA is stored with the resource, so every server fetches and compiles the same commit.
- **Between plugins**: `script` reads the compiled libraries through a service of `library` passed when the server makes them
  (`new ScriptPlugin(libraries = ...)`), as `stream` gets `stream.script`.
- **Out of v1**: Maven dependencies, libraries depending on libraries, webhooks from the git host (sync is manual or periodic).

### Scala nodes compiled from the editor

Scala code written in the editor becomes a versioned artifact, not text run on the fly.

1. A Scala node (or a Scala module shared between flows) has its code (classes implementing the node API) and extra
   dependencies in sbt form; `flow-nodes-api` is included as `Provided`.
2. On draft save / validation the server writes an sbt project, compiles it and packages the jar.
3. Compiler errors (file, line, column, message) come back as node errors; the flow stays a draft.
4. The jar's identity is the hash of code + dependencies; a published revision points to that hash.
5. At run time the node loads the jar (classloader cached by hash, as for `jvm` plugins) and runs it.

- Speed: a warm sbt (or Bloop) build service, 1–3 s per recompile; same hash, no compile; compilation is asynchronous
  (the editor shows "compiling…").
- Dependencies via Coursier with an allow-list of repositories (maybe organizations).
- Durability: jars are immutable; an instance retried later runs the code of its revision. A jar is removed only when
  no revision references it; its classloader is released when no published revision or live instance uses it. In
  Mongo mode jars are kept in GridFS so every server has them.
- Security: in-process by default with a separate permission ("may write Scala"); the interface allows running the jar
  in a separate worker (JVM or container) when code comes from untrusted editors.
- The same API as plugins: a Scala node that grows can move to a plugin or module unchanged.

### Where things live

Everything local is under the data directory (`--data-dir`, `./flow-data` by default); plugins under `--plugins-dir`,
`<data-dir>/plugins` by default:

```
<data-dir>/
  plugins/<name>/                  one plugin: either jars dropped in by hand, or an sbt project generated for Scala nodes
  plugins/<name>/build.sbt         (generated) scalaVersion, flow-nodes-api % Provided, the user's dependencies
  plugins/<name>/src/main/scala/   (generated) the code from the editor
  plugins/<name>/jars/<hash>.jar   built jars, immutable, kept while a revision references them
```

Generated projects stay there so they can be opened, built and debugged with a normal sbt or IDE.
