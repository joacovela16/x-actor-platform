// Motor de mentira: valida y ejecuta flujos siguiendo las reglas del DESIGN.md (puertos, broadcast, join, error,
// RunPolicy, subflows, temporizadores cron y queue.publish) y produce una auditoría con las formas REALES del backend
// (engine/AuditEntry.scala). No es el engine real: no conecta a brokers ni espera de verdad en las pruebas.
import type { Edge, FlowDef, FlowError, JsonValue, Mock, NodeDef, TraceStep, Trace } from '../src/api/types.ts';
import { parseMapParams, parseExpr } from '../src/graph/expr.ts';
import { condenseFlow, edgeProblem, findSegments, SEGMENT_KIND, type NodeIO } from '../src/graph/stream.ts';
import { runSegment } from './stream.ts';
import { parseCron } from '../src/graph/cron.ts';
import { catalog } from './data.ts';

type Json = JsonValue;

/** Marca de valor redactado en la auditoría. */
export const REDACTED: Json = { $redacted: true };

export function kindPorts(node: NodeDef): string[] {
  const entry = catalog.find((c) => c.kind === node.kind);
  if (entry) return [...new Set([...entry.ports.map((p) => (typeof p === 'string' ? p : p.name)), 'error'])];
  if (node.kind === 'core.if') return ['then', 'else', 'error'];
  if (node.kind === 'core.subflow' || node.kind === 'core.noop' || node.kind === 'core.cron' || node.kind === 'queue.publish') return ['out', 'error'];
  return ['error'];
}

/** Forma de los nodos para la validación de streams: salidas, entradas y papel, del catálogo. */
export function ioOfFlow(flow: FlowDef): Record<string, NodeIO> {
  const io: Record<string, NodeIO> = {};
  for (const n of flow.nodes) {
    const entry = catalog.find((c) => c.kind === n.kind);
    if (!entry) continue;
    const outs = entry.ports.map((p) => (typeof p === 'string' ? { name: p } : p));
    io[n.id] = { role: entry.role ?? 'node', outs, ins: entry.inputs ?? [{ name: 'in' }] };
  }
  return io;
}

function checkConfig(kind: string, params: Record<string, Json>): string[] {
  const entry = catalog.find((c) => c.kind === kind);
  if (!entry) return [];
  const known = new Set(entry.config.fields.map((f) => f.name));
  const problems: string[] = [];
  for (const f of entry.config.fields) {
    const v = params[f.name];
    if (v === undefined) {
      if (f.required !== false && f.default === undefined) problems.push(`missing field '${f.name}'`);
      continue;
    }
    const t = f.type.type;
    const ok = t === 'str' ? typeof v === 'string' : t === 'num' ? typeof v === 'number' : t === 'bool' ? typeof v === 'boolean' : true;
    if (!ok) problems.push(`field '${f.name}' must be ${t}`);
    else if (f.oneOf?.length && !f.oneOf.some((o) => JSON.stringify(o) === JSON.stringify(v)))
      problems.push(`field '${f.name}' must be one of ${f.oneOf.map((o) => JSON.stringify(o)).join(', ')}`);
  }
  for (const k of Object.keys(params)) if (!known.has(k)) problems.push(`unknown field '${k}'`);
  // checkParams de los nodos de stream: las expresiones (map, filter) se leen como en el servidor.
  if (kind === 'stream.map') {
    const p = parseMapParams(params);
    if (!p.ok) problems.push(p.error);
  }
  if (kind === 'stream.filter' && params.where !== undefined) {
    const p = parseExpr(params.where);
    if (!p.ok) problems.push(`where: ${p.error}`);
  }
  // checkParams del backend: la expresión y la zona de los nodos cron.
  if (kind === 'core.cron') {
    const expr = typeof params.expression === 'string' ? params.expression : '';
    const parsed = parseCron(expr);
    if (!parsed.ok) problems.push(`expression: ${parsed.error}`);
  }
  return problems;
}

function hasCycle(flow: FlowDef): boolean {
  const adj = new Map<string, string[]>();
  for (const e of flow.edges) adj.set(e.from, [...(adj.get(e.from) ?? []), e.to]);
  const state = new Map<string, 1 | 2>();
  const visit = (n: string): boolean => {
    if (state.get(n) === 1) return true;
    if (state.get(n) === 2) return false;
    state.set(n, 1);
    for (const m of adj.get(n) ?? []) if (visit(m)) return true;
    state.set(n, 2);
    return false;
  };
  return flow.nodes.some((n) => visit(n.id));
}

/** Validación de mentira: cubre lo más visible (kinds, configuración, puertos, ciclos, flujos llamados). */
export function validate(flow: FlowDef, flowsKnown: Set<string>): FlowError[] {
  const errors: FlowError[] = [];
  const ids = new Set<string>();
  for (const n of flow.nodes) {
    if (ids.has(n.id)) errors.push({ code: 'duplicate-node', node: n.id, message: `there are several nodes called '${n.id}'` });
    ids.add(n.id);
    if (!catalog.some((c) => c.kind === n.kind)) {
      errors.push({ code: 'unknown-kind', node: n.id, message: `node '${n.id}' is of kind '${n.kind}', which does not exist` });
      continue;
    }
    const problems = checkConfig(n.kind, n.params);
    if (problems.length) errors.push({ code: 'invalid-params', node: n.id, message: `node '${n.id}' is badly configured: ${problems.join('; ')}` });
    if (n.kind === 'core.subflow' && typeof n.params.flowId === 'string' && !flowsKnown.has(n.params.flowId))
      errors.push({ code: 'unknown-flow', node: n.id, message: `node '${n.id}' calls the flow '${n.params.flowId}', which does not exist` });
  }
  for (const e of flow.edges) {
    const from = flow.nodes.find((n) => n.id === e.from);
    const missing = [e.from, e.to].filter((id) => !flow.nodes.some((n) => n.id === id));
    for (const m of missing) errors.push({ code: 'unknown-node', node: null, message: `the edge ${e.from} -> ${e.to} uses the node '${m}', which does not exist` });
    if (from && !kindPorts(from).includes(e.port))
      errors.push({ code: 'unknown-port', node: from.id, message: `node '${from.id}' has no port '${e.port}'; its ports are ${kindPorts(from).join(', ')}` });
  }
  if (hasCycle(flow)) errors.push({ code: 'cycle', node: null, message: 'the flow has a cycle' });
  // Streams: las aristas (token con stream, tipo, entrada) y los segmentos sin fuente o sin sumidero.
  const io = ioOfFlow(flow);
  for (const e of flow.edges) {
    const p = edgeProblem(e, io);
    if (p) errors.push({ code: p.code, node: e.to, message: p.message });
  }
  for (const seg of findSegments(flow.nodes, flow.edges, io)) {
    if (seg.sources.length === 0) errors.push({ code: 'stream-without-source', node: seg.members[0], nodes: seg.members, message: `the stream among ${seg.members.join(', ')} has no source` });
    if (seg.sinks.length === 0) errors.push({ code: 'stream-without-sink', node: seg.members[0], nodes: seg.members, message: `the stream among ${seg.members.join(', ')} has no sink` });
  }
  return errors;
}

export function validateFlowDef(body: unknown, flowsKnown: Set<string>): FlowError[] {
  return validate(body as FlowDef, flowsKnown);
}

export function readPath(root: Json | undefined, path: string): Json | undefined {
  return path.split('.').reduce<Json | undefined>((cur, seg) => {
    if (cur && typeof cur === 'object' && !Array.isArray(cur) && seg in cur) return cur[seg];
    return undefined;
  }, root);
}

export function writePath(root: Json, path: string, value: Json): Json {
  const [head, ...rest] = path.split('.');
  const base: Record<string, Json> = root && typeof root === 'object' && !Array.isArray(root) ? { ...root } : {};
  base[head] = rest.length ? writePath(base[head] ?? {}, rest.join('.'), value) : value;
  return base;
}

export interface Write {
  path: string;
  previous: Json | null;
  value: Json;
}

/** Estado final de un nodo en una ejecución. Lo usan los retries para aplicar RunPolicy. */
export interface NodeFinal {
  status: 'succeeded' | 'failed' | 'skipped';
  port: string | null;
  error: string | null;
  writes: Write[]; // valores reales (para reaplicarlos al estado)
  childInstanceId?: string;
  reused?: boolean;
  /** Lo que el nodo dice de su ejecución (métricas y dead letters de un segmento). */
  info?: Record<string, Json>;
}

/** Entrada de auditoría sin seq ni hora absoluta: `atMs` es el tiempo desde el inicio. */
export interface RawEntry {
  kind: string;
  atMs: number;
  [key: string]: unknown;
}

export interface ChildResult {
  instanceId?: string;
  status: 'succeeded' | 'failed';
  durationMs: number;
  outputs: Record<string, Json>;
  error?: string;
}

export interface RunOptions {
  flowsKnown: Set<string>;
  mocks?: Record<string, Mock>;
  /** Nodos que fallan sin mock (para sembrar fallos reales en datos de ejemplo). */
  faults?: Record<string, string>;
  previous?: Record<string, NodeFinal>;
  /** Origen de la instancia (forma estructurada, `type` = manual | trigger | retry | subflow | cron). */
  origin: Record<string, unknown>;
  /** Un retry de la MISMA instancia: `of` es su id. */
  retry?: { of: string; by?: string; revision: number };
  redactPaths: string[];
  runChild: (flowId: string, inputs: Json, parent: NodeDef, detached: boolean) => ChildResult;
  /** Epoch ms del inicio. Los horarios de los temporizadores se calculan desde aquí. */
  now: number;
  /** Id de la instancia (para el `x-idempotency-key`). */
  instanceId: string;
  /** Simulación (test): `queue.publish` no conecta y responde el mensaje completo. */
  simulation?: boolean;
  /** Próxima coincidencia de una expresión en una zona, después de un instante (epoch ms). */
  nextFire: (expression: string, zone: string, afterMs: number) => number | null;
}

export interface NodeFinalWithEntries extends NodeFinal {}

export interface RunResult {
  /** Un segmento sin fin (cron sin maxTicks, cola): la instancia queda en curso hasta cancelarla. */
  endless?: string;
  entries: RawEntry[];
  nodes: Record<string, NodeFinal>;
  state: Json;
  status: 'succeeded' | 'failed' | 'running';
  durationMs: number;
}

/** Etiqueta corta del origen, como la devuelve la API (`manual`, `trigger:<id>`, `retry:<id>`, `subflow:<id>`, `cron:<id>`). */
export function originLabel(o: Record<string, unknown>): string {
  switch (o.type ?? o.kind) {
    case 'trigger': return `trigger:${String(o.triggerId)}`;
    case 'retry': return `retry:${String(o.of ?? o.retryOf)}`;
    case 'subflow': return `subflow:${String(o.parentInstanceId)}`;
    case 'cron': return `cron:${String(o.schedule)}`;
    default: return 'manual';
  }
}

const MAX_WAIT_RE = /^(\d+)([smhd])$/;
const UNIT_MS: Record<string, number> = { s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };

/** Ejecuta el grafo. Emite auditoría: lecturas, escrituras con valor previo, decisiones y aristas. */
export function runAudited(original: FlowDef, inputs: Json, o: RunOptions): RunResult {
  // Como el motor: cada segmento de stream es un solo nodo (`stream.segment`) con sus miembros dentro.
  const flow = condenseFlow(original, ioOfFlow(original)).flow;
  let state: Json = inputs ?? {};
  let endlessAt: string | undefined;
  const parked = new Set<string>();
  const entries: RawEntry[] = [];
  let t = 0;
  const redact = (path: string) => o.redactPaths.includes(path);
  const red = (path: string, v: Json | undefined): Json => (v === undefined ? null : redact(path) ? REDACTED : v);

  const resolution = new Map<Edge, 'fired' | 'dead'>();
  const finals: Record<string, NodeFinal> = {};
  const readsOf: Record<string, { path: string; value: Json | null }[]> = {};
  const announced = new Set<string>();
  const incoming = (id: string) => flow.edges.filter((e) => e.to === id);
  const outgoing = (id: string) => flow.edges.filter((e) => e.from === id);
  const nodeOf = (id: string) => flow.nodes.find((n) => n.id === id)!;
  const simulated = o.simulation === true;

  const redactedInputs = JSON.parse(JSON.stringify(inputs ?? {})) as Json;
  let auditInputs: Json = redactedInputs;
  for (const p of o.redactPaths) if (readPath(redactedInputs, p) !== undefined) auditInputs = writePath(auditInputs, p, REDACTED);
  entries.push({
    kind: 'instance-started', atMs: 0, flowId: flow.id, revision: flow.revision, inputs: auditInputs,
    flowHash: `sha256:${flow.id}-r${flow.revision}`, origin: originLabel(o.origin), originDetail: o.origin,
  });
  if (o.retry) {
    entries.push({
      kind: 'retry-requested', atMs: 0, origin: `retry:${o.retry.of}`, originDetail: { type: 'retry', of: o.retry.of, by: o.retry.by ?? null },
      revision: o.retry.revision,
    });
  }

  const named = (node: NodeDef, path: string) => node.outputs.find((d) => d.target === path)?.name ?? path;
  const writtenOf = (node: NodeDef, writes: Write[]) =>
    writes.map((w) => ({ name: named(node, w.path), path: w.path, previous: red(w.path, w.previous), value: red(w.path, w.value) }));

  const emitEdges = (nodeId: string, port: string | null) => {
    for (const e of outgoing(nodeId)) {
      const fired = e.port === port;
      resolution.set(e, fired ? 'fired' : 'dead');
      entries.push(
        fired
          ? { kind: 'edge-fired', atMs: t, edge: { from: e.from, port: e.port, to: e.to } }
          : { kind: 'edge-dead', atMs: t, edge: { from: e.from, port: e.port, to: e.to }, reason: 'other-port' },
      );
    }
  };

  const announceWaiting = (targetId: string) => {
    if (announced.has(targetId) || finals[targetId]) return;
    const target = nodeOf(targetId);
    const ins = incoming(targetId);
    const arrived = ins.filter((e) => resolution.has(e));
    const fired = ins.filter((e) => resolution.get(e) === 'fired');
    const ready = ins.length === 0 || (target.joinPolicy === 'any' ? fired.length > 0 : arrived.length === ins.length);
    // Espera solo si algunas entradas llegaron y otras faltan: un nodo con todas resueltas se ejecuta o se salta.
    if (ready || arrived.length === 0 || arrived.length === ins.length) return;
    announced.add(targetId);
    entries.push({
      kind: 'node-waiting', atMs: t, node: targetId, nodeKind: target.kind, attempt: 1,
      decision: { reason: 'waiting-join', pending: ins.filter((e) => !resolution.has(e)).map((e) => ({ from: e.from, port: e.port, to: e.to })) },
    });
  };

  const skipDead = (node: NodeDef, ins: Edge[]) => {
    const causes = ins.map((e) => {
      const src = nodeOf(e.from);
      const srcFinal = finals[src.id];
      const reads = readsOf[src.id];
      const ran = srcFinal && srcFinal.status !== 'skipped';
      const read = src.kind === 'core.if' && reads?.[0] ? [{ path: reads[0].path, value: reads[0].value }] : [];
      return ran
        ? { from: e.from, port: e.port, why: 'other-port', took: srcFinal.port, decidedWith: read }
        : { from: e.from, port: e.port, why: 'source-not-run', took: null, decidedWith: [] };
    });
    entries.push({
      kind: 'node-skipped', atMs: t, node: node.id, nodeKind: node.kind, attempt: 1,
      decision: { reason: 'skipped-dead-branch', causes },
    });
    emitEdges(node.id, null);
    finals[node.id] = { status: 'skipped', port: null, error: null, writes: [] };
  };

  const reuse = (node: NodeDef, prev: NodeFinal) => {
    const policy = node.runPolicy;
    const rule = prev.status === 'failed' ? `whenFailed=${policy.whenFailed}` : `whenSucceeded=${policy.whenSucceeded}`;
    entries.push({
      kind: 'node-skipped', atMs: t, node: node.id, nodeKind: node.kind, attempt: 1,
      decision: {
        reason: 'skipped-by-run-policy', rule, port: prev.port,
        outputs: writtenOf(node, prev.writes), error: prev.error,
      },
    });
    if (prev.writes.length) {
      for (const w of prev.writes) state = writePath(state, w.path, w.value);
      entries.push({ kind: 'state-changed', atMs: t, node: node.id, attempt: 1, changes: writtenOf(node, prev.writes) });
    }
    emitEdges(node.id, prev.port);
    finals[node.id] = { ...prev, reused: true };
  };

  const run = (node: NodeDef) => {
    const started: RawEntry = { kind: 'node-started', atMs: t, node: node.id, nodeKind: node.kind, attempt: 1, reads: [] as { path: string; value: Json | null }[], memory: {}, params: node.params };
    entries.push(started);
    const reads: { path: string; value: Json | null }[] = [];
    let port: string = 'out';
    let error: string | null = null;
    let duration = 3;
    let writes: Write[] = [];
    let childInstanceId: string | undefined;
    let mocked = false;
    let info: Record<string, Json> | undefined;
    let outcome: 'ran' | 'mocked' | 'simulated' = simulated ? 'ran' : 'ran';
    let mockUsed: Mock | undefined;
    const mock = o.mocks?.[node.id];
    const fault = o.faults?.[node.id];
    const hasInput = incoming(node.id).length > 0;

    // Un segmento sin fin no termina: la instancia queda en curso (sin nodo terminado) hasta cancelarla.
    if (node.kind === SEGMENT_KIND && !fault && !simulated && runSegment(node, state, (id) => o.mocks?.[id], { now: o.now + t, ticks: 0 }).endless) {
      endlessAt = node.id;
      parked.add(node.id);
      return;
    }

    if (fault) {
      port = 'error'; error = fault; duration = 420;
    } else if (node.kind === SEGMENT_KIND) {
      const r = runSegment(node, state, (id) => o.mocks?.[id], { now: o.now + t, ticks: simulated ? 5 : undefined });
      port = r.port;
      error = r.error;
      duration = 20;
      writes = r.writes.map((w) => ({ path: w.path, previous: readPath(state, w.path) ?? null, value: w.value }));
      info = { metrics: r.metrics as unknown as Json, deadLetters: r.deadLetters as unknown as Json };
    } else if (mock) {
      mocked = true;
      outcome = 'mocked';
      mockUsed = mock;
      duration = 1;
      const legacy = mock as { error?: string; port?: string; values?: Record<string, Json> };
      if (legacy.error) { port = 'error'; error = legacy.error; }
      else {
        port = legacy.port ?? 'out';
        writes = node.outputs.flatMap((d) => (legacy.values && d.name in legacy.values ? [{ path: d.target, previous: readPath(state, d.target) ?? null, value: legacy.values[d.name] }] : []));
      }
    } else if (node.kind === 'core.if') {
      const path = typeof node.params.path === 'string' ? node.params.path : '';
      const value = readPath(state, path);
      reads.push({ path, value: value === undefined ? null : red(path, value) });
      readsOf[node.id] = reads;
      duration = 2;
      if (value === undefined) { port = 'error'; error = `'${path}' is not in the state`; }
      else if (typeof value !== 'boolean') { port = 'error'; error = `'${path}' in the state is not a boolean: ${JSON.stringify(value)}`; }
      else port = value !== (node.params.negate === true) ? 'then' : 'else';
    } else if (node.kind === 'core.noop') {
      duration = 1;
    } else if (node.kind === 'core.cron') {
      const parsed = typeof node.params.expression === 'string' ? parseCron(node.params.expression) : null;
      const zone = typeof node.params.timezone === 'string' ? node.params.timezone : 'UTC';
      if (!parsed || !parsed.ok) { port = 'error'; error = parsed && !parsed.ok ? parsed.error : 'missing expression'; duration = 1; }
      else if (!hasInput) {
        // Lanzador: la programación inicia la instancia; dentro de ella no hace nada.
        duration = 1;
      } else {
        // Temporizador: espera hasta la próxima coincidencia, registrada para sobrevivir a un reinicio.
        const nowAbs = o.now + t;
        const fire = o.nextFire(String(node.params.expression), zone, nowAbs);
        const maxWait = typeof node.params.maxWait === 'string' ? MAX_WAIT_RE.exec(node.params.maxWait) : null;
        if (fire === null) { port = 'error'; error = `no hay próxima coincidencia de '${String(node.params.expression)}'`; duration = 1; }
        else if (maxWait && fire - nowAbs > Number(maxWait[1]) * UNIT_MS[maxWait[2]]) {
          port = 'error'; error = `the next time is beyond maxWait ${String(node.params.maxWait)}`; duration = 1;
        } else {
          entries.push({
            kind: 'node-waiting', atMs: t, node: node.id, nodeKind: node.kind, attempt: 1,
            decision: { reason: 'scheduled', fireAt: fire },
          });
          duration = fire - nowAbs;
          info = { fireAt: fire, expression: String(node.params.expression), timezone: zone };
        }
      }
    } else if (node.kind === 'queue.publish') {
      const connection = String(node.params.connection ?? '');
      const destination = String(node.params.destination ?? '');
      const payloadPath = String(node.params.payload ?? '');
      const keyPath = typeof node.params.key === 'string' ? node.params.key : '';
      const payload = readPath(state, payloadPath);
      const key = keyPath ? readPath(state, keyPath) : undefined;
      reads.push({ path: payloadPath, value: payload === undefined ? null : red(payloadPath, payload) });
      if (keyPath) reads.push({ path: keyPath, value: key === undefined ? null : red(keyPath, key) });
      const idem = `${o.instanceId}:${node.id}`;
      duration = 12;
      if (payload === undefined) { port = 'error'; error = `'${payloadPath}' is not in the state`; }
      else if (!connection) { port = 'error'; error = 'the connection is missing'; }
      else {
        const message = { key: key === undefined ? undefined : String(key), payload, headers: { 'x-idempotency-key': idem } };
        if (simulated) {
          outcome = 'simulated';
          info = {
            connection, destination, ...(message.key !== undefined ? { key: message.key } : {}),
            headers: message.headers, payload: message.payload,
          };
        } else {
          // Sin broker en el mock: se registra lo que se enviaría, sin conectar.
          info = {
            connection, destination, ...(message.key !== undefined ? { key: message.key } : {}),
            'x-idempotency-key': idem, mock: 'sin broker en el mock: no se envió',
          };
        }
      }
    } else if (node.kind === 'core.subflow') {
      const flowId = typeof node.params.flowId === 'string' ? node.params.flowId : '';
      const wait = node.params.wait !== false;
      const map = (node.params.inputs && typeof node.params.inputs === 'object' && !Array.isArray(node.params.inputs) ? node.params.inputs : {}) as Record<string, string>;
      const childInputs: Record<string, Json> = {};
      for (const [name, path] of Object.entries(map)) {
        const value = readPath(state, path);
        reads.push({ path, value: value === undefined ? null : red(path, value) });
        if (value !== undefined) childInputs[name] = value;
      }
      readsOf[node.id] = reads;
      if (!o.flowsKnown.has(flowId)) { port = 'error'; error = `flow '${flowId}' does not exist`; duration = 2; }
      else {
        const child = o.runChild(flowId, childInputs, node, !wait);
        childInstanceId = child.instanceId;
        if (!wait) { duration = 12; }
        else {
          duration = child.durationMs;
          if (child.status === 'succeeded') {
            writes = node.outputs.map((d) => ({ path: d.target, previous: readPath(state, d.target) ?? null, value: child.outputs[d.name] ?? null }));
          } else {
            port = 'error';
            error = `flow '${flowId}' failed: ${child.error ?? 'error'}`;
          }
        }
      }
    } else {
      port = 'error'; error = `unknown kind '${node.kind}'`; duration = 1;
    }
    started.reads = reads;
    if (node.kind === 'core.if') started.reads = reads;

    t += duration;
    for (const w of writes) state = writePath(state, w.path, w.value);
    const simulation = simulated || mocked
      ? { outcome, ...(mockUsed ? { mock: mockUsed } : {}) }
      : undefined;
    entries.push({
      kind: 'node-finished', atMs: t, node: node.id, nodeKind: node.kind, attempt: 1,
      startedAt: started.atMs, durationMs: duration, status: error ? 'failed' : 'succeeded',
      port: error ? 'error' : port, error, outputs: writtenOf(node, writes), memoryAfter: {},
      ...(childInstanceId ? { childInstanceId } : {}),
      ...(simulation ? { simulation } : {}),
      ...(info ? { info } : {}),
    });
    if (writes.length) {
      entries.push({ kind: 'state-changed', atMs: t, node: node.id, attempt: 1, changes: writtenOf(node, writes) });
    }
    emitEdges(node.id, error ? 'error' : port);
    finals[node.id] = { status: error ? 'failed' : 'succeeded', port: error ? 'error' : port, error, writes, childInstanceId, ...(info ? { info } : {}) };
  };

  // Bucle de ejecución: un nodo corre cuando sus entradas llegaron (join all) o llegó una (any).
  let progressed = true;
  while (progressed) {
    progressed = false;
    for (const node of flow.nodes) {
      if (finals[node.id] || parked.has(node.id)) continue;
      const ins = incoming(node.id);
      const fired = ins.filter((e) => resolution.get(e) === 'fired');
      const resolved = ins.every((e) => resolution.has(e));
      if (ins.length > 0 && fired.length === 0 && resolved) {
        progressed = true;
        skipDead(node, ins);
      } else {
        const ready = ins.length === 0 || (node.joinPolicy === 'any' ? fired.length > 0 : resolved);
        if (!ready) continue;
        progressed = true;
        const prev = o.previous?.[node.id];
        const policy: RunPolicy = node.runPolicy;
        const skip = prev && ((prev.status === 'succeeded' && policy.whenSucceeded === 'skip') || (prev.status === 'failed' && policy.whenFailed === 'skip'));
        if (prev && skip) reuse(node, prev);
        else run(node);
      }
      for (const e of outgoing(node.id)) announceWaiting(e.to);
    }
  }

  const failedUnhandled = Object.entries(finals).some(([id, f]) => f.status === 'failed' && !outgoing(id).some((e) => e.port === 'error'));
  if (endlessAt) return { endless: endlessAt, entries, nodes: finals, state, status: 'running', durationMs: t };
  const status = failedUnhandled ? 'failed' : 'succeeded';
  entries.push({ kind: 'instance-finished', atMs: t, status, durationMs: t });
  return { entries, nodes: finals, state, status, durationMs: t };
}

type RunPolicy = NodeDef['runPolicy'];

/** Simulación (test): corre el grafo con mocks, sin guardar instancias. Los subflows también se simulan. */
export function simulate(
  flow: FlowDef,
  inputs: Json,
  mocks: Record<string, Mock>,
  flowsKnown: Set<string>,
  runFlow: (id: string) => FlowDef | undefined,
  nextFire: RunOptions['nextFire'],
): Trace {
  const runChild = (flowId: string, childInputs: Json): ChildResult => {
    const child = runFlow(flowId);
    if (!child) return { status: 'failed', durationMs: 1, outputs: {}, error: 'not found' };
    const r = runAudited(child, childInputs, {
      flowsKnown, redactPaths: [], runChild: () => ({ status: 'succeeded', durationMs: 1, outputs: {} }),
      origin: { type: 'subflow', parentInstanceId: 'simulation', parentNodeId: '-' }, now: Date.now(), instanceId: 'simulation', simulation: true, nextFire,
    });
    const outputs: Record<string, Json> = {};
    for (const d of child.outputs) outputs[d.name] = readPath(r.state, d.target) ?? null;
    const status = r.status === 'failed' ? 'failed' : 'succeeded';
    return { status, durationMs: r.durationMs, outputs, error: status === 'failed' ? 'fallo en el flujo llamado' : undefined };
  };
  const r = runAudited(flow, inputs ?? {}, {
    flowsKnown, mocks, redactPaths: [], runChild, origin: { type: 'manual' }, now: Date.now(), instanceId: 'simulation', simulation: true, nextFire,
  });
  const steps: TraceStep[] = r.entries
    .filter((e) => e.kind === 'node-finished')
    .map((e) => ({
      nodeId: String(e.node),
      kind: String(e.nodeKind),
      port: (e.port as string) ?? null,
      error: (e.error as string) ?? null,
      mocked: (e.simulation as { outcome?: string } | undefined)?.outcome === 'mocked',
      outcome: (e.simulation as { outcome?: string } | undefined)?.outcome ?? 'ran',
      simulated: true,
      values: Object.fromEntries(((e.outputs as Array<{ name: string; value: Json }> | undefined) ?? []).map((o) => [o.name, o.value])),
      ...(e.info ? { info: e.info as Record<string, unknown> } : {}),
      state: null,
    }));
  return {
    status: r.status,
    steps,
    state: r.state,
    audit: r.entries.map((e, i) => ({ ...e, seq: i, simulated: true })),
  } as unknown as Trace;
}
