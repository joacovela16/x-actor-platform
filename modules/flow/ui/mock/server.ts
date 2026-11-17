// Plugin de Vite que simula la API REST del DESIGN.md (solo en `npm run dev`; no entra en el build).
// Las formas de respuesta son las del backend (http/FlowRoutes, ApiJson, engine/AuditEntry): tiempos en epoch ms,
// SSE con el AuditEntry completo más `type`, retry sobre la MISMA instancia, 422 con `{errors:[...]}`.
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Plugin } from 'vite';
import type { EntityDef, FlowDef, JsonValue, Mock, NodeDef, Trigger } from '../src/api/types.ts';
import { nextFire as nextFireOf, parseCron, validZone } from '../src/graph/cron.ts';
import { catalog, connectionKinds, entityOutputs, REDACTED_PATHS, seedFlows, seedTriggers } from './data.ts';
import { ioOfFlow, originLabel, readPath, REDACTED, runAudited, simulate, validate, validateFlowDef, writePath, type NodeFinal, type RawEntry, type RunResult } from './engine.ts';
import { runSegment } from './stream.ts';
import { checkValue, seedEntities, transitionValue, validateEntity } from './entities.ts';
import { condenseFlow, SEGMENT_KIND } from '../src/graph/stream.ts';

type Json = JsonValue;
/** Un trozo de tiempo real entre dos entradas de una instancia en vivo: el mock no espera más de esto. */
const STEP_MIN_MS = 150;
const STEP_MAX_MS = 900;
const WAIT_MAX_MS = 25_000;

interface AuditEntryOut {
  seq: number;
  at: number;
  kind: string;
  node?: string;
  [key: string]: unknown;
}

interface Rec {
  id: string;
  flowId: string;
  revision: number;
  origin: Record<string, unknown>;
  rawInputs: Json;
  startedAt: number;
  endedAt: number | null;
  status: 'running' | 'succeeded' | 'failed';
  final: RunResult;
  /** Todas las entradas de la instancia (incluidos los reintentos, que la continúan). */
  entries: AuditEntryOut[];
  revealed: number;
  subscribers: Set<ServerResponse>;
  timer?: NodeJS.Timeout;
  /** Cancelada con POST .../cancel (solo las sin fin pueden estar en curso tanto tiempo). */
  cancelled?: boolean;
}

interface Store {
  flows: Map<string, FlowDef>;
  /** Borradores: sin validar, uno por flujo. Como el servidor (FlowDrafts). */
  drafts: Map<string, FlowDef>;
  history: Map<string, FlowDef[]>;
  enabled: Set<string>;
  triggers: Map<string, Trigger>;
  connections: Map<string, { kind: string; params: Record<string, unknown>; secretsSet: string[] }>;
  instances: Map<string, Rec>;
  /** Entidades del modelo de negocio, cada una en su última revisión, con `source`. */
  entities: Map<string, EntityDef>;
  seq: number;
}

const seedConnections = (): Array<[string, { kind: string; params: Record<string, unknown>; secretsSet: string[] }]> => [
  ['kafka-prod', { kind: 'kafka', params: { bootstrapServers: 'k1.internal:9092', securityProtocol: 'SASL_SSL', saslMechanism: 'SCRAM-SHA-512', username: 'flow' }, secretsSet: ['password'] }],
  ['artemis-dev', { kind: 'activemq', params: { url: 'tcp://localhost:61616', flavor: 'artemis', username: 'flow' }, secretsSet: ['password'] }],
  ['redis-cache', { kind: 'redis', params: { uri: 'redis://localhost:6379' }, secretsSet: [] }],
  ['partner-api', { kind: 'http', params: { baseUrl: 'https://api.partner.example' }, secretsSet: ['token'] }],
  ['orders-db', { kind: 'postgres', params: { host: 'db.internal', port: 5432, database: 'orders', username: 'flow', ssl: true, maxPoolSize: 5 }, secretsSet: ['password'] }],
  ['claude-main', { kind: 'claude', params: { baseUrl: 'https://api.anthropic.com', model: 'claude-sonnet-5-5', anthropicVersion: '2023-06-01' }, secretsSet: ['apiKey'] }],
];

function freshStore(): Store {
  const flows = new Map(seedFlows.map((f) => [f.id, structuredClone(f)]));
  return {
    flows,
    drafts: new Map(),
    history: new Map(seedHistory(flows)),
    enabled: new Set(),
    triggers: new Map(seedTriggers.map((t) => [t.id, structuredClone(t)])),
    connections: new Map(seedConnections()),
    instances: new Map(),
    entities: new Map(seedEntities.map((e) => [e.name, structuredClone(e)])),
    seq: 0,
  };
}

/** Revisiones anteriores de `order-approval` (la 3 es la actual): una versión sin el nodo de unión y otra sin subflujo. */
function seedHistory(flows: Map<string, FlowDef>): Array<[string, FlowDef[]]> {
  const current = flows.get('order-approval')!;
  const r2: FlowDef = {
    ...structuredClone(current),
    revision: 2,
    name: 'Aprobación de pedido',
    nodes: current.nodes.filter((n) => n.id !== 'merge'),
    edges: current.edges.filter((e) => e.to !== 'merge' && e.from !== 'merge'),
  };
  const r1: FlowDef = {
    ...structuredClone(current),
    revision: 1,
    name: 'Aprobación de pedido (borrador)',
    nodes: current.nodes.filter((n) => n.id === 'check'),
    edges: [],
    layout: { check: current.layout?.check ?? { x: 80, y: 120 } },
  };
  // Los demás flujos de ejemplo tienen solo su revisión actual (o una anterior igual que la actual, numerada antes).
  const onlyCurrent = (id: string): [string, FlowDef[]] => {
    const f = flows.get(id)!;
    const earlier = Array.from({ length: Math.max(0, f.revision - 1) }, (_, i) => ({ ...structuredClone(f), revision: i + 1 }));
    return [id, [...earlier, structuredClone(f)]];
  };
  return [
    ['order-approval', [r1, r2, structuredClone(current)]],
    onlyCurrent('notify-customer'),
    onlyCurrent('heartbeat'),
    onlyCurrent('nightly-report'),
  ];
}

function redactState(state: Json): Json {
  let s = state;
  for (const p of REDACTED_PATHS) if (readPath(s, p) !== undefined) s = writePath(s, p, REDACTED);
  return s;
}

/** Hora de próxima coincidencia (epoch ms) de una expresión en una zona, o null. */
function nextFire(expression: string, zone: string, afterMs: number): number | null {
  const p = parseCron(expression);
  if (!p.ok || !validZone(zone)) return null;
  return nextFireOf(p.spec, zone, afterMs);
}

/** Nodos a partir de las entradas reveladas (instancia en curso). */
function nodesFromEntries(entries: AuditEntryOut[]): Record<string, { status: string; port?: string | null; error?: string | null; fireAt?: number }> {
  const out: Record<string, { status: string; port?: string | null; error?: string | null; fireAt?: number }> = {};
  for (const e of entries) {
    const id = e.node as string | undefined;
    if (!id) continue;
    if (e.kind === 'node-started') out[id] = { status: 'running' };
    if (e.kind === 'node-finished') out[id] = { status: e.status as string, port: e.port as string, error: (e.error as string) ?? null };
    if (e.kind === 'node-skipped') out[id] = { status: 'skipped' };
    if (e.kind === 'node-waiting' && (e.decision as { reason?: string } | undefined)?.reason === 'scheduled') {
      out[id] = { status: 'running', fireAt: (e.decision as { fireAt: number }).fireAt };
    }
  }
  return out;
}

/** Lanzadores de un flujo: los nodos cron sin aristas de entrada, con su próxima ejecución. */
function scheduleOf(flow: FlowDef, enabled: boolean, now: number) {
  const launchers = flow.nodes.filter((n) => n.kind === 'core.cron' && !flow.edges.some((e) => e.to === n.id));
  return {
    enabled,
    schedules: launchers.map((n) => {
      const expression = String(n.params.expression ?? '');
      const timezone = String(n.params.timezone ?? 'UTC');
      return {
        node: n.id,
        expression,
        timezone,
        misfire: String(n.params.misfire ?? 'fire-once'),
        nextFireAt: nextFire(expression, timezone, now),
      };
    }),
  };
}

/** Problemas de la configuración de un nodo (describe): la misma comprobación que la validación de un flujo. */
function validateParams(kind: string, params: Record<string, Json>): string[] {
  const flow = { version: 1, id: 'describe', name: 'describe', revision: 1, inputs: [], outputs: [], edges: [], nodes: [{ id: 'n', kind, params, outputs: [], runPolicy: { whenSucceeded: 'run', whenFailed: 'run' }, joinPolicy: 'all' }] } as unknown as FlowDef;
  return validate(flow, new Set()).filter((e) => e.code === 'invalid-params').flatMap((e) => e.message.replace(/^node 'n' is badly configured: /, '').split('; '));
}

export function mockApi(): Plugin {
  const store = freshStore();
  const flowsKnown = () => new Set(store.flows.keys());
  const nextFireFn = (expr: string, zone: string, after: number) => nextFire(expr, zone, after);

  const send = (res: ServerResponse, status: number, body?: unknown): void => {
    res.statusCode = status;
    if (body === undefined) {
      res.end();
      return;
    }
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(body));
  };

  const readBody = (req: IncomingMessage): Promise<unknown> =>
    new Promise((resolve, reject) => {
      let data = '';
      req.on('data', (c) => (data += c));
      req.on('end', () => {
        try {
          resolve(data ? JSON.parse(data) : undefined);
        } catch (e) {
          reject(e);
        }
      });
      req.on('error', reject);
    });

  const sse = (e: AuditEntryOut) => `data: ${JSON.stringify({ ...e, type: e.kind })}\n\n`;

  const emit = (rec: Rec, e: AuditEntryOut) => {
    for (const res of rec.subscribers) res.write(sse(e));
  };

  const nextId = () => `inst-${++store.seq}`;

  /** Entradas de una ejecución con `seq` y tiempo absoluto (`at`, epoch ms). `offset`: desde el inicio de la instancia. */
  const stamp = (entries: RawEntry[], startedAt: number, offset: number, seqFrom: number): AuditEntryOut[] =>
    entries.map((e, i) => {
      const { atMs, ...rest } = e;
      return { ...rest, seq: seqFrom + i, at: startedAt + offset + atMs, kind: e.kind } as AuditEntryOut;
    });

  /** Reproduce en vivo las entradas que aún no se han revelado (y las emite por SSE). */
  const startReveal = (rec: Rec) => {
    const step = () => {
      if (rec.revealed >= rec.entries.length) {
        rec.status = rec.final.status;
        rec.endedAt = rec.startedAt + rec.final.durationMs;
        return;
      }
      const e = rec.entries[rec.revealed++];
      emit(rec, e);
      const next = rec.entries[rec.revealed];
      const gap = next ? next.at - e.at : 0;
      const delay = next ? Math.min(gap > 2000 ? WAIT_MAX_MS : STEP_MAX_MS, Math.max(STEP_MIN_MS, gap)) : 0;
      rec.timer = setTimeout(step, delay);
    };
    rec.timer = setTimeout(step, 150);
  };

  /** Crea una instancia (o una subflow hija) con su ejecución. `live` la revela con retardo por SSE. */
  const createRun = (p: {
    flowId: string;
    inputs: Json;
    origin: Record<string, unknown>;
    startedAt: number;
    live: boolean;
    faults?: Record<string, string>;
  }): Rec => {
    const flow = store.flows.get(p.flowId)!;
    const id = nextId();
    const spawnChild = (fid: string, childInputs: Json, parent: NodeDef, detached: boolean) => {
      const child = createRun({
        flowId: fid,
        inputs: childInputs,
        origin: { type: 'subflow', parentInstanceId: id, parentNodeId: parent.id, detached },
        startedAt: p.startedAt + 400,
        live: false,
      });
      const cf = store.flows.get(fid)!;
      const outputs: Record<string, Json> = {};
      for (const d of cf.outputs) outputs[d.name] = readPath(child.final.state, d.target) ?? null;
      return {
        instanceId: child.id,
        status: child.status === 'succeeded' ? ('succeeded' as const) : ('failed' as const),
        durationMs: child.final.durationMs,
        outputs,
        error: child.status === 'failed' ? 'fallo en el flujo llamado' : undefined,
      };
    };
    const final = runAudited(flow, p.inputs, {
      flowsKnown: flowsKnown(),
      faults: p.faults,
      redactPaths: REDACTED_PATHS,
      runChild: spawnChild,
      origin: p.origin,
      now: p.startedAt,
      instanceId: id,
      nextFire: nextFireFn,
    });
    const rec: Rec = {
      id, flowId: p.flowId, revision: flow.revision, origin: p.origin, rawInputs: p.inputs,
      startedAt: p.startedAt, endedAt: p.startedAt + final.durationMs,
      status: p.live ? 'running' : final.status, final,
      entries: stamp(final.entries, p.startedAt, 0, 0),
      revealed: p.live ? 0 : final.entries.length, subscribers: new Set(),
    };
    store.instances.set(id, rec);
    if (p.live) startReveal(rec);
    return rec;
  };

  /**
   * Un retry vuelve a correr la MISMA instancia (mismo id): sus entradas se añaden a las anteriores tras un
   * `retry-requested`, y su RunPolicy reutiliza el resultado de la ejecución previa.
   */
  const retryInPlace = (old: Rec, by: string, live: boolean, at: number): Rec => {
    const flow = store.flows.get(old.flowId)!;
    const offset = at - old.startedAt;
    const r = runAudited(flow, old.rawInputs, {
      flowsKnown: flowsKnown(),
      redactPaths: REDACTED_PATHS,
      runChild: (fid, childInputs, parent, detached) => {
        const child = createRun({
          flowId: fid, inputs: childInputs,
          origin: { type: 'subflow', parentInstanceId: old.id, parentNodeId: parent.id, detached },
          startedAt: at + 400, live: false,
        });
        const cf = store.flows.get(fid)!;
        const outputs: Record<string, Json> = {};
        for (const d of cf.outputs) outputs[d.name] = readPath(child.final.state, d.target) ?? null;
        return { instanceId: child.id, status: child.status === 'succeeded' ? 'succeeded' : 'failed', durationMs: child.final.durationMs, outputs, error: child.status === 'failed' ? 'fallo en el flujo llamado' : undefined };
      },
      origin: old.origin,
      now: at,
      instanceId: old.id,
      nextFire: nextFireFn,
      previous: old.final.nodes as Record<string, NodeFinal>,
      retry: { of: old.id, by, revision: flow.revision },
    });
    // La nueva ejecución empieza con su propio `instance-started`: la instancia ya existe, así que se descarta.
    const added = r.entries.slice(1);
    const seqFrom = old.entries.length;
    const newEntries = stamp(added, old.startedAt, offset, seqFrom);
    old.entries = [...old.entries, ...newEntries];
    old.final = r;
    old.revision = flow.revision;
    old.status = live ? 'running' : r.status;
    old.endedAt = old.startedAt + offset + r.durationMs;
    if (live) {
      old.revealed = seqFrom;
      startReveal(old);
    } else {
      old.revealed = old.entries.length;
    }
    return old;
  };

  const instanceSummary = (r: Rec) => ({
    id: r.id, flowId: r.flowId, revision: r.revision, status: r.status,
    origin: originLabel(r.origin), originDetail: r.origin,
    startedAt: r.startedAt, endedAt: r.status === 'running' ? null : r.endedAt,
  });

  /** Métricas en vivo de un segmento sin fin: una fuente sin fin da un elemento por segundo transcurrido (tope 300). */
  const liveSegment = (r: Rec) => {
    const anchor = r.final.endless;
    if (!anchor) return undefined;
    const flow = store.flows.get(r.flowId)!;
    const node = condenseFlow(flow, ioOfFlow(flow)).flow.nodes.find((n) => n.id === anchor);
    if (!node) return undefined;
    const ticks = Math.min(300, Math.floor((Date.now() - r.startedAt) / 1000));
    return runSegment(node, r.rawInputs, () => undefined, { now: r.startedAt, ticks });
  };

  /** Los miembros de un segmento y su estado: un paso, así que todos llevan el estado del anclaje. */
  const segmentMembers = (entries: AuditEntryOut[]): Array<{ anchor: string; members: string[] }> =>
    entries
      .filter((e) => e.kind === 'node-started' && e.nodeKind === SEGMENT_KIND && e.node)
      .map((e) => ({ anchor: e.node as string, members: ((e.params as { members?: Array<{ id: string }> })?.members ?? []).map((m) => m.id) }));

  const instanceView = (r: Rec) => {
    const running = r.status === 'running';
    const shown = running ? r.entries.slice(0, r.revealed) : r.entries;
    const nodes: Record<string, Json> = running
      ? { ...nodesFromEntries(shown) }
      : Object.fromEntries(
          Object.entries(r.final.nodes).map(([k, f]) => [k, { status: f.status === 'skipped' ? 'skipped' : f.status, port: f.port, error: f.error }]),
        );
    const deadLetters: Json[] = [];
    const live = running ? liveSegment(r) : undefined;
    const segMap = segmentMembers(shown);
    for (const { anchor, members } of segMap) {
      const status = (nodes[anchor] as { status?: string } | undefined)?.status ?? 'running';
      const info = running ? undefined : r.final.nodes[anchor]?.info;
      const metrics = (live?.metrics ?? (info?.metrics as Record<string, Json> | undefined)) as Record<string, Json> | undefined;
      const dead = (live?.deadLetters ?? info?.deadLetters ?? []) as Json[];
      if (!running) deadLetters.push(...dead);
      for (const m of members) nodes[m] = { status, port: (nodes[anchor] as { port?: string })?.port ?? null, error: (nodes[anchor] as { error?: string })?.error ?? null, ...(metrics?.[m] ? { metrics: metrics[m] } : {}) };
      if (running && live) deadLetters.push(...dead);
    }
    if (live?.metrics) for (const m of Object.keys(live.metrics)) if (!segMap.some((x) => x.members.includes(m))) nodes[m] = { status: 'running', metrics: live.metrics[m] as unknown as Json };
    return {
      id: r.id, flowId: r.flowId, revision: r.revision, status: r.status,
      state: running ? r.rawInputs : redactState(r.final.state),
      nodes,
      deadLetters,
    };
  };

  // Ejemplos: la instancia inst-1 falla por timeout en `notify`; su primer reintento (en la misma instancia) vuelve
  // a correrlo y llama al subflujo; el segundo reutiliza ese resultado por RunPolicy (whenSucceeded: skip).
  const seedInputs = { order: { approved: true, customer: 'ACME', cardToken: 'tok_live_8f2c' } };
  const T0 = Date.parse('2026-11-11T09:14:02.110Z');
  const seed = createRun({
    flowId: 'order-approval', inputs: seedInputs,
    origin: { type: 'trigger', triggerId: 'orders-http', requestId: 'req-42' },
    startedAt: T0, live: false, faults: { notify: 'timeout al llamar al servicio de avisos' },
  });
  retryInPlace(seed, 'ana', false, T0 + 3 * 3600_000);
  retryInPlace(seed, 'ana', false, T0 + 3 * 3600_000 + 20 * 60_000);

  // «heartbeat» arranca habilitado (sus lanzadores disparan); «nightly-report» no, para ver el aviso de habilitar.
  store.enabled.add('heartbeat');

  const auditOf = (r: Rec) => ({
    instance: {
      id: r.id, flowId: r.flowId, revision: r.revision, flowHash: `sha256:${r.flowId}-r${r.revision}`,
      status: r.status, origin: originLabel(r.origin), originDetail: r.origin,
      startedAt: r.startedAt, endedAt: r.status === 'running' ? null : r.endedAt,
      inputs: redactState(r.rawInputs),
    },
    entries: r.entries.slice(0, r.revealed),
  });

  const problemsOf = (body: unknown, id: string): string[] => {
    const t = body as Trigger;
    const out: string[] = [];
    if (!/^[A-Za-z0-9_.-]+$/.test(id)) out.push('the id may only have letters, digits, \'_\', \'.\' and \'-\'');
    if (!t.flowId) out.push('flowId is empty');
    if (t.kind === 'http') {
      if (!(t.path ?? '').replace(/\//g, '')) out.push('an http trigger needs a path');
      const method = (t.method ?? 'POST').toUpperCase();
      if (!['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) out.push(`unknown method '${method}'`);
      if (t.auth && !['apiKey', 'bearer', 'hmac'].includes(t.auth.scheme)) out.push(`unknown auth scheme '${t.auth.scheme}'`);
      if (t.auth && !t.auth.secret) out.push('auth needs a secret');
      const route = (t.path ?? '').split('/').filter(Boolean).join('/');
      for (const o of store.triggers.values()) {
        if (o.id !== id && o.kind === 'http' && (o.method ?? 'POST').toUpperCase() === method && (o.path ?? '').split('/').filter(Boolean).join('/') === route) {
          out.push(`the trigger '${o.id}' already serves ${method} /hooks/${route}`);
        }
      }
    }
    if (t.kind === 'cron') {
      const p = parseCron(t.cron ?? '');
      if (!t.cron) out.push('a cron trigger needs a cron expression');
      else if (!p.ok) out.push(`cron: ${p.error}`);
      if (t.timezone && !validZone(t.timezone)) out.push(`'${t.timezone}' is not a time zone id`);
    }
    if (t.kind === 'queue') {
      if (!t.queue) out.push('a queue trigger needs a queue');
      if (!t.connection) out.push('a queue trigger needs a connection');
      if (t.destinationType && !['queue', 'topic'].includes(t.destinationType)) out.push('destinationType must be one of queue, topic');
    }
    if (t.timeoutMs !== undefined && !(t.timeoutMs > 0)) out.push('timeoutMs must be positive');
    return out;
  };

  const handle = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const url = new URL(req.url ?? '/', 'http://x');
    const path = url.pathname.replace(/^\/api/, '');
    const method = req.method ?? 'GET';
    const seg = path.split('/').filter(Boolean).map(decodeURIComponent);
    const body = method === 'GET' || method === 'DELETE' ? undefined : await readBody(req);

    // salud: lo mismo que /api/health, calculado con las ejecuciones del mock
    if (method === 'GET' && path === '/health') {
      const byFlow = new Map<string, Rec[]>();
      for (const r of store.instances.values()) byFlow.set(r.flowId, [...(byFlow.get(r.flowId) ?? []), r]);
      const pct = (xs: number[], p: number) => (xs.length ? xs[Math.max(0, Math.ceil(p * xs.length) - 1)] : null);
      const flows = [...byFlow.entries()].map(([flowId, rs]) => {
        const done = rs.filter((r) => r.status !== 'running');
        const durations = done.map((r) => (r.endedAt ?? r.startedAt) - r.startedAt).sort((x, y) => x - y);
        return {
          flowId,
          running: rs.length - done.length,
          succeeded: done.filter((r) => r.status === 'succeeded').length,
          failed: done.filter((r) => r.status === 'failed').length,
          p50Ms: pct(durations, 0.5),
          p95Ms: pct(durations, 0.95),
          lastFailures: done
            .filter((r) => r.status === 'failed')
            .sort((x, y) => (y.endedAt ?? 0) - (x.endedAt ?? 0))
            .slice(0, 5)
            .map((r) => ({ instanceId: r.id, at: r.endedAt ?? r.startedAt, durationMs: (r.endedAt ?? r.startedAt) - r.startedAt, error: 'falló' })),
        };
      });
      return send(res, 200, { flows, triggers: [] });
    }

    // bundles: exportar los flujos publicados e importar comparando con lo que hay (sin triggers ni conexiones en el mock)
    if (path === '/bundle' && method === 'GET') {
      const only = (url.searchParams.get('flows') ?? '').split(',').filter(Boolean);
      const flows = [...store.flows.values()].filter((f) => only.length === 0 || only.includes(f.id)).sort((x, y) => x.id.localeCompare(y.id));
      const bundle = {
        format: 'x-flow-bundle',
        version: 1,
        flows: flows.map((f) => ({ flow: { ...f, revision: undefined }, enabled: store.enabled.has(f.id) })),
        triggers: [],
        entities: [],
        connections: [],
        requires: { kinds: [...new Set(flows.flatMap((f) => f.nodes.map((n) => n.kind)))].sort(), connectionKinds: [] },
      };
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return void res.end(JSON.stringify(bundle, null, 2));
    }
    if (path === '/bundle' && method === 'POST') {
      const b = (body ?? {}) as { format?: string; flows?: Array<{ flow: FlowDef; enabled: boolean }> };
      if (b.format !== 'x-flow-bundle') return send(res, 400, { error: `not a bundle: format '${b.format}', expected 'x-flow-bundle'` });
      const kinds = new Set(catalog.map((c) => c.kind));
      const incoming = b.flows ?? [];
      const problems = [...new Set(incoming.flatMap((f) => f.flow.nodes.map((n) => n.kind)).filter((k) => !kinds.has(k)))].map(
        (k) => `this server has no node kind '${k}'`,
      );
      const items = incoming.map(({ flow }) => {
        const old = store.flows.get(flow.id);
        const same = old && JSON.stringify({ ...old, revision: undefined }) === JSON.stringify({ ...flow, revision: undefined });
        return { type: 'flow', id: flow.id, action: !old ? 'create' : same ? 'same' : 'update' };
      });
      const plan = { items, problems, warnings: [] as string[] };
      if (url.searchParams.get('dryRun') === 'true') return send(res, 200, plan);
      if (problems.length) return send(res, 422, plan);
      for (const { flow, enabled } of incoming) {
        const old = store.flows.get(flow.id);
        store.flows.set(flow.id, { ...flow, revision: (old?.revision ?? 0) + 1 });
        if (enabled) store.enabled.add(flow.id);
        else store.enabled.delete(flow.id);
      }
      return send(res, 200, plan);
    }

    // catálogo
    if (method === 'GET' && path === '/catalog') return send(res, 200, catalog);
    if (method === 'POST' && seg[0] === 'catalog' && seg[2] === 'describe') {
      const entry = catalog.find((c) => c.kind === seg[1]);
      if (!entry) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `node kind '${seg[1]}' does not exist` }] });
      const params = (body ?? {}) as Record<string, Json>;
      const reads: Array<{ path: string; type: Json }> = [];
      if (seg[1] === 'core.if' && typeof params.path === 'string') reads.push({ path: params.path, type: { type: 'bool' } });
      if (seg[1] === 'stream.source.list' && typeof params.path === 'string') reads.push({ path: params.path, type: { type: 'list', element: { type: 'any' } } });
      if (seg[1] === 'entity.validate' && typeof params.path === 'string') reads.push({ path: params.path, type: { type: 'any' } });
      if (seg[1] === 'entity.transition' && typeof params.path === 'string' && typeof params.entity === 'string') reads.push({ path: params.path, type: { type: 'entity', name: params.entity } });
      const entityDefaults =
        seg[1] === 'entity.build' ? entityOutputs(params.entity, typeof params.entity === 'string' && params.entity ? params.entity[0].toLowerCase() + params.entity.slice(1) : '')
        : seg[1] === 'entity.validate' || seg[1] === 'entity.transition' ? entityOutputs(params.entity, typeof params.path === 'string' ? params.path : '')
        : null;
      return send(res, 200, {
        ports: entry.ports,
        inputs: entry.inputs ?? [{ name: 'in' }],
        role: entry.role ?? 'node',
        defaultOutputs: entityDefaults ?? entry.defaultOutputs,
        reads,
        problems: validateParams(seg[1], params),
      });
    }

    // flujos
    if (method === 'GET' && path === '/flows') {
      const published = [...store.flows.values()].map((f) => ({ id: f.id, name: f.name, revision: f.revision, enabled: store.enabled.has(f.id), draft: store.drafts.has(f.id) }));
      const onlyDraft = [...store.drafts.values()].filter((d) => !store.flows.has(d.id)).map((d) => ({ id: d.id, name: d.name, revision: 0, enabled: false, draft: true }));
      return send(res, 200, [...published, ...onlyDraft].sort((a, b) => a.id.localeCompare(b.id)));
    }
    if (seg[0] === 'flows' && seg.length === 3 && seg[2] === 'draft') {
      const id = seg[1];
      const notFound = () => send(res, 404, { errors: [{ code: 'not-found', node: null, message: `flow '${id}' has no draft` }] });
      if (method === 'GET') {
        const d = store.drafts.get(id);
        return d ? send(res, 200, { flow: d, errors: validate(d, new Set([...flowsKnown(), id])) }) : notFound();
      }
      if (method === 'PUT') {
        const d = { ...(body as FlowDef), id };
        store.drafts.set(id, d);
        return send(res, 200, { flow: d, errors: validate(d, new Set([...flowsKnown(), id])) });
      }
      if (method === 'DELETE') {
        store.drafts.delete(id);
        return send(res, 204);
      }
    }
    if (seg[0] === 'flows' && seg.length === 3 && seg[2] === 'publish' && method === 'POST') {
      const id = seg[1];
      const d = store.drafts.get(id);
      if (!d) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `flow '${id}' has no draft` }] });
      const errors = validate(d, new Set([...flowsKnown(), id]));
      if (errors.length) return send(res, 422, { errors });
      const prev = store.flows.get(id);
      const saved: FlowDef = { ...d, id, revision: prev ? prev.revision + 1 : 1 };
      store.flows.set(id, saved);
      store.history.set(id, [...(store.history.get(id) ?? []), structuredClone(saved)]);
      store.drafts.delete(id);
      return send(res, 200, saved);
    }
    if (method === 'POST' && path === '/flows/validate')
      return send(res, 200, { errors: validateFlowDef(body, flowsKnown()) });
    if (seg[0] === 'flows' && seg.length === 3 && seg[2] === 'schedule' && method === 'GET') {
      const f = store.flows.get(seg[1]);
      return f ? send(res, 200, scheduleOf(f, store.enabled.has(f.id), Date.now())) : send(res, 404, { errors: [{ code: 'not-found', node: null, message: `flow '${seg[1]}' does not exist` }] });
    }
    if (seg[0] === 'flows' && seg.length === 3 && (seg[2] === 'enable' || seg[2] === 'disable') && method === 'POST') {
      const f = store.flows.get(seg[1]);
      if (!f) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `flow '${seg[1]}' does not exist` }] });
      if (seg[2] === 'enable') store.enabled.add(f.id);
      else store.enabled.delete(f.id);
      return send(res, 200, scheduleOf(f, store.enabled.has(f.id), Date.now()));
    }
    if (seg[0] === 'flows' && seg.length === 2) {
      const id = seg[1];
      if (method === 'GET') {
        const rev = url.searchParams.get('revision');
        if (rev) {
          const n = Number(rev);
          const found = store.history.get(id)?.find((f) => f.revision === n);
          return found ? send(res, 200, found) : send(res, 404, { errors: [{ code: 'not-found', node: null, message: `flow '${id}' does not exist in revision ${rev}` }] });
        }
        const f = store.flows.get(id);
        return f ? send(res, 200, f) : send(res, 404, { errors: [{ code: 'not-found', node: null, message: `flow '${id}' does not exist` }] });
      }
      if (method === 'PUT') {
        const flow = body as FlowDef;
        const errors = validate(flow, new Set([...flowsKnown(), id]));
        if (errors.length) return send(res, 422, { errors });
        const prev = store.flows.get(id);
        const saved: FlowDef = { ...flow, id, revision: prev ? prev.revision + 1 : 1 };
        store.flows.set(id, saved);
        store.history.set(id, [...(store.history.get(id) ?? []), structuredClone(saved)]);
        store.drafts.delete(id);
        return send(res, 200, saved);
      }
      if (method === 'DELETE') {
        store.flows.delete(id);
        store.drafts.delete(id);
        store.history.delete(id);
        store.enabled.delete(id);
        return send(res, 204);
      }
    }
    if (seg[0] === 'flows' && seg[2] === 'instances' && method === 'POST') {
      const flowId = seg[1];
      if (!store.flows.has(flowId)) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `flow '${flowId}' does not exist` }] });
      const b = (body ?? {}) as { inputs?: Json };
      const rec = createRun({ flowId, inputs: b.inputs ?? {}, origin: { type: 'manual' }, startedAt: Date.now(), live: true });
      return send(res, 202, { instanceId: rec.id });
    }
    if (seg[0] === 'flows' && seg[2] === 'test' && method === 'POST') {
      const b = body as { flow?: FlowDef; inputs?: Json; mocks?: Record<string, Mock> };
      const flow = b.flow ?? store.flows.get(seg[1]);
      if (!flow) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `flow '${seg[1]}' does not exist` }] });
      return send(res, 200, simulate(flow, b.inputs ?? {}, b.mocks ?? {}, flowsKnown(), (id) => store.flows.get(id), nextFireFn));
    }

    // instancias: historial, detalle, auditoría, retry, eventos
    if (seg[0] === 'instances' && seg.length === 1 && method === 'GET') {
      const q = url.searchParams;
      const limit = Math.min(Number(q.get('limit') ?? 50), 500);
      let list = [...store.instances.values()].sort((a, b) => b.startedAt - a.startedAt || (a.id < b.id ? 1 : -1));
      const flowId = q.get('flowId');
      const status = q.get('status');
      const origin = q.get('origin');
      const from = q.get('from') ? Date.parse(q.get('from')!) : null;
      const to = q.get('to') ? Date.parse(q.get('to')!) : null;
      if (flowId) list = list.filter((r) => r.flowId === flowId);
      if (status) list = list.filter((r) => r.status === status);
      if (origin) list = list.filter((r) => { const l = originLabel(r.origin); return l === origin || l.startsWith(`${origin}:`); });
      if (from !== null) list = list.filter((r) => r.startedAt >= from);
      if (to !== null) list = list.filter((r) => r.startedAt <= to);
      const after = q.get('after');
      if (after) {
        const idx = list.findIndex((r) => r.id === after);
        list = idx >= 0 ? list.slice(idx + 1) : [];
      }
      return send(res, 200, list.slice(0, limit).map(instanceSummary));
    }
    if (seg[0] === 'instances' && seg[2] === 'cancel' && method === 'POST') {
      const r = store.instances.get(seg[1]);
      if (!r) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `instance '${seg[1]}' does not exist` }] });
      if (r.status === 'running') {
        // El stream se cierra: lo que corría queda fallido con `cancelled` y lo que no había empezado, `dead` (skipped).
        const anchor = r.final.endless;
        const now = Date.now();
        const seq0 = r.entries.length;
        const stop: AuditEntryOut[] = [];
        if (anchor) {
          const live = liveSegment(r);
          stop.push({ seq: seq0, at: now, kind: 'node-finished', node: anchor, nodeKind: SEGMENT_KIND, attempt: 1, status: 'failed', port: 'error', error: 'cancelled', outputs: [], info: { metrics: live?.metrics ?? {}, deadLetters: live?.deadLetters ?? [] } } as AuditEntryOut);
          r.final.nodes[anchor] = { status: 'failed', port: 'error', error: 'cancelled', writes: [], info: { metrics: (live?.metrics ?? {}) as unknown as Json, deadLetters: (live?.deadLetters ?? []) as unknown as Json } };
        }
        stop.push({ seq: seq0 + stop.length, at: now, kind: 'instance-finished', status: 'failed', durationMs: now - r.startedAt } as AuditEntryOut);
        r.entries.push(...stop);
        for (const e of stop) emit(r, e);
        if (r.timer) clearTimeout(r.timer);
        r.revealed = r.entries.length;
        r.cancelled = true;
        r.status = 'failed';
        r.endedAt = now;
        r.final = { ...r.final, status: 'failed' };
      }
      return send(res, 202, { instanceId: seg[1] });
    }
    if (seg[0] === 'instances' && seg.length === 2 && method === 'GET') {
      const r = store.instances.get(seg[1]);
      return r ? send(res, 200, instanceView(r)) : send(res, 404, { errors: [{ code: 'not-found', node: null, message: `instance '${seg[1]}' does not exist` }] });
    }
    if (seg[0] === 'instances' && seg[2] === 'audit' && method === 'GET') {
      const r = store.instances.get(seg[1]);
      return r ? send(res, 200, auditOf(r)) : send(res, 404, { errors: [{ code: 'not-found', node: null, message: `instance '${seg[1]}' does not exist` }] });
    }
    if (seg[0] === 'instances' && seg[2] === 'retry' && method === 'POST') {
      const old = store.instances.get(seg[1]);
      if (!old) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `instance '${seg[1]}' does not exist` }] });
      if (old.status === 'running') {
        return send(res, 409, { errors: [{ code: 'not-finished', node: null, message: `la instancia ${old.id} no ha terminado` }] });
      }
      const rec = retryInPlace(old, 'ui', true, Date.now());
      return send(res, 202, { instanceId: rec.id });
    }
    if (seg[0] === 'instances' && seg[2] === 'events' && method === 'GET') {
      const r = store.instances.get(seg[1]);
      if (!r) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `instance '${seg[1]}' does not exist` }] });
      res.statusCode = 200;
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      for (const e of r.entries.slice(0, r.revealed)) res.write(sse(e));
      r.subscribers.add(res);
      req.on('close', () => r.subscribers.delete(res));
      return;
    }

    // triggers
    if (seg[0] === 'triggers') {
      if (seg.length === 1 && method === 'GET') return send(res, 200, [...store.triggers.values()]);
      if (seg.length === 2 && method === 'GET') {
        const t = store.triggers.get(seg[1]);
        return t ? send(res, 200, t) : send(res, 404, { errors: [{ code: 'not-found', node: null, message: `trigger '${seg[1]}' does not exist` }] });
      }
      if (seg.length === 2 && method === 'PUT') {
        const sent = { ...(body as Trigger), id: seg[1] };
        if (!store.flows.has(sent.flowId)) {
          return send(res, 422, { errors: [{ code: 'invalid-trigger', node: null, message: `the flow '${sent.flowId}' does not exist` }] });
        }
        const problems = problemsOf(sent, seg[1]);
        if (problems.length) return send(res, 422, { errors: problems.map((m) => ({ code: 'invalid-trigger', node: null, message: m })) });
        store.triggers.set(sent.id, sent);
        return send(res, 200, sent);
      }
      if (seg.length === 2 && method === 'DELETE') {
        if (!store.triggers.has(seg[1])) return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `trigger '${seg[1]}' does not exist` }] });
        store.triggers.delete(seg[1]);
        return send(res, 204);
      }
    }

    // entidades del modelo de negocio (las de código o de plugins no se cambian)
    if (seg[0] === 'entities') {
      const all = store.entities;
      const notFound = (n: string) => send(res, 404, { errors: [{ code: 'not-found', node: null, message: `the entity '${n}' does not exist` }] });
      const users = (n: string) => [...all.values()].filter((e) => e.name !== n && JSON.stringify(e.fields).includes(`"${n}"`)).map((e) => e.name);
      if (seg.length === 1 && method === 'GET') {
        const list = [...all.values()].sort((a, b) => a.name.localeCompare(b.name));
        if (url.searchParams.get('full') === 'true') return send(res, 200, list);
        return send(res, 200, list.map((e) => ({ name: e.name, kind: e.kind, label: e.label ?? null, revision: e.revision ?? 1, source: e.source ?? 'store', fields: e.fields.length })));
      }
      if (seg.length === 2 && seg[1] === 'validate' && method === 'POST') {
        const e = body as EntityDef;
        return send(res, 200, { errors: validateEntity(e, new Map([...all, [e.name, e]])) });
      }
      const name = seg[1];
      if (seg.length === 2 && method === 'GET') return all.has(name) ? send(res, 200, all.get(name)) : notFound(name);
      if (seg.length === 3 && seg[2] === 'revisions' && method === 'GET') return all.has(name) ? send(res, 200, [all.get(name)]) : notFound(name);
      if (seg.length === 2 && method === 'PUT') {
        const prev = all.get(name);
        if (prev && prev.source && prev.source !== 'store') return send(res, 409, { errors: [{ code: 'read-only', node: null, message: `the entity '${name}' is defined in ${prev.source}` }] });
        const e = { ...(body as EntityDef), name };
        const errors = validateEntity(e, new Map([...all, [name, e]]));
        if (errors.length) return send(res, 422, { errors });
        const saved: EntityDef = { ...e, revision: (prev?.revision ?? 0) + 1, source: 'store' };
        all.set(name, saved);
        return send(res, 200, { entity: saved, affected: [] });
      }
      if (seg.length === 2 && method === 'DELETE') {
        const prev = all.get(name);
        if (!prev) return notFound(name);
        if (prev.source && prev.source !== 'store') return send(res, 409, { errors: [{ code: 'read-only', node: null, message: `the entity '${name}' is defined in ${prev.source}` }] });
        const using = users(name);
        if (using.length) return send(res, 409, { errors: [{ code: 'in-use', node: null, message: `the entity '${name}' is used by the entities ${using.join(', ')}` }] });
        all.delete(name);
        return send(res, 204);
      }
      if (seg.length === 3 && seg[2] === 'check' && method === 'POST') {
        const b = (body ?? {}) as { value?: Json; transition?: string; entity?: EntityDef };
        const catalog = b.entity ? new Map([...all, [name, { ...b.entity, name }]]) : all;
        if (!catalog.has(name)) return notFound(name);
        const r = b.transition ? transitionValue(name, b.value ?? {}, b.transition, catalog) : checkValue(name, b.value ?? {}, catalog);
        return send(res, 200, { ok: r.problems.length === 0, value: r.problems.length && b.transition ? b.value ?? {} : r.value, problems: r.problems });
      }
    }

    // conexiones (los secretos se guardan aparte y nunca se devuelven)
    // El mock no abre clientes: el pool siempre está vacío.
    if (method === 'GET' && path === '/connection-clients') return send(res, 200, []);
    if (method === 'POST' && /^\/connections\/[^/]+\/close$/.test(path)) return send(res, 204, '');
    if (method === 'GET' && path === '/connection-kinds') return send(res, 200, connectionKinds);
    if (seg[0] === 'connections') {
      const view = (name: string) => {
        const c = store.connections.get(name)!;
        return { name, kind: c.kind, params: c.params, secretsSet: c.secretsSet };
      };
      if (seg.length === 1 && method === 'GET') return send(res, 200, [...store.connections.keys()].sort().map(view));
      if (seg.length === 2 && method === 'PUT') {
        const b = body as { kind: string; params: Record<string, unknown>; secrets: Record<string, string> };
        if (!connectionKinds.some((k) => k.kind === b.kind)) {
          return send(res, 422, { errors: [{ code: 'invalid-connection', node: null, message: `unknown connection kind '${b.kind}'` }] });
        }
        const prev = store.connections.get(seg[1]);
        const secretsSet = new Set([...(prev?.kind === b.kind ? prev.secretsSet : []), ...Object.entries(b.secrets ?? {}).filter(([, v]) => v).map(([k]) => k)]);
        store.connections.set(seg[1], { kind: b.kind, params: b.params ?? {}, secretsSet: [...secretsSet] });
        return send(res, 200, view(seg[1]));
      }
      if (seg.length === 2 && method === 'DELETE') {
        store.connections.delete(seg[1]);
        return send(res, 204);
      }
    }

    return send(res, 404, { errors: [{ code: 'not-found', node: null, message: `no mock for ${method} ${path}` }] });
  };

  return {
    name: 'flow-mock-api',
    apply: 'serve',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        if (!req.url?.startsWith('/api/')) return next();
        handle(req, res).catch((e: Error) => send(res, 500, { errors: [{ code: 'mock', node: null, message: e.message }] }));
      });
    },
  };
}

