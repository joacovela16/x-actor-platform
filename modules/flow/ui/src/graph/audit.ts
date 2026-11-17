import type {
  AuditDecision, AuditEdgeRef, AuditEntry, AuditInstance, AuditOrigin, AuditRead, AuditTrail, AuditWrite, Edge, FlowDef,
  InstanceSummary, JsonValue, Stamp,
} from '../api/types';
import { edgeKey } from './graph';
import { fmtClock, toMillis } from './time';

export { fmtDateTime, fmtDuration, fmtTime } from './time';

/** Valor redactado: la auditoría lo marca como `{"$redacted": true}` o como el texto "[redacted]". */
export function isRedacted(v: unknown): boolean {
  if (v === '[redacted]') return true;
  return typeof v === 'object' && v !== null && !Array.isArray(v) && (v as Record<string, unknown>).$redacted === true;
}

export const REDACTED_TEXT = '••• redacted';

const PLACEHOLDER = '__flow_redacted__';

/** Texto de un valor de la auditoría. Los redactados se muestran como «••• redacted» en cualquier nivel. */
export function renderValue(v: unknown): string {
  if (isRedacted(v)) return REDACTED_TEXT;
  if (v === undefined) return '—';
  const json = JSON.stringify(v, (_k, x: unknown) => (isRedacted(x) ? PLACEHOLDER : x), 2);
  return json === undefined ? '—' : json.split(`"${PLACEHOLDER}"`).join(REDACTED_TEXT);
}

type Raw = Record<string, unknown>;
const isObj = (v: unknown): v is Raw => typeof v === 'object' && v !== null && !Array.isArray(v);
const str = (v: unknown): string | undefined => (typeof v === 'string' ? v : undefined);

/** Origen: texto corto (`trigger:<id>`) o estructurado (`{type: trigger|manual|subflow|retry|cron, ...}`). Devuelve `kind`. */
export function normalizeOrigin(o: unknown): AuditOrigin | undefined {
  if (typeof o === 'string') {
    const [head, ...rest] = o.split(':');
    const tail = rest.join(':');
    switch (head) {
      case 'trigger': return { kind: 'trigger', triggerId: tail };
      case 'retry': return { kind: 'retry', retryOf: tail };
      case 'subflow': return { kind: 'subflow', parentInstanceId: tail };
      case 'cron': return { kind: 'cron', schedule: tail };
      case 'manual': return { kind: 'manual' };
      default: return { kind: head };
    }
  }
  if (!isObj(o)) return undefined;
  const type = str(o.type) ?? str(o.kind);
  switch (type) {
    case 'trigger': return { kind: 'trigger', triggerId: str(o.triggerId), requestId: str(o.requestId) };
    case 'manual': return { kind: 'manual', user: str(o.user) };
    case 'subflow': return { kind: 'subflow', parentInstanceId: str(o.parentInstanceId), parentNode: str(o.parentNodeId) ?? str(o.parentNode) };
    case 'retry': return { kind: 'retry', retryOf: str(o.of) ?? str(o.retryOf), reason: str(o.by) ?? str(o.reason) };
    case 'cron': return { kind: 'cron', schedule: str(o.schedule) };
    default: return { kind: type ?? 'desconocido' };
  }
}

function normalizeWrites(list: unknown): AuditWrite[] | undefined {
  if (!Array.isArray(list)) return undefined;
  return list.filter(isObj).map((w) => ({ path: String(w.path ?? ''), previous: w.previous, value: w.value }));
}

function normalizeReads(list: unknown): AuditRead[] | undefined {
  if (!Array.isArray(list)) return undefined;
  return list.filter(isObj).map((r) => ({ path: String(r.path ?? ''), value: r.value }));
}

function edgeRef(v: unknown): AuditEdgeRef | undefined {
  if (!isObj(v)) return undefined;
  const ref: AuditEdgeRef = { from: String(v.from ?? ''), port: String(v.port ?? ''), to: String(v.to ?? '') };
  // Una arista que entra en un segmento lleva la fuente que toca (`input`); el destino es el anclaje del segmento.
  if (typeof v.input === 'string' && v.input !== 'in') ref.input = v.input;
  return ref;
}

/**
 * Decisión del motor. Acepta la forma real (`skipped-by-run-policy {rule, port, outputs, error}`,
 * `skipped-dead-branch {causes}`, `waiting-join {pending}`, `scheduled {fireAt}`) y la antigua del mock.
 */
export function normalizeDecision(d: unknown, node: string | undefined): AuditDecision | undefined {
  if (!isObj(d)) return undefined;
  const reason = str(d.reason);
  switch (reason) {
    case 'skipped-by-run-policy': {
      const outputs = normalizeWrites(d.outputs) ?? normalizeWrites((d.reused as Raw | undefined)?.writes);
      const rule = str(d.rule);
      const reused = isObj(d.reused) ? d.reused : undefined;
      return {
        reason,
        rule,
        previousStatus: rule?.includes('whenFailed') ? 'failed' : rule?.includes('whenSucceeded') ? 'succeeded' : str(d.previousStatus),
        reused: {
          port: (str(d.port) ?? str(reused?.port)) ?? null,
          error: (d.error as string | null | undefined) ?? (reused?.error as string | null | undefined) ?? null,
          childInstanceId: (reused?.childInstanceId as string | null | undefined) ?? null,
          writes: outputs,
        },
      };
    }
    case 'skipped-dead-branch': {
      if (Array.isArray(d.causes)) {
        const causes = d.causes.filter(isObj);
        return {
          reason,
          deadEdges: causes.map((c) => ({ from: String(c.from), port: String(c.port), to: node ?? '' })),
          decidedBy: causes.map((c) => {
            const read = Array.isArray(c.decidedWith) ? (c.decidedWith[0] as Raw | undefined) : undefined;
            const took = (c.took as string | null | undefined) ?? (c.why === 'source-not-run' ? 'no ejecutado' : undefined);
            return {
              node: String(c.from),
              edgePort: String(c.port),
              portTaken: took,
              path: read ? str(read.path) : undefined,
              value: read?.value,
            };
          }),
        };
      }
      return { reason, deadEdges: (d.deadEdges as AuditEdgeRef[] | undefined) ?? [], decidedBy: (d.decidedBy as AuditDecision['decidedBy']) ?? [] };
    }
    case 'waiting-join': {
      const pending = Array.isArray(d.pending) ? d.pending : Array.isArray(d.pendingEdges) ? d.pendingEdges : [];
      return { reason, pendingEdges: pending.map(edgeRef).filter((x): x is AuditEdgeRef => !!x) };
    }
    case 'scheduled':
      return { reason, fireAt: toMillis(d.fireAt as Stamp) ?? undefined };
    default:
      return { reason: reason ?? undefined };
  }
}

/** Una entrada de auditoría tal como llega (forma real o antigua) pasada a la forma que usa la UI. */
export function normalizeEntry(raw: unknown, index: number): AuditEntry {
  const r: Raw = isObj(raw) ? raw : {};
  const node = str(r.node) ?? str(r.nodeId);
  const simulation = isObj(r.simulation) ? r.simulation : undefined;
  const decision = normalizeDecision(r.decision, node);
  const kind = str(r.kind) ?? str(r.type) ?? 'desconocido';
  const entry: AuditEntry = {
    seq: typeof r.seq === 'number' ? r.seq : index,
    at: r.at as Stamp,
    kind,
    node,
    nodeKind: str(r.nodeKind),
    attempt: typeof r.attempt === 'number' ? r.attempt : undefined,
    durationMs: typeof r.durationMs === 'number' ? r.durationMs : undefined,
    reads: normalizeReads(r.reads),
    writes: normalizeWrites(r.writes) ?? normalizeWrites(r.outputs) ?? normalizeWrites(r.changes),
    memoryBefore: r.memoryBefore ?? r.memory,
    memoryAfter: r.memoryAfter,
    params: r.params,
    port: (r.port as string | null | undefined) ?? undefined,
    error: (r.error as string | null | undefined) ?? undefined,
    mocked: r.mocked === true || simulation?.outcome === 'mocked',
    simulated: r.simulated === true || simulation !== undefined,
    decision,
    edge: edgeRef(r.edge),
    childInstanceId: (r.childInstanceId as string | null | undefined) ?? undefined,
    reason: str(r.reason),
    status: str(r.status),
    inputs: r.inputs,
    info: isObj(r.info) ? r.info : undefined,
  };
  if (kind === 'retry-requested') {
    entry.retryOf = normalizeOrigin(r.originDetail ?? r.origin)?.retryOf ?? str(r.retryOf);
  } else if (typeof r.retryOf === 'string') {
    entry.retryOf = r.retryOf;
  }
  if (kind === 'instance-started' && isObj(r.originDetail)) entry.origin = normalizeOrigin(r.originDetail);
  return entry;
}

/** Cabecera + entradas de `GET /api/instances/{id}/audit`, normalizadas. Acepta también la forma antigua. */
export function normalizeTrail(raw: unknown): AuditTrail {
  const r: Raw = isObj(raw) ? raw : {};
  const inst: Raw = isObj(r.instance) ? r.instance : {};
  const instance: AuditInstance = {
    id: String(inst.id ?? ''),
    flowId: String(inst.flowId ?? ''),
    revision: typeof inst.revision === 'number' ? inst.revision : undefined,
    status: (str(inst.status) === 'dead' ? 'skipped' : str(inst.status)) ?? 'running',
    origin: normalizeOrigin(inst.originDetail ?? inst.origin),
    startedAt: inst.startedAt as Stamp,
    endedAt: inst.endedAt as Stamp,
    inputs: inst.inputs as JsonValue | undefined,
  };
  const list = Array.isArray(r.entries) ? r.entries : [];
  return { instance, entries: list.map((e, i) => normalizeEntry(e, i)) };
}

/** Fila de `GET /api/instances` con el origen normalizado (`origin` es la etiqueta corta; `originDetail`, la estructurada). */
export function normalizeInstanceSummary(raw: InstanceSummary): InstanceSummary {
  const detail = raw.originDetail ?? (typeof raw.origin === 'object' ? raw.origin : undefined);
  return {
    ...raw,
    origin: typeof raw.origin === 'string' ? raw.origin : normalizeOriginLabel(raw.origin),
    originDetail: isObj(detail) ? detail : undefined,
  };
}

function normalizeOriginLabel(o: unknown): string | undefined {
  const n = normalizeOrigin(o);
  if (!n?.kind) return undefined;
  switch (n.kind) {
    case 'trigger': return `trigger:${n.triggerId ?? ''}`;
    case 'retry': return `retry:${n.retryOf ?? ''}`;
    case 'subflow': return `subflow:${n.parentInstanceId ?? ''}`;
    case 'cron': return `cron:${n.schedule ?? ''}`;
    default: return n.kind;
  }
}

/** Texto corto y en español de cada tipo de entrada. Los tipos desconocidos se muestran tal cual. */
export const KIND_LABEL: Record<string, string> = {
  'instance-started': 'inicio',
  'retry-requested': 'reintento',
  'node-started': 'nodo empieza',
  'node-finished': 'nodo termina',
  'node-skipped': 'nodo omitido',
  'node-waiting': 'espera',
  'edge-fired': 'arista disparada',
  'edge-dead': 'arista muerta',
  'state-changed': 'estado cambia',
  'instance-finished': 'fin',
};

export const REASON_LABEL: Record<string, string> = {
  'skipped-by-run-policy': 'omitido por RunPolicy',
  'skipped-dead-branch': 'omitido: rama no tomada',
  'waiting-join': 'esperando entradas del join',
  scheduled: 'esperando al horario',
};

export function kindLabel(kind: string): string {
  return KIND_LABEL[kind] ?? kind;
}

/** Clase de color de una entrada (para la línea de tiempo). */
export function kindTone(e: AuditEntry): 'ok' | 'err' | 'skip' | 'run' | 'wait' | 'neutral' {
  if (e.kind === 'node-finished') return e.error ? 'err' : 'ok';
  if (e.kind === 'node-skipped') return 'skip';
  if (e.kind === 'node-waiting') return 'wait';
  if (e.kind === 'node-started') return 'run';
  if (e.kind === 'instance-finished') return e.status === 'failed' ? 'err' : 'ok';
  return 'neutral';
}

/** Tokens que gastó un `ai.agent` (`node-finished.info.usage`). */
export interface UsageView {
  inputTokens: number;
  outputTokens: number;
}

export function usageOf(info: Record<string, unknown> | undefined): UsageView | undefined {
  const u = info?.usage;
  if (!isObj(u)) return undefined;
  const i = Number(u.inputTokens);
  const o = Number(u.outputTokens);
  if (!Number.isFinite(i) && !Number.isFinite(o)) return undefined;
  return { inputTokens: Number.isFinite(i) ? i : 0, outputTokens: Number.isFinite(o) ? o : 0 };
}

/** Líneas que un `script.js` / `stream.script` escribió con `log(...)` (`node-finished.info.logs`). */
export function logsOf(info: Record<string, unknown> | undefined): string[] {
  const l = info?.logs;
  return Array.isArray(l) ? l.map((x) => (typeof x === 'string' ? x : JSON.stringify(x))) : [];
}

/** Una línea de resumen para la lista de la línea de tiempo. */
export function summaryOf(e: AuditEntry): string {
  if (e.kind === 'node-finished') {
    const parts = [e.error ? `error: ${e.error}` : `puerto ${e.port ?? '—'}`];
    const u = usageOf(e.info);
    if (u) parts.push(`${u.inputTokens + u.outputTokens} tokens`);
    const logs = logsOf(e.info);
    if (logs.length) parts.push(`${logs.length} log${logs.length === 1 ? '' : 's'}`);
    return parts.join(' · ');
  }
  if (e.kind === 'node-skipped' && e.decision?.reason) return REASON_LABEL[e.decision.reason] ?? e.decision.reason;
  if (e.kind === 'node-waiting') {
    if (e.decision?.reason === 'scheduled' && e.decision.fireAt !== undefined) return `hasta ${fmtClock(e.decision.fireAt)}`;
    return REASON_LABEL[e.decision?.reason ?? 'waiting-join'] ?? 'esperando';
  }
  if (e.kind === 'edge-fired' || e.kind === 'edge-dead') {
    const ed = e.edge;
    return ed ? `${ed.from}.${ed.port} → ${ed.to}` : '';
  }
  if (e.kind === 'retry-requested') return e.retryOf ? `de ${e.retryOf}` : '';
  if (e.kind === 'instance-finished') return `estado ${String(e.status ?? '?')}`;
  return '';
}

export function edgeRefKey(e: AuditEdgeRef | Edge): string {
  return edgeKey({ from: e.from, port: e.port, to: e.to, input: typeof e.input === 'string' ? e.input : undefined });
}

export interface Snapshot {
  nodeStatus: Record<string, string>;
  /** Nodos en espera de un temporizador: epoch ms en que siguen. */
  fireAt: Record<string, number>;
  current: string | null;
  visited: Set<string>;
  edgeClasses: Record<string, string>;
}

/** Un segmento de stream tal como lo muestra la auditoría (`node-started.params` del anclaje). */
export interface SegmentView {
  members: string[];
  sources: string[];
  sinks: string[];
}

const idsOf = (list: unknown): string[] =>
  (Array.isArray(list) ? list : [])
    .map((x) => (typeof x === 'string' ? x : isObj(x) && typeof x.id === 'string' ? x.id : ''))
    .filter((x) => x !== '');

/** Los miembros de un segmento. `members` trae los nodos completos (`NodeDef`); se toma su id. */
export function segmentView(params: unknown): SegmentView | undefined {
  if (!isObj(params) || !Array.isArray(params.members)) return undefined;
  return { members: idsOf(params.members), sources: idsOf(params.sources), sinks: idsOf(params.sinks) };
}

/**
 * Estado del grafo tras la entrada `idx` de la auditoría: nodos ejecutados, omitidos, en espera y aristas
 * disparadas o muertas. Un segmento de stream es un paso: su anclaje y todos sus miembros toman su estado. Las aristas
 * que la auditoría da contra el anclaje se traducen a las del flujo (entrada a una fuente; salida de un sumidero o error de
 * cualquier miembro). Si la auditoría no trae `edge-*` para un nodo, las aristas se deducen del puerto tomado.
 */
export function snapshotAt(entries: AuditEntry[], idx: number, flow: FlowDef | null): Snapshot {
  const nodeStatus: Record<string, string> = {};
  const fireAt: Record<string, number> = {};
  const edgeClasses: Record<string, string> = {};
  const visited = new Set<string>();
  const upto = entries.slice(0, Math.max(0, idx) + 1);

  const segs = new Map<string, SegmentView>();
  for (const e of entries) {
    if (e.kind === 'node-started' && e.nodeKind === 'stream.segment' && e.node) {
      const v = segmentView(e.params);
      if (v) segs.set(e.node, v);
    }
  }
  const group = (node: string): string[] => [node, ...(segs.get(node)?.members ?? [])];

  const hasEdgeEntriesFrom = new Set<string>();
  for (const e of entries) if (e.kind.startsWith('edge-') && e.edge) hasEdgeEntriesFrom.add(e.edge.from);

  /** Salidas del flujo de un nodo (o de un segmento: sus sumideros por `out`, o cualquier miembro por `error`). Las internas no cuentan. */
  const outgoing = (node: string): Array<{ key: string; port: string; takes: boolean }> => {
    if (!flow) return [];
    const seg = segs.get(node);
    const from = seg ? flow.edges.filter((ed) => seg.members.includes(ed.from) && !seg.members.includes(ed.to)) : flow.edges.filter((ed) => ed.from === node);
    return from.map((ed) => ({ key: edgeKey(ed), port: ed.port, takes: seg ? ed.port === 'error' || seg.sinks.includes(ed.from) : true }));
  };

  /** Marca las salidas de `node`: la que tomó `port` como disparada, las demás como muertas. */
  const deduce = (node: string, port: string | null) => {
    if (!flow || hasEdgeEntriesFrom.has(node)) return;
    for (const o of outgoing(node)) edgeClasses[o.key] = o.takes && o.port === port ? 'edge-fired' : 'edge-dead';
  };

  /** Claves de las aristas del flujo que corresponden a una arista de la auditoría. */
  const keysOf = (ref: AuditEdgeRef): string[] => {
    if (segs.has(ref.from)) return outgoing(ref.from).filter((o) => o.takes && o.port === ref.port).map((o) => o.key);
    // Una entrada que entra en un segmento lleva la fuente que toca: esa es la arista del flujo.
    return [edgeKey({ from: ref.from, port: ref.port, to: typeof ref.input === 'string' ? ref.input : ref.to })];
  };

  for (const e of upto) {
    const n = e.node;
    switch (e.kind) {
      case 'node-started':
        if (n) for (const id of group(n)) { nodeStatus[id] = 'running'; visited.add(id); delete fireAt[id]; }
        break;
      case 'node-finished':
        if (n) {
          const status = e.error || e.port === 'error' ? 'failed' : 'succeeded';
          for (const id of group(n)) { nodeStatus[id] = status; delete fireAt[id]; visited.add(id); }
          deduce(n, e.port ?? null);
        }
        break;
      case 'node-skipped':
        if (n) {
          nodeStatus[n] = e.decision?.reason === 'skipped-by-run-policy' ? 'reused' : 'skipped';
          if (e.decision?.reason === 'skipped-by-run-policy') visited.add(n);
          if (e.decision?.reason === 'skipped-by-run-policy') deduce(n, e.decision.reused?.port ?? null);
          else deduce(n, null);
        }
        break;
      case 'node-waiting':
        if (n && e.decision?.reason === 'scheduled') {
          nodeStatus[n] = 'waiting';
          if (e.decision.fireAt !== undefined) fireAt[n] = e.decision.fireAt;
          visited.add(n);
        } else if (n && !nodeStatus[n]) nodeStatus[n] = 'waiting';
        break;
      case 'edge-fired':
        if (e.edge) for (const k of keysOf(e.edge)) edgeClasses[k] = 'edge-fired';
        break;
      case 'edge-dead':
        if (e.edge) for (const k of keysOf(e.edge)) edgeClasses[k] = 'edge-dead';
        break;
      default:
        break;
    }
  }
  const current = entries[idx]?.node ?? null;
  return { nodeStatus, fireAt, current, visited, edgeClasses };
}

/** Índice de la última entrada que habla de un nodo (para enlazar un clic en el grafo con la línea de tiempo). */
export function lastEntryOfNode(entries: AuditEntry[], node: string): number {
  for (let i = entries.length - 1; i >= 0; i--) if (entries[i].node === node) return i;
  return -1;
}

/** Descripción de un origen para la cabecera. */
export function originText(origin: unknown): string {
  if (typeof origin === 'string') return origin;
  const o = normalizeOrigin(origin);
  if (!o) return '—';
  switch (o.kind) {
    case 'trigger': return `disparador ${o.triggerId ?? '?'}${o.requestId ? ` · petición ${o.requestId}` : ''}`;
    case 'retry': return `reintento de ${o.retryOf ?? '?'}${o.reason ? ` · ${o.reason}` : ''}`;
    case 'subflow': return `subflujo${o.parentNode ? ` del nodo ${o.parentNode}` : ''}`;
    case 'cron': return `horario ${o.schedule ?? ''}`;
    case 'manual': return 'lanzada a mano';
    default: return String(o.kind ?? 'origen desconocido');
  }
}
