// Ejecución simulada de un segmento de stream (mock). Sigue las reglas de nodes-stream: fuentes, operadores y sumideros
// sobre listas de elementos; un elemento que un nodo no puede tratar es un dead letter y no para el stream. Sin tiempo
// real: una fuente sin fin (cron sin maxTicks, cola) da solo `ticks` elementos y se marca como `endless`.
import type { DeadLetter, Edge, JsonValue, Mock, NodeDef, NodeMetrics } from '../src/api/types.ts';
import { filterElement, getPath, parseExpr, parseMapParams, same, transformElement } from '../src/graph/expr.ts';
import { parseCron } from '../src/graph/cron.ts';

type Json = JsonValue;

export interface SegmentRun {
  port: 'out' | 'error';
  error: string | null;
  /** Escrituras en el estado: la primera salida de cada sumidero. */
  writes: Array<{ path: string; value: Json }>;
  /** Valores por nombre de salida del segmento (`<sumidero>.<nombre>`). */
  values: Record<string, Json>;
  metrics: Record<string, NodeMetrics>;
  deadLetters: DeadLetter[];
  /** Una fuente sin fin: el segmento no termina hasta que se cancela. */
  endless: boolean;
}

const SAMPLE = 5;
const UNBOUNDED_TICKS = 5;

/** Nodos del segmento en orden topológico (las aristas internas van de un nodo a otro). */
function topo(members: NodeDef[], edges: Edge[]): NodeDef[] {
  const indeg = new Map(members.map((m) => [m.id, 0]));
  for (const e of edges) indeg.set(e.to, (indeg.get(e.to) ?? 0) + 1);
  const queue = members.filter((m) => indeg.get(m.id) === 0).map((m) => m.id);
  const order: string[] = [];
  while (queue.length) {
    const id = queue.shift()!;
    order.push(id);
    for (const e of edges.filter((x) => x.from === id)) {
      indeg.set(e.to, indeg.get(e.to)! - 1);
      if (indeg.get(e.to) === 0) queue.push(e.to);
    }
  }
  return order.map((id) => members.find((m) => m.id === id)!);
}

const isObj = (j: Json | undefined): j is { [k: string]: Json } => typeof j === 'object' && j !== null && !Array.isArray(j);
const str = (j: Json | undefined): string | undefined => (typeof j === 'string' ? j : undefined);

/**
 * Corre un segmento. `mockOf` da el mock de una fuente (`{type:"output", values:{elements}}` o `{type:"fail", error}`).
 * `ticks` cuenta los elementos de una fuente sin fin (cron sin maxTicks, cola).
 */
export function runSegment(
  seg: NodeDef,
  state: Json,
  mockOf: (id: string) => Mock | undefined,
  opts: { now: number; ticks?: number },
): SegmentRun {
  const members = ((seg.params.members as unknown as NodeDef[]) ?? []).slice();
  const internal = (seg.params.edges as unknown as Edge[]) ?? [];
  const order = topo(members, internal);
  const out = new Map<string, Json[]>();
  const metrics: Record<string, NodeMetrics> = {};
  const deadLetters: DeadLetter[] = [];
  const values: Record<string, Json> = {};
  const writes: Array<{ path: string; value: Json }> = [];
  let endless = false;
  let error: string | null = null;

  const metricsOf = (id: string): NodeMetrics => (metrics[id] ??= { in: 0, out: 0, dropped: 0, failed: 0, latencyMs: null, sampleIn: [], sampleOut: [] });
  const dead = (node: string, msg: string, element: Json) => {
    metricsOf(node).failed++;
    if (deadLetters.length < 100) deadLetters.push({ node, error: msg, element, at: opts.now });
  };
  const sample = (list: Json[], into: Json[]) => {
    for (const x of list) if (into.length < SAMPLE) into.push(x);
  };

  for (const node of order) {
    const m = metricsOf(node.id);
    const inputs = (input: string) => internal.filter((e) => e.to === node.id && (e.input ?? 'in') === input).flatMap((e) => out.get(e.from) ?? []);
    const elemIn = inputs('in');
    const kind = node.kind;
    let produced: Json[] = [];
    m.in = elemIn.length;
    sample(elemIn, m.sampleIn);

    if (kind.startsWith('stream.source.')) {
      m.in = 0;
      const mock = mockOf(node.id);
      if (mock?.type === 'fail') {
        error ??= mock.error;
        continue;
      }
      if (mock?.type === 'output' && Array.isArray(mock.values?.elements)) {
        produced = mock.values!.elements as Json[];
      } else if (kind === 'stream.source.list') {
        const path = str(node.params.path) ?? '';
        const list = path ? getPath(state, path) : undefined;
        if (!Array.isArray(list)) {
          error ??= `'${path}' is not a list in the state`;
          continue;
        }
        produced = list;
      } else if (kind === 'stream.source.range') {
        const from = typeof node.params.from === 'number' ? node.params.from : 0;
        const to = typeof node.params.to === 'number' ? node.params.to : NaN;
        const step = typeof node.params.step === 'number' ? node.params.step : 1;
        if (!Number.isFinite(to) || step <= 0) {
          error ??= 'range needs a finite "to" and a positive step';
          continue;
        }
        for (let n = from; n < to && produced.length < 100_000; n += step) produced.push(n);
      } else if (kind === 'stream.source.cron') {
        const parsed = typeof node.params.expression === 'string' ? parseCron(node.params.expression) : null;
        if (!parsed || !parsed.ok) {
          error ??= parsed && !parsed.ok ? parsed.error : 'missing expression';
          continue;
        }
        const max = typeof node.params.maxTicks === 'number' ? node.params.maxTicks : undefined;
        const count = max ?? (opts.ticks ?? UNBOUNDED_TICKS);
        if (max === undefined) endless = true;
        for (let i = 0; i < count; i++) produced.push({ at: opts.now + i * 1000 });
      } else if (kind === 'stream.source.queue') {
        // Sin broker en el mock: una cola nunca da nada y no termina.
        endless = true;
        produced = [];
      }
      m.out = produced.length;
      sample(produced, m.sampleOut);
      out.set(node.id, produced);
      continue;
    }

    if (kind === 'stream.map') {
      const parsed = parseMapParams(node.params);
      if (!parsed.ok) {
        error ??= parsed.error;
        continue;
      }
      for (const el of elemIn) {
        const r = transformElement(parsed.p, el);
        if (r.ok) produced.push(r.v);
        else dead(node.id, r.error, el);
      }
    } else if (kind === 'stream.filter') {
      const e = parseExpr(node.params.where ?? null);
      if (!e.ok) {
        error ??= e.error;
        continue;
      }
      for (const el of elemIn) {
        const r = filterElement(e.expr, el);
        if (!r.ok) dead(node.id, r.error, el);
        else if (r.pass) produced.push(el);
        else m.dropped++;
      }
    } else if (kind === 'stream.flatMap') {
      const path = str(node.params.path) ?? '';
      const merge = node.params.merge === true;
      for (const el of elemIn) {
        const list = getPath(el, path);
        if (!Array.isArray(list)) {
          dead(node.id, `'${path}' is not a list in the element`, el);
          continue;
        }
        for (const item of list) produced.push(merge && isObj(item) && isObj(el) ? { ...el, ...item } : item);
      }
    } else if (kind === 'stream.merge' || kind === 'stream.throttle') {
      produced = elemIn;
    } else if (kind === 'stream.dedupe') {
      const key = str(node.params.key);
      const max = typeof node.params.max === 'number' ? node.params.max : 10000;
      const seen: Json[] = [];
      for (const el of elemIn) {
        const k = key ? getPath(el, key) ?? null : el;
        if (seen.some((s) => same(s, k))) {
          m.dropped++;
          continue;
        }
        seen.push(k);
        if (seen.length > max) seen.shift();
        produced.push(el);
      }
    } else if (kind === 'stream.window') {
      const size = typeof node.params.size === 'number' ? node.params.size : 1;
      for (let i = 0; i < elemIn.length; i += size) {
        const items = elemIn.slice(i, i + size);
        produced.push({ items, count: items.length, start: opts.now, end: opts.now });
      }
    } else if (kind === 'stream.aggregate') {
      produced = aggregate(node.params, elemIn, (msg, el) => dead(node.id, msg, el));
    } else if (kind === 'stream.zip') {
      const left = inputs('left');
      const right = inputs('right');
      for (let i = 0; i < Math.min(left.length, right.length); i++) produced.push({ left: left[i], right: right[i] });
      m.in = left.length + right.length;
    } else if (kind === 'stream.join') {
      const left = inputs('left');
      const right = inputs('right');
      const lk = str(node.params.leftKey) ?? '';
      const rk = str(node.params.rightKey) ?? '';
      m.in = left.length + right.length;
      for (const l of left) for (const r of right) if (same(getPath(l, lk) ?? null, getPath(r, rk) ?? null)) produced.push({ left: l, right: r });
    } else if (kind === 'stream.sink.collect') {
      const limit = typeof node.params.limit === 'number' ? node.params.limit : 1000;
      const path = str(node.params.path);
      const kept = elemIn.slice(0, limit).map((el) => (path ? getPath(el, path) ?? null : el));
      setSinkOutput(node, kept);
      m.out = 0;
    } else if (kind === 'stream.sink.reduce') {
      const op = str(node.params.op) ?? 'count';
      const path = str(node.params.path);
      setSinkOutput(node, reduce(op, path ?? null, elemIn, (msg, el) => dead(node.id, msg, el)));
      m.out = 0;
    } else if (kind === 'stream.sink.flow' || kind === 'stream.sink.publish' || kind === 'stream.sink.http') {
      // Escriben fuera: en la simulación no se hace y el resultado es el número que se habría enviado.
      setSinkOutput(node, elemIn.length);
      m.out = 0;
    }
    if (kind !== 'stream.source.list' && !kind.startsWith('stream.sink.')) {
      m.out = produced.length;
      sample(produced, m.sampleOut);
      out.set(node.id, produced);
    }
    m.latencyMs = m.out > 0 || elemIn.length > 0 ? 2 : null;
  }

  function setSinkOutput(node: NodeDef, value: Json) {
    const first = node.outputs[0];
    if (first) {
      values[first.name] = value;
      writes.push({ path: first.target, value });
    }
  }

  const finalMetrics: Record<string, NodeMetrics> = {};
  for (const m of members) finalMetrics[m.id] = metricsOf(m.id);
  return {
    port: error ? 'error' : 'out',
    error,
    writes: error ? [] : writes,
    values: error ? {} : values,
    metrics: finalMetrics,
    deadLetters,
    endless,
  };
}

function reduce(op: string, path: string | null, elements: Json[], fail: (msg: string, el: Json) => void): Json {
  const field = (el: Json): Json | undefined => (path ? getPath(el, path) : el);
  switch (op) {
    case 'count':
      return elements.length;
    case 'sum':
    case 'avg':
    case 'min':
    case 'max': {
      const nums: number[] = [];
      for (const el of elements) {
        const v = field(el);
        if (typeof v === 'number') nums.push(v);
        else fail(`not a number: ${JSON.stringify(v ?? null)}`, el);
      }
      if (op === 'sum') return nums.reduce((a, b) => a + b, 0);
      if (op === 'avg') return nums.length ? nums.reduce((a, b) => a + b, 0) / nums.length : null;
      if (!nums.length) return null;
      return op === 'min' ? Math.min(...nums) : Math.max(...nums);
    }
    case 'any':
    case 'all': {
      const bools = elements.map((el) => field(el));
      if (bools.some((b) => typeof b !== 'boolean')) bools.forEach((b, i) => typeof b !== 'boolean' && fail('not true or false', elements[i]));
      const xs = bools.filter((b): b is boolean => typeof b === 'boolean');
      return op === 'any' ? xs.some(Boolean) : xs.length > 0 && xs.every(Boolean);
    }
    default:
      return null;
  }
}

/** `stream.aggregate`: por clave (o todo junto), cada agregado con su operación. Las filas salen al final del stream. */
function aggregate(params: Record<string, Json>, elements: Json[], fail: (msg: string, el: Json) => void): Json[] {
  const aggs = isObj(params.aggregations) ? params.aggregations : {};
  const keyPath = str(params.key);
  const groups = new Map<string, { key: Json; items: Json[] }>();
  for (const el of elements) {
    const key = keyPath ? getPath(el, keyPath) ?? null : null;
    const id = JSON.stringify(key);
    const g = groups.get(id) ?? { key, items: [] };
    g.items.push(el);
    groups.set(id, g);
  }
  const rows: Json[] = [];
  for (const g of groups.values()) {
    const row: { [k: string]: Json } = { key: g.key };
    for (const [name, spec] of Object.entries(aggs)) {
      const s = isObj(spec) ? spec : {};
      const op = str(s.op) ?? 'count';
      const path = str(s.path);
      if (op === 'count') row[name] = g.items.length;
      else if (op === 'collect') row[name] = g.items.map((el) => (path ? getPath(el, path) ?? null : el));
      else if (op === 'first') row[name] = g.items[0] && (path ? getPath(g.items[0], path) ?? null : g.items[0]);
      else if (op === 'last') row[name] = g.items.length ? (path ? getPath(g.items[g.items.length - 1], path) ?? null : g.items[g.items.length - 1]) : null;
      else row[name] = reduce(op, path ?? null, g.items, fail);
    }
    rows.push(row);
  }
  return rows;
}
