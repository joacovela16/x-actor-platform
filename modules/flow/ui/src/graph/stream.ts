// Lógica de streams del lado del cliente: compatibilidad de aristas, segmentos y su condensado. Espejo de
// model/StreamSegments.scala, FlowValidator (stream-token-mix, stream-type-mismatch, unknown-input) y DataType.conformsTo.
// Sin dependencias de runtime: lo usan el lienzo, el editor y el mock.
import type { DataType, Edge, FlowDef, JsonValue, NodeDef, PortInfo } from '../api/types';

export type Role = 'node' | 'source' | 'operator' | 'sink';

/** Lo que el lienzo necesita de un nodo: sus entradas, sus salidas (con `error`) y su papel en un stream. */
export interface NodeIO {
  role: Role;
  ins: PortInfo[];
  outs: PortInfo[];
}

export const SEGMENT_KIND = 'stream.segment';

/** `DataType.conformsTo`: un valor de tipo `found` sirve donde se pide `required`. Sin las definiciones de las entidades, una
 *  entidad solo es ella misma y una referencia solo otra a la misma entidad (el servidor sabe más: también la clave). */
export function conformsTo(found: DataType, required: DataType): boolean {
  if (required.type === 'any') return true;
  if (found.type === 'list' && required.type === 'list') return conformsTo(found.element, required.element);
  if (found.type === 'obj' && required.type === 'obj') {
    return Object.entries(required.fields).every(([name, t]) => {
      const have = found.fields[name];
      return have !== undefined && conformsTo(have, t);
    });
  }
  if (found.type === 'entity' && required.type === 'entity') return found.name === required.name;
  if (found.type === 'ref' && required.type === 'ref') return found.entity === required.entity;
  return found.type === required.type && ['bool', 'num', 'str'].includes(found.type);
}

export function streamOf(io: NodeIO | undefined, port: string): DataType | undefined {
  return io?.outs.find((p) => p.name === port)?.stream;
}

export function streamIn(io: NodeIO | undefined, input: string): DataType | undefined {
  return io?.ins.find((p) => p.name === input)?.stream;
}

export function inputOf(e: Edge): string {
  return e.input ?? 'in';
}

/** Una arista es de stream si alguno de sus extremos lo es (StreamSegments.isStreamEdge). */
export function isStreamEdge(e: Edge, io: Record<string, NodeIO>): boolean {
  return streamOf(io[e.from], e.port) !== undefined || streamIn(io[e.to], inputOf(e)) !== undefined;
}

/** Un nodo forma parte de un stream si tiene alguna entrada o salida de stream. */
export function isStreamNode(io: NodeIO | undefined): boolean {
  return !!io && (io.outs.some((p) => p.stream) || io.ins.some((p) => p.stream));
}

export type EdgeProblemCode = 'stream-token-mix' | 'stream-type-mismatch' | 'unknown-input';

export interface EdgeProblem {
  code: EdgeProblemCode;
  message: string;
}

/**
 * Problemas de una arista que el cliente puede decidir sin el servidor: entrada que no existe, stream unido a un puerto
 * de token, y tipos de elemento incompatibles. `null` si la arista está bien (o si no hay datos de alguno de los nodos).
 */
export function edgeProblem(e: { from: string; port: string; to: string; input?: string }, io: Record<string, NodeIO>): EdgeProblem | null {
  const from = io[e.from];
  const to = io[e.to];
  if (!from || !to) return null;
  const input = e.input ?? 'in';
  if (!to.ins.some((p) => p.name === input)) {
    const names = to.ins.map((p) => p.name).join(', ');
    return { code: 'unknown-input', message: `'${e.to}' no tiene la entrada '${input}'; sus entradas son ${names}` };
  }
  const out = streamOf(from, e.port);
  const inp = streamIn(to, input);
  if ((out === undefined) !== (inp === undefined)) {
    return {
      code: 'stream-token-mix',
      message: `${e.from}.${e.port} y ${e.to}.${input} no son del mismo tipo: un stream no se une a un puerto de token (hace falta una fuente o un sumidero)`,
    };
  }
  if (out && inp && !conformsTo(out, inp)) {
    return {
      code: 'stream-type-mismatch',
      message: `${e.from}.${e.port} lleva elementos de tipo ${renderDataType(out)}, pero ${e.to}.${input} espera ${renderDataType(inp)}`,
    };
  }
  return null;
}

export function renderDataType(t: DataType): string {
  switch (t.type) {
    case 'list': return `list[${renderDataType(t.element)}]`;
    case 'obj': return `{${Object.entries(t.fields).map(([k, v]) => `${k}: ${renderDataType(v)}`).join(', ')}}`;
    case 'entity': return t.name;
    case 'ref': return `ref ${t.entity}`;
    default: return t.type;
  }
}

export interface Segment {
  /** Nodo que representa el segmento (la primera fuente, por orden de id). */
  anchor: string;
  members: string[];
  sources: string[];
  sinks: string[];
}

const byText = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** Segmentos del flujo: componentes conexas de nodos de stream unidas por aristas de stream (union-find, como StreamSegments.find). */
export function findSegments(nodes: NodeDef[], edges: Edge[], io: Record<string, NodeIO>): Segment[] {
  const ids = [...new Set(nodes.map((n) => n.id))].filter((id) => isStreamNode(io[id]));
  const inStream = new Set(ids);
  const parent = new Map(ids.map((id) => [id, id]));
  const root = (id: string): string => {
    let r = id;
    while (parent.get(r) !== r) r = parent.get(r)!;
    let cur = id;
    while (parent.get(cur) !== r) {
      const next = parent.get(cur)!;
      parent.set(cur, r);
      cur = next;
    }
    return r;
  };
  for (const e of edges) {
    if (!inStream.has(e.from) || !inStream.has(e.to) || !isStreamEdge(e, io)) continue;
    const a = root(e.from);
    const b = root(e.to);
    if (a !== b) parent.set(a, b);
  }
  const groups = new Map<string, string[]>();
  for (const id of ids) {
    const r = root(id);
    groups.set(r, [...(groups.get(r) ?? []), id]);
  }
  const out: Segment[] = [];
  for (const members of groups.values()) {
    const sorted = [...members].sort(byText);
    const sources = sorted.filter((id) => io[id].role === 'source');
    const sinks = sorted.filter((id) => io[id].role === 'sink');
    out.push({ anchor: sources[0] ?? sorted[0], members: sorted, sources, sinks });
  }
  return out.sort((a, b) => byText(a.anchor, b.anchor));
}

/** Mapa de segmento por nodo miembro (id del nodo -> índice del segmento). */
export function segmentIndexOf(segments: Segment[]): Map<string, number> {
  const m = new Map<string, number>();
  segments.forEach((s, i) => s.members.forEach((id) => m.set(id, i)));
  return m;
}

/**
 * El flujo tal como lo ejecuta el motor (StreamSegments.condense): cada segmento es un nodo `stream.segment` con su
 * anclaje, y las aristas que entran en sus fuentes o salen de sus sumideros se enganchan a él.
 */
export function condenseFlow(flow: FlowDef, io: Record<string, NodeIO>): { flow: FlowDef; segments: Segment[] } {
  const segments = findSegments(flow.nodes, flow.edges, io);
  if (segments.length === 0) return { flow, segments };
  const byId = new Map(flow.nodes.map((n) => [n.id, n]));
  const anchorOf = new Map<string, string>();
  for (const s of segments) for (const m of s.members) anchorOf.set(m, s.anchor);
  const internal = flow.edges.filter((e) => anchorOf.get(e.from) !== undefined && anchorOf.get(e.from) === anchorOf.get(e.to) && isStreamEdge(e, io));

  const nodes: NodeDef[] = [];
  for (const n of flow.nodes) {
    const a = anchorOf.get(n.id);
    if (a === undefined) {
      nodes.push(n);
      continue;
    }
    if (a !== n.id) continue;
    const s = segments.find((x) => x.anchor === a)!;
    const members = s.members.map((id) => byId.get(id)!);
    const entries = [...new Set(flow.edges.filter((e) => s.sources.includes(e.to) && anchorOf.get(e.from) !== a).map((e) => e.to))].sort(byText);
    const first = byId.get(a)!;
    const outputs = members.flatMap((m) => m.outputs.map((o) => ({ ...o, name: `${m.id}.${o.name}` })));
    nodes.push({
      id: a,
      kind: SEGMENT_KIND,
      params: {
        members: members as unknown as JsonValue,
        edges: internal.filter((e) => anchorOf.get(e.from) === a) as unknown as JsonValue,
        sources: s.sources,
        sinks: s.sinks,
        entries,
        reads: [],
      },
      outputs,
      runPolicy: first.runPolicy,
      joinPolicy: 'all',
      label: first.label,
    });
  }
  const edges: Edge[] = [];
  const seen = new Set<string>();
  for (const e of flow.edges) {
    if (internal.includes(e)) continue;
    const from = anchorOf.get(e.from) ?? e.from;
    const to = anchorOf.get(e.to) ?? e.to;
    const port = anchorOf.has(e.from) ? (e.port === 'error' ? 'error' : 'out') : e.port;
    const input = anchorOf.has(e.to) ? e.to : inputOf(e);
    const edge: Edge = { from, to, port, input };
    const key = `${from}|${port}|${to}|${input}`;
    if (!seen.has(key)) {
      seen.add(key);
      edges.push(edge);
    }
  }
  return { flow: { ...flow, nodes, edges }, segments };
}
