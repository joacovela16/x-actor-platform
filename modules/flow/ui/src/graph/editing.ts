// Operaciones de edición del lienzo sin React: historial (deshacer/rehacer), copiar y pegar nodos y partir una arista con un nodo.
import type { CatalogEntry, Edge, FlowDef, NodeDef } from '../api/types';
import { edgeKey } from './graph';

/** Historial de documentos. Los cambios seguidos en menos de `mergeMs` (escribir en un campo) cuentan como uno solo. */
export class History {
  private past: FlowDef[] = [];
  private future: FlowDef[] = [];
  private at = Number.NEGATIVE_INFINITY;

  constructor(private readonly limit = 100, private readonly mergeMs = 600) {}

  /** Antes de cambiar `current`: lo guarda para poder volver, salvo que sea parte del mismo cambio que el anterior. */
  record(current: FlowDef, now = Date.now()): void {
    if (now - this.at > this.mergeMs && this.past[this.past.length - 1] !== current) {
      this.past.push(current);
      if (this.past.length > this.limit) this.past.shift();
    }
    this.at = now;
    this.future = [];
  }

  undo(current: FlowDef): FlowDef | null {
    const prev = this.past.pop();
    if (!prev) return null;
    this.future.push(current);
    this.at = Number.NEGATIVE_INFINITY;
    return prev;
  }

  redo(current: FlowDef): FlowDef | null {
    const next = this.future.pop();
    if (!next) return null;
    this.past.push(current);
    this.at = Number.NEGATIVE_INFINITY;
    return next;
  }

  clear(): void {
    this.past = [];
    this.future = [];
    this.at = Number.NEGATIVE_INFINITY;
  }

  get canUndo(): boolean {
    return this.past.length > 0;
  }

  get canRedo(): boolean {
    return this.future.length > 0;
  }
}

/** Lo que se copia: unos nodos, las aristas entre ellos y sus posiciones. */
export interface Clip {
  nodes: NodeDef[];
  edges: Edge[];
  layout: Record<string, { x: number; y: number }>;
}

export function copy(flow: FlowDef, ids: string[]): Clip | null {
  const set = new Set(ids);
  const nodes = flow.nodes.filter((n) => set.has(n.id));
  if (nodes.length === 0) return null;
  return {
    nodes,
    edges: flow.edges.filter((e) => set.has(e.from) && set.has(e.to)),
    layout: Object.fromEntries(nodes.flatMap((n) => (flow.layout?.[n.id] ? [[n.id, flow.layout[n.id]]] : []))),
  };
}

/** Un id libre a partir de `base`: `base`, `base-2`, `base-3`... */
export function freeId(base: string, taken: Set<string>): string {
  if (!taken.has(base)) return base;
  const stem = base.replace(/-\d+$/, '');
  for (let i = 2; ; i++) if (!taken.has(`${stem}-${i}`)) return `${stem}-${i}`;
}

/** Pega `clip` en `flow`: ids nuevos donde choquen, aristas renombradas, desplazado `offset`. Devuelve el flujo y los ids nuevos. */
export function paste(flow: FlowDef, clip: Clip, offset = 40): { flow: FlowDef; ids: string[] } {
  const taken = new Set(flow.nodes.map((n) => n.id));
  const rename: Record<string, string> = {};
  for (const n of clip.nodes) {
    const id = freeId(n.id, taken);
    taken.add(id);
    rename[n.id] = id;
  }
  const nodes = clip.nodes.map((n) => ({ ...structuredClone(n), id: rename[n.id] }));
  const edges = clip.edges.map((e) => ({ ...e, from: rename[e.from], to: rename[e.to] }));
  const layout = Object.fromEntries(
    clip.nodes.map((n, i) => {
      const p = clip.layout[n.id] ?? { x: 80 + i * 40, y: 80 };
      return [rename[n.id], { x: p.x + offset, y: p.y + offset }];
    }),
  );
  return {
    flow: { ...flow, nodes: [...flow.nodes, ...nodes], edges: [...flow.edges, ...edges], layout: { ...(flow.layout ?? {}), ...layout } },
    ids: nodes.map((n) => n.id),
  };
}

/** El primer puerto de salida (no `error`) y la primera entrada de un tipo de nodo. */
export function mainPorts(entry: CatalogEntry): { out: string; in: string } {
  const outs = entry.ports.map((p) => (typeof p === 'string' ? p : p.name)).filter((p) => p !== 'error');
  return { out: outs[0] ?? 'out', in: entry.inputs?.[0]?.name ?? 'in' };
}

/** Pone `node` en medio de la arista `key`: `from → node → to`, por los puertos principales del nodo. */
export function splitEdge(flow: FlowDef, key: string, node: NodeDef, entry: CatalogEntry): FlowDef {
  const edge = flow.edges.find((e) => edgeKey(e) === key);
  if (!edge) return flow;
  const ports = mainPorts(entry);
  const first: Edge = ports.in !== 'in' ? { from: edge.from, port: edge.port, to: node.id, input: ports.in } : { from: edge.from, port: edge.port, to: node.id };
  const second: Edge = edge.input && edge.input !== 'in' ? { from: node.id, port: ports.out, to: edge.to, input: edge.input } : { from: node.id, port: ports.out, to: edge.to };
  return { ...flow, edges: [...flow.edges.filter((e) => edgeKey(e) !== key), first, second] };
}
