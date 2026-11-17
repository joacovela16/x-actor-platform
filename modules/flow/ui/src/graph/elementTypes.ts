// Tipo de los elementos que circulan por cada stream del flujo, inferido en el cliente: parte de los tipos que el flujo declara
// (sus inputs, las salidas de los nodos, las entidades) y sigue las aristas de stream aplicando lo que hace cada operador. Sirve
// para sugerir los campos del elemento en los nodos de stream y para mostrar el tipo en las aristas. Si algo no se sabe, es `any`.
import type { DataType, EntityDef, FlowDef, NodeDef, OutputDecl } from '../api/types';
import { inputOf, type NodeIO } from './stream';

const ANY: DataType = { type: 'any' };
const NUM: DataType = { type: 'num' };
const obj = (fields: Record<string, DataType>): DataType => ({ type: 'obj', fields });
const list = (element: DataType): DataType => ({ type: 'list', element });

export interface ElementTypes {
  /** Tipo del elemento que entra en cada nodo de stream, por id y entrada (`in`, `left`, `right`). */
  inputs: Record<string, Record<string, DataType>>;
  /** Tipo del elemento que sale por cada puerto de stream, por `nodo.puerto`. */
  outputs: Record<string, DataType>;
}

/** Los campos de un objeto o de una entidad (si se conoce). */
export function fieldsOf(t: DataType, entities: Record<string, EntityDef>): Record<string, DataType> | null {
  if (t.type === 'obj') return t.fields;
  if (t.type === 'entity') {
    const e = entities[t.name];
    return e ? Object.fromEntries(e.fields.map((f) => [f.name, f.type])) : null;
  }
  return null;
}

/** El tipo en una ruta punteada dentro de `t`, o `any` si no se sabe. Una ruta vacía o `$` es el propio `t`. */
export function typeAt(t: DataType, path: string, entities: Record<string, EntityDef>): DataType {
  if (path === '' || path === '$') return t;
  let cur: DataType = t;
  for (const seg of path.split('.')) {
    const fields = fieldsOf(cur, entities);
    if (!fields || !(seg in fields)) return ANY;
    cur = fields[seg];
  }
  return cur;
}

/** El tipo de una ruta del estado, según lo que declaran el flujo y sus nodos (la declaración más larga que la cubre). */
export function stateTypeAt(decls: OutputDecl[], path: string, entities: Record<string, EntityDef>): DataType {
  let best: OutputDecl | undefined;
  for (const d of decls) {
    if (path === d.target || path.startsWith(`${d.target}.`)) if (!best || d.target.length > best.target.length) best = d;
  }
  return best ? typeAt(best.type, path.slice(best.target.length + 1), entities) : ANY;
}

/** Fija `value` en `path` de un objeto (si se sabe que lo es; si no, un objeto con solo ese campo). */
function withField(t: DataType, path: string, value: DataType, entities: Record<string, EntityDef>): DataType {
  const [head, ...rest] = path.split('.');
  const fields = { ...(fieldsOf(t, entities) ?? {}) };
  fields[head] = rest.length ? withField(fields[head] ?? obj({}), rest.join('.'), value, entities) : value;
  return obj(fields);
}

const str = (v: unknown): string => (typeof v === 'string' ? v : '');

/** Lo que sale de un nodo de stream por cada puerto, dado lo que le entra. `null`: el nodo no es de stream o no se sabe. */
function outputsOf(node: NodeDef, ins: Record<string, DataType>, decls: OutputDecl[], entities: Record<string, EntityDef>): Record<string, DataType> | null {
  const p = node.params as Record<string, unknown>;
  const input = ins.in ?? ANY;
  const one = (t: DataType) => ({ out: t });
  switch (node.kind) {
    case 'stream.source.list': {
      const t = stateTypeAt(decls, str(p.path), entities);
      return one(t.type === 'list' ? t.element : ANY);
    }
    case 'stream.source.range': return one(NUM);
    case 'stream.source.cron': return one(obj({ at: NUM }));
    case 'stream.filter':
    case 'stream.take':
    case 'stream.drop':
    case 'stream.takeWhile':
    case 'stream.dropWhile':
    case 'stream.interruptAfter':
    case 'stream.throttle':
    case 'stream.dedupe':
    case 'stream.changes':
      return one(input);
    case 'stream.branch': return { then: input, else: input };
    case 'stream.merge': return one(input);
    case 'stream.zip':
    case 'stream.join':
      return one(obj({ left: ins.left ?? ANY, right: ins.right ?? ANY }));
    case 'stream.flatMap': {
      const l = typeAt(input, str(p.path), entities);
      const item = l.type === 'list' ? l.element : ANY;
      if (p.merge === true && fieldsOf(item, entities) && fieldsOf(input, entities)) {
        const rest = { ...fieldsOf(input, entities) };
        delete rest[str(p.path).split('.')[0]];
        return one(obj({ ...rest, ...fieldsOf(item, entities) }));
      }
      return one(item);
    }
    case 'stream.map': {
      if (p.replace !== undefined) return one(ANY);
      let t = input;
      for (const name of Object.keys((p.set as Record<string, unknown>) ?? {})) t = withField(t, name, ANY, entities);
      return one(t);
    }
    case 'stream.window': return one(obj({ items: list(input), count: NUM, start: NUM, end: NUM }));
    case 'stream.groupAdjacentBy': return one(obj({ key: typeAt(input, str(p.key), entities), items: list(input), count: NUM }));
    case 'stream.zipWithIndex': return one(obj({ value: input, index: NUM }));
    case 'stream.zipWithPrevious': return one(obj({ previous: input, value: input }));
    case 'stream.zipWithNext': return one(obj({ value: input, next: input }));
    case 'stream.evalMap': return one(str(p.into) ? withField(input, str(p.into), ANY, entities) : ANY);
    default:
      return null;
  }
}

/** Infiere los tipos de todos los streams del flujo, siguiendo las aristas de stream desde las fuentes. */
export function inferElementTypes(flow: FlowDef, io: Record<string, NodeIO>, entities: Record<string, EntityDef>): ElementTypes {
  const decls = [...flow.inputs, ...flow.nodes.flatMap((n) => n.outputs)];
  const inputs: Record<string, Record<string, DataType>> = {};
  const outputs: Record<string, DataType> = {};
  const streamEdges = flow.edges.filter((e) => io[e.from]?.outs.some((p) => p.name === e.port && p.stream));
  // Los streams son acíclicos: tantas pasadas como nodos bastan para que todo llegue.
  for (let pass = 0; pass <= flow.nodes.length; pass++) {
    let changed = false;
    for (const n of flow.nodes) {
      const ins: Record<string, DataType> = {};
      for (const e of streamEdges) {
        if (e.to !== n.id) continue;
        const t = outputs[`${e.from}.${e.port}`];
        if (t && !ins[inputOf(e)]) ins[inputOf(e)] = t;
      }
      inputs[n.id] = ins;
      const outs = outputsOf(n, ins, decls, entities);
      for (const [port, t] of Object.entries(outs ?? {})) {
        const key = `${n.id}.${port}`;
        if (JSON.stringify(outputs[key]) !== JSON.stringify(t)) {
          outputs[key] = t;
          changed = true;
        }
      }
    }
    if (!changed) break;
  }
  return { inputs, outputs };
}

/** Las rutas de los campos de un elemento (sin la raíz), para sugerirlas en los campos de los nodos de stream. */
export function elementPaths(t: DataType | undefined, entities: Record<string, EntityDef>, depth = 4): string[] {
  if (!t) return [];
  const out: string[] = [];
  const walk = (base: string, cur: DataType, d: number, inside: string[]) => {
    if (d <= 0 || (cur.type === 'entity' && inside.includes(cur.name))) return;
    const fields = fieldsOf(cur, entities);
    if (!fields) return;
    const next = cur.type === 'entity' ? [...inside, cur.name] : inside;
    for (const [name, ft] of Object.entries(fields)) {
      const p = base ? `${base}.${name}` : name;
      out.push(p);
      walk(p, ft, d - 1, next);
    }
  };
  walk('', t, depth, []);
  return out;
}
