import type { CatalogEntry, DataType, Edge, EntityDef, FlowDef, JsonValue, NodeDef, OutputDecl, PortInfo } from '../api/types';

export const ERROR_PORT = 'error';
export const DEFAULT_PORT = 'out';

/** Nombre de un puerto, acepte la forma objeto o string. */
export function portName(p: PortInfo | string): string {
  return typeof p === 'string' ? p : p.name;
}

export function portsOf(list: Array<PortInfo | string> | undefined): PortInfo[] {
  const seen = new Set<string>();
  const out: PortInfo[] = [];
  for (const p of list ?? []) {
    const info: PortInfo = typeof p === 'string' ? { name: p } : p;
    if (!seen.has(info.name)) {
      seen.add(info.name);
      out.push(info);
    }
  }
  if (!seen.has(ERROR_PORT)) out.push({ name: ERROR_PORT, label: 'error' });
  return out;
}

/** Añadir la arista from -> to crearía un ciclo (incluye auto-bucles y duplicados). */
export function wouldCreateCycle(edges: Edge[], from: string, to: string): boolean {
  if (from === to) return true;
  const adj = new Map<string, string[]>();
  for (const e of edges) {
    const list = adj.get(e.from) ?? [];
    list.push(e.to);
    adj.set(e.from, list);
  }
  // ¿hay camino to ->* from? Si lo hay, from -> to cierra un ciclo.
  const stack = [to];
  const seen = new Set<string>();
  while (stack.length) {
    const cur = stack.pop()!;
    if (cur === from) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    for (const next of adj.get(cur) ?? []) stack.push(next);
  }
  return false;
}

/** Clave estable de una arista (from|port|to). Con entrada distinta de `in` se añade la entrada (join/zip: dos aristas a un mismo nodo). */
export function edgeKey(e: Edge): string {
  const base = `${e.from}|${e.port}|${e.to}`;
  return e.input && e.input !== 'in' ? `${base}|${e.input}` : base;
}

export function uniqueNodeId(kind: string, nodes: NodeDef[]): string {
  const base = kind.split('.').pop()!.replace(/[^a-zA-Z0-9_-]/g, '') || 'node';
  const taken = new Set(nodes.map((n) => n.id));
  let i = 1;
  while (taken.has(`${base}-${i}`)) i++;
  return `${base}-${i}`;
}

/** Un nodo nuevo: los campos con `ui.template` (p. ej. el código de los scripts) empiezan con su plantilla. */
export function newNodeDef(entry: CatalogEntry, nodes: NodeDef[]): NodeDef {
  const params: Record<string, JsonValue> = {};
  for (const f of entry.config.fields) if (f.ui?.template !== undefined) params[f.name] = f.ui.template;
  return {
    id: uniqueNodeId(entry.kind, nodes),
    kind: entry.kind,
    params,
    outputs: entry.defaultOutputs ?? [],
    runPolicy: { whenSucceeded: 'run', whenFailed: 'run' },
    joinPolicy: 'all',
  };
}

export function emptyFlow(id: string, name: string): FlowDef {
  return { version: 1, id, name, revision: 1, inputs: [], outputs: [], nodes: [], edges: [] };
}

export function isValidStatePath(text: string): boolean {
  return text.length > 0 && text.split('.').every((s) => s.length > 0);
}

export function renderType(t: DataType): string {
  switch (t.type) {
    case 'bool': return 'bool';
    case 'num': return 'num';
    case 'str': return 'str';
    case 'any': return 'any';
    case 'list': return `list[${renderType(t.element)}]`;
    case 'obj': return `{${Object.entries(t.fields).map(([k, v]) => `${k}: ${renderType(v)}`).join(', ')}}`;
    case 'entity': return t.name;
    case 'ref': return `ref ${t.entity}`;
  }
}

/** Un tipo nuevo de esa clase. `entity` y `ref` nombran la primera entidad que se ofrezca (o ninguna). */
export function defaultType(tag: DataType['type'], firstEntity = ''): DataType {
  switch (tag) {
    case 'list': return { type: 'list', element: { type: 'any' } };
    case 'obj': return { type: 'obj', fields: {} };
    case 'entity': return { type: 'entity', name: firstEntity };
    case 'ref': return { type: 'ref', entity: firstEntity };
    default: return { type: tag };
  }
}

/** Valor de ejemplo para un tipo (para plantillas de inputs). Una entidad da sus campos obligatorios que no se calculan. */
export function templateFor(t: DataType, entities: Record<string, EntityDef> = {}, inside: string[] = []): JsonValue {
  switch (t.type) {
    case 'bool': return false;
    case 'num': return 0;
    case 'str': return '';
    case 'any': return null;
    case 'list': return [];
    case 'obj': {
      const o: Record<string, JsonValue> = {};
      for (const [k, v] of Object.entries(t.fields)) o[k] = templateFor(v, entities, inside);
      return o;
    }
    case 'entity': {
      const e = entities[t.name];
      if (!e || inside.includes(t.name)) return {};
      const o: Record<string, JsonValue> = {};
      for (const f of e.fields) {
        if (f.computed !== undefined || f.required === false) continue;
        o[f.name] = f.default !== undefined ? f.default : templateFor(f.type, entities, [...inside, t.name]);
      }
      return o;
    }
    case 'ref': {
      const e = entities[t.entity];
      const key = e?.fields.find((f) => f.name === e.key);
      return key ? templateFor(key.type, entities, inside) : '';
    }
  }
}

/** Los campos de un tipo, por nombre: los de un `obj` o los de la entidad que nombra. */
export function fieldsOf(t: DataType, entities: Record<string, EntityDef>): Record<string, DataType> | null {
  if (t.type === 'obj') return t.fields;
  if (t.type === 'entity') {
    const e = entities[t.name];
    return e ? Object.fromEntries(e.fields.map((f) => [f.name, f.type])) : null;
  }
  return null;
}

/** Las rutas que hay dentro de un valor de tipo `t` en `base`: la propia y las de sus campos, a través de objetos y entidades, hasta
 *  `depth` niveles (una entidad dentro de sí misma se corta). */
export function pathsWithin(base: string, t: DataType, entities: Record<string, EntityDef>, depth = 4, inside: string[] = []): string[] {
  const own = [base];
  if (depth <= 0) return own;
  if (t.type === 'entity' && inside.includes(t.name)) return own;
  const fields = fieldsOf(t, entities);
  if (!fields) return own;
  const next = t.type === 'entity' ? [...inside, t.name] : inside;
  return [...own, ...Object.entries(fields).flatMap(([name, ft]) => pathsWithin(`${base}.${name}`, ft, entities, depth - 1, next))];
}

/** Escribe `value` en la ruta punteada dentro de `obj` (crea objetos intermedios). Devuelve un objeto nuevo. */
export function setAtPath(obj: JsonValue, path: string, value: JsonValue): JsonValue {
  const [head, ...rest] = path.split('.');
  const base: Record<string, JsonValue> =
    obj && typeof obj === 'object' && !Array.isArray(obj) ? { ...obj } : {};
  base[head] = rest.length ? setAtPath(base[head] ?? {}, rest.join('.'), value) : value;
  return base;
}

/** Plantilla de JSON de entrada a partir de las declaraciones (según su target). */
export function templateFromDecls(decls: OutputDecl[], entities: Record<string, EntityDef> = {}): JsonValue {
  let root: JsonValue = {};
  for (const d of decls) root = setAtPath(root, d.target, templateFor(d.type, entities));
  return root;
}

/** Rutas declaradas (inputs del flujo y salidas de nodos) para autocompletar, con los campos de los objetos y entidades que
 *  declaran. */
export function declaredPaths(flow: FlowDef, entities: Record<string, EntityDef> = {}): string[] {
  const set = new Set<string>();
  for (const d of [...flow.inputs, ...flow.nodes.flatMap((n) => n.outputs)]) for (const p of pathsWithin(d.target, d.type, entities)) set.add(p);
  return [...set].sort();
}

/** Posición por defecto de un nodo sin posición guardada. */
export function fallbackPosition(index: number): { x: number; y: number } {
  return { x: 80 + (index % 4) * 240, y: 80 + Math.floor(index / 4) * 160 };
}
