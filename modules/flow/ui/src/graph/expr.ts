// Lenguaje de expresiones de los nodos de stream (`stream.map`, `stream.filter`...) y de las reglas, campos calculados y guardas de
// las entidades. Port de model/Expr.scala y de MapOp.transform. Sin dependencias: lo usan el editor, la vista previa y el mock. Es
// JSON, no código.
import type { JsonValue } from '../api/types';

export type Json = JsonValue;

/** Expresión ya leída. `path` guarda la ruta como texto con puntos. */
export type Expr =
  | { k: 'const'; value: Json }
  | { k: 'path'; path: string; hasDefault: boolean; dflt?: Json }
  | { k: 'exists'; path: string }
  | { k: 'op'; name: string; args: Expr[] }
  | { k: 'if'; c: Expr; y: Expr; n: Expr }
  | { k: 'over'; name: string; list: string; body: Expr | null };

export const BINARY = ['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'matches', 'round'] as const;
export const VARIADIC = ['and', 'or', 'add', 'sub', 'mul', 'div', 'concat'] as const;
export const UNARY = ['not', 'len'] as const;
/** Operan sobre los ítems de la lista de una ruta: `{"sum": ["lines", {"path": "subtotal"}]}`. */
export const COLLECTION = ['sum', 'count', 'min', 'max', 'all', 'any'] as const;

const isObj = (j: Json): j is { [k: string]: Json } => typeof j === 'object' && j !== null && !Array.isArray(j);

/** Ruta de estado: segmentos no vacíos separados por puntos (StatePath.parse). */
export function parsePath(text: string): string | null {
  if (typeof text !== 'string' || text.length === 0 || text.split('.').some((s) => s.length === 0)) return null;
  return text;
}

/** Lee `expr` como lo hace `Expr.parse`. Devuelve la expresión o el motivo del error. */
export function parseExpr(json: Json): { ok: true; expr: Expr } | { ok: false; error: string } {
  const res = parseInner(json);
  return res;
}

function parseInner(json: Json): { ok: true; expr: Expr } | { ok: false; error: string } {
  if (!isObj(json)) return { ok: true, expr: { k: 'const', value: json } };
  const entries = Object.entries(json);
  const fail = (error: string) => ({ ok: false as const, error });
  const sub = (j: Json) => parseInner(j);
  if (entries.length === 1 && entries[0][0] === 'const') return { ok: true, expr: { k: 'const', value: entries[0][1] } };
  if (entries.length >= 1 && entries[0][0] === 'path' && entries.slice(1).every(([k]) => k === 'default')) {
    const p = entries[0][1];
    if (typeof p !== 'string') return fail("'path' must be text");
    const path = parsePath(p);
    if (path === null) return fail(`not a valid state path: '${p}'`);
    const hasDefault = entries.length === 2;
    return { ok: true, expr: { k: 'path', path, hasDefault, dflt: hasDefault ? entries[1][1] : undefined } };
  }
  if (entries.length === 1 && entries[0][0] === 'exists') {
    const p = entries[0][1];
    if (typeof p !== 'string') return fail("'exists' must be a path");
    const path = parsePath(p);
    if (path === null) return fail(`not a valid state path: '${p}'`);
    return { ok: true, expr: { k: 'exists', path } };
  }
  if (entries.length === 1 && (entries[0][0] === 'not' || entries[0][0] === 'len')) {
    const r = sub(entries[0][1]);
    return r.ok ? { ok: true, expr: { k: 'op', name: entries[0][0], args: [r.expr] } } : r;
  }
  if (entries.length === 1 && entries[0][0] === 'if') {
    const a = entries[0][1];
    if (!Array.isArray(a) || a.length !== 3) return fail("'if' takes [condition, then, else]");
    const [c, y, n] = [sub(a[0]), sub(a[1]), sub(a[2])];
    if (!c.ok) return c;
    if (!y.ok) return y;
    if (!n.ok) return n;
    return { ok: true, expr: { k: 'if', c: c.expr, y: y.expr, n: n.expr } };
  }
  if (entries.length === 1 && (COLLECTION as readonly string[]).includes(entries[0][0])) {
    const [name, a] = entries[0];
    if (!Array.isArray(a) || a.length < 1 || a.length > 2) return fail(`'${name}' takes [path of a list, expression]`);
    if (typeof a[0] !== 'string') return fail(`'${name}' takes the path of a list first`);
    const list = parsePath(a[0]);
    if (list === null) return fail(`not a valid state path: '${a[0]}'`);
    if (a.length === 1) return { ok: true, expr: { k: 'over', name, list, body: null } };
    const body = sub(a[1]);
    return body.ok ? { ok: true, expr: { k: 'over', name, list, body: body.expr } } : body;
  }
  if (entries.length === 1 && ((BINARY as readonly string[]).includes(entries[0][0]) || (VARIADIC as readonly string[]).includes(entries[0][0]))) {
    const [name, a] = entries[0];
    if (!Array.isArray(a)) return fail(`'${name}' takes a list`);
    const args: Expr[] = [];
    for (const item of a) {
      const r = sub(item);
      if (!r.ok) return r;
      args.push(r.expr);
    }
    if ((BINARY as readonly string[]).includes(name) && args.length !== 2) return fail(`'${name}' takes two values`);
    if (args.length < 2 && name !== 'and' && name !== 'or') return fail(`'${name}' takes at least two values`);
    if (name === 'matches') {
      const p = args[1];
      if (p.k !== 'const' || typeof p.value !== 'string') return fail("the pattern of 'matches' must be a fixed text");
      const bad = regexError(p.value);
      if (bad) return fail(bad);
    }
    return { ok: true, expr: { k: 'op', name, args } };
  }
  const empty = entries.length === 0 ? ' (empty)' : '';
  return fail(`not an expression: ${JSON.stringify(json)}${empty}`);
}

/** Igualdad que ve 1 y 1.0 como lo mismo (Expr.same). */
export function same(a: Json, b: Json): boolean {
  if (typeof a === 'number' && typeof b === 'number') return a === b;
  return deepEqual(a, b);
}

function deepEqual(a: Json, b: Json): boolean {
  if (a === b) return true;
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((x, i) => deepEqual(x, b[i]));
  if (isObj(a) && isObj(b)) {
    const ka = Object.keys(a);
    return ka.length === Object.keys(b).length && ka.every((k) => k in b && deepEqual(a[k], b[k]));
  }
  return false;
}

/** Ordena números o textos (Expr.compare); otro tipo no se puede comparar. */
export function compare(a: Json, b: Json): { ok: true; c: number } | { ok: false; error: string } {
  if (typeof a === 'number' && typeof b === 'number') return { ok: true, c: a < b ? -1 : a > b ? 1 : 0 };
  if (typeof a === 'string' && typeof b === 'string') return { ok: true, c: a < b ? -1 : a > b ? 1 : 0 };
  return { ok: false, error: `cannot compare ${JSON.stringify(a)} with ${JSON.stringify(b)}` };
}

/** Lee la ruta en un valor: solo campos de objeto (StatePath.get). `undefined` si no está. */
export function getPath(root: Json | undefined, path: string): Json | undefined {
  let cur: Json | undefined = root;
  for (const seg of path.split('.')) {
    if (cur === undefined || !isObj(cur)) return undefined;
    if (!(seg in cur)) return undefined;
    cur = cur[seg];
  }
  return cur;
}

/** Escribe `value` en la ruta (StatePath.set): crea objetos y reemplaza lo que estorba. */
export function setPath(root: Json, path: string, value: Json): Json {
  const segs = path.split('.');
  const go = (cur: Json, i: number): Json => {
    if (i === segs.length) return value;
    const obj: { [k: string]: Json } = isObj(cur) ? { ...cur } : {};
    obj[segs[i]] = go(obj[segs[i]] ?? null, i + 1);
    return obj;
  };
  return go(root, 0);
}

/** Quita la ruta (MapOp.removeAt): nada si no existe. */
export function removePath(root: Json, path: string): Json {
  const segs = path.split('.');
  const go = (cur: Json, i: number): Json => {
    if (!isObj(cur)) return cur;
    const obj: { [k: string]: Json } = { ...cur };
    if (i === segs.length - 1) {
      delete obj[segs[i]];
      return obj;
    }
    if (segs[i] in obj) obj[segs[i]] = go(obj[segs[i]], i + 1);
    return obj;
  };
  return go(root, 0);
}

const num = (j: Json): { ok: true; n: number } | { ok: false; error: string } =>
  typeof j === 'number' ? { ok: true, n: j } : { ok: false, error: `not a number: ${JSON.stringify(j)}` };

const bool = (j: Json): { ok: true; b: boolean } | { ok: false; error: string } =>
  typeof j === 'boolean' ? { ok: true, b: j } : { ok: false, error: `not a boolean: ${JSON.stringify(j)}` };

type R = { ok: true; v: Json } | { ok: false; error: string };

/** Evalúa `expr` sobre `element`. El error es el mismo motivo que daría el backend. */
export function evalExpr(expr: Expr, element: Json): R {
  switch (expr.k) {
    case 'const':
      return { ok: true, v: expr.value };
    case 'path': {
      const v = getPath(element, expr.path);
      if (v !== undefined) return { ok: true, v };
      if (expr.hasDefault) return { ok: true, v: expr.dflt ?? null };
      return { ok: false, error: `'${expr.path}' is not in the element` };
    }
    case 'exists':
      return { ok: true, v: getPath(element, expr.path) !== undefined };
    case 'if': {
      const c = evalExpr(expr.c, element);
      if (!c.ok) return c;
      if (typeof c.v !== 'boolean') return { ok: false, error: `the condition of 'if' is not a boolean: ${JSON.stringify(c.v)}` };
      return evalExpr(c.v ? expr.y : expr.n, element);
    }
    case 'op': {
      const vals: Json[] = [];
      for (const a of expr.args) {
        const r = evalExpr(a, element);
        if (!r.ok) return r;
        vals.push(r.v);
      }
      return applyOp(expr.name, vals);
    }
    case 'over':
      return evalOver(expr, element);
  }
}

/** Un patrón que no compila, o null. */
export function regexError(pattern: string): string | null {
  try {
    new RegExp(pattern);
    return null;
  } catch (e) {
    return `not a valid pattern '${pattern}': ${(e as Error).message}`;
  }
}

/** Una lista que no está (o es null) está vacía. Sin cuerpo, los ítems mismos. */
function evalOver(expr: Extract<Expr, { k: 'over' }>, element: Json): R {
  const raw = getPath(element, expr.list);
  if (raw !== undefined && raw !== null && !Array.isArray(raw)) return { ok: false, error: `'${expr.list}' is not a list: ${JSON.stringify(raw)}` };
  const items = (raw ?? []) as Json[];
  const vals: Json[] = [];
  for (const item of items) {
    if (!expr.body) vals.push(item);
    else {
      const r = evalExpr(expr.body, item);
      if (!r.ok) return r;
      vals.push(r.v);
    }
  }
  const bools = (): { ok: true; bs: boolean[] } | { ok: false; error: string } => {
    const bs: boolean[] = [];
    for (const v of vals) {
      if (typeof v !== 'boolean') return { ok: false, error: `'${expr.name}' needs booleans: ${JSON.stringify(v)}` };
      bs.push(v);
    }
    return { ok: true, bs };
  };
  switch (expr.name) {
    case 'sum': {
      let total = 0;
      for (const v of vals) {
        const n = num(v);
        if (!n.ok) return n;
        total += n.n;
      }
      return { ok: true, v: total };
    }
    case 'count': {
      if (!expr.body) return { ok: true, v: vals.length };
      const b = bools();
      return b.ok ? { ok: true, v: b.bs.filter(Boolean).length } : b;
    }
    case 'all':
    case 'any': {
      const b = bools();
      if (!b.ok) return b;
      return { ok: true, v: expr.name === 'all' ? b.bs.every(Boolean) : b.bs.some(Boolean) };
    }
    default: {
      let best: Json | undefined = undefined;
      for (const v of vals) {
        const self = compare(v, v);
        if (!self.ok) return self;
        if (best === undefined) best = v;
        else {
          const c = compare(v, best);
          if (!c.ok) return c;
          if ((expr.name === 'min' && c.c < 0) || (expr.name === 'max' && c.c > 0)) best = v;
        }
      }
      return { ok: true, v: best ?? null };
    }
  }
}

function applyOp(name: string, vals: Json[]): R {
  const numbers = (): { ok: true; ns: number[] } | { ok: false; error: string } => {
    const ns: number[] = [];
    for (const v of vals) {
      const r = num(v);
      if (!r.ok) return r;
      ns.push(r.n);
    }
    return { ok: true, ns };
  };
  const fold = (f: (a: number, b: number) => number | string): R => {
    const r = numbers();
    if (!r.ok) return r;
    let acc = r.ns[0];
    for (const n of r.ns.slice(1)) {
      const next = f(acc, n);
      if (typeof next === 'string') return { ok: false, error: next };
      acc = next;
    }
    return { ok: true, v: acc };
  };
  switch (name) {
    case 'eq':
      return { ok: true, v: same(vals[0], vals[1]) };
    case 'neq':
      return { ok: true, v: !same(vals[0], vals[1]) };
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte': {
      const c = compare(vals[0], vals[1]);
      if (!c.ok) return c;
      const v = name === 'gt' ? c.c > 0 : name === 'gte' ? c.c >= 0 : name === 'lt' ? c.c < 0 : c.c <= 0;
      return { ok: true, v };
    }
    case 'in': {
      if (!Array.isArray(vals[1])) return { ok: false, error: `'in' needs a list: ${JSON.stringify(vals[1])}` };
      return { ok: true, v: vals[1].some((x) => same(vals[0], x)) };
    }
    case 'matches': {
      if (typeof vals[0] !== 'string') return { ok: false, error: `'matches' needs a text: ${JSON.stringify(vals[0])}` };
      if (typeof vals[1] !== 'string') return { ok: false, error: `not a pattern: ${JSON.stringify(vals[1])}` };
      const bad = regexError(vals[1]);
      return bad ? { ok: false, error: bad } : { ok: true, v: new RegExp(vals[1]).test(vals[0]) };
    }
    case 'round': {
      const n = num(vals[0]);
      if (!n.ok) return n;
      const d = vals[1];
      if (typeof d !== 'number' || !Number.isInteger(d) || d < 0) return { ok: false, error: `'round' needs a number of decimals: ${JSON.stringify(d)}` };
      // Mitad hacia arriba (lejos del cero), como RoundingMode.HALF_UP.
      const f = 10 ** d;
      return { ok: true, v: (Math.sign(n.n) * Math.round(Math.abs(n.n) * f + Number.EPSILON)) / f };
    }
    case 'and':
    case 'or': {
      let acc = name === 'and';
      for (const v of vals) {
        const b = bool(v);
        if (!b.ok) return b;
        acc = name === 'and' ? acc && b.b : acc || b.b;
      }
      return { ok: true, v: acc };
    }
    case 'not': {
      const b = bool(vals[0]);
      return b.ok ? { ok: true, v: !b.b } : b;
    }
    case 'add':
      return fold((a, b) => a + b);
    case 'sub':
      return fold((a, b) => a - b);
    case 'mul':
      return fold((a, b) => a * b);
    case 'div':
      return fold((a, b) => (b === 0 ? 'division by zero' : a / b));
    case 'concat':
      return { ok: true, v: vals.map((v) => (typeof v === 'string' ? v : JSON.stringify(v))).join('') };
    case 'len': {
      const a = vals[0];
      if (Array.isArray(a)) return { ok: true, v: a.length };
      if (typeof a === 'string') return { ok: true, v: a.length };
      if (isObj(a)) return { ok: true, v: Object.keys(a).length };
      return { ok: false, error: `no length: ${JSON.stringify(a)}` };
    }
    default:
      return { ok: false, error: `cannot apply '${name}'` };
  }
}

/** Parámetros de `stream.map` leídos como en MapOp.params. */
export interface MapParams {
  set: Array<[string, Expr]>;
  remove: string[];
  replace: Expr | null;
}

export function parseMapParams(params: Json): { ok: true; p: MapParams } | { ok: false; error: string } {
  const obj = isObj(params) ? params : {};
  const set: Array<[string, Expr]> = [];
  const setObj = isObj(obj.set) ? obj.set : {};
  for (const name of Object.keys(setObj).sort()) {
    const path = parsePath(name);
    if (path === null) return { ok: false, error: `not a valid state path: '${name}'` };
    const e = parseExpr(setObj[name]);
    if (!e.ok) return { ok: false, error: `set.${name}: ${e.error}` };
    set.push([path, e.expr]);
  }
  const remove: string[] = [];
  for (const r of Array.isArray(obj.remove) ? obj.remove : []) {
    const path = typeof r === 'string' ? parsePath(r) : null;
    if (path === null) return { ok: false, error: `not a valid state path: ${JSON.stringify(r)}` };
    remove.push(path);
  }
  let replace: Expr | null = null;
  if (obj.replace !== undefined && obj.replace !== null) {
    const e = parseExpr(obj.replace);
    if (!e.ok) return { ok: false, error: e.error };
    replace = e.expr;
  }
  return { ok: true, p: { set, remove, replace } };
}

/** MapOp.transform: replace, luego set (todas las expresiones leen el elemento original), luego remove. */
export function transformElement(p: MapParams, element: Json): R {
  let base: Json = element;
  if (p.replace) {
    const r = evalExpr(p.replace, element);
    if (!r.ok) return r;
    base = r.v;
  }
  let acc: Json = base;
  for (const [path, expr] of p.set) {
    if (!isObj(acc)) return { ok: false, error: 'only an object can have fields set' };
    const r = evalExpr(expr, element);
    if (!r.ok) return r;
    acc = setPath(acc, path, r.v);
  }
  for (const path of p.remove) acc = removePath(acc, path);
  return { ok: true, v: acc };
}

/** FilterOp: `where` debe dar true o false. Devuelve si pasa, o el motivo del fallo (dead letter). */
export function filterElement(where: Expr, element: Json): { ok: true; pass: boolean } | { ok: false; error: string } {
  const r = evalExpr(where, element);
  if (!r.ok) return r;
  if (typeof r.v !== 'boolean') return { ok: false, error: `the condition is not true or false: ${JSON.stringify(r.v)}` };
  return { ok: true, pass: r.v };
}
