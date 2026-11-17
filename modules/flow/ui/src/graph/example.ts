// Valores de ejemplo verosímiles para una entidad: respetan tipos, restricciones (oneOf, min/max, largos, pattern, ítems), defaults,
// el estado inicial y las referencias (con la forma de la clave de la entidad apuntada). Cada llamada da un ejemplo distinto.
import type { DataType, EntityDef, EntityField, JsonValue } from '../api/types';

type Rnd = () => number;
const int = (rnd: Rnd, lo: number, hi: number) => lo + Math.floor(rnd() * (hi - lo + 1));
const pick = <T,>(rnd: Rnd, xs: readonly T[]): T => xs[Math.floor(rnd() * xs.length)];

const FIRST = ['Ana', 'Lucía', 'Martín', 'Sofía', 'Diego', 'Valentina', 'Julián', 'Camila'];
const LAST = ['García', 'Fernández', 'López', 'Martínez', 'Romero', 'Sosa', 'Díaz', 'Pereyra'];
const COMPANIES = ['Acme SA', 'Distribuidora Sur', 'Tecnología Andina SRL', 'Logística Delta', 'Panificadora Norte'];
const CITIES = ['Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza', 'Montevideo', 'Madrid'];
const STREETS = ['Av. Corrientes', 'San Martín', 'Belgrano', 'Rivadavia', 'Mitre', 'Sarmiento'];
const WORDS = ['alfa', 'beta', 'gamma', 'delta', 'norte', 'sur', 'azul', 'verde'];

/** Un string con la forma de la expresión regular (lo común: literales, clases, \d \w \s, grupos con |, cuantificadores). */
export function stringFor(pattern: string, rnd: Rnd = Math.random): string | null {
  let i = 0;
  const src = pattern;
  const LETTERS = 'abcdefghijklmnopqrstuvwxyz';
  const DIGITS = '0123456789';
  const classOf = (c: string): string => (c === 'd' ? DIGITS : c === 'w' ? LETTERS + DIGITS : c === 's' ? ' ' : c === 'D' || c === 'W' ? '-' : c === 'S' ? 'x' : c);

  type Gen = () => string;
  const alt = (): Gen => {
    const branches: Gen[][] = [[]];
    while (i < src.length && src[i] !== ')') {
      if (src[i] === '|') { i++; branches.push([]); continue; }
      branches[branches.length - 1].push(quantified());
    }
    return () => pick(rnd, branches).map((g) => g()).join('');
  };
  const atom = (): Gen => {
    const c = src[i++];
    if (c === '^' || c === '$') return () => '';
    if (c === '(') {
      if (src.startsWith('?:', i)) i += 2;
      const g = alt();
      if (src[i++] !== ')') throw new Error('group');
      return g;
    }
    if (c === '[') {
      const negated = src[i] === '^';
      if (negated) i++;
      let set = '';
      while (i < src.length && src[i] !== ']') {
        let a = src[i++];
        if (a === '\\') a = classOf(src[i++]);
        if (src[i] === '-' && src[i + 1] !== ']' && a.length === 1) {
          i++;
          let b = src[i++];
          if (b === '\\') b = src[i++];
          for (let k = a.charCodeAt(0); k <= b.charCodeAt(0); k++) set += String.fromCharCode(k);
        } else set += a;
      }
      i++;
      const chars = negated ? (LETTERS + DIGITS).split('').filter((x) => !set.includes(x)).join('') || 'x' : set || 'x';
      return () => pick(rnd, chars.split(''));
    }
    if (c === '\\') {
      const set = classOf(src[i++]);
      return () => pick(rnd, set.split(''));
    }
    if (c === '.') return () => pick(rnd, LETTERS.split(''));
    return () => c;
  };
  const quantified = (): Gen => {
    const g = atom();
    let lo = 1, hi = 1;
    const q = src[i];
    if (q === '?') { lo = 0; hi = 1; i++; }
    else if (q === '*') { lo = 0; hi = 3; i++; }
    else if (q === '+') { lo = 1; hi = 4; i++; }
    else if (q === '{') {
      const m = /^\{(\d+)(,(\d*))?\}/.exec(src.slice(i));
      if (m) {
        lo = Number(m[1]);
        hi = m[2] === undefined ? lo : m[3] ? Number(m[3]) : lo + 3;
        i += m[0].length;
      }
    }
    if (src[i] === '?' || src[i] === '+') i++; // perezoso / posesivo
    return () => Array.from({ length: int(rnd, lo, hi) }, g).join('');
  };
  try {
    const g = alt();
    if (i < src.length) return null;
    const re = new RegExp(pattern);
    for (let n = 0; n < 10; n++) {
      const s = g();
      if (re.test(s)) return s;
    }
    return null;
  } catch {
    return null;
  }
}

/** Un string que suena al nombre del campo. */
function textFor(name: string, rnd: Rnd, ctx: { entity: string; company: boolean }): string {
  const n = name.toLowerCase();
  const person = `${pick(rnd, FIRST)} ${pick(rnd, LAST)}`;
  if (n === 'id' || n.endsWith('id') && n.length > 2) {
    if (/tax|cuit|cuil/.test(n)) return `${pick(rnd, ['20', '27', '30'])}-${int(rnd, 10000000, 99999999)}-${int(rnd, 0, 9)}`;
    if (/track/.test(n)) return `TRK${int(rnd, 100000000, 999999999)}`;
    return `${ctx.entity.slice(0, 1).toUpperCase()}-${int(rnd, 1, 9999)}`;
  }
  if (/mail/.test(n)) return `${pick(rnd, FIRST).toLowerCase().normalize('NFD').replace(/[^a-z]/g, '')}@ejemplo.com`;
  if (/phone|tel/.test(n)) return `+54 11 ${int(rnd, 4000, 6999)}-${int(rnd, 1000, 9999)}`;
  if (/city|ciudad/.test(n)) return pick(rnd, CITIES);
  if (/street|calle|address|direcc/.test(n)) return `${pick(rnd, STREETS)} ${int(rnd, 100, 4999)}`;
  if (/zip|postal|cp$/.test(n)) return `${int(rnd, 1000, 9999)}`;
  if (/sku|code|codigo/.test(n)) return `${pick(rnd, ['ABC', 'XYZ', 'PRD', 'SKU'])}-${int(rnd, 100, 999)}`;
  if (/company|empresa|razon/.test(n)) return pick(rnd, COMPANIES);
  if (/name|nombre/.test(n)) return ctx.company ? pick(rnd, COMPANIES) : person;
  if (/country|pais/.test(n)) return pick(rnd, ['Argentina', 'Uruguay', 'España', 'Chile']);
  if (/date|fecha|_at$|At$/.test(name)) return new Date(Date.now() - int(rnd, 0, 90) * 86400000).toISOString().slice(0, 10);
  if (/desc|note|nota|comment/.test(n)) return `Ejemplo de ${pick(rnd, WORDS)} ${pick(rnd, WORDS)}`;
  if (/url|web/.test(n)) return `https://ejemplo.com/${pick(rnd, WORDS)}`;
  if (/currency|moneda/.test(n)) return pick(rnd, ['ARS', 'USD', 'EUR']);
  return `${pick(rnd, WORDS)}-${int(rnd, 1, 99)}`;
}

function numberFor(name: string, rnd: Rnd, min?: number, max?: number): number {
  const n = name.toLowerCase();
  const [lo0, hi0, cents] =
    /qty|quantity|cantidad|count|items/.test(n) ? [1, 5, false]
    : /price|precio|amount|monto|cost|costo|total/.test(n) ? [10, 500, true]
    : /age|edad/.test(n) ? [18, 80, false]
    : /percent|porcentaje|discount|descuento/.test(n) ? [0, 30, false]
    : /year|anio|año/.test(n) ? [2020, 2026, false]
    : [1, 100, false];
  const lo = Math.max(lo0, min ?? -Infinity);
  const hi = Math.min(hi0, max ?? Infinity);
  const [a, b] = lo <= hi ? [lo, hi] : [min ?? max ?? 0, max ?? (min ?? 0) + 100];
  const v = cents ? Math.round((a + rnd() * (b - a)) * 100) / 100 : int(rnd, Math.ceil(a), Math.floor(b));
  return Number.isFinite(v) ? v : a;
}

interface Ctx {
  entities: Record<string, EntityDef>;
  rnd: Rnd;
  inside: string[];
}

function valueFor(t: DataType, name: string, ctx: Ctx, field?: EntityField, owner?: { entity: string; company: boolean }): JsonValue {
  const k = field?.constraints ?? {};
  if (k.oneOf?.length) return pick(ctx.rnd, k.oneOf);
  switch (t.type) {
    case 'bool': return ctx.rnd() < 0.5;
    case 'any': return null;
    case 'num': return numberFor(name, ctx.rnd, k.min, k.max);
    case 'str': {
      let s = (k.pattern && stringFor(k.pattern, ctx.rnd)) || textFor(name, ctx.rnd, owner ?? { entity: name, company: false });
      if (!k.pattern) {
        if (k.maxLength !== undefined) s = s.slice(0, k.maxLength);
        if (k.minLength !== undefined) while (s.length < k.minLength) s += 'x';
      }
      return s;
    }
    case 'list': {
      const lo = Math.max(k.minItems ?? 1, 1);
      const hi = Math.max(lo, Math.min(k.maxItems ?? lo + 2, lo + 2));
      const singular = name.replace(/s$/, '');
      return Array.from({ length: int(ctx.rnd, lo, hi) }, () => valueFor(t.element, singular, ctx, undefined, owner));
    }
    case 'obj': return Object.fromEntries(Object.entries(t.fields).map(([n, ft]) => [n, valueFor(ft, n, ctx, undefined, owner)]));
    case 'entity': return entityValue(t.name, ctx);
    case 'ref': {
      const e = ctx.entities[t.entity];
      const key = e?.fields.find((f) => f.name === e.key);
      return key ? valueFor(key.type, key.name, ctx, key, { entity: e!.name, company: false }) : `${t.entity.slice(0, 1)}-1`;
    }
  }
}

function entityValue(name: string, ctx: Ctx): JsonValue {
  const e = ctx.entities[name];
  if (!e || ctx.inside.includes(name)) return {};
  const inner = { ...ctx, inside: [...ctx.inside, name] };
  const o: Record<string, JsonValue> = {};
  const owner = { entity: name, company: false };
  for (const f of e.fields) {
    if (f.computed !== undefined) continue;
    if (e.states && f.name === e.states.field) { o[f.name] = e.states.initial; continue; }
    // Los opcionales casi siempre van (así se ven y las reglas del tipo «si X, debe existir Y» se cumplen).
    if (f.required === false && ctx.rnd() < 0.15) continue;
    const v = f.constraints?.oneOf?.length ? pick(ctx.rnd, f.constraints.oneOf) : valueFor(f.type, f.name, inner, f, owner);
    o[f.name] = v;
    if (/kind|type|tipo/.test(f.name) && v === 'company') owner.company = true;
  }
  // Un nombre generado antes de saber que es empresa: rehacerlo.
  if (owner.company) for (const f of e.fields) if (/name|nombre/i.test(f.name) && f.type.type === 'str' && !f.constraints?.pattern && !f.constraints?.oneOf) o[f.name] = pick(ctx.rnd, COMPANIES);
  return o;
}

/** Un valor de ejemplo de la entidad `name`. */
export function exampleFor(name: string, entities: Record<string, EntityDef>, rnd: Rnd = Math.random): JsonValue {
  return entityValue(name, { entities, rnd, inside: [] });
}
