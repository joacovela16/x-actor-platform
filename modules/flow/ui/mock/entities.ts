// Entidades del modelo de negocio para el mock: ejemplos, el chequeo de una definición (lo principal de EntityValidator) y el de
// un valor (EntityCatalog.check / transition). Port simplificado de modules/flow/entity: lo justo para probar la UI sin backend.
import type { DataType, EntityDef, EntityError, EntityField, EntityProblem, JsonValue } from '../src/api/types.ts';
import { evalExpr, parseExpr, regexError, same } from '../src/graph/expr.ts';

type Json = JsonValue;
const isObj = (j: Json | undefined): j is { [k: string]: Json } => typeof j === 'object' && j !== null && !Array.isArray(j);

export const seedEntities: EntityDef[] = [
  {
    name: 'Customer', label: 'Cliente', kind: 'root', key: 'id', revision: 1, source: 'store',
    fields: [
      { name: 'id', type: { type: 'str' }, constraints: { pattern: '^C-[0-9]+$' } },
      { name: 'name', type: { type: 'str' }, constraints: { minLength: 1 } },
      { name: 'kind', type: { type: 'str' }, default: 'person', constraints: { oneOf: ['person', 'company'] } },
      { name: 'taxId', type: { type: 'str' }, required: false },
      { name: 'address', type: { type: 'entity', name: 'Address' }, required: false },
    ],
    rules: [{ name: 'company-tax-id', when: { eq: [{ path: 'kind' }, 'company'] }, check: { exists: 'taxId' }, message: 'Una empresa necesita CUIT' }],
  },
  {
    name: 'Address', label: 'Dirección', kind: 'value', revision: 1, source: 'code',
    fields: [
      { name: 'street', type: { type: 'str' } },
      { name: 'city', type: { type: 'str' } },
      { name: 'zip', type: { type: 'str' }, required: false },
    ],
  },
  {
    name: 'OrderLine', label: 'Línea de pedido', kind: 'value', revision: 1, source: 'store',
    fields: [
      { name: 'sku', type: { type: 'str' } },
      { name: 'qty', type: { type: 'num' }, constraints: { min: 1 } },
      { name: 'price', type: { type: 'num' }, constraints: { min: 0 } },
      { name: 'subtotal', type: { type: 'num' }, computed: { mul: [{ path: 'qty' }, { path: 'price' }] } },
    ],
  },
  {
    name: 'Order', label: 'Pedido', kind: 'root', key: 'id', revision: 2, source: 'store',
    fields: [
      { name: 'id', type: { type: 'str' } },
      { name: 'customer', type: { type: 'ref', entity: 'Customer' } },
      { name: 'lines', type: { type: 'list', element: { type: 'entity', name: 'OrderLine' } }, constraints: { minItems: 1 } },
      { name: 'total', type: { type: 'num' }, computed: { sum: ['lines', { path: 'subtotal' }] } },
      { name: 'status', type: { type: 'str' } },
      { name: 'trackingId', type: { type: 'str' }, required: false },
    ],
    rules: [{ name: 'shipped-tracking', when: { eq: [{ path: 'status' }, 'shipped'] }, check: { exists: 'trackingId' }, message: 'Un pedido enviado necesita seguimiento' }],
    states: {
      field: 'status', initial: 'draft', states: ['draft', 'approved', 'shipped', 'cancelled'],
      transitions: [
        { name: 'approve', from: ['draft'], to: 'approved', guard: { gt: [{ path: 'total' }, 0] }, message: 'El total debe ser positivo' },
        { name: 'ship', from: ['approved'], to: 'shipped' },
        { name: 'cancel', from: ['draft', 'approved'], to: 'cancelled' },
      ],
    },
  },
];

const err = (code: string, entity: string, message: string, field: string | null = null): EntityError => ({ code, entity, field, message });

function mentions(t: DataType): string[] {
  switch (t.type) {
    case 'list': return mentions(t.element);
    case 'obj': return Object.values(t.fields).flatMap(mentions);
    case 'entity': return [t.name];
    case 'ref': return [t.entity];
    default: return [];
  }
}

/** Lo principal de EntityValidator: nombres, clave, entidades que existen, restricciones que aplican, expresiones que se leen,
 *  estados coherentes. */
export function validateEntity(e: EntityDef, all: Map<string, EntityDef>): EntityError[] {
  const out: EntityError[] = [];
  const n = e.name;
  if (!/^[A-Za-z][A-Za-z0-9_]*$/.test(n)) out.push(err('invalid-name', n, `'${n}' is not a valid name`));
  if (!e.fields?.length) out.push(err('no-fields', n, 'an entity needs at least one field'));
  const names = (e.fields ?? []).map((f) => f.name);
  for (const f of new Set(names.filter((x, i) => names.indexOf(x) !== i))) out.push(err('duplicate-field', n, `there are several fields called '${f}'`, f));
  if (e.kind === 'root') {
    const k = e.fields.find((f) => f.name === e.key);
    if (!e.key) out.push(err('missing-key', n, 'a root entity needs a key: the field that identifies its values'));
    else if (!k) out.push(err('unknown-key', n, `the key '${e.key}' is not a field`, e.key));
    else if (k.type.type !== 'str' && k.type.type !== 'num') out.push(err('invalid-key', n, 'the key must be str or num', e.key));
    else if (k.required === false || k.computed !== undefined) out.push(err('invalid-key', n, 'the key must be required and not computed', e.key));
  }
  for (const f of e.fields ?? []) {
    for (const m of mentions(f.type)) if (m !== n && !all.has(m)) out.push(err('unknown-entity', n, `the entity '${m}' does not exist`, f.name));
    const k = f.constraints ?? {};
    const only = (set: boolean, what: string, fits: boolean) => { if (set && !fits) out.push(err('invalid-constraint', n, `'${what}' does not apply to ${f.type.type}`, f.name)); };
    only(k.min !== undefined || k.max !== undefined, 'min/max', f.type.type === 'num');
    only(k.minLength !== undefined || k.maxLength !== undefined || k.pattern !== undefined, 'minLength/maxLength/pattern', f.type.type === 'str');
    only(k.minItems !== undefined || k.maxItems !== undefined, 'minItems/maxItems', f.type.type === 'list');
    if (k.pattern) {
      const bad = regexError(k.pattern);
      if (bad) out.push(err('invalid-constraint', n, bad, f.name));
    }
    if (f.computed !== undefined) {
      const p = parseExpr(f.computed);
      if (!p.ok) out.push(err('invalid-expression', n, `computed: ${p.error}`, f.name));
    }
  }
  for (const r of e.rules ?? []) {
    for (const [what, x] of [['check', r.check], ['when', r.when]] as const) {
      if (x === undefined) continue;
      const p = parseExpr(x);
      if (!p.ok) out.push(err('invalid-expression', n, `rule '${r.name}' ${what}: ${p.error}`));
    }
  }
  const m = e.states;
  if (m) {
    const known = new Set(m.states);
    if (!e.fields.some((f) => f.name === m.field && f.type.type === 'str')) out.push(err('invalid-states', n, `the state field '${m.field}' must be a str field`));
    if (!known.has(m.initial)) out.push(err('invalid-states', n, `the initial state '${m.initial}' is not one of the states`));
    for (const t of m.transitions) {
      for (const s of [...t.from, t.to]) if (!known.has(s)) out.push(err('invalid-states', n, `the transition '${t.name}' uses the state '${s}', which is not one of the states`));
      if (!t.from.length) out.push(err('invalid-states', n, `the transition '${t.name}' starts from no state`));
    }
  }
  return out;
}

const present = (o: { [k: string]: Json }, name: string) => (o[name] === undefined || o[name] === null ? undefined : o[name]);

/** Un valor con su tipo: el valor como lo hacen sus entidades, o sus problemas. */
function conformType(t: DataType, v: Json, all: Map<string, EntityDef>, path: string[]): { v: Json; problems: EntityProblem[] } {
  const p = (rule: string, message: string): EntityProblem => ({ path: path.join('.'), rule, message });
  switch (t.type) {
    case 'bool': return typeof v === 'boolean' ? { v, problems: [] } : { v, problems: [p('type', 'must be bool')] };
    case 'num': return typeof v === 'number' ? { v, problems: [] } : { v, problems: [p('type', 'must be num')] };
    case 'str': return typeof v === 'string' ? { v, problems: [] } : { v, problems: [p('type', 'must be str')] };
    case 'any': return { v, problems: [] };
    case 'ref': {
      const target = all.get(t.entity);
      const key = target?.fields.find((f) => f.name === target.key);
      const ok = key ? conformType(key.type, v, all, path).problems.length === 0 : typeof v === 'string' || typeof v === 'number';
      return ok ? { v, problems: [] } : { v, problems: [p('type', `must be ref ${t.entity}`)] };
    }
    case 'list': {
      if (!Array.isArray(v)) return { v, problems: [p('type', `must be list`)] };
      const each = v.map((x, i) => conformType(t.element, x, all, [...path, String(i)]));
      return { v: each.map((x) => x.v), problems: each.flatMap((x) => x.problems) };
    }
    case 'obj': {
      if (!isObj(v)) return { v, problems: [p('type', 'must be an object')] };
      const o = { ...v };
      const problems: EntityProblem[] = [];
      for (const [name, ft] of Object.entries(t.fields)) {
        if (o[name] === undefined) problems.push({ path: [...path, name].join('.'), rule: 'required', message: 'is missing' });
        else {
          const r = conformType(ft, o[name], all, [...path, name]);
          o[name] = r.v;
          problems.push(...r.problems);
        }
      }
      return { v: o, problems };
    }
    case 'entity': {
      const r = checkValue(t.name, v, all);
      return { v: r.value, problems: r.problems.map((x) => ({ ...x, path: [...path, ...(x.path ? [x.path] : [])].join('.') })) };
    }
  }
}

const plural = (n: number, noun: string) => (n === 1 ? `1 ${noun}` : `${n} ${noun}s`);

function constraints(f: EntityField, v: Json, path: string): EntityProblem[] {
  const k = f.constraints ?? {};
  const out: EntityProblem[] = [];
  const p = (rule: string, message: string) => out.push({ path, rule, message });
  if (typeof v === 'number') {
    if (k.min !== undefined && v < k.min) p('min', `must be at least ${k.min}`);
    if (k.max !== undefined && v > k.max) p('max', `must be at most ${k.max}`);
  }
  if (typeof v === 'string') {
    if (k.minLength !== undefined && v.length < k.minLength) p('minLength', `must have at least ${plural(k.minLength, 'character')}`);
    if (k.maxLength !== undefined && v.length > k.maxLength) p('maxLength', `must have at most ${plural(k.maxLength, 'character')}`);
    if (k.pattern && !regexError(k.pattern) && !new RegExp(k.pattern).test(v)) p('pattern', `must match ${k.pattern}`);
  }
  if (Array.isArray(v)) {
    if (k.minItems !== undefined && v.length < k.minItems) p('minItems', `must have at least ${plural(k.minItems, 'item')}`);
    if (k.maxItems !== undefined && v.length > k.maxItems) p('maxItems', `must have at most ${plural(k.maxItems, 'item')}`);
  }
  if (k.oneOf?.length && !k.oneOf.some((x) => same(x, v))) p('oneOf', `must be one of ${k.oneOf.map((x) => JSON.stringify(x)).join(', ')}`);
  return out;
}

function holds(expr: Json, value: Json): { ok: true; b: boolean } | { ok: false; error: string } {
  const p = parseExpr(expr);
  if (!p.ok) return p;
  const r = evalExpr(p.expr, value);
  if (!r.ok) return r;
  return typeof r.v === 'boolean' ? { ok: true, b: r.v } : { ok: false, error: `it gives ${JSON.stringify(r.v)}, not a boolean` };
}

/** EntityCatalog.check: defaults y estado inicial, campos (entidades anidadas primero), calculados, restricciones, estado, reglas. */
export function checkValue(name: string, value: Json, all: Map<string, EntityDef>): { value: Json; problems: EntityProblem[] } {
  const e = all.get(name);
  if (!e) return { value, problems: [{ path: '', rule: 'unknown-entity', message: `the entity '${name}' does not exist` }] };
  if (!isObj(value)) return { value, problems: [{ path: '', rule: 'type', message: `must be an object (a value of ${name})` }] };
  const o: { [k: string]: Json } = { ...value };
  for (const f of e.fields) if (present(o, f.name) === undefined && f.default !== undefined) o[f.name] = f.default;
  if (e.states && present(o, e.states.field) === undefined) o[e.states.field] = e.states.initial;

  const fieldProblems: EntityProblem[] = [];
  for (const f of e.fields.filter((x) => x.computed === undefined)) {
    const v = present(o, f.name);
    if (v === undefined) {
      if (f.required !== false) fieldProblems.push({ path: f.name, rule: 'required', message: 'is missing' });
      continue;
    }
    const r = conformType(f.type, v, all, [f.name]);
    if (r.problems.length) fieldProblems.push(...r.problems);
    else o[f.name] = r.v;
  }
  // Calculados en el orden en que se declaran, repitiendo hasta que no cambien (los que leen a otros quedan bien).
  const computeProblems: EntityProblem[] = [];
  const computed = e.fields.filter((f) => f.computed !== undefined);
  for (let round = 0; round < Math.max(1, computed.length); round++) {
    computeProblems.length = 0;
    for (const f of computed) {
      const p = parseExpr(f.computed!);
      const r = p.ok ? evalExpr(p.expr, o) : p;
      if (r.ok) o[f.name] = r.v;
      else {
        delete o[f.name];
        computeProblems.push({ path: f.name, rule: 'computed', message: `cannot be computed: ${r.error}` });
      }
    }
  }
  const constraintProblems = e.fields.flatMap((f) => (present(o, f.name) === undefined ? [] : constraints(f, o[f.name], f.name)));
  const stateProblems: EntityProblem[] =
    e.states && typeof o[e.states.field] === 'string' && !e.states.states.includes(o[e.states.field] as string)
      ? [{ path: e.states.field, rule: 'state', message: `must be one of ${e.states.states.join(', ')}` }]
      : [];
  const before = [...fieldProblems, ...(fieldProblems.length ? [] : computeProblems), ...constraintProblems, ...stateProblems];
  const ruleProblems: EntityProblem[] = [];
  for (const r of e.rules ?? []) {
    const applies = r.when === undefined ? { ok: true as const, b: true } : holds(r.when, o);
    // Una regla que no aplica se cumple.
    const result = applies.ok && !applies.b ? { ok: true as const, b: true } : applies.ok ? holds(r.check, o) : applies;
    if (result.ok && !result.b) ruleProblems.push({ path: '', rule: r.name, message: r.message ?? `breaks the rule '${r.name}'` });
    else if (!result.ok && before.length === 0) ruleProblems.push({ path: '', rule: r.name, message: `cannot be checked: ${result.error}` });
  }
  return { value: o, problems: [...before, ...ruleProblems] };
}

/** EntityCatalog.transition. */
export function transitionValue(name: string, value: Json, transition: string, all: Map<string, EntityDef>): { value: Json; problems: EntityProblem[] } {
  const fail = (path: string, rule: string, message: string) => ({ value, problems: [{ path, rule, message }] });
  const e = all.get(name);
  if (!e) return fail('', 'unknown-entity', `the entity '${name}' does not exist`);
  const m = e.states;
  if (!m) return fail('', 'transition', `the entity '${name}' has no states`);
  const t = m.transitions.find((x) => x.name === transition);
  if (!t) return fail('', 'transition', `the entity '${name}' has no transition '${transition}'`);
  const current = checkValue(name, value, all);
  if (current.problems.length) return { value, problems: current.problems };
  const o = current.value as { [k: string]: Json };
  const state = typeof o[m.field] === 'string' ? (o[m.field] as string) : '';
  if (!t.from.includes(state)) return fail(m.field, 'transition', `cannot '${transition}' from '${state}': only from ${t.from.join(', ')}`);
  if (t.guard !== undefined) {
    const g = holds(t.guard, o);
    if (!g.ok) return fail('', transition, `the condition of '${transition}' cannot be checked: ${g.error}`);
    if (!g.b) return fail('', transition, t.message ?? `the condition of '${transition}' does not hold`);
  }
  return checkValue(name, { ...o, [m.field]: t.to }, all);
}

