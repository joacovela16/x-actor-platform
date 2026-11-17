// Parser y calendario de cron en el cliente, sin dependencias. Misma gramática que `CronExpression` (Scala):
// 5 campos (m h dom mon dow) o 6 con segundos delante; listas, rangos, pasos, nombres de mes y día; @yearly...;
// si dom y dow están restringidos, basta con que uno coincida (como cron). Calcula horas con Intl en la zona dada.
// Aproximado en los cambios de hora (horas que no existen o se repiten): se avisa en la UI.

export interface CronSpec {
  text: string;
  seconds: number[];
  minutes: number[];
  hours: number[];
  daysOfMonth: number[];
  months: number[];
  daysOfWeek: number[];
  anyDayOfMonth: boolean;
  anyDayOfWeek: boolean;
}

export type CronParse = { ok: true; spec: CronSpec } | { ok: false; error: string };

const SHORTCUTS: Record<string, string> = {
  '@yearly': '0 0 1 1 *',
  '@annually': '0 0 1 1 *',
  '@monthly': '0 0 1 * *',
  '@weekly': '0 0 * * 0',
  '@daily': '0 0 * * *',
  '@midnight': '0 0 * * *',
  '@hourly': '0 * * * *',
};

const MONTHS: Record<string, number> = { JAN: 1, FEB: 2, MAR: 3, APR: 4, MAY: 5, JUN: 6, JUL: 7, AUG: 8, SEP: 9, OCT: 10, NOV: 11, DEC: 12 };
const DAYS: Record<string, number> = { SUN: 0, MON: 1, TUE: 2, WED: 3, THU: 4, FRI: 5, SAT: 6 };

interface Spec {
  name: string;
  min: number;
  max: number;
  names?: Record<string, number>;
}
const SECOND: Spec = { name: 'segundo', min: 0, max: 59 };
const MINUTE: Spec = { name: 'minuto', min: 0, max: 59 };
const HOUR: Spec = { name: 'hora', min: 0, max: 23 };
const DOM: Spec = { name: 'día del mes', min: 1, max: 31 };
const MONTH: Spec = { name: 'mes', min: 1, max: 12, names: MONTHS };
const DOW: Spec = { name: 'día de la semana', min: 0, max: 7, names: DAYS };

class CronError extends Error {}

function number(text: string, spec: Spec): number {
  const named = spec.names?.[text.toUpperCase()];
  let n: number;
  if (named !== undefined) n = named;
  else if (/^\d{1,4}$/.test(text)) n = Number(text);
  else throw new CronError(`${spec.name}: '${text}' no es un número${spec.names ? ' ni un nombre' : ''}`);
  if (n < spec.min || n > spec.max) throw new CronError(`${spec.name}: ${n} está fuera de ${spec.min}-${spec.max}`);
  return n;
}

function item(text: string, spec: Spec): number[] {
  const parts = text.split('/');
  if (parts.length > 2) throw new CronError(`${spec.name}: '${text}' tiene más de una barra`);
  const range = parts[0];
  let by = 1;
  if (parts.length === 2) {
    if (!/^\d{1,4}$/.test(parts[1]) || Number(parts[1]) <= 0) throw new CronError(`${spec.name}: el paso '${parts[1]}' debe ser un número positivo`);
    by = Number(parts[1]);
  }
  let from: number;
  let to: number;
  if (range === '*' || range === '?') {
    from = spec.min;
    to = spec.max;
  } else if (range.includes('-')) {
    const [a, b, ...rest] = range.split('-');
    if (rest.length || b === undefined) throw new CronError(`${spec.name}: '${range}' no es un rango`);
    from = number(a, spec);
    to = number(b, spec);
    if (from > to) throw new CronError(`${spec.name}: el rango '${range}' va al revés`);
  } else {
    from = number(range, spec);
    to = parts.length === 2 ? spec.max : from; // `5/10` empieza en 5 y sigue cada 10
  }
  const out: number[] = [];
  for (let v = from; v <= to; v += by) out.push(v);
  return out;
}

function field(text: string, spec: Spec): number[] {
  const values = new Set<number>();
  for (const part of text.split(',')) {
    if (part === '') throw new CronError(`${spec.name}: hay una coma sobrante`);
    for (const v of item(part, spec)) values.add(v);
  }
  return [...values].sort((a, b) => a - b);
}

const anyField = (text: string) => text.startsWith('*') || text.startsWith('?');

export function parseCron(input: string): CronParse {
  const text = input.trim();
  const lower = text.toLowerCase();
  if (text.startsWith('@') && !(lower in SHORTCUTS)) {
    return { ok: false, error: `atajo desconocido '${text}'; usa ${Object.keys(SHORTCUTS).join(', ')}` };
  }
  const expanded = SHORTCUTS[lower] ?? text;
  const parts = expanded.split(/\s+/).filter(Boolean);
  if (parts.length !== 5 && parts.length !== 6) {
    return { ok: false, error: `una expresión cron tiene 5 o 6 campos y esta tiene ${parts.length}` };
  }
  const [s, mi, h, dom, mo, dow] = parts.length === 5 ? ['0', ...parts] : parts;
  try {
    const spec: CronSpec = {
      text,
      seconds: field(s, SECOND),
      minutes: field(mi, MINUTE),
      hours: field(h, HOUR),
      daysOfMonth: field(dom, DOM),
      months: field(mo, MONTH),
      daysOfWeek: [...new Set(field(dow, DOW).map((d) => d % 7))].sort((a, b) => a - b),
      anyDayOfMonth: anyField(dom),
      anyDayOfWeek: anyField(dow),
    };
    return { ok: true, spec };
  } catch (e) {
    if (e instanceof CronError) return { ok: false, error: e.message };
    throw e;
  }
}

interface Parts {
  month: number;
  day: number;
  weekday: number;
  hour: number;
  minute: number;
  second: number;
}

const WEEKDAYS: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
const formatters = new Map<string, Intl.DateTimeFormat>();

function formatterFor(zone: string): Intl.DateTimeFormat {
  let f = formatters.get(zone);
  if (!f) {
    f = new Intl.DateTimeFormat('en-US', {
      timeZone: zone, hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric', weekday: 'short',
      hour: 'numeric', minute: 'numeric', second: 'numeric',
    });
    formatters.set(zone, f);
  }
  return f;
}

/** Zona válida para Intl (si no, `null`). */
export function validZone(zone: string): boolean {
  try {
    formatterFor(zone);
    return true;
  } catch {
    return false;
  }
}

function partsAt(zone: string, ms: number): Parts {
  const out: Record<string, string> = {};
  for (const p of formatterFor(zone).formatToParts(ms)) out[p.type] = p.value;
  return {
    month: Number(out.month),
    day: Number(out.day),
    weekday: WEEKDAYS[out.weekday] ?? 0,
    hour: Number(out.hour) % 24,
    minute: Number(out.minute),
    second: Number(out.second),
  };
}

const YEARS_TO_SEARCH = 8; // 29 de febrero cae como mucho cada 8 años

/**
 * Las próximas `count` horas (epoch ms) en que la expresión coincide, estrictamente después de `afterMs`, en `zone`.
 * Avanza por horas y minutos locales, no por milisegundos. Si no hay coincidencias en ese plazo, devuelve lo hallado.
 */
export function nextFires(spec: CronSpec, zone: string, afterMs: number, count: number): number[] {
  const out: number[] = [];
  let t = Math.floor(afterMs / 1000) * 1000 + 1000;
  const limit = afterMs + YEARS_TO_SEARCH * 366 * 86_400_000;
  const dayMatch = (p: Parts) =>
    spec.months.includes(p.month) &&
    (spec.anyDayOfMonth && spec.anyDayOfWeek
      ? true
      : spec.anyDayOfMonth
        ? spec.daysOfWeek.includes(p.weekday)
        : spec.anyDayOfWeek
          ? spec.daysOfMonth.includes(p.day)
          : spec.daysOfMonth.includes(p.day) || spec.daysOfWeek.includes(p.weekday));
  while (t < limit && out.length < count) {
    const p = partsAt(zone, t);
    const secondsIntoHour = p.minute * 60 + p.second;
    if (!dayMatch(p)) {
      t += (3600 - secondsIntoHour) * 1000; // siguiente hora local
      continue;
    }
    if (!spec.hours.includes(p.hour)) {
      t += (3600 - secondsIntoHour) * 1000;
      continue;
    }
    if (!spec.minutes.includes(p.minute)) {
      t += (60 - p.second) * 1000;
      continue;
    }
    if (!spec.seconds.includes(p.second)) {
      t += 1000;
      continue;
    }
    out.push(t);
    t += 1000;
  }
  return out;
}

/** Próxima coincidencia o `null` si no hay en el plazo de búsqueda. */
export function nextFire(spec: CronSpec, zone: string, afterMs: number): number | null {
  return nextFires(spec, zone, afterMs, 1)[0] ?? null;
}
