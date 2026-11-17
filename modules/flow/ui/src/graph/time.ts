import type { Stamp } from '../api/types';

/** Epoch ms de un instante (número, texto con dígitos o ISO). `null` si no es un instante. */
export function toMillis(v: Stamp): number | null {
  if (v === null || v === undefined || v === '') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (/^\d+$/.test(v)) return Number(v);
  const t = Date.parse(v);
  return Number.isNaN(t) ? null : t;
}

const p2 = (n: number, w = 2) => String(n).padStart(w, '0');

/** Hora local `HH:MM:SS.mmm` (o `—`). */
export function fmtTime(v: Stamp): string {
  const ms = toMillis(v);
  if (ms === null) return '—';
  const d = new Date(ms);
  return `${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}.${p2(d.getMilliseconds(), 3)}`;
}

/** Hora local `HH:MM:SS`. */
export function fmtClock(v: Stamp, zone?: string): string {
  const ms = toMillis(v);
  if (ms === null) return '—';
  if (zone) {
    try {
      return new Intl.DateTimeFormat('es-ES', { timeZone: zone, hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' }).format(ms);
    } catch {
      /* zona no válida: se muestra en la local */
    }
  }
  const d = new Date(ms);
  return `${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}`;
}

/** Fecha y hora local. */
export function fmtDateTime(v: Stamp): string {
  const ms = toMillis(v);
  if (ms === null) return '—';
  return `${new Date(ms).toLocaleDateString()} ${fmtTime(ms)}`;
}

export function fmtDuration(ms?: number | null): string {
  if (ms === undefined || ms === null) return '—';
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(2)} s`;
}

/** Cuenta atrás: `en 3 s`, `en 2 min 5 s`, o `ahora`. */
export function fmtCountdown(fireAt: number, now: number): string {
  const s = Math.max(0, Math.round((fireAt - now) / 1000));
  if (s === 0) return 'ahora';
  if (s < 60) return `en ${s} s`;
  const m = Math.floor(s / 60);
  const rest = s % 60;
  if (m < 60) return `en ${m} min ${rest} s`;
  return `en ${Math.floor(m / 60)} h ${m % 60} min`;
}

/** Zonas horarias IANA (con `UTC` siempre). */
let zonesCache: string[] | null = null;
export function timeZones(): string[] {
  if (zonesCache) return zonesCache;
  let list: string[] = [];
  try {
    const f = (Intl as unknown as { supportedValuesOf?: (k: string) => string[] }).supportedValuesOf;
    list = f ? f('timeZone') : [];
  } catch {
    list = [];
  }
  if (!list.includes('UTC')) list = ['UTC', ...list];
  zonesCache = list;
  return list;
}

const ZONE_FORMATTERS = new Map<string, Intl.DateTimeFormat>();

/** Fecha y hora en una zona IANA (la de la expresión), con texto en castellano. */
export function fmtInZone(ms: number, zone: string): string {
  let f = ZONE_FORMATTERS.get(zone);
  if (!f) {
    f = new Intl.DateTimeFormat('es-ES', { timeZone: zone, dateStyle: 'medium', timeStyle: 'medium', hourCycle: 'h23' });
    ZONE_FORMATTERS.set(zone, f);
  }
  return f.format(ms);
}
