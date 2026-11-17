import { useDeferredValue, useMemo } from 'react';
import { nextFires, parseCron, validZone } from '../graph/cron';
import { timeZones } from '../graph/time';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';

const ZONE_FMT = new Map<string, Intl.DateTimeFormat>();

function fmtInZone(ms: number, zone: string): string {
  let f = ZONE_FMT.get(zone);
  if (!f) {
    f = new Intl.DateTimeFormat('es-ES', { timeZone: zone, dateStyle: 'medium', timeStyle: 'medium', hourCycle: 'h23' });
    ZONE_FMT.set(zone, f);
  }
  return f.format(ms);
}

const EXAMPLES: Array<[string, string]> = [
  ['*/15 * * * *', 'cada 15 minutos'],
  ['0 9 * * 1-5', 'laborables a las 09:00'],
  ['30 2 * * *', 'cada día a las 02:30'],
  ['0 0 1 * *', 'día 1 de cada mes a medianoche'],
  ['*/20 * * * * *', 'cada 20 segundos (6 campos: segundos primero)'],
  ['@daily', 'atajo: cada día a medianoche'],
];

interface CronProps {
  id?: string;
  value: string;
  /** Zona en la que se calcula la vista previa. */
  timezone: string;
  onChange: (text: string) => void;
  placeholder?: string;
}

/** Expresión cron con ayuda, validación y vista previa de las próximas 5 ejecuciones (calculadas en el cliente). */
export function CronField({ id, value, timezone, onChange, placeholder }: CronProps) {
  const deferred = useDeferredValue(value);
  const zone = timezone.trim() || 'UTC';
  const zoneOk = validZone(zone);
  const parsed = useMemo(() => parseCron(deferred), [deferred]);
  const fires = useMemo(
    () => (parsed.ok && zoneOk ? nextFires(parsed.spec, zone, Date.now(), 5) : []),
    [parsed, zone, zoneOk],
  );
  const error = !value.trim() ? 'falta la expresión' : !parsed.ok ? parsed.error : !zoneOk ? `zona horaria no válida: '${zone}'` : null;
  return (
    <div className="cron-field">
      <Input
        id={id}
        className={`font-mono ${error ? 'border-error' : ''}`}
        aria-label="expresión cron"
        value={value}
        placeholder={placeholder ?? '*/15 * * * *'}
        onChange={(e) => onChange(e.target.value)}
      />
      {error && <span className="my-0.5 text-xs text-error">{error}</span>}
      <details className="cron-help">
        <summary className="text-xs text-base-content/60">Ayuda: campos y ejemplos</summary>
        <p className="text-xs text-base-content/60 font-mono">minuto hora día-mes mes día-semana</p>
        <p className="text-xs text-base-content/60">Con seis campos, el primero son los segundos. Listas (1,15), rangos (9-17), pasos (*/10, 5/10), nombres (MON-FRI, JAN). @hourly, @daily, @weekly, @monthly, @yearly. Si día del mes y día de la semana están restringidos, basta con que uno coincida.</p>
        <ul className="cron-examples">
          {EXAMPLES.map(([expr, text]) => (
            <li key={expr}>
              <Button type="button" variant="link" size="xs" className="h-auto p-0 font-mono" onClick={() => onChange(expr)}>{expr}</Button>
              <span className="text-xs text-base-content/60"> {text}</span>
            </li>
          ))}
        </ul>
      </details>
      {!error && (
        <div className="cron-preview">
          <span className="text-xs text-base-content/60">Próximas ejecuciones ({zone}):</span>
          {fires.length === 0 ? (
            <span className="text-xs text-base-content/60"> ninguna en los próximos años</span>
          ) : (
            <ol className="font-mono">
              {fires.map((ms) => <li key={ms}>{fmtInZone(ms, zone)}</li>)}
            </ol>
          )}
          <span className="text-xs text-base-content/60">Vista previa aproximada en los cambios de hora de la zona.</span>
        </div>
      )}
    </div>
  );
}

interface ZoneProps {
  id?: string;
  value: string;
  onChange: (zone: string) => void;
  listId: string;
}

/** Zona horaria IANA: texto con autocompletado de `Intl.supportedValuesOf('timeZone')`. */
export function ZoneField({ id, value, onChange, listId }: ZoneProps) {
  const ok = value.trim() === '' || validZone(value.trim());
  return (
    <>
      <Input
        id={id}
        className={`font-mono ${ok ? '' : 'border-error'}`}
        aria-label="zona horaria"
        list={listId}
        value={value}
        placeholder="UTC"
        onChange={(e) => onChange(e.target.value)}
      />
      {!ok && <span className="my-0.5 text-xs text-error">zona no válida: usa un id IANA, como Europe/Madrid</span>}
      <datalist id={listId}>
        {timeZones().map((z) => <option key={z} value={z} />)}
      </datalist>
    </>
  );
}
