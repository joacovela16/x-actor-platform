import { useEffect, useState } from 'react';
import type { ScheduleView } from '../api/types';
import { fmtCountdown, fmtInZone } from '../graph/time';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Alert } from '@/components/ui/alert';

interface Props {
  schedule: ScheduleView | null;
  error: string | null;
  /** Hay cambios sin guardar: el horario es el de la última revisión guardada. */
  dirty: boolean;
  toggling: boolean;
  onToggle: () => void;
}

function useNow(): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  return now;
}

/** Programación del flujo: sus lanzadores cron (los temporizadores no aparecen aquí: esperan dentro de una instancia). */
export function SchedulePanel({ schedule, error, dirty, toggling, onToggle }: Props) {
  const now = useNow();
  if (error) return <p className="my-0.5 text-xs text-error">No se pudo leer la programación: {error}</p>;
  if (!schedule) return <p className="text-xs text-base-content/60">Cargando programación…</p>;
  const launchers = schedule.schedules;
  return (
    <div className="schedule">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3>Programación</h3>
          <Badge variant={schedule.enabled ? 'success' : 'outline'}>{schedule.enabled ? 'habilitado' : 'deshabilitado'}</Badge>
        </div>
        <Button variant="outline" type="button" onClick={onToggle} disabled={toggling}>
          {schedule.enabled ? 'Deshabilitar' : 'Habilitar'}
        </Button>
      </div>

      {launchers.length > 0 && !schedule.enabled && (
        <Alert>
          Este flujo tiene un lanzador cron y no se dispara hasta que lo habilites.
        </Alert>
      )}
      {dirty && <p className="text-xs text-base-content/60">Los horarios son los de la última revisión guardada; los cambios sin guardar aún no cuentan.</p>}

      {launchers.length === 0 ? (
        <p className="text-xs text-base-content/60">
          Este flujo no tiene lanzadores cron (un nodo «core.cron» sin aristas de entrada). Un temporizador, con entrada, espera
          dentro de cada ejecución y no depende de habilitar el flujo.
        </p>
      ) : (
        <ul className="sched-list">
          {launchers.map((s) => (
            <li key={s.node}>
              <div><span className="font-mono">{s.node}</span> <span className="font-mono text-xs text-base-content/60">{s.expression}</span></div>
              <div className="text-xs text-base-content/60">{s.timezone} · misfire {s.misfire}</div>
              <div>
                <span className="text-xs text-base-content/60">próxima ejecución: </span>
                {s.nextFireAt === null ? (
                  <span className="text-xs text-base-content/60">sin próxima ejecución</span>
                ) : (
                  <>
                    <span className="font-mono">{fmtInZone(s.nextFireAt, s.timezone)}</span>
                    <div className="text-xs text-base-content/60">{fmtCountdown(s.nextFireAt, now)}{!schedule.enabled ? ' · no se disparará hasta habilitar' : ''}</div>
                  </>
                )}
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
