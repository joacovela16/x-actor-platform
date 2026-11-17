import { useCallback, useContext, useEffect, useState } from 'react';
import { api, errorMessage } from '../api/client';
import type { InstanceDeadLetter } from '../api/types';
import { AppContext } from '../App';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';

/**
 * Los elementos que los streams de una ejecución descartaron por error. Los que vinieron de una cola se pueden volver a publicar
 * en ella (su mensaje original), para que el flujo los vuelva a tomar una vez corregido lo que fallaba.
 */
export function DeadLettersPanel({ instanceId }: { instanceId: string }) {
  const { toast } = useContext(AppContext);
  const [letters, setLetters] = useState<InstanceDeadLetter[] | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    api.deadLetters(instanceId).then(setLetters).catch(() => setLetters([]));
  }, [instanceId]);

  useEffect(load, [load]);

  if (!letters || letters.length === 0) return null;
  const reinjectable = letters.filter((l) => l.reinjectable);

  const reinject = async (indexes?: number[]) => {
    const n = indexes?.length ?? reinjectable.length;
    if (!window.confirm(`¿Volver a publicar ${n} mensaje(s) en su cola de origen? El flujo los procesará otra vez.`)) return;
    setBusy(true);
    try {
      const r = await api.reinject(instanceId, indexes);
      const skipped = r.skipped.length ? ` ${r.skipped.length} no: ${r.skipped.map((s) => `#${s.index} ${s.reason}`).join('; ')}` : '';
      toast(`${r.reinjected} mensaje(s) publicados de nuevo.${skipped}`, r.skipped.length ? 'error' : 'ok');
    } catch (e) {
      toast(`No se pudieron publicar: ${errorMessage(e)}`, 'error');
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card className="flex flex-col gap-2 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h2 className="font-semibold">Elementos descartados por error ({letters.length})</h2>
        {reinjectable.length > 0 && (
          <Button type="button" variant="outline" disabled={busy} onClick={() => reinject()}>
            Reinyectar los {reinjectable.length} de cola
          </Button>
        )}
      </div>
      <p className="text-xs text-base-content/60">
        Solo los que salieron de una fuente de cola se pueden reinyectar: se publica el mensaje original, con la cabecera
        <span className="font-mono"> x-reinjected-from</span>.
      </p>
      <ul className="dead-letters">
        {letters.map((l) => (
          <li key={l.index} className="dead-letter">
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-mono">#{l.index}</span>
              <span className="font-mono">{l.node}</span>
              {l.source && <Badge variant="outline">fuente {l.source}</Badge>}
              <span className="flex-1 text-xs text-error">{l.error}</span>
              {l.reinjectable && (
                <Button type="button" variant="ghost" size="xs" disabled={busy} onClick={() => reinject([l.index])}>
                  Reinyectar
                </Button>
              )}
            </div>
            <pre className="max-h-40 overflow-auto rounded-md bg-base-200 p-2 text-xs">{JSON.stringify(l.element, null, 2)}</pre>
            {l.origin !== undefined && JSON.stringify(l.origin) !== JSON.stringify(l.element) && (
              <details>
                <summary className="text-xs text-base-content/60">original, como lo dio la fuente</summary>
                <pre className="max-h-40 overflow-auto rounded-md bg-base-200 p-2 text-xs">{JSON.stringify(l.origin, null, 2)}</pre>
              </details>
            )}
          </li>
        ))}
      </ul>
    </Card>
  );
}
