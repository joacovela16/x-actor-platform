import { useEffect, useState } from 'react';
import { api, errorMessage } from '../api/client';
import type { FlowDef } from '../api/types';

const MAX_SHOWN = 30;

interface Props {
  flowId: string;
  /** Última revisión (la que devuelve `GET /api/flows/{id}`). */
  latest: number;
  /** Revisión que se está mirando, si es una antigua. */
  current?: number;
}

/**
 * Revisiones del flujo. La API no tiene una lista (`GET /api/flows/{id}/revisions` no existe): se pide cada
 * `?revision=n` desde la última hacia atrás. Una revisión que no existe (404) se muestra como no disponible.
 */
export function RevisionsPanel({ flowId, latest, current }: Props) {
  const [rows, setRows] = useState<Array<{ rev: number; flow: FlowDef | null; error?: string }> | null>(null);

  useEffect(() => {
    let alive = true;
    const first = Math.max(1, latest - MAX_SHOWN + 1);
    const nums = Array.from({ length: latest - first + 1 }, (_, i) => latest - i);
    setRows(null);
    Promise.allSettled(nums.map((n) => api.getFlow(flowId, n))).then((res) => {
      if (!alive) return;
      setRows(
        res.map((r, i) =>
          r.status === 'fulfilled'
            ? { rev: nums[i], flow: r.value }
            : { rev: nums[i], flow: null, error: errorMessage(r.reason) },
        ),
      );
    });
    return () => {
      alive = false;
    };
  }, [flowId, latest]);

  if (!rows) return <p className="text-xs text-base-content/60">Cargando revisiones…</p>;
  return (
    <div className="revisions">
      <h3>Revisiones</h3>
      {latest > MAX_SHOWN && <p className="text-xs text-base-content/60">Se muestran las últimas {MAX_SHOWN}.</p>}
      <ul className="flex flex-col gap-1.5">
        {rows.map((r) => (
          <li key={r.rev} className={`list-flex flex-wrap items-center gap-2 ${r.rev === current ? 'is-current' : ''}`}>
            {r.flow ? (
              <a href={`#/flow/${encodeURIComponent(flowId)}?rev=${r.rev}`} className={r.rev === current ? 'font-mono font-semibold' : 'font-mono'}>
                revisión {r.rev}
              </a>
            ) : (
              <span className="font-mono text-xs text-base-content/60">revisión {r.rev}: no disponible</span>
            )}
            <span className="text-xs text-base-content/60">
              {r.flow ? `${r.flow.name} · ${r.flow.nodes.length} nodos` : r.error}
              {r.rev === latest ? ' · última' : ''}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
