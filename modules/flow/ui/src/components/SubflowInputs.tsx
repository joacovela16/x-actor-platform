import { useEffect, useState } from 'react';
import { api } from '../api/client';
import type { FlowDef } from '../api/types';
import { renderType } from '../graph/graph';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { FormField } from '@/components/ui/form-field';

interface Props {
  flowId: string | undefined;
  value: Record<string, string>;
  onChange: (v: Record<string, string>) => void;
  paths: string[];
}

/**
 * Entradas de un `core.subflow`: para cada input del flujo llamado, la ruta del estado del llamador que lo rellena.
 * Lee el flujo llamado con GET /api/flows/{id} para conocer sus inputs.
 */
export function SubflowInputs({ flowId, value, onChange, paths }: Props) {
  const [called, setCalled] = useState<FlowDef | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Lo anterior se queda en pantalla mientras se escribe el id; se busca cuando se deja de teclear un momento, así el panel no
  // alterna entre «Cargando…» y el resultado en cada tecla.
  useEffect(() => {
    if (!flowId) {
      setCalled(null);
      setError(null);
      return;
    }
    let alive = true;
    const t = setTimeout(() => {
      api
        .getFlow(flowId)
        .then((f) => {
          if (!alive) return;
          setCalled(f);
          setError(null);
        })
        .catch(() => {
          if (!alive) return;
          setCalled(null);
          setError(`No se encuentra el flujo '${flowId}'.`);
        });
    }, 300);
    return () => {
      alive = false;
      clearTimeout(t);
    };
  }, [flowId]);

  if (!flowId) return <p className="text-xs text-base-content/60">Elige primero el flujo a ejecutar.</p>;
  if (error) return <p className="my-0.5 text-xs text-error">{error}</p>;
  if (!called) return <p className="text-xs text-base-content/60">Cargando entradas de '{flowId}'…</p>;

  const declared = new Set(called.inputs.map((d) => d.name));
  const extra = Object.keys(value).filter((k) => !declared.has(k));
  const set = (name: string, path: string) => {
    const next = { ...value };
    if (path.trim() === '') delete next[name];
    else next[name] = path;
    onChange(next);
  };

  return (
    <div className="subflow-inputs">
      {called.inputs.length === 0 && <p className="text-xs text-base-content/60">El flujo llamado no tiene entradas.</p>}
      {called.inputs.map((d) => (
        <FormField
          key={d.name}
          label={<>
            {d.name} <span>({renderType(d.type)})</span>
            {value[d.name] === undefined && <Badge variant="warning">sin rellenar</Badge>}
          </>}
        >
          <Input
            className="font-mono"
            list="state-paths"
            value={value[d.name] ?? ''}
            placeholder="ruta en el estado del llamador"
            onChange={(e) => set(d.name, e.target.value)}
          />
        </FormField>
      ))}
      {extra.map((k) => (
        <div className="my-0.5 text-xs text-error flex flex-wrap items-center justify-between gap-2" key={k}>
          <span>'{k}' no existe en el flujo llamado</span>
          <Button variant="ghost" className="text-error hover:text-error" type="button" onClick={() => set(k, '')}>quitar</Button>
        </div>
      ))}
      <datalist id="state-paths">
        {paths.map((p) => <option key={p} value={p} />)}
      </datalist>
    </div>
  );
}
