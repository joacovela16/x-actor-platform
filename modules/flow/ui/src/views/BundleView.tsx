import { useContext, useEffect, useMemo, useState } from 'react';
import { DownloadIcon, UploadIcon } from 'lucide-react';
import { api, errorMessage } from '../api/client';
import type { BundleItem, BundlePlan, FlowSummary } from '../api/types';
import { AppContext } from '../App';
import { Alert } from '@/components/ui/alert';
import { Badge, type BadgeVariant } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardBody } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

const ACTION: Record<BundleItem['action'], { label: string; variant: BadgeVariant }> = {
  create: { label: 'crea', variant: 'success' },
  update: { label: 'cambia', variant: 'info' },
  draft: { label: 'borrador', variant: 'warning' },
  skip: { label: 'no se toca', variant: 'ghost' },
  same: { label: 'igual', variant: 'ghost' },
};

const TYPE: Record<BundleItem['type'], string> = { flow: 'Flujo', trigger: 'Trigger', entity: 'Entidad', connection: 'Conexión' };

/** Mover flujos entre servidores (dev → staging → prod): exportarlos a un bundle, e importar uno viendo antes qué haría. Lo mismo
 *  que `flow-server.jar bundle ...`, para hacerlo sin terminal.
 */
export function BundleView() {
  return (
    <section className="mx-auto flex w-full max-w-4xl flex-col gap-4 p-4">
      <div>
        <h1 className="mb-1 text-xl font-semibold">Mover flujos</h1>
        <p className="text-sm text-base-content/60">
          Un bundle lleva flujos, sus triggers, las conexiones que usan y las entidades editadas aquí, sin secretos ni revisiones. Para
          guardarlo en git como un archivo por elemento, usa <span className="font-mono">flow-server.jar bundle export --dir</span>.
        </p>
      </div>
      <ExportCard />
      <ImportCard />
    </section>
  );
}

function ExportCard() {
  const { toast } = useContext(AppContext);
  const [flows, setFlows] = useState<FlowSummary[] | null>(null);
  const [chosen, setChosen] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.listFlows().then((fs) => setFlows(fs.filter((f) => f.revision > 0)), (e) => toast(errorMessage(e), 'error'));
  }, [toast]);

  const toggle = (id: string) =>
    setChosen((s) => {
      const next = new Set(s);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  const download = async () => {
    setBusy(true);
    try {
      const text = await api.exportBundle([...chosen]);
      const url = URL.createObjectURL(new Blob([text], { type: 'application/json' }));
      const a = document.createElement('a');
      a.href = url;
      a.download = 'bundle.json';
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      toast(errorMessage(e), 'error');
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card title="Exportar">
      <CardBody className="gap-3 pt-0 text-sm">
        <p className="text-base-content/60">Elige flujos publicados, o ninguno para exportarlos todos. Los borradores no se exportan.</p>
        {flows && flows.length === 0 && <p className="text-base-content/60">No hay flujos publicados.</p>}
        <div className="flex max-h-60 flex-col gap-1 overflow-y-auto">
          {flows?.map((f) => (
            <label key={f.id} className="flex cursor-pointer items-center gap-2">
              <input type="checkbox" className="checkbox checkbox-sm" checked={chosen.has(f.id)} onChange={() => toggle(f.id)} />
              <span>{f.name}</span>
              <span className="font-mono text-xs text-base-content/50">{f.id}</span>
              {f.enabled && <Badge variant="success">activo</Badge>}
            </label>
          ))}
        </div>
        <div>
          <Button type="button" onClick={download} disabled={busy || !flows || flows.length === 0}>
            <DownloadIcon />
            {chosen.size === 0 ? 'Descargar todos' : `Descargar ${chosen.size}`}
          </Button>
        </div>
      </CardBody>
    </Card>
  );
}

function ImportCard() {
  const { toast } = useContext(AppContext);
  const [name, setName] = useState<string | null>(null);
  const [bundle, setBundle] = useState<unknown>(null);
  const [plan, setPlan] = useState<BundlePlan | null>(null);
  const [done, setDone] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const choose = async (file: File | undefined) => {
    setPlan(null);
    setDone(false);
    setError(null);
    setBundle(null);
    if (!file) return;
    setName(file.name);
    let parsed: unknown;
    try {
      parsed = JSON.parse(await file.text());
    } catch {
      setError(`${file.name} no es JSON`);
      return;
    }
    setBusy(true);
    try {
      setPlan(await api.planBundle(parsed));
      setBundle(parsed);
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  const apply = async () => {
    setBusy(true);
    try {
      const r = await api.importBundle(bundle);
      setPlan(r.plan);
      setDone(r.ok);
      toast(r.ok ? 'Bundle importado' : 'El bundle no se importó', r.ok ? 'ok' : 'error');
    } catch (e) {
      toast(errorMessage(e), 'error');
    } finally {
      setBusy(false);
    }
  };

  const changes = useMemo(() => plan?.items.filter((i) => i.action !== 'same') ?? [], [plan]);
  const same = (plan?.items.length ?? 0) - changes.length;

  return (
    <Card title="Importar">
      <CardBody className="gap-3 pt-0 text-sm">
        <p className="text-base-content/60">
          Primero se muestra qué haría, sin cambiar nada. Las conexiones que ya existen no se tocan; nada se borra.
        </p>
        <label className="flex items-center gap-2">
          <span className="btn btn-sm btn-outline border-base-300">
            <UploadIcon className="size-4" />
            Elegir bundle.json
          </span>
          <input type="file" accept="application/json,.json" className="hidden" onChange={(e) => choose(e.target.files?.[0])} />
          {name && <span className="font-mono text-xs text-base-content/60">{name}</span>}
        </label>
        {error && <Alert variant="error">{error}</Alert>}
        {plan && (
          <>
            {plan.problems.length > 0 && (
              <Alert variant="error">
                <div>
                  <div className="font-medium">No se puede importar:</div>
                  <ul className="list-disc pl-4">
                    {plan.problems.map((p) => (
                      <li key={p}>{p}</li>
                    ))}
                  </ul>
                </div>
              </Alert>
            )}
            {plan.warnings.length > 0 && (
              <Alert variant="warning">
                <ul className="list-disc pl-4">
                  {plan.warnings.map((w) => (
                    <li key={w}>{w}</li>
                  ))}
                </ul>
              </Alert>
            )}
            {changes.length === 0 ? (
              <p className="text-base-content/60">Nada cambia: todo es igual a lo que hay aquí.</p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Qué</TableHead>
                    <TableHead>Nombre</TableHead>
                    <TableHead>{done ? 'Hecho' : 'Haría'}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {changes.map((i) => (
                    <TableRow key={`${i.type}:${i.id}`}>
                      <TableCell>{TYPE[i.type]}</TableCell>
                      <TableCell className="font-mono">{i.id}</TableCell>
                      <TableCell>
                        <Badge variant={ACTION[i.action].variant}>{ACTION[i.action].label}</Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
            {same > 0 && <p className="text-xs text-base-content/60">{same} sin cambios.</p>}
            {done ? (
              <Alert variant="success">Importado.</Alert>
            ) : (
              <div>
                <Button type="button" onClick={apply} disabled={busy || plan.problems.length > 0 || changes.length === 0}>
                  Importar
                </Button>
              </div>
            )}
          </>
        )}
      </CardBody>
    </Card>
  );
}
