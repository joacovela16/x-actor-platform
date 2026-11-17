import { useContext, useEffect, useState } from 'react';
import { api } from '../api/client';
import type { FlowSummary, InstanceSummary } from '../api/types';
import { AppContext } from '../App';
import { fmtDateTime, fmtDuration } from '../graph/audit';
import { StatusBadge } from '@/components/StatusBadge';
import { Alert } from '@/components/ui/alert';
import { Button } from '@/components/ui/button';
import { Card, CardBody } from '@/components/ui/card';
import { FormField } from '@/components/ui/form-field';
import { Input } from '@/components/ui/input';
import { SearchSelect } from '@/components/ui/search-select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

const PAGE = 20;

interface Filters {
  flowId: string;
  status: string;
  origin: string;
  from: string; // datetime-local
  to: string;
}

const EMPTY: Filters = { flowId: '', status: '', origin: '', from: '', to: '' };

const toIso = (v: string) => (v ? new Date(v).toISOString() : undefined);

/** Historial de instancias: filtros (flujo, estado, origen, rango de fechas) y paginación por cursor. */
export function InstanceHistory() {
  const { toast } = useContext(AppContext);
  const [flows, setFlows] = useState<FlowSummary[]>([]);
  const [draft, setDraft] = useState<Filters>(EMPTY);
  const [applied, setApplied] = useState<Filters>(EMPTY);
  // cursors[i] es el parámetro `after` de la página i (undefined en la primera).
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined]);
  const [page, setPage] = useState(0);
  const [rows, setRows] = useState<InstanceSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.listFlows().then(setFlows).catch(() => setFlows([]));
  }, []);

  useEffect(() => {
    let alive = true;
    setRows(null);
    setError(null);
    api
      .listInstances({
        flowId: applied.flowId || undefined,
        status: applied.status || undefined,
        origin: applied.origin || undefined,
        from: toIso(applied.from),
        to: toIso(applied.to),
        limit: PAGE,
        after: cursors[page],
      })
      .then((r) => alive && setRows(r))
      .catch((e: Error) => {
        if (alive) setError(`No se pudo cargar el historial: ${e.message}`);
      });
    return () => {
      alive = false;
    };
  }, [applied, page, cursors]);

  const apply = (e: React.FormEvent) => {
    e.preventDefault();
    if (draft.from && draft.to && draft.from > draft.to) return toast('El rango de fechas está al revés.', 'error');
    setApplied(draft);
    setCursors([undefined]);
    setPage(0);
  };

  const next = () => {
    if (!rows || rows.length === 0) return;
    setCursors((c) => [...c.slice(0, page + 1), rows[rows.length - 1].id]);
    setPage(page + 1);
  };

  const field = (k: keyof Filters) => ({
    value: draft[k],
    onChange: (e: React.ChangeEvent<HTMLInputElement>) => setDraft({ ...draft, [k]: e.target.value }),
  });
  const choice = (k: keyof Filters) => ({ value: draft[k], onChange: (v: string) => setDraft({ ...draft, [k]: v }) });

  return (
    <section className="mx-auto w-full max-w-6xl space-y-3 p-4">
      <h1 className="text-xl font-semibold">Ejecuciones</h1>

      <Card>
        <CardBody>
          <form className="flex flex-col gap-3" onSubmit={apply}>
            <div className="grid grid-cols-[repeat(auto-fit,minmax(180px,1fr))] gap-3">
              <FormField label="Flujo">
                <SearchSelect
                  {...choice('flowId')}
                  searchPlaceholder="Buscar flujo…"
                  options={[{ value: '', label: 'todos' }, ...flows.map((f) => ({ value: f.id, label: f.name, hint: f.id }))]}
                />
              </FormField>
              <FormField label="Estado">
                <SearchSelect
                  {...choice('status')}
                  options={[
                    { value: '', label: 'todos' },
                    { value: 'running', label: 'en curso' },
                    { value: 'succeeded', label: 'correcta' },
                    { value: 'failed', label: 'fallida' },
                  ]}
                />
              </FormField>
              <FormField label="Origen">
                <SearchSelect
                  {...choice('origin')}
                  options={[
                    { value: '', label: 'cualquiera' },
                    { value: 'trigger', label: 'disparador' },
                    { value: 'manual', label: 'manual' },
                    { value: 'cron', label: 'horario (cron)' },
                    { value: 'subflow', label: 'subflujo' },
                  ]}
                />
              </FormField>
              <FormField label="Desde">
                <Input type="datetime-local" {...field('from')} />
              </FormField>
              <FormField label="Hasta">
                <Input type="datetime-local" {...field('to')} />
              </FormField>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button type="submit">Aplicar filtros</Button>
              <Button type="button" variant="ghost" onClick={() => { setDraft(EMPTY); setApplied(EMPTY); setCursors([undefined]); setPage(0); }}>
                Limpiar
              </Button>
            </div>
          </form>
        </CardBody>
      </Card>

      {error && (
        <Alert variant="error">
          {error}
        </Alert>
      )}
      {!rows && !error && <p className="text-xs text-base-content/60">Cargando…</p>}
      {rows && rows.length === 0 && <p className="text-xs text-base-content/60">Ninguna ejecución con esos filtros.</p>}

      {rows && rows.length > 0 && (
        <Card className="overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Instancia</TableHead><TableHead>Flujo</TableHead><TableHead>Rev.</TableHead><TableHead>Estado</TableHead>
                <TableHead>Origen</TableHead><TableHead>Inicio</TableHead><TableHead>Duración</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((r) => {
                const dur = r.startedAt && r.endedAt ? new Date(r.endedAt).getTime() - new Date(r.startedAt).getTime() : null;
                return (
                  <TableRow key={r.id}>
                    <TableCell><a className="font-mono text-primary underline-offset-4 hover:underline" href={`#/instance/${encodeURIComponent(r.id)}`}>{r.id}</a></TableCell>
                    <TableCell className="font-mono">{r.flowId}</TableCell>
                    <TableCell>{r.revision ?? '—'}</TableCell>
                    <TableCell><StatusBadge status={r.status} /></TableCell>
                    <TableCell className="font-mono">{r.origin ?? '—'}</TableCell>
                    <TableCell>{fmtDateTime(r.startedAt)}</TableCell>
                    <TableCell>{fmtDuration(dur)}</TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </Card>
      )}

      <div className="flex items-center gap-2">
        <Button type="button" variant="outline" disabled={page === 0 || !rows} onClick={() => setPage(page - 1)}>Anterior</Button>
        <span className="text-xs text-base-content/60">página {page + 1}</span>
        <Button type="button" variant="outline" disabled={!rows || rows.length < PAGE} onClick={next}>Siguiente</Button>
      </div>
    </section>
  );
}
