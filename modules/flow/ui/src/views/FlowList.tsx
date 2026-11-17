import { useContext, useEffect, useState } from 'react';
import { api, errorMessage } from '../api/client';
import type { FlowSummary } from '../api/types';
import { emptyFlow } from '../graph/graph';
import { AppContext } from '../App';
import { Alert } from '@/components/ui/alert';
import { Plus, Trash2 } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Modal } from '@/components/ui/modal';
import { Switch } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { FormField } from '@/components/ui/form-field';
import { Input } from '@/components/ui/input';

const ID_RE = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/;

export function FlowList() {
  const { toast, navigate } = useContext(AppContext);
  const [flows, setFlows] = useState<FlowSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [id, setId] = useState('');
  const [name, setName] = useState('');
  const [busy, setBusy] = useState(false);
  const [creating, setCreating] = useState(false);
  const [toggling, setToggling] = useState<string | null>(null);

  const load = () =>
    api
      .listFlows()
      .then((list) => {
        setFlows(list);
        setError(null);
      })
      .catch((e: unknown) => setError(`No se pudieron listar los flujos: ${errorMessage(e)}`));

  /** Habilita o deshabilita los lanzadores cron del flujo; la fila se actualiza con la respuesta. */
  const toggle = async (f: FlowSummary) => {
    setToggling(f.id);
    try {
      const next = f.enabled ? await api.disableFlow(f.id) : await api.enableFlow(f.id);
      setFlows((list) => list?.map((x) => (x.id === f.id ? { ...x, enabled: next.enabled } : x)) ?? null);
    } catch (e) {
      toast(`No se pudo cambiar: ${errorMessage(e)}`, 'error');
    } finally {
      setToggling(null);
    }
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!ID_RE.test(id)) return toast('El id solo admite letras, números, guion y guion bajo.', 'error');
    setBusy(true);
    try {
      const res = await api.saveFlow(emptyFlow(id, name || id));
      if (!res.ok) return toast(res.errors.map((x) => x.message).join('; '), 'error');
      navigate(`#/flow/${encodeURIComponent(id)}`);
    } catch (err) {
      toast(`No se pudo crear: ${errorMessage(err)}`, 'error');
    } finally {
      setBusy(false);
    }
  };

  const remove = async (f: FlowSummary) => {
    if (!window.confirm(`¿Eliminar el flujo '${f.name}' (${f.id})?`)) return;
    try {
      await api.deleteFlow(f.id);
      toast('Flujo eliminado', 'ok');
      load();
    } catch (err) {
      toast(`No se pudo eliminar: ${errorMessage(err)}`, 'error');
    }
  };

  return (
    <section className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-4 flex items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold">Flujos</h1>
          <p className="text-sm text-base-content/60">Habilitar un flujo activa sus lanzadores cron.</p>
        </div>
        <Button onClick={() => setCreating(true)}><Plus /> Nuevo flujo</Button>
        <Modal
          open={creating}
          onClose={() => setCreating(false)}
          title="Nuevo flujo"
          description="El id no se puede cambiar después."
          onSubmit={create}
          actions={<Button type="submit" disabled={busy || !id}>Crear y abrir</Button>}
        >
          <div className="flex flex-col gap-4">
            <FormField label="Nombre">
              <Input autoFocus value={name} onChange={(e) => setName(e.target.value)} placeholder="Aprobación de pedido" />
            </FormField>
            <FormField label="Id">
              <Input className="font-mono" value={id} onChange={(e) => setId(e.target.value)} placeholder="order-approval" required />
            </FormField>
          </div>
        </Modal>
      </div>

      {error && (
        <Alert variant="error" className="mb-3">
          {error}
        </Alert>
      )}

      {flows === null && !error && <p className="text-sm text-base-content/60">Cargando…</p>}
      {flows?.length === 0 && (
        <div className="rounded-box border border-dashed border-base-300 p-10 text-center text-sm text-base-content/60">
          Todavía no hay flujos. Crea el primero con «Nuevo flujo».
        </div>
      )}
      {flows && flows.length > 0 && (
        <div className="overflow-hidden rounded-box border border-base-300 bg-base-100">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Nombre</TableHead>
                <TableHead className="w-24 text-right">Revisión</TableHead>
                <TableHead className="w-32">Habilitado</TableHead>
                <TableHead className="w-12" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {flows.map((f) => (
                <TableRow key={f.id} className="cursor-pointer" onClick={() => navigate(`#/flow/${encodeURIComponent(f.id)}`)}>
                  <TableCell>
                    <div className="flex items-center gap-2 font-medium">
                      {f.name}
                      {f.draft && (
                        <Badge variant="info" title={f.revision === 0 ? 'Sin publicar todavía' : 'Tiene cambios sin publicar'}>
                          borrador
                        </Badge>
                      )}
                    </div>
                    <div className="font-mono text-xs text-base-content/60">{f.id}</div>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{f.revision === 0 ? '—' : f.revision}</TableCell>
                  <TableCell onClick={(e) => e.stopPropagation()}>
                    <Switch
                      checked={f.enabled}
                      disabled={toggling === f.id || f.revision === 0}
                      onCheckedChange={() => toggle(f)}
                      aria-label={`${f.enabled ? 'Deshabilitar' : 'Habilitar'} ${f.name}`}
                    />
                  </TableCell>
                  <TableCell onClick={(e) => e.stopPropagation()}>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon-sm"
                      className="text-base-content/60 hover:text-error"
                      aria-label={`Eliminar ${f.name}`}
                      title="Eliminar"
                      onClick={() => remove(f)}
                    >
                      <Trash2 />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </section>
  );
}
