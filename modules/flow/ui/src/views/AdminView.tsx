import { type ReactNode, useContext, useEffect, useMemo, useState } from 'react';
import { api, errorMessage } from '../api/client';
import type { Connection, ConnectionClient, ConnectionKind, FlowSummary, JsonValue, Trigger } from '../api/types';
import { SpecForm } from '../components/SpecForm';
import { TriggerForm } from '../components/TriggerForm';
import { AppContext } from '../App';
import { fmtClock } from '../graph/time';
import { nextFire, parseCron } from '../graph/cron';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { FormField } from '@/components/ui/form-field';
import { SearchSelect } from '@/components/ui/search-select';
import { Input } from '@/components/ui/input';

function Page({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="mx-auto w-full max-w-4xl p-4">
      <h1 className="mb-3 text-xl font-semibold">{title}</h1>
      {children}
    </section>
  );
}

/** Los triggers: lo que arranca flujos desde afuera (HTTP, colas, cron). */
export function TriggersView() {
  return (
    <Page title="Triggers">
      <TriggersSection />
    </Page>
  );
}

/** Las conexiones. Los secretos solo se escriben: nunca se muestran. */
export function ConnectionsView() {
  return (
    <Page title="Conexiones">
      <ConnectionsSection />
    </Page>
  );
}

/** Resumen de un trigger para la lista: lo que lo dispara. */
function summary(t: Trigger, now: number): string {
  switch (t.kind) {
    case 'http': return `${t.method ?? 'POST'} /hooks/${t.path ?? ''}${t.response === 'sync' ? ' · sync' : ''}${t.auth ? ` · ${t.auth.scheme}` : ''}`;
    case 'cron': {
      const p = parseCron(t.cron ?? '');
      if (!p.ok) return `${t.cron ?? ''} (no válido)`;
      const zone = t.timezone || 'UTC';
      const next = nextFire(p.spec, zone, now);
      return `${t.cron} (${zone})${next ? ` · próxima ${fmtClock(next, zone)}` : ''}`;
    }
    case 'queue': return `${t.connection ?? '?'} → ${t.queue ?? '?'}${t.destinationType ? ` (${t.destinationType})` : ''}`;
    default: return 'manual';
  }
}

function TriggersSection() {
  const { toast } = useContext(AppContext);
  const [triggers, setTriggers] = useState<Trigger[]>([]);
  const [flows, setFlows] = useState<FlowSummary[]>([]);
  const [editing, setEditing] = useState<{ trigger: Trigger; isNew: boolean } | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const now = useMemo(() => Date.now(), [triggers]);

  const load = () => {
    api.listTriggers().then((t) => { setTriggers(t); setLoadError(null); }).catch((e: unknown) => setLoadError(errorMessage(e)));
    api.listFlows().then(setFlows).catch(() => setFlows([]));
  };
  useEffect(load, []); // eslint-disable-line react-hooks/exhaustive-deps

  const blank = (): Trigger => ({ id: '', flowId: flows[0]?.id ?? '', kind: 'http', method: 'POST', path: '', inputs: {} });

  const remove = async (id: string) => {
    if (!window.confirm(`¿Eliminar el trigger '${id}'?`)) return;
    try {
      await api.deleteTrigger(id);
      if (editing?.trigger.id === id) setEditing(null);
      load();
    } catch (e) {
      toast(`No se pudo eliminar: ${errorMessage(e)}`, 'error');
    }
  };

  return (
    <Card title="Triggers" actions={<Button type="button" size="sm" onClick={() => setEditing({ trigger: blank(), isNew: true })}>+ nuevo</Button>}>
      {loadError && <p className="my-0.5 text-xs text-error">{loadError}</p>}
      {triggers.length === 0 && !loadError && <p className="text-xs text-base-content/60">Sin triggers.</p>}
      <ul className="flex flex-col divide-y divide-base-300">
        {triggers.map((t) => (
          <li key={t.id} className="flex items-center justify-between gap-2 py-1.5">
            <Button type="button" variant="link" className="block h-auto min-w-0 flex-1 p-0 text-left font-normal whitespace-normal text-base-content" onClick={() => setEditing({ trigger: structuredClone(t), isNew: false })}>
              <span className="font-mono">{t.id}</span> → {t.flowId}
              <span className="text-xs font-normal text-base-content/60"> · {t.kind} · {summary(t, now)}</span>
            </Button>
            <Button variant="ghost" size="xs" className="text-error hover:text-error" type="button" onClick={() => remove(t.id)}>borrar</Button>
          </li>
        ))}
      </ul>
      <p className="text-xs text-base-content/60">La API no expone el estado de los triggers (solo su definición).</p>

      {editing && (
        <TriggerForm
          key={editing.isNew ? 'new' : editing.trigger.id}
          initial={editing.trigger}
          isNew={editing.isNew}
          flows={flows}
          onCancel={() => setEditing(null)}
          onSaved={() => {
            toast('Trigger guardado', 'ok');
            setEditing(null);
            load();
          }}
        />
      )}
    </Card>
  );
}

function ago(ms: number): string {
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s} s`;
  const m = Math.round(s / 60);
  return m < 60 ? `${m} min` : `${Math.round(m / 60)} h`;
}

/** Los clientes abiertos de una conexión: cuántos, cuántos en uso y desde cuándo está ocioso cada uno (en el tooltip). */
function ClientsBadge({ clients }: { clients: ConnectionClient[] }) {
  if (clients.length === 0) return <span className="text-xs text-base-content/60">sin clientes</span>;
  const inUse = clients.reduce((n, c) => n + c.inUse, 0);
  const detail = clients
    .map((c) => {
      const what = [c.kind, c.extra].filter(Boolean).join(' ');
      const state = c.opening ? 'abriendo' : c.inUse > 0 ? `${c.inUse} en uso` : `ocioso ${ago(c.idleMillis)}`;
      return `${what}: ${state}${c.retired ? ' (se cierra al devolverse)' : ''}`;
    })
    .join('\n');
  return (
    <Badge variant={inUse > 0 ? 'success' : 'outline'} title={detail}>
      {clients.length} {clients.length === 1 ? 'cliente' : 'clientes'}
      {inUse > 0 ? ` · ${inUse} en uso` : ''}
    </Badge>
  );
}

function ConnectionsSection() {
  const { toast } = useContext(AppContext);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [kinds, setKinds] = useState<ConnectionKind[]>([]);
  const [editing, setEditing] = useState<{ name: string; kind: string; isNew: boolean; saved?: Connection } | null>(null);
  // Un solo cajón con params y secretos; se separan al guardar.
  const [draft, setDraft] = useState<Record<string, JsonValue>>({});

  const [clients, setClients] = useState<ConnectionClient[]>([]);
  const loadClients = () => api.connectionClients().then(setClients).catch(() => setClients([]));
  const load = () => {
    loadClients();
    return api.listConnections().then(setConnections).catch((e: unknown) => toast(errorMessage(e), 'error'));
  };
  useEffect(() => {
    // Los clientes cambian solos (se abren al usarse y se cierran por inactividad): se refrescan cada pocos segundos.
    const t = setInterval(loadClients, 5000);
    return () => clearInterval(t);
  }, []);
  useEffect(() => {
    load();
    api.connectionKinds().then(setKinds).catch((e: unknown) => toast(`Tipos de conexión: ${errorMessage(e)}`, 'error'));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const kindDef = useMemo(() => kinds.find((k) => k.kind === editing?.kind), [kinds, editing?.kind]);

  const open = (c?: Connection) => {
    if (c) {
      setEditing({ name: c.name, kind: c.kind, isNew: false, saved: c });
      setDraft({ ...c.params });
    } else {
      setEditing({ name: '', kind: kinds[0]?.kind ?? '', isNew: true });
      setDraft({});
    }
  };

  const save = async () => {
    if (!editing || !kindDef) return;
    const params: Record<string, JsonValue> = {};
    const secrets: Record<string, string> = {};
    for (const f of kindDef.config.fields) {
      const v = draft[f.name];
      if (v === undefined) continue;
      if (f.secret) {
        if (typeof v === 'string' && v !== '') secrets[f.name] = v; // vacío = no cambiar
      } else params[f.name] = v;
    }
    try {
      await api.saveConnection({ name: editing.name, kind: editing.kind, params, secrets });
      toast('Conexión guardada', 'ok');
      setEditing(null);
      load();
    } catch (e) {
      toast(`No se pudo guardar: ${errorMessage(e)}`, 'error');
    }
  };

  const closeClients = async (name: string) => {
    await api.closeConnectionClients(name).then(() => toast(`Clientes de '${name}' cerrados`, 'ok')).catch((e: unknown) => toast(errorMessage(e), 'error'));
    loadClients();
  };

  const remove = async (name: string) => {
    if (!window.confirm(`¿Eliminar la conexión '${name}'?`)) return;
    await api.deleteConnection(name).catch((e: unknown) => toast(errorMessage(e), 'error'));
    load();
  };

  return (
    <Card title="Conexiones" actions={<Button type="button" size="sm" onClick={() => open()} disabled={kinds.length === 0}>+ nueva</Button>}>
      {connections.length === 0 && <p className="text-xs text-base-content/60">Sin conexiones.</p>}
      <ul className="flex flex-col divide-y divide-base-300">
        {connections.map((c) => (
          <li key={c.name} className="flex items-center justify-between gap-2 py-1.5">
            <Button type="button" variant="link" className="block h-auto min-w-0 flex-1 p-0 text-left font-normal whitespace-normal text-base-content" onClick={() => open(c)}>
              <span className="font-mono">{c.name}</span> <span className="text-xs text-base-content/60">({c.kind})</span>
              <span className="text-xs font-normal text-base-content/60"> · secretos: {c.secretsSet.length ? c.secretsSet.join(', ') : 'ninguno'}</span>
              {c.envMissing?.length ? (
                <Badge variant="warning" title={`Variables de entorno sin definir en el servidor: ${c.envMissing.join(', ')}`}>
                  falta {c.envMissing.join(', ')}
                </Badge>
              ) : null}
            </Button>
            <ClientsBadge clients={clients.filter((x) => x.connection === c.name)} />
            {clients.some((x) => x.connection === c.name) && (
              <Button variant="ghost" size="xs" type="button" title="Cierra los clientes abiertos; el próximo uso abre uno nuevo" onClick={() => closeClients(c.name)}>
                cerrar clientes
              </Button>
            )}
            <Button variant="ghost" size="xs" className="text-error hover:text-error" type="button" onClick={() => remove(c.name)}>borrar</Button>
          </li>
        ))}
      </ul>

      {editing && kindDef && (
        <div className="flex flex-col gap-3 rounded-box border border-base-300 bg-base-200/40 p-3">
          <h3 className="text-sm font-semibold">{editing.isNew ? 'Nueva conexión' : `Conexión ${editing.name}`}</h3>
          <FormField label="Nombre (lo usan los nodos en sus params)">
            <Input className="font-mono" value={editing.name} disabled={!editing.isNew} onChange={(e) => setEditing({ ...editing, name: e.target.value })} />
          </FormField>
          <FormField label="Tipo">
            <SearchSelect
              value={editing.kind}
              disabled={!editing.isNew}
              onChange={(kind) => {
                setEditing({ ...editing, kind });
                setDraft({});
              }}
              options={kinds.map((k) => ({ value: k.kind, label: k.label, hint: k.kind }))}
            />
          </FormField>
          <p className="text-xs text-base-content/60">
            Cualquier campo, también los secretos, puede leer una variable de entorno del servidor: <code className="font-mono">{'${env.DB_HOST}'}</code> o, con
            valor por defecto, <code className="font-mono">{'${env.DB_PORT:-5432}'}</code>. Se resuelven al usar la conexión, así el secreto no se guarda aquí.
          </p>
          {editing.saved?.env?.length ? (
            <p className={`text-xs ${editing.saved.envMissing?.length ? 'text-warning' : 'text-base-content/60'}`}>
              Usa {editing.saved.env.join(', ')}
              {editing.saved.envMissing?.length ? ` — sin definir en el servidor: ${editing.saved.envMissing.join(', ')}` : ' — todas definidas'}.
            </p>
          ) : null}
          {editing.saved?.secretsSet.length ? (
            <p className="text-xs text-base-content/60">Secretos guardados (no se muestran): {editing.saved.secretsSet.join(', ')}. Dejar un campo secreto vacío lo conserva.</p>
          ) : null}
          <SpecForm
            fields={kindDef.config.fields}
            values={draft}
            onChange={setDraft}
            paths={[]}
            secretsSet={editing.saved?.secretsSet ?? []}
            emptyText="Este tipo de conexión no tiene campos."
          />
          <div className="flex flex-wrap items-center gap-2">
            <Button type="button" onClick={save} disabled={!editing.name}>Guardar</Button>
            <Button variant="ghost" type="button" onClick={() => setEditing(null)}>Cancelar</Button>
          </div>
        </div>
      )}
    </Card>
  );
}
