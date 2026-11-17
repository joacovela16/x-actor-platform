import { useEffect, useState } from 'react';
import { api, errorMessage } from '../api/client';
import type { Connection, FlowSummary, JsonValue, Trigger, TriggerKind } from '../api/types';
import { CronField, ZoneField } from './ScheduleFields';
import { parseCron } from '../graph/cron';
import { Button } from '@/components/ui/button';
import { FormField } from '@/components/ui/form-field';
import { SearchSelect } from '@/components/ui/search-select';
import { Input } from '@/components/ui/input';
import { parseRelaxed } from '@/lib/relaxedJson';

const ID_RE = /^[A-Za-z0-9_.-]+$/;
// mongodb: the queue is a collection and each change to it is a message (change stream).
const QUEUE_KINDS = ['kafka', 'activemq', 'redis', 'mongodb'];
const METHODS = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'];

type ValueMode = 'ref' | 'texto' | 'json';
interface MapRow {
  name: string;
  mode: ValueMode;
  text: string;
}

/** Valor de una entrada según cómo se escribió: `$...` es una referencia; "texto" es una cadena; "json" un valor JSON. */
function rowFrom(name: string, value: JsonValue): MapRow {
  if (typeof value === 'string' && value.startsWith('$')) return { name, mode: 'ref', text: value };
  if (typeof value === 'string') return { name, mode: 'texto', text: value };
  return { name, mode: 'json', text: JSON.stringify(value) };
}

function valueOf(row: MapRow): JsonValue | { error: string } {
  if (row.mode === 'ref' || row.mode === 'texto') return row.text;
  try {
    return parseRelaxed(row.text);
  } catch {
    return { error: `'${row.name}': el valor JSON no es válido` };
  }
}

/** Mapeo `inputs` del trigger: nombre de la entrada del flujo -> referencia a la petición, texto o JSON. */
function InputsMapper({ value, onChange, hint }: { value: Record<string, JsonValue>; onChange: (v: Record<string, JsonValue>, error: string | null) => void; hint: string }) {
  const [rows, setRows] = useState<MapRow[]>(() => Object.entries(value).map(([k, v]) => rowFrom(k, v)));

  const emit = (next: MapRow[]) => {
    setRows(next);
    const out: Record<string, JsonValue> = {};
    let error: string | null = null;
    for (const r of next) {
      if (!r.name.trim()) continue;
      const v = valueOf(r);
      if (typeof v === 'object' && v !== null && !Array.isArray(v) && 'error' in v) error = (v as { error: string }).error;
      else out[r.name.trim()] = v as JsonValue;
    }
    onChange(out, error);
  };

  const set = (i: number, patch: Partial<MapRow>) => emit(rows.map((r, j) => (j === i ? { ...r, ...patch } : r)));

  return (
    <div className="mapper">
      <span className="flex items-center gap-1.5 text-xs font-normal text-base-content/60">Entradas del flujo (inputs)</span>
      <span className="text-xs text-base-content/60">{hint}</span>
      {rows.map((r, i) => (
        <div className="mapper-row" key={i}>
          <Input className="font-mono" aria-label="nombre de la entrada" placeholder="order" value={r.name} onChange={(e) => set(i, { name: e.target.value })} />
          <SearchSelect<ValueMode>
            aria-label="tipo de valor"
            value={r.mode}
            onChange={(mode) => set(i, { mode })}
            options={[
              { value: 'ref', label: 'referencia' },
              { value: 'texto', label: 'texto' },
              { value: 'json', label: 'JSON' },
            ]}
          />
          <Input className="font-mono" aria-label="valor" placeholder={r.mode === 'ref' ? '${request.body.order.id}' : r.mode === 'json' ? '42' : 'texto'} value={r.text} onChange={(e) => set(i, { text: e.target.value })} />
          <Button variant="ghost" size="xs" type="button" onClick={() => emit(rows.filter((_, j) => j !== i))}>quitar</Button>
        </div>
      ))}
      <Button variant="outline" size="xs" type="button" onClick={() => emit([...rows, { name: '', mode: 'ref', text: '' }])}>+ entrada</Button>
    </div>
  );
}

/** Problemas que el cliente ve antes de enviar (el servidor valida lo demás y lo devuelve en 422). */
function localProblems(t: Trigger): string[] {
  const out: string[] = [];
  if (!t.id) out.push('falta el id');
  else if (!ID_RE.test(t.id)) out.push('el id solo admite letras, dígitos, "_", "." y "-"');
  if (!t.flowId) out.push('falta el flujo');
  if (t.kind === 'http') {
    if (!(t.path ?? '').replace(/^\/+|\/+$/g, '')) out.push('un trigger http necesita una ruta');
    if (t.timeoutMs !== undefined && !(t.timeoutMs > 0)) out.push('timeoutMs debe ser positivo');
    if (t.auth && t.auth.scheme !== 'none' && !t.auth.secret) out.push('la autenticación necesita un secreto (conexión.campo)');
  }
  if (t.kind === 'cron') {
    const p = parseCron(t.cron ?? '');
    if (!p.ok) out.push(`cron: ${p.error}`);
  }
  if (t.kind === 'queue') {
    if (!t.connection) out.push('un trigger de cola necesita una conexión');
    if (!t.queue) out.push('un trigger de cola necesita una cola (o topic, o stream)');
  }
  return out;
}

export function TriggerForm({
  initial, isNew, flows, onSaved, onCancel,
}: {
  initial: Trigger;
  isNew: boolean;
  flows: FlowSummary[];
  onSaved: () => void;
  onCancel: () => void;
}) {
  const [draft, setDraft] = useState<Trigger>(initial);
  const [inputsError, setInputsError] = useState<string | null>(null);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [saving, setSaving] = useState(false);
  const [problems, setProblems] = useState<string[]>([]);

  useEffect(() => {
    let alive = true;
    api.listConnections().then((c) => alive && setConnections(c)).catch(() => alive && setConnections([]));
    return () => {
      alive = false;
    };
  }, []);

  const set = (patch: Partial<Trigger>) => setDraft((d) => ({ ...d, ...patch }));
  const kind = draft.kind;
  const auth = draft.auth ?? { scheme: 'none', secret: '' };
  const secretOptions = connections.flatMap((c) => c.secretsSet.map((f) => `${c.name}.${f}`));
  const queueConnections = connections.filter((c) => QUEUE_KINDS.includes(c.kind));
  const inputsHint =
    kind === 'queue'
      ? 'Referencias: ${message.body} (o ${message.body.x}), ${message.key}, ${message.headers.x}, ${message.source}, ${message.position}, ${env.NOMBRE}. Sin entradas, el cuerpo es la entrada si es un objeto.'
      : kind === 'http'
        ? 'Referencias: ${request.query.x}, ${request.body.x}, ${request.headers.x}, ${request.method}, ${request.path}, ${env.NOMBRE}. Sin entradas, el cuerpo JSON es la entrada.'
        : 'Literales: este lanzador no tiene petición de la que leer.';

  const save = async () => {
    const probs = localProblems(draft);
    setProblems(probs);
    if (probs.length || inputsError) return;
    // Solo los campos del tipo elegido, y sin vacíos: así el JSON es el que espera el servidor.
    const t: Trigger = { id: draft.id, flowId: draft.flowId, kind: draft.kind };
    if (kind === 'http') {
      t.method = draft.method || 'POST';
      t.path = draft.path;
      if (draft.response === 'sync') {
        t.response = 'sync';
        if (draft.timeoutMs) t.timeoutMs = draft.timeoutMs;
      }
      if (auth.scheme !== 'none') {
        t.auth = { scheme: auth.scheme, secret: auth.secret, ...(auth.header ? { header: auth.header } : {}) };
      }
    }
    if (kind === 'cron') {
      t.cron = draft.cron;
      if (draft.timezone) t.timezone = draft.timezone;
    }
    if (kind === 'queue') {
      t.connection = draft.connection;
      t.queue = draft.queue;
      if (draft.group) t.group = draft.group;
      if (draft.destinationType) t.destinationType = draft.destinationType;
    }
    if (draft.inputs && Object.keys(draft.inputs).length) t.inputs = draft.inputs;
    setSaving(true);
    try {
      await api.saveTrigger(t);
      onSaved();
    } catch (e) {
      setProblems(errorMessage(e).split('; '));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="trigger-form flex flex-col gap-3 rounded-lg border bg-base-200/40 p-3">
      <h3>{isNew ? 'Nuevo trigger' : `Trigger ${draft.id}`}</h3>
      <div className="flex flex-wrap items-center gap-2">
        <FormField label="Id" className="min-w-35 flex-1">
          <Input className="font-mono" value={draft.id} disabled={!isNew} onChange={(e) => set({ id: e.target.value })} />
        </FormField>
        <FormField label="Flujo" className="min-w-35 flex-1">
          <SearchSelect
            value={draft.flowId}
            onChange={(flowId) => set({ flowId })}
            options={flows.map((f) => ({ value: f.id, label: f.name, hint: f.id }))}
            searchPlaceholder="Buscar flujo…"
          />
        </FormField>
        <FormField label="Tipo">
          <SearchSelect<TriggerKind>
            value={kind}
            onChange={(kind) => set({ kind })}
            options={(['http', 'cron', 'queue', 'manual'] as const).map((k) => ({ value: k, label: k }))}
          />
        </FormField>
      </div>

      {kind === 'http' && (
        <>
          <div className="flex flex-wrap items-center gap-2">
            <FormField label="Método">
              <SearchSelect
                value={draft.method ?? 'POST'}
                onChange={(method) => set({ method })}
                options={METHODS.map((m) => ({ value: m, label: m }))}
              />
            </FormField>
            <FormField label="Ruta bajo /hooks" className="min-w-35 flex-1">
              <Input className="font-mono" value={draft.path ?? ''} placeholder="orders" onChange={(e) => set({ path: e.target.value })} />
            </FormField>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <FormField label="Respuesta">
              <SearchSelect<'async' | 'sync'>
                value={draft.response ?? 'async'}
                onChange={(response) => set({ response })}
                options={[
                  { value: 'async', label: 'async: 202 con el id de la instancia' },
                  { value: 'sync', label: 'sync: espera un nodo «Responder»' },
                ]}
              />
            </FormField>
            {draft.response === 'sync' && (
              <FormField label="Tiempo máximo (ms)">
                <Input
                  type="number"
                  min={1}
                  
                  value={draft.timeoutMs ?? ''}
                  placeholder="10000"
                  onChange={(e) => set({ timeoutMs: e.target.value === '' ? undefined : Number(e.target.value) })}
                />
                <span className="text-xs text-base-content/60">Pasado el tiempo, responde 202.</span>
              </FormField>
            )}
          </div>
          <fieldset className="auth-box">
            <legend className="flex items-center gap-1.5 text-xs font-normal text-base-content/60">Autenticación</legend>
            <div className="flex flex-wrap items-center gap-2">
              <FormField label="Esquema">
                <SearchSelect
                  value={auth.scheme}
                  onChange={(scheme) => set({ auth: scheme === 'none' ? undefined : { ...auth, scheme } })}
                  options={[
                    { value: 'none', label: 'ninguna' },
                    { value: 'apiKey', label: 'apiKey: cabecera = secreto' },
                    { value: 'bearer', label: 'bearer: Authorization: Bearer = secreto' },
                    { value: 'hmac', label: 'hmac: X-Signature = HMAC-SHA256 del cuerpo' },
                  ]}
                />
              </FormField>
              {auth.scheme !== 'none' && (
                <>
                  <FormField label="Secreto (conexión.campo)" className="min-w-35 flex-1">
                    <Input
                      className="font-mono"
                      list="trigger-secrets"
                      value={auth.secret}
                      placeholder="claves.token"
                      onChange={(e) => set({ auth: { ...auth, secret: e.target.value } })}
                    />
                    <datalist id="trigger-secrets">{secretOptions.map((s) => <option key={s} value={s} />)}</datalist>
                  </FormField>
                  {auth.scheme !== 'bearer' && (
                    <FormField label="Cabecera">
                      <Input
                        className="font-mono"
                        value={auth.header ?? ''}
                        placeholder={auth.scheme === 'hmac' ? 'X-Signature' : 'X-Api-Key'}
                        onChange={(e) => set({ auth: { ...auth, header: e.target.value || undefined } })}
                      />
                    </FormField>
                  )}
                </>
              )}
            </div>
            {auth.scheme !== 'none' && <span className="text-xs text-base-content/60">Un secreto que no existe rechaza todas las peticiones.</span>}
          </fieldset>
          <InputsMapper value={draft.inputs ?? {}} hint={inputsHint} onChange={(v, err) => { set({ inputs: v }); setInputsError(err); }} />
        </>
      )}

      {kind === 'queue' && (
        <>
          <div className="flex flex-wrap items-center gap-2">
            <FormField label="Conexión" className="min-w-35 flex-1">
              <Input className="font-mono" list="trigger-queue-conns" value={draft.connection ?? ''} placeholder="kafka-prod" onChange={(e) => set({ connection: e.target.value })} />
              <datalist id="trigger-queue-conns">
                {queueConnections.map((c) => <option key={c.name} value={c.name}>{c.kind}</option>)}
              </datalist>
            </FormField>
            <FormField label="Cola / topic / stream" className="min-w-35 flex-1">
              <Input className="font-mono" value={draft.queue ?? ''} onChange={(e) => set({ queue: e.target.value })} />
            </FormField>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <FormField label="Grupo de consumidores" className="min-w-35 flex-1">
              <Input className="font-mono" value={draft.group ?? ''} placeholder={`flow-${draft.id || '<id>'}`} onChange={(e) => set({ group: e.target.value || undefined })} />
              <span className="text-xs text-base-content/60">Servidores con el mismo grupo se reparten los mensajes.</span>
            </FormField>
            <FormField label="Tipo de destino">
              <SearchSelect
                value={draft.destinationType ?? ''}
                onChange={(v) => set({ destinationType: v || undefined })}
                options={[
                  { value: '', label: 'por defecto (queue)' },
                  { value: 'queue', label: 'queue' },
                  { value: 'topic', label: 'topic' },
                ]}
              />
              <span className="text-xs text-base-content/60">Solo ActiveMQ distingue cola de topic.</span>
            </FormField>
          </div>
          <InputsMapper value={draft.inputs ?? {}} hint={inputsHint} onChange={(v, err) => { set({ inputs: v }); setInputsError(err); }} />
        </>
      )}

      {kind === 'cron' && (
        <>
          <FormField label="Expresión cron">
            <CronField value={draft.cron ?? ''} timezone={draft.timezone || 'UTC'} onChange={(text) => set({ cron: text })} />
          </FormField>
          <FormField label="Zona horaria">
            <ZoneField value={draft.timezone ?? ''} listId="trigger-zones" onChange={(z) => set({ timezone: z || undefined })} />
          </FormField>
          <InputsMapper value={draft.inputs ?? {}} hint={inputsHint} onChange={(v, err) => { set({ inputs: v }); setInputsError(err); }} />
          <p className="text-xs text-base-content/60">Un trigger cron está activo desde que existe (no hace falta habilitar el flujo). Sus instancias siempre usan misfire fire-once.</p>
        </>
      )}

      {kind === 'manual' && (
        <>
          <p className="text-xs text-base-content/60">Un trigger manual solo guarda datos: el flujo se lanza a mano desde el editor o la API.</p>
          <InputsMapper value={draft.inputs ?? {}} hint="Entradas literales por defecto." onChange={(v, err) => { set({ inputs: v }); setInputsError(err); }} />
        </>
      )}

      {inputsError && <p className="my-0.5 text-xs text-error">{inputsError}</p>}
      {problems.length > 0 && (
        <ul className="issue-list">
          {problems.map((p, i) => <li key={i} className="my-0.5 text-xs text-error">{p}</li>)}
        </ul>
      )}
      <div className="flex flex-wrap items-center gap-2">
        <Button type="button" disabled={saving || !!inputsError} onClick={save}>Guardar</Button>
        <Button variant="ghost" type="button" onClick={onCancel}>Cancelar</Button>
      </div>
    </div>
  );
}
