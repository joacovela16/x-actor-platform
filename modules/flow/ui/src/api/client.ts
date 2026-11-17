import type {
  AuditTrail, CatalogEntry, Connection, ConnectionClient, ConnectionKind, Describe, Draft, EntityCheck, EntityDef, EntityError, FlowDef,
  FlowError, FlowSummary, Instance, InstanceEvent, InstanceSummary, JsonValue, Mock, ScheduleView, Trace, Trigger, PluginStatus, BundlePlan, Health } from './types';
import { normalizeInstanceSummary, normalizeTrail } from '../graph/audit';

const BASE = '/api';

export class ApiError extends Error {
  constructor(public status: number, public body: unknown) {
    super(`HTTP ${status}`);
  }
}

/** Mensaje legible de un error de la API: `{errors:[{message}]}` (422, 409...) o `{error}`; si no, el código HTTP. */
export function errorMessage(e: unknown): string {
  if (e instanceof ApiError) {
    const body = e.body as { errors?: Array<{ message?: string }>; error?: string } | string | undefined;
    if (body && typeof body === 'object') {
      const msgs = (body.errors ?? []).map((x) => x.message).filter(Boolean);
      if (msgs.length) return msgs.join('; ');
      if (typeof body.error === 'string') return body.error;
    }
    if (typeof body === 'string' && body) return body;
    return `HTTP ${e.status}`;
  }
  return e instanceof Error ? e.message : String(e);
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(BASE + path, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data: unknown = undefined;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  if (!res.ok) throw new ApiError(res.status, data);
  return data as T;
}

export type SaveResult = { ok: true; flow: FlowDef } | { ok: false; errors: FlowError[] };

/** Guardar una entidad: la revisión guardada y los flujos que validaban y ya no (un aviso), o sus errores. */
export type EntitySaveResult =
  | { ok: true; entity: EntityDef; affected: Array<{ flowId: string; errors: FlowError[] }> }
  | { ok: false; errors: EntityError[] };

const enc = encodeURIComponent;

export const api = {
  health: () => request<Health>('GET', '/health'),
  /** El bundle de esos flujos (todos si no se dan), como texto estable para guardar tal cual. */
  exportBundle: async (flows: string[] = []): Promise<string> => {
    const res = await fetch(`${BASE}/bundle${flows.length ? `?flows=${enc(flows.join(','))}` : ''}`);
    const text = await res.text();
    if (!res.ok) {
      let body: unknown = text;
      try {
        body = JSON.parse(text);
      } catch {
        /* texto */
      }
      throw new ApiError(res.status, body);
    }
    return text;
  },
  /** Lo que importar el bundle haría, sin cambiar nada. */
  planBundle: (bundle: unknown) => request<BundlePlan>('POST', '/bundle?dryRun=true', bundle),
  /** Importa el bundle si su plan no tiene problemas; si los tiene, `ok: false` con el plan. */
  importBundle: async (bundle: unknown): Promise<{ ok: boolean; plan: BundlePlan }> => {
    try {
      return { ok: true, plan: await request<BundlePlan>('POST', '/bundle', bundle) };
    } catch (e) {
      if (e instanceof ApiError && e.status === 422) return { ok: false, plan: e.body as BundlePlan };
      throw e;
    }
  },

  catalog: () => request<CatalogEntry[]>('GET', '/catalog'),
  describe: (kind: string, params: JsonValue) => request<Describe>('POST', `/catalog/${enc(kind)}/describe`, params),

  listFlows: () => request<FlowSummary[]>('GET', '/flows'),
  /** Sin `revision`, la última guardada. Una revisión que no existe responde 404. */
  getFlow: (id: string, revision?: number) =>
    request<FlowDef>('GET', `/flows/${enc(id)}${revision ? `?revision=${revision}` : ''}`),
  /** PUT valida y guarda una revisión nueva (también crea el flujo si no existe). 422 devuelve los errores. */
  saveFlow: async (flow: FlowDef): Promise<SaveResult> => {
    try {
      const saved = await request<FlowDef>('PUT', `/flows/${enc(flow.id)}`, flow);
      return { ok: true, flow: saved };
    } catch (e) {
      if (e instanceof ApiError && e.status === 422) {
        const errors = (e.body as { errors?: FlowError[] } | undefined)?.errors ?? [];
        return { ok: false, errors };
      }
      throw e;
    }
  },
  deleteFlow: (id: string) => request<void>('DELETE', `/flows/${enc(id)}`),

  /** El borrador del flujo, con lo que impide publicarlo; `null` si no tiene. */
  getDraft: async (id: string): Promise<Draft | null> => {
    try {
      return await request<Draft>('GET', `/flows/${enc(id)}/draft`);
    } catch (e) {
      if (e instanceof ApiError && e.status === 404) return null;
      throw e;
    }
  },
  /** Guarda el borrador tal cual, sin validar; responde también sus errores. */
  saveDraft: (flow: FlowDef) => request<Draft>('PUT', `/flows/${enc(flow.id)}/draft`, flow),
  discardDraft: (id: string) => request<void>('DELETE', `/flows/${enc(id)}/draft`),
  /** Valida el borrador y lo guarda como revisión nueva (y quita el borrador). 422 devuelve los errores. */
  publish: async (id: string): Promise<SaveResult> => {
    try {
      return { ok: true, flow: await request<FlowDef>('POST', `/flows/${enc(id)}/publish`) };
    } catch (e) {
      if (e instanceof ApiError && e.status === 422) return { ok: false, errors: (e.body as { errors?: FlowError[] } | undefined)?.errors ?? [] };
      throw e;
    }
  },
  validate: async (flow: FlowDef): Promise<FlowError[]> =>
    (await request<{ errors: FlowError[] }>('POST', '/flows/validate', flow)).errors,

  /** Habilitar o deshabilitar los lanzadores cron del flujo. Devuelve lo mismo que `schedule`. */
  enableFlow: (id: string) => request<ScheduleView>('POST', `/flows/${enc(id)}/enable`),
  disableFlow: (id: string) => request<ScheduleView>('POST', `/flows/${enc(id)}/disable`),
  /** Horarios de la última revisión guardada. */
  schedule: (id: string) => request<ScheduleView>('GET', `/flows/${enc(id)}/schedule`),

  startInstance: (flowId: string, body: { inputs: JsonValue; revision?: number; instanceId?: string }) =>
    request<{ instanceId: string }>('POST', `/flows/${enc(flowId)}/instances`, body),
  getInstance: async (id: string): Promise<Instance> => {
    const raw = await request<Instance>('GET', `/instances/${enc(id)}`);
    // El backend ya no manda `dead` (se muestra `skipped`); se acepta por si acaso.
    const nodes = Object.fromEntries(
      Object.entries(raw.nodes ?? {}).map(([k, n]) => [k, { ...n, status: n.status === 'dead' ? 'skipped' : n.status }]),
    );
    return { ...raw, nodes };
  },
  /** Cancela una instancia que corre (un stream sin fin se cierra). 202 con su id; 404 si no existe. */
  cancelInstance: (id: string) => request<{ instanceId: string }>('POST', `/instances/${enc(id)}/cancel`),
  /** Un retry vuelve a correr la MISMA instancia: responde su mismo `instanceId` (202). 409 si aún no ha terminado. */
  retryInstance: (id: string, latest = false) =>
    request<{ instanceId: string }>('POST', `/instances/${enc(id)}/retry${latest ? '?latest=true' : ''}`),
  listInstances: async (params: Record<string, string | number | undefined>): Promise<InstanceSummary[]> => {
    const q = Object.entries(params)
      .filter(([, v]) => v !== undefined && v !== '')
      .map(([k, v]) => `${enc(k)}=${enc(String(v))}`)
      .join('&');
    const rows = await request<InstanceSummary[]>('GET', `/instances${q ? `?${q}` : ''}`);
    return rows.map(normalizeInstanceSummary);
  },
  getAudit: async (id: string): Promise<AuditTrail> => normalizeTrail(await request<unknown>('GET', `/instances/${enc(id)}/audit`)),

  simulate: (flowId: string, body: { flow: FlowDef; inputs: JsonValue; mocks: Record<string, Mock> }) =>
    request<Trace>('POST', `/flows/${enc(flowId)}/test`, body),

  listTriggers: () => request<Trigger[]>('GET', '/triggers'),
  getTrigger: (id: string) => request<Trigger>('GET', `/triggers/${enc(id)}`),
  saveTrigger: (t: Trigger) => request<Trigger>('PUT', `/triggers/${enc(t.id)}`, t),
  deleteTrigger: (id: string) => request<void>('DELETE', `/triggers/${enc(id)}`),

  connectionKinds: () => request<ConnectionKind[]>('GET', '/connection-kinds'),
  listConnections: () => request<Connection[]>('GET', '/connections'),
  /** Los secretos se envían aquí y no vuelven nunca. */
  saveConnection: (c: { name: string; kind: string; params: Record<string, JsonValue>; secrets: Record<string, string> }) =>
    request<Connection>('PUT', `/connections/${enc(c.name)}`, c),
  deleteConnection: (name: string) => request<void>('DELETE', `/connections/${enc(name)}`),
  /** Los clientes abiertos ahora en el pool del servidor. */
  connectionClients: () => request<ConnectionClient[]>('GET', '/connection-clients'),
  /** Cierra los clientes de una conexión; el próximo uso abre uno nuevo. */
  closeConnectionClients: (name: string) => request<void>('POST', `/connections/${enc(name)}/close`),
  /** Los plugins que el servidor encontró al arrancar, cargados o no. */
  plugins: () => request<PluginStatus[]>('GET', '/plugins'),

  /** Todas las entidades, enteras, cada una con su `source`. */
  listEntities: () => request<EntityDef[]>('GET', '/entities?full=true'),
  getEntity: (name: string) => request<EntityDef>('GET', `/entities/${enc(name)}`),
  /** Guarda la revisión siguiente. 422 devuelve los errores; 409 si es de código o de un plugin. */
  saveEntity: async (e: EntityDef): Promise<EntitySaveResult> => {
    const { source: _source, revision: _revision, ...body } = e;
    try {
      const r = await request<{ entity: EntityDef; affected: Array<{ flowId: string; errors: FlowError[] }> }>('PUT', `/entities/${enc(e.name)}`, body);
      return { ok: true, entity: r.entity, affected: r.affected };
    } catch (err) {
      if (err instanceof ApiError && err.status === 422) return { ok: false, errors: (err.body as { errors?: EntityError[] } | undefined)?.errors ?? [] };
      throw err;
    }
  },
  /** 409 si otra entidad o un flujo la usa, o si es de código. */
  deleteEntity: (name: string) => request<void>('DELETE', `/entities/${enc(name)}`),
  validateEntity: async (e: EntityDef): Promise<EntityError[]> => {
    const { source: _source, revision: _revision, ...body } = e;
    return (await request<{ errors: EntityError[] }>('POST', '/entities/validate', body)).errors;
  },
  /** El valor como lo hace la entidad (la guardada o `entity`, una definición sin guardar), o movido por `transition`. */
  checkEntity: (name: string, body: { value: JsonValue; transition?: string; entity?: EntityDef }) =>
    request<EntityCheck>('POST', `/entities/${enc(name)}/check`, body),
};

/** Suscripción SSE a las entradas de una instancia (replay + live). Devuelve la función para cerrarla. */
export function subscribeInstance(
  instanceId: string,
  onEvent: (e: InstanceEvent) => void,
  onError: () => void,
): () => void {
  const es = new EventSource(`${BASE}/instances/${enc(instanceId)}/events`);
  es.onmessage = (msg) => {
    try {
      onEvent(JSON.parse(msg.data) as InstanceEvent);
    } catch {
      /* línea que no es JSON: se ignora */
    }
  };
  es.onerror = () => onError();
  return () => es.close();
}
