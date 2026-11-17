// Formas JSON del módulo flow. Espejo de los codecs de modules/flow/model, engine y http (Scala).
// Lo que el DESIGN.md no fija se marca con "SUPUESTO" para que el backend lo respete.
// Tiempos: el backend los escribe como número (epoch ms). La UI también acepta texto ISO (ver graph/time.ts).

import type { UiSpec } from './uiSpec';

export type { UiSpec } from './uiSpec';

export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

/** Un instante: epoch ms (lo que escribe el backend) o texto ISO (tolerancia). */
export type Stamp = number | string | null | undefined;

/** `DataType`: {"type":"bool"|"num"|"str"|"any"|"list"|"obj"|"entity"|"ref", ...}. `entity` es un valor entero de una entidad del
 *  modelo de negocio (sus campos y reglas); `ref`, una referencia a una (el valor de su clave). */
export type DataType =
  | { type: 'bool' }
  | { type: 'num' }
  | { type: 'str' }
  | { type: 'any' }
  | { type: 'list'; element: DataType }
  | { type: 'obj'; fields: Record<string, DataType> }
  | { type: 'entity'; name: string }
  | { type: 'ref'; entity: string };

export type DataTypeTag = DataType['type'];

/** `StatePath` se serializa como texto con puntos: "order.customer.id". */
export type StatePath = string;

/** `OutputDecl`: {name, target, type}. Se usa para salidas de nodo y para inputs/outputs del flujo. */
export interface OutputDecl {
  name: string;
  target: StatePath;
  type: DataType;
}

export type RunAction = 'run' | 'skip';

/** `RunPolicy`: {whenSucceeded, whenFailed}. */
export interface RunPolicy {
  whenSucceeded: RunAction;
  whenFailed: RunAction;
}

/** `JoinPolicy`: "all" | "any". */
export type JoinPolicy = 'all' | 'any';

/** `NodeDef`. `label` se omite cuando no hay. */
export interface NodeDef {
  id: string;
  kind: string;
  params: Record<string, JsonValue>;
  outputs: OutputDecl[];
  runPolicy: RunPolicy;
  joinPolicy: JoinPolicy;
  label?: string;
}

/** `Edge`: {from, to, port, input?}. `port` por defecto "out"; `input` (la entrada que se toca) se omite cuando es `in`. */
export interface Edge {
  from: string;
  to: string;
  port: string;
  input?: string;
}

/** Posición de un nodo en el lienzo: `FlowDef.layout` (el backend la guarda al escribir el flujo). */
export interface NodePosition {
  x: number;
  y: number;
}

/** `FlowDef`. Campos del encoder Scala: version, id, name, revision, inputs, outputs, nodes, edges, layout (si hay). */
export interface FlowDef {
  version: number;
  id: string;
  name: string;
  revision: number;
  inputs: OutputDecl[];
  outputs: OutputDecl[];
  nodes: NodeDef[];
  edges: Edge[];
  layout?: Record<string, NodePosition>;
  /** Casos de prueba que el flujo guarda consigo (`FlowTest`). */
  tests?: FlowTest[];
}

/** Lo que tiene que cumplirse tras simular un caso de prueba. `state`: lo que el estado final debe contener. */
export interface FlowTestExpect {
  status?: 'succeeded' | 'failed';
  state?: JsonValue;
  visited?: string[];
  notVisited?: string[];
}

/** `FlowTest`: entradas y mocks (como los manda el panel de pruebas) y lo que se espera. */
export interface FlowTest {
  name: string;
  inputs: JsonValue;
  mocks: Record<string, JsonValue>;
  expect: FlowTestExpect;
}

/** Resultado de un caso en `POST /api/flows/{id}/tests`. */
export interface FlowTestResult {
  name: string;
  passed: boolean;
  problems: string[];
  status: string;
  state: JsonValue;
}

/** `GET /api/flows`: `FlowSummary(id, name, revision)` más `enabled` (el backend lo añade). */
export interface FlowSummary {
  id: string;
  name: string;
  /** 0 si solo existe el borrador. */
  revision: number;
  enabled?: boolean;
  /** Tiene un borrador sin publicar. */
  draft?: boolean;
}

/** Un flujo que se edita sin publicar: se guarda sin validar; `errors` es lo que impide publicarlo. */
export interface Draft {
  flow: FlowDef;
  errors: FlowError[];
}

/** `GET /api/flows/{id}/schedule` y respuesta de `POST enable|disable`. */
export interface ScheduleEntry {
  node: string;
  expression: string;
  timezone: string;
  misfire: 'fire-once' | 'skip' | string;
  nextFireAt: number | null;
}

export interface ScheduleView {
  enabled: boolean;
  schedules: ScheduleEntry[];
}

/** `ConfigSchema.Field`. El encoder omite los nulos; `required` por defecto true. */
export interface ConfigField {
  name: string;
  type: DataType;
  required?: boolean;
  default?: JsonValue;
  secret?: boolean;
  oneOf?: JsonValue[];
  description?: string;
  /** Cómo dibujarlo (ver `uiSpec.ts`). Opcional: sin él se infiere del tipo. */
  ui?: UiSpec;
}

export interface ConfigSchema {
  fields: ConfigField[];
}

export type SideEffects = 'pure' | 'read' | 'write';

/** `Port` = {name, label?, stream?, writes?}. `stream` es el tipo de los elementos si el puerto lleva un stream; `writes`, las
 *  salidas que escribe un nodo que sale por él cuando no son todas (sin él, todas). */
export interface PortInfo {
  name: string;
  label?: string;
  stream?: DataType;
  writes?: string[];
}

/** `role` de un nodo: `node` (fuera de streams), `source`, `operator` o `sink`. */
export type NodeRole = 'node' | 'source' | 'operator' | 'sink';

/** Entrada de `GET /api/catalog`. `inputs` trae las entradas con nombre (`in`, o `left`/`right` en join y zip). */
export interface CatalogEntry {
  kind: string;
  category: string;
  label: string;
  /** Qué hace el nodo, en una o dos frases. */
  description?: string;
  config: ConfigSchema;
  sideEffects: SideEffects;
  ports: Array<PortInfo | string>;
  inputs?: PortInfo[];
  role?: NodeRole;
  defaultOutputs: OutputDecl[];
}

/** `POST /api/catalog/{kind}/describe`. `problems` son los fallos de la configuración (incluida la sintaxis de las expresiones). */
export interface Describe {
  ports: Array<PortInfo | string>;
  inputs?: PortInfo[];
  role?: NodeRole;
  defaultOutputs: OutputDecl[];
  reads: Array<{ path: StatePath; type: DataType }>;
  problems?: string[];
}

/** `FlowError` = {code, node, message}. `node` es null cuando el error no es de un nodo. `nodes` (o `chain`) lo dice cuando son varios. */
export interface FlowError {
  code: string;
  node: string | null;
  message: string;
  nodes?: string[];
  chain?: string[];
}

/** Métricas de un nodo de un segmento de stream (`nodes[id].metrics`). Las muestras son las primeras 5. */
export interface NodeMetrics {
  in: number;
  out: number;
  dropped: number;
  failed: number;
  /** Media desde que el elemento entra en el segmento; `null` si aún no salió ninguno. */
  latencyMs: number | null;
  sampleIn: JsonValue[];
  sampleOut: JsonValue[];
}

/** Elemento que un nodo no pudo tratar (`deadLetters`). El stream sigue. */
export interface DeadLetter {
  node: string;
  error: string;
  element: JsonValue;
  at: number;
  /** Lo que dio la fuente, y cuál (para volver a publicarlo). */
  origin?: JsonValue;
  source?: string;
}

/** `GET /api/instances/{id}/dead-letters`: cada uno numerado, y si se puede volver a publicar en su cola. */
export interface InstanceDeadLetter extends DeadLetter {
  index: number;
  segment: string;
  reinjectable: boolean;
  sourceKind?: string;
}

/** Estado de un nodo en `GET /api/instances/{id}`: `pending|running|succeeded|failed|skipped` (el backend nunca manda `dead`). */
export interface InstanceNodeState {
  status: string;
  port?: string | null;
  error?: string | null;
  /** Solo un temporizador (cron) que espera: epoch ms en que sigue. */
  fireAt?: number | null;
  /** Solo los nodos de un segmento de stream: métricas del nodo (en vivo mientras corre, finales al terminar). */
  metrics?: NodeMetrics;
}

/** `GET /api/instances/{id}`. `status`: running | succeeded | failed. */
export interface Instance {
  id: string;
  flowId: string;
  revision: number;
  status: 'running' | 'succeeded' | 'failed';
  state: JsonValue;
  nodes: Record<string, InstanceNodeState>;
  deadLetters?: DeadLetter[];
}

/** Un `InstanceEvent` por línea SSE: `type` (= kind de la entrada), `seq` y `node` si aplica (no `nodeId`). */
export interface InstanceEvent {
  type: string;
  seq?: number;
  node?: string | null;
  [key: string]: unknown;
}

/**
 * Mock de un nodo en test: `{type:"output", port?, values}` (para una fuente de stream, `values.elements` es su lista de
 * elementos) o `{type:"fail", error}`. El backend acepta también la forma antigua `{values?, port?, error?}`.
 */
export type Mock =
  | { type: 'output'; port?: string; values?: Record<string, JsonValue> }
  | { type: 'fail'; error: string };

/** Traza de `POST /api/flows/{id}/test`. Un segmento de stream es un paso: `kind: stream.segment` y `info.metrics`. */
export interface TraceStep {
  nodeId: string;
  kind?: string;
  outcome?: string;
  port?: string | null;
  error?: string | null;
  mocked?: boolean;
  simulated?: boolean;
  state?: JsonValue;
  /** Salidas del paso, por nombre (`<nodo>.<salida>` en un segmento). */
  values?: Record<string, JsonValue>;
  info?: {
    metrics?: Record<string, NodeMetrics>;
    deadLetters?: DeadLetter[];
    [key: string]: unknown;
  };
}

export interface Trace {
  status?: string;
  steps: TraceStep[];
  state?: JsonValue;
  [key: string]: unknown;
}

// ---- Triggers ------------------------------------------------------------

export type TriggerKind = 'http' | 'cron' | 'queue' | 'manual';

/** Mapeo de una entrada del flujo: `${request.query.x}`, `${request.body.x}`, `${message.body}`, `${env.X}`... o un literal. */
export type TriggerInputs = Record<string, JsonValue>;

/** `auth` de un trigger http. `secret` es `<connection>.<field>`. */
export interface TriggerAuth {
  scheme: 'apiKey' | 'bearer' | 'hmac' | string;
  secret: string;
  header?: string;
}

/** `TriggerDef` (forma plana). Los campos vacíos no vienen en el JSON. */
export interface Trigger {
  id: string;
  flowId: string;
  kind: TriggerKind;
  method?: string;
  path?: string;
  cron?: string;
  timezone?: string;
  queue?: string;
  connection?: string;
  group?: string;
  destinationType?: string;
  inputs?: TriggerInputs;
  auth?: TriggerAuth;
  /** `async` (por defecto, no viene) o `sync`. */
  response?: 'async' | 'sync';
  timeoutMs?: number;
}

/** Tipo de conexión: `GET /api/connection-kinds`. */
export interface ConnectionKind {
  kind: string;
  label: string;
  config: ConfigSchema;
}

/** Conexión: `GET /api/connections`. Los secretos nunca se devuelven; `secretsSet` dice cuáles están guardados. */
export interface Connection {
  name: string;
  kind: string;
  params: Record<string, JsonValue>;
  secretsSet: string[];
  /** Variables de entorno a las que se refieren sus parámetros y secretos (`${NOMBRE}`). */
  env?: string[];
  /** Las de `env` que no están definidas en el servidor y no tienen valor por defecto. */
  envMissing?: string[];
}

// ---- Historial y auditoría ------------------------------------------------
// Formas reales (ver engine/AuditEntry.scala). El normalizador de graph/audit.ts acepta también la forma antigua del mock.

/** Fila de `GET /api/instances` (array plano). `origin` es la etiqueta corta; `originDetail` la forma estructurada. */
export interface InstanceSummary {
  id: string;
  flowId: string;
  revision?: number;
  status: 'running' | 'succeeded' | 'failed' | string;
  origin?: string;
  originDetail?: Record<string, unknown>;
  startedAt?: Stamp;
  endedAt?: Stamp;
  [key: string]: unknown;
}

/** `origin` estructurado: `{type: manual|trigger|subflow|retry|cron, ...}`. Ya normalizado a `kind`. */
export interface AuditOrigin {
  kind?: 'trigger' | 'manual' | 'retry' | 'subflow' | 'cron' | string;
  triggerId?: string;
  requestId?: string;
  parentInstanceId?: string;
  parentNode?: string;
  retryOf?: string;
  reason?: string;
  schedule?: string;
  [key: string]: unknown;
}

export interface AuditInstance {
  id: string;
  flowId: string;
  revision?: number;
  status?: string;
  origin?: AuditOrigin | string;
  startedAt?: Stamp;
  endedAt?: Stamp;
  inputs?: JsonValue;
  [key: string]: unknown;
}

/** Arista: `{from, port, to}`. */
export interface AuditEdgeRef {
  from: string;
  port: string;
  to: string;
  [key: string]: unknown;
}

/** Escritura en el estado global: valor previo y nuevo en una ruta. */
export interface AuditWrite {
  path: string;
  previous?: unknown;
  value?: unknown;
  [key: string]: unknown;
}

/** Lectura del estado. */
export interface AuditRead {
  path: string;
  value?: unknown;
  [key: string]: unknown;
}

/**
 * Decisión del motor cuando un nodo no corre o espera. Ya normalizada (ver `normalizeDecision` en graph/audit.ts).
 * reason: skipped-by-run-policy | skipped-dead-branch | waiting-join | scheduled.
 */
export interface AuditDecision {
  reason?: string;
  deadEdges?: AuditEdgeRef[];
  decidedBy?: Array<{ node?: string; edgePort?: string; portTaken?: string; path?: string; value?: unknown }>;
  pendingEdges?: AuditEdgeRef[];
  previousStatus?: string;
  reused?: { port?: string | null; error?: string | null; childInstanceId?: string | null; writes?: AuditWrite[] };
  rule?: string;
  fireAt?: number;
  [key: string]: unknown;
}

/**
 * Una entrada de la auditoría. `kind`: instance-started, retry-requested, node-started, node-finished, node-skipped,
 * node-waiting, edge-fired, edge-dead, state-changed, instance-finished (y los que el backend añada).
 */
export interface AuditEntry {
  seq: number;
  at?: Stamp;
  kind: string;
  node?: string;
  nodeKind?: string;
  attempt?: number;
  durationMs?: number;
  reads?: AuditRead[];
  /** Escrituras normalizadas (`outputs` o `changes` del backend). */
  writes?: AuditWrite[];
  memoryBefore?: unknown;
  memoryAfter?: unknown;
  params?: unknown;
  port?: string | null;
  error?: string | null;
  mocked?: boolean;
  simulated?: boolean;
  decision?: AuditDecision;
  edge?: AuditEdgeRef;
  childInstanceId?: string | null;
  retryOf?: string;
  reason?: string;
  status?: string;
  inputs?: unknown;
  /** `node-finished.info`: lo que el nodo dice de su ejecución (p. ej. el mensaje que `queue.publish` habría enviado). */
  info?: Record<string, unknown>;
  [key: string]: unknown;
}

/** `GET /api/instances/{id}/audit`. */
export interface AuditTrail {
  instance: AuditInstance;
  entries: AuditEntry[];
  [key: string]: unknown;
}

/** Un cliente abierto de una conexión (pool del servidor). Nunca trae la configuración, solo lo que lo distingue. */
export interface ConnectionClient {
  connection: string;
  /** mongodb, jdbc, publisher… */
  kind: string;
  /** Lo que distingue clientes de la misma conexión (p. ej. el destino de un publisher). */
  extra: string;
  inUse: number;
  idleMillis: number;
  /** La conexión cambió: se cierra en cuanto se devuelva. */
  retired: boolean;
  opening: boolean;
}

/** Un plugin encontrado al arrancar el servidor (`GET /api/plugins`). */
/** `GET /api/health`: cómo van los flujos en las últimas 24 h (en este servidor) y las fuentes de triggers. */
export interface Health {
  flows: Array<{
    flowId: string;
    running: number;
    succeeded: number;
    failed: number;
    p50Ms: number | null;
    p95Ms: number | null;
    lastFailures: Array<{ instanceId: string; at: number; durationMs: number; error: string | null }>;
  }>;
  triggers: Array<{ triggerId: string; kind: string; connection: string | null; state: 'running' | 'waiting' | 'unserved' }>;
}

/** Lo que un import haría (o hizo) con cada elemento de un bundle. */
export interface BundleItem {
  type: 'flow' | 'trigger' | 'entity' | 'connection';
  id: string;
  action: 'create' | 'update' | 'same' | 'skip' | 'draft';
}

/** El plan de un import: lo que cambia, lo que lo impide (`problems`) y lo que conviene saber (`warnings`). */
export interface BundlePlan {
  items: BundleItem[];
  problems: string[];
  warnings: string[];
}

export interface PluginStatus {
  /** Su directorio, relativo al de plugins; para uno del servidor, su nombre. */
  dir: string;
  /** `builtin`: viene con el servidor; `directory`: está en el directorio de plugins. */
  source: 'builtin' | 'directory';
  /** Ausente si no se pudo leer el manifiesto. */
  manifest: { name: string; version: string; apiVersion: string; transport: 'jvm' | 'process'; command: string[] } | null;
  state: 'loaded' | 'failed';
  kinds?: string[];
  connectionKinds?: string[];
  /** Las entidades del modelo de negocio que trae. */
  entities?: string[];
  /** Los tipos de trigger que atiende (`queue`). */
  triggers?: string[];
  error?: string;
}

// ---- entidades del modelo de negocio (modules/flow/entity, `/api/entities`) ----

export type EntityKind = 'root' | 'value';

/** Límites de un campo. Cada uno aplica a un tipo: min/max a números, minLength/maxLength/pattern a texto, minItems/maxItems a
 *  listas, oneOf a cualquiera. El patrón se busca en cualquier parte del texto salvo que se ancle con `^...$`. */
export interface EntityConstraints {
  min?: number;
  max?: number;
  minLength?: number;
  maxLength?: number;
  pattern?: string;
  minItems?: number;
  maxItems?: number;
  oneOf?: JsonValue[];
}

export interface EntityField {
  name: string;
  type: DataType;
  /** Por defecto true. */
  required?: boolean;
  default?: JsonValue;
  constraints?: EntityConstraints;
  /** Expresión (`Expr` JSON) con la que se calcula a partir de los demás campos. */
  computed?: JsonValue;
  label?: string;
  description?: string;
}

/** Condición que cumple todo valor: `check` debe ser verdadero si `when` lo es (sin `when`, siempre). */
export interface EntityRule {
  name: string;
  when?: JsonValue;
  check: JsonValue;
  message?: string;
}

export interface EntityTransition {
  name: string;
  from: string[];
  to: string;
  guard?: JsonValue;
  message?: string;
}

export interface EntityStateMachine {
  field: string;
  states: string[];
  initial: string;
  transitions: EntityTransition[];
}

/** `EntityDef`. `source` lo agrega la API: `store` (editada aquí), `code` o `plugin:<nombre>` (de solo lectura). */
export interface EntityDef {
  version?: number;
  name: string;
  label?: string;
  description?: string;
  kind: EntityKind;
  key?: string;
  revision?: number;
  fields: EntityField[];
  rules?: EntityRule[];
  states?: EntityStateMachine;
  source?: string;
}

export interface EntitySummary {
  name: string;
  kind: EntityKind;
  label?: string | null;
  revision: number;
  source: string;
  fields: number;
}

/** Problema de la definición de una entidad. */
export interface EntityError {
  code: string;
  entity: string;
  field: string | null;
  message: string;
}

/** Problema de un valor: dónde (ruta con puntos, vacía para el valor entero), qué regla rompe y por qué. */
export interface EntityProblem {
  path: string;
  rule: string;
  message: string;
}

export interface EntityCheck {
  ok: boolean;
  value: JsonValue;
  problems: EntityProblem[];
}
