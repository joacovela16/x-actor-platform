// Datos del mock: catálogo de nodos (con sus ConfigSchema como en el backend), tipos de conexión (como en
// ConnectionKind.scala y QueueKinds.scala), flujos y triggers de ejemplo.
import type { CatalogEntry, ConnectionKind, DataType, FlowDef, JsonValue, NodeDef, OutputDecl, Trigger } from '../src/api/types.ts';
// Los nodos de stream, tal como los describe el backend (GET /api/catalog del jar, sin cambios).
import streamCatalogJson from './streamCatalog.json';

const RUN = { whenSucceeded: 'run', whenFailed: 'run' } as const;

const coreCatalog: CatalogEntry[] = [
  {
    kind: 'core.if',
    category: 'Control de flujo',
    label: 'If',
    sideEffects: 'pure',
    ports: [{ name: 'then', label: 'sí' }, { name: 'else', label: 'no' }],
    defaultOutputs: [],
    config: {
      fields: [
        { name: 'path', type: { type: 'str' }, required: true, description: 'Ruta del booleano, por ejemplo order.approved' },
        { name: 'source', type: { type: 'str' }, required: false, default: 'state', oneOf: ['state', 'memory'], description: 'Dónde está la variable: estado global o memoria de este nodo' },
        { name: 'negate', type: { type: 'bool' }, required: false, default: false },
      ],
    },
  },
  {
    kind: 'core.subflow',
    category: 'Control de flujo',
    label: 'Ejecutar flujo',
    sideEffects: 'write',
    ports: [{ name: 'out' }],
    defaultOutputs: [],
    config: {
      fields: [
        { name: 'flowId', type: { type: 'str' }, required: true, description: 'El flujo a ejecutar' },
        { name: 'revision', type: { type: 'num' }, required: false, description: 'Revisión del flujo; la última si se deja vacío' },
        { name: 'wait', type: { type: 'bool' }, required: false, default: true, description: 'Esperar al final del flujo y tomar sus salidas' },
        { name: 'inputs', type: { type: 'any' }, required: false, default: {}, description: 'Entrada del flujo llamado -> ruta en este estado' },
      ],
    },
  },
  {
    // Lanzador (sin aristas de entrada) o temporizador (con entrada): el modo lo decide el grafo.
    kind: 'core.cron',
    category: 'Triggers',
    label: 'Cron',
    sideEffects: 'pure',
    ports: [{ name: 'out' }],
    defaultOutputs: [],
    config: {
      fields: [
        { name: 'expression', type: { type: 'str' }, description: 'Cron expression: 5 fields (minute to day of week) or 6 with seconds first; also @hourly, @daily...' },
        { name: 'timezone', type: { type: 'str' }, required: false, default: 'UTC', description: 'IANA time zone, such as Europe/Madrid' },
        { name: 'misfire', type: { type: 'str' }, required: false, default: 'fire-once', oneOf: ['fire-once', 'skip'], description: 'Launcher: what to do with the times missed while the server was stopped' },
        { name: 'maxWait', type: { type: 'str' }, required: false, description: 'Timer: fail if the next time is farther than this, such as 30m or 1d' },
        { name: 'inputs', type: { type: 'any' }, required: false, default: {}, description: 'Launcher: inputs of the flow for the instances that the schedule starts' },
      ],
    },
  },
  {
    kind: 'queue.publish',
    category: 'Messaging',
    label: 'Publish',
    sideEffects: 'write',
    ports: [{ name: 'out' }],
    defaultOutputs: [],
    config: {
      fields: [
        { name: 'connection', type: { type: 'str' }, description: 'Name of the connection (Kafka, ActiveMQ or Redis)' },
        { name: 'destination', type: { type: 'str' }, description: 'Topic, queue or stream' },
        { name: 'payload', type: { type: 'str' }, description: 'Path in the state of the value to publish' },
        { name: 'key', type: { type: 'str' }, required: false, description: 'Path in the state of the key that orders messages' },
        { name: 'format', type: { type: 'str' }, required: false, default: 'json', oneOf: ['json', 'text'] },
        { name: 'destinationType', type: { type: 'str' }, required: false, default: 'queue', oneOf: ['queue', 'topic'], description: 'ActiveMQ only' },
        { name: 'headers', type: { type: 'any' }, required: false, default: {}, description: 'Fixed headers, as text' },
      ],
    },
  },
  {
    kind: 'core.respond',
    category: 'Triggers',
    label: 'Responder',
    sideEffects: 'pure',
    ports: [],
    defaultOutputs: [],
    config: {
      fields: [
        { name: 'status', type: { type: 'num' }, required: false, default: 200 },
        { name: 'body', type: { type: 'str' }, required: false, description: 'Path in the state of the body' },
        { name: 'headers', type: { type: 'any' }, required: false, default: {} },
      ],
    },
  },
  {
    // Solo del mock: une ramas (join). No existe en nodes-core.
    kind: 'core.noop',
    category: 'Control de flujo',
    label: 'Unir ramas (mock)',
    sideEffects: 'pure',
    ports: [{ name: 'out' }],
    defaultOutputs: [],
    config: { fields: [] },
  },
];

/**
 * Nodos de script, base de datos e IA con su `ui` (como lo mandará el backend). `ui` decide el widget de cada campo:
 * `code` (JavaScript o SQL), `list`, `json`, `checkbox`, `visibleWhen`, `group`...
 */
const FILES_TYPE: DataType = { type: 'list', element: { type: 'obj', fields: { name: { type: 'str' }, code: { type: 'str' } } } };
const TEMPLATE_OUTPUTS: OutputDecl[] = [
  { name: 'total', target: 'total', type: { type: 'num' } },
  { name: 'big', target: 'big', type: { type: 'bool' } },
];
const JS_TEMPLATE = `// state: the values of the flow; memory: what this node kept last time; params: its args.
// The files of the tabs run first: what they declare can be used here.
const items = state.order?.items ?? [];
const total = items.reduce((sum, i) => sum + i.price * i.qty, 0);
log("total", total);

return {
  values: { total, big: total > 1000 },     // by the name of the outputs of the node
  port: "out",                              // one of its ports (see ports)
  memory: { runs: (memory.runs ?? 0) + 1 }, // what it sees as memory next time
};
`;
const SCALA_TEMPLATE = `// state, memory and params are io.circe.Json; log(text) writes a line.
// The files of the tabs are compiled with this code: what they define can be used here.
val items = state.hcursor.downField("order").get[List[Json]]("items").getOrElse(Nil)
val total = items.map { i =>
  val c = i.hcursor
  c.get[Double]("price").getOrElse(0d) * c.get[Double]("qty").getOrElse(0d)
}.sum
log(s"total $total")

// out(...) goes on through out; via("port", ...) through another of its ports.
out("total" -> total.asJson, "big" -> (total > 1000).asJson)
`;

const scriptDbAiCatalog: CatalogEntry[] = [
  {
    kind: 'script.scala',
    category: 'Scripting',
    label: 'Script (Scala)',
    sideEffects: 'pure',
    ports: [{ name: 'out' }],
    defaultOutputs: TEMPLATE_OUTPUTS,
    config: {
      fields: [
        {
          name: 'code', type: { type: 'str' }, required: true,
          description: 'Scala: the body of a function of (state, memory, params, log) that returns Json.',
          ui: { widget: 'code', language: 'scala', rows: 16, template: SCALA_TEMPLATE, filesField: 'files' },
        },
        { name: 'files', type: FILES_TYPE, required: false, default: [], description: 'More files of the script, such as helpers.scala: [{name, code}]', ui: { widget: 'files', language: 'scala' } },
        { name: 'ports', type: { type: 'list', element: { type: 'str' } }, required: false, default: ['out'], description: 'Ports the script can leave through, besides error', ui: { widget: 'list' } },
        { name: 'args', type: { type: 'any' }, required: false, default: {}, description: 'A value the script sees as `params`', ui: { widget: 'json', rows: 3 } },
        { name: 'timeoutMs', type: { type: 'num' }, required: false, description: 'Time limit in milliseconds; it cannot exceed the server\'s' },
      ],
    },
  },
  {
    kind: 'script.js',
    category: 'Scripting',
    label: 'Script JavaScript',
    sideEffects: 'pure',
    ports: [{ name: 'out' }],
    defaultOutputs: TEMPLATE_OUTPUTS,
    config: {
      fields: [
        {
          name: 'code', type: { type: 'str' }, required: true,
          description: 'JavaScript: the body of a function of (state, memory, params, log). Return {values, port, memory}.',
          ui: { widget: 'code', language: 'javascript', rows: 16, template: JS_TEMPLATE, filesField: 'files' },
        },
        { name: 'files', type: FILES_TYPE, required: false, default: [], description: 'More files of the script, such as helpers.js: [{name, code}]', ui: { widget: 'files', language: 'javascript' } },
        { name: 'ports', type: { type: 'list', element: { type: 'str' } }, required: false, default: ['out'], description: 'Ports the script can leave through, besides error', ui: { widget: 'list', help: 'Puertos por los que puede salir, además de error' } },
        { name: 'args', type: { type: 'any' }, required: false, default: {}, description: 'A value the script sees as `params`', ui: { widget: 'json', rows: 3 } },
        { name: 'timeoutMs', type: { type: 'num' }, required: false, description: 'Time limit in milliseconds; it cannot exceed the server\'s' },
      ],
    },
  },
  {
    kind: 'db.query',
    category: 'Database',
    label: 'Consulta SQL',
    sideEffects: 'read',
    ports: [{ name: 'out' }],
    defaultOutputs: [],
    config: {
      fields: [
        { name: 'connection', type: { type: 'str' }, description: 'Name of the connection (postgres, mariadb or sqlite)', ui: { widget: 'text', placeholder: 'nombre de la conexión' } },
        {
          name: 'sql', type: { type: 'str' }, description: 'The statement, with ? for each parameter.',
          ui: { widget: 'code', language: 'sql', rows: 8, placeholder: 'SELECT id, total FROM orders WHERE customer = ?' },
        },
        { name: 'params', type: { type: 'any' }, required: false, default: [], description: 'One for each ?: {"path": "state.path"}, {"value": 1} or {"context": "key"}', ui: { widget: 'json', rows: 4 } },
        { name: 'maxRows', type: { type: 'num' }, required: false, default: 1000, description: 'Most rows kept', ui: { widget: 'number' } },
        { name: 'timeoutSeconds', type: { type: 'num' }, required: false, default: 30, description: 'Time limit of the statement', ui: { widget: 'number' } },
      ],
    },
  },
  {
    kind: 'db.execute',
    category: 'Database',
    label: 'Ejecutar SQL',
    sideEffects: 'write',
    ports: [{ name: 'out' }],
    defaultOutputs: [],
    config: {
      fields: [
        { name: 'connection', type: { type: 'str' }, description: 'Name of the connection (postgres, mariadb or sqlite)', ui: { widget: 'text', placeholder: 'nombre de la conexión' } },
        {
          name: 'sql', type: { type: 'str' }, description: 'The statement, with ? for each parameter.',
          ui: { widget: 'code', language: 'sql', rows: 8, placeholder: 'INSERT INTO audit (note) VALUES (?)' },
        },
        { name: 'params', type: { type: 'any' }, required: false, default: [], description: 'One for each ?', ui: { widget: 'json', rows: 4 } },
        { name: 'timeoutSeconds', type: { type: 'num' }, required: false, default: 30, description: 'Time limit of the statement', ui: { widget: 'number' } },
      ],
    },
  },
  {
    kind: 'ai.agent',
    category: 'AI',
    label: 'Agente IA',
    sideEffects: 'read',
    ports: [{ name: 'out' }],
    defaultOutputs: [],
    config: {
      fields: [
        { name: 'connection', type: { type: 'str' }, description: 'Name of the connection (claude, openai or codex)', ui: { widget: 'text', group: 'Entrada', order: 1, placeholder: 'nombre de la conexión' } },
        { name: 'prompt', type: { type: 'str' }, description: 'What to ask. {{path}} is replaced by a value of the state.', ui: { widget: 'textarea', group: 'Entrada', order: 2, rows: 4, help: '{{ruta}} se sustituye por un valor del estado' } },
        { name: 'system', type: { type: 'str' }, required: false, description: 'System prompt, also with {{path}}', ui: { widget: 'textarea', group: 'Entrada', order: 3, rows: 3 } },
        { name: 'model', type: { type: 'str' }, required: false, description: 'Model; the one of the connection if empty', ui: { widget: 'text', group: 'Modelo', order: 4, placeholder: 'el de la conexión' } },
        { name: 'maxTokens', type: { type: 'num' }, required: false, default: 1024, description: 'Most tokens in the answer', ui: { widget: 'number', group: 'Modelo', order: 5 } },
        { name: 'temperature', type: { type: 'num' }, required: false, description: '0 to 2', ui: { widget: 'number', group: 'Modelo', order: 6 } },
        { name: 'json', type: { type: 'bool' }, required: false, default: false, description: 'Ask for a JSON answer, written to the output json', ui: { widget: 'checkbox', group: 'Salida', order: 7, help: 'Pedir una respuesta JSON (sale en la salida json)' } },
        {
          name: 'schema', type: { type: 'any' }, required: false, description: 'Data type the JSON answer must fit',
          ui: { widget: 'json', group: 'Salida', order: 8, rows: 4, visibleWhen: { field: 'json', equals: true } },
        },
        { name: 'timeoutSeconds', type: { type: 'num' }, required: false, default: 60, description: 'Time limit of each call', ui: { widget: 'number', group: 'Llamada', order: 9 } },
        { name: 'retries', type: { type: 'num' }, required: false, default: 2, description: 'Retries after a 429, a 5xx or a timeout', ui: { widget: 'number', group: 'Llamada', order: 10 } },
      ],
    },
  },
];

const ERRORS_TYPE = { type: 'list', element: { type: 'obj', fields: { path: { type: 'str' }, rule: { type: 'str' }, message: { type: 'str' } } } } as const;
const entityField = { name: 'entity', type: { type: 'str' }, description: 'The entity of the value', ui: { widget: 'entity', order: 0 } } as const;
const pathField = { name: 'path', type: { type: 'str' }, description: 'Path of the value in the state, such as order', ui: { order: 1 } } as const;

/** Las salidas por defecto de un nodo de entidad (EntityNodes.outputs): el valor con el tipo de su entidad y los problemas al lado. */
export function entityOutputs(entity: unknown, path: string): CatalogEntry['defaultOutputs'] {
  if (typeof entity !== 'string' || !entity || !path) return [];
  const segs = path.split('.');
  const errors = [...segs.slice(0, -1), `${segs[segs.length - 1]}Errors`].join('.');
  return [
    { name: 'value', target: path, type: { type: 'entity', name: entity } },
    { name: 'errors', target: errors, type: structuredClone(ERRORS_TYPE) as unknown as CatalogEntry['defaultOutputs'][number]['type'] },
  ];
}

/** Los nodos de entidades (nodes-core/EntityNodes), como los describe el backend. */
const entityCatalog: CatalogEntry[] = [
  {
    kind: 'entity.build', category: 'Entities', label: 'Build entity', sideEffects: 'pure',
    ports: [{ name: 'out', writes: ['value'] }, { name: 'invalid', writes: ['errors'] }], defaultOutputs: [],
    config: { fields: [entityField, { name: 'values', type: { type: 'any' }, description: 'The fields of the value: values or references such as ${state.order.id}', ui: { widget: 'json', order: 1 } }] },
  },
  {
    kind: 'entity.validate', category: 'Entities', label: 'Validate entity', sideEffects: 'pure',
    ports: [{ name: 'valid', writes: ['value'] }, { name: 'invalid', writes: ['errors'] }], defaultOutputs: [],
    config: { fields: [entityField, pathField] },
  },
  {
    kind: 'entity.transition', category: 'Entities', label: 'Entity transition', sideEffects: 'pure',
    ports: [{ name: 'out', writes: ['value'] }, { name: 'rejected', writes: ['errors'] }], defaultOutputs: [],
    config: { fields: [entityField, pathField, { name: 'transition', type: { type: 'str' }, description: 'The name of the transition, such as approve', ui: { order: 2 } }] },
  },
] as unknown as CatalogEntry[];

/** Catálogo del mock: los nodos de core (del mock), los de script/db/IA, los de entidades y los de stream (los del backend). */
export const catalog: CatalogEntry[] = [...coreCatalog, ...scriptDbAiCatalog, ...entityCatalog, ...(streamCatalogJson as unknown as CatalogEntry[])];

/** Rutas que la auditoría redacta (en el mock, los campos con `secret`). */
export const REDACTED_PATHS = ['order.cardToken'];

/** Tipos de conexión del backend: `secret` y `http` (ConnectionKind) y los de cola (QueueKinds). */
export const connectionKinds: ConnectionKind[] = [
  {
    kind: 'secret',
    label: 'Shared secret',
    config: { fields: [{ name: 'token', type: { type: 'str' }, required: true, secret: true, description: 'The secret value' }] },
  },
  {
    kind: 'http',
    label: 'HTTP service',
    config: {
      fields: [
        { name: 'baseUrl', type: { type: 'str' }, required: true, description: 'Base URL, such as https://api.example.com' },
        { name: 'token', type: { type: 'str' }, required: false, secret: true, description: 'Bearer token, if the service needs one' },
      ],
    },
  },
  {
    kind: 'kafka',
    label: 'Kafka',
    config: {
      fields: [
        { name: 'bootstrapServers', type: { type: 'str' }, required: true, description: 'host:port of one or more brokers, comma separated' },
        { name: 'securityProtocol', type: { type: 'str' }, required: false, default: 'PLAINTEXT', oneOf: ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'] },
        { name: 'saslMechanism', type: { type: 'str' }, required: false, default: 'PLAIN', oneOf: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'] },
        { name: 'username', type: { type: 'str' }, required: false },
        { name: 'password', type: { type: 'str' }, required: false, secret: true },
        { name: 'properties', type: { type: 'any' }, required: false, default: {}, description: 'Other Kafka client properties, as text' },
      ],
    },
  },
  {
    kind: 'activemq',
    label: 'ActiveMQ / Artemis',
    config: {
      fields: [
        { name: 'url', type: { type: 'str' }, required: true, description: 'Broker URL, such as tcp://localhost:61616' },
        { name: 'flavor', type: { type: 'str' }, required: false, default: 'artemis', oneOf: ['artemis', 'classic'] },
        { name: 'username', type: { type: 'str' }, required: false },
        { name: 'password', type: { type: 'str' }, required: false, secret: true },
      ],
    },
  },
  {
    kind: 'redis',
    label: 'Redis Streams',
    config: {
      fields: [
        { name: 'uri', type: { type: 'str' }, required: true, description: 'Such as redis://localhost:6379' },
        { name: 'password', type: { type: 'str' }, required: false, secret: true },
      ],
    },
  },
  ...dbConnectionKinds(),
  {
    kind: 'sqlite',
    label: 'SQLite',
    config: {
      fields: [
        {
          name: 'file', type: { type: 'str' }, description: 'Path of the database file; it is created if it does not exist',
          ui: { widget: 'text', group: 'Archivo', placeholder: '/var/data/app.db' },
        },
      ],
    },
  },
  ...aiConnectionKinds(),
];

/** postgres y mariadb comparten la forma del backend (DbKinds.server): servidor, base de datos y opciones. */
function dbConnectionKinds(): ConnectionKind[] {
  const server = (kind: string, label: string, port: number, sslHelp: string): ConnectionKind => ({
    kind,
    label,
    config: {
      fields: [
        { name: 'host', type: { type: 'str' }, description: 'Host name or address', ui: { widget: 'text', group: 'Servidor', order: 1, placeholder: 'db.internal' } },
        { name: 'port', type: { type: 'num' }, required: false, default: port, ui: { widget: 'number', group: 'Servidor', order: 2 } },
        { name: 'database', type: { type: 'str' }, description: 'Name of the database', ui: { widget: 'text', group: 'Base de datos', order: 3 } },
        { name: 'username', type: { type: 'str' }, required: false, ui: { widget: 'text', group: 'Base de datos', order: 4 } },
        { name: 'password', type: { type: 'str' }, required: false, secret: true, ui: { widget: 'password', group: 'Base de datos', order: 5 } },
        { name: 'ssl', type: { type: 'bool' }, required: false, default: false, description: sslHelp, ui: { widget: 'checkbox', group: 'Opciones', order: 6 } },
        { name: 'maxPoolSize', type: { type: 'num' }, required: false, default: 5, description: 'Most connections open at once', ui: { widget: 'number', group: 'Opciones', order: 7 } },
      ],
    },
  });
  return [
    server('postgres', 'PostgreSQL', 5432, 'Require an encrypted connection (sslmode=require)'),
    server('mariadb', 'MariaDB / MySQL', 3306, 'Encrypt the connection, without checking the certificate of the server'),
  ];
}

/** claude, openai y codex (AiKinds): URL base, clave secreta y modelo; `anthropicVersion` solo en claude. */
function aiConnectionKinds(): ConnectionKind[] {
  const service = (kind: string, label: string, baseUrl: string, model: string, extra: boolean): ConnectionKind => ({
    kind,
    label,
    config: {
      fields: [
        { name: 'baseUrl', type: { type: 'str' }, required: false, default: baseUrl, description: 'Base URL of the service', ui: { widget: 'text', group: 'Servicio', order: 1 } },
        ...(extra
          ? [{ name: 'anthropicVersion', type: { type: 'str' as const }, required: false, default: '2023-06-01', description: 'Value of the anthropic-version header', ui: { widget: 'text' as const, group: 'Servicio', order: 2 } }]
          : []),
        { name: 'apiKey', type: { type: 'str' }, secret: true, description: 'API key', ui: { widget: 'password', group: 'Acceso', order: 3 } },
        { name: 'model', type: { type: 'str' }, required: false, default: model, description: 'Model to use when the node does not name one', ui: { widget: 'text', group: 'Modelo', order: 4 } },
      ],
    },
  });
  return [
    service('claude', 'Claude (Anthropic)', 'https://api.anthropic.com', 'claude-sonnet-5-5', true),
    service('openai', 'OpenAI-compatible', 'https://api.openai.com/v1', 'gpt-4.1', false),
    service('codex', 'Codex (OpenAI-compatible)', 'https://api.openai.com/v1', 'codex-mini-latest', false),
  ];
}

const cronRun = (expression: string, extra: Record<string, unknown> = {}) => ({
  id: 'tick', kind: 'core.cron', params: { expression, timezone: 'Europe/Madrid', ...extra }, outputs: [], runPolicy: RUN, joinPolicy: 'all' as const,
});

export const seedFlows: FlowDef[] = [
  {
    version: 1,
    id: 'order-approval',
    name: 'Aprobación de pedido',
    revision: 3,
    inputs: [
      { name: 'approved', target: 'order.approved', type: { type: 'bool' } },
      { name: 'customer', target: 'order.customer', type: { type: 'str' } },
      { name: 'cardToken', target: 'order.cardToken', type: { type: 'str' } },
    ],
    outputs: [],
    nodes: [
      { id: 'check', kind: 'core.if', params: { path: 'order.approved' }, outputs: [], runPolicy: RUN, joinPolicy: 'all', label: '¿Aprobado?' },
      {
        id: 'notify-reject', kind: 'core.subflow',
        params: { flowId: 'notify-customer', wait: false, inputs: { vip: 'order.approved' } },
        outputs: [], runPolicy: RUN, joinPolicy: 'any', label: 'Aviso de rechazo',
      },
      {
        id: 'notify', kind: 'core.subflow',
        params: { flowId: 'notify-customer', wait: true, inputs: { vip: 'order.approved' } },
        outputs: [{ name: 'vip', target: 'order.notifiedVip', type: { type: 'bool' } }],
        runPolicy: { whenSucceeded: 'skip', whenFailed: 'run' }, joinPolicy: 'all', label: 'Avisar al cliente',
      },
      { id: 'merge', kind: 'core.noop', params: {}, outputs: [], runPolicy: RUN, joinPolicy: 'all', label: 'Unir' },
    ],
    edges: [
      { from: 'check', port: 'then', to: 'notify' },
      { from: 'check', port: 'else', to: 'notify-reject' },
      { from: 'notify', port: 'out', to: 'merge' },
      { from: 'notify-reject', port: 'out', to: 'merge' },
    ],
    layout: {
      check: { x: 80, y: 120 },
      'notify-reject': { x: 360, y: 220 },
      notify: { x: 360, y: 40 },
      merge: { x: 640, y: 130 },
    },
  } as FlowDef,
  {
    version: 1,
    id: 'notify-customer',
    name: 'Avisar al cliente',
    revision: 1,
    inputs: [{ name: 'vip', target: 'vip', type: { type: 'bool' } }],
    outputs: [{ name: 'vip', target: 'vip', type: { type: 'bool' } }],
    nodes: [
      { id: 'vip-check', kind: 'core.if', params: { path: 'vip' }, outputs: [], runPolicy: RUN, joinPolicy: 'all', label: '¿Cliente VIP?' },
    ],
    edges: [],
    layout: { 'vip-check': { x: 120, y: 120 } },
  } as FlowDef,
  {
    // Lanzador (tick, sin entrada) y temporizador (wait, con entrada): cada 20 segundos, en Madrid.
    version: 1,
    id: 'heartbeat',
    name: 'Latido (cada 20 s)',
    revision: 2,
    inputs: [],
    outputs: [],
    nodes: [
      { ...cronRun('*/20 * * * * *'), id: 'tick', label: 'Cada 20 s' },
      { ...cronRun('*/20 * * * * *'), id: 'wait', label: 'Esperar a la ventana' },
      { id: 'done', kind: 'core.noop', params: {}, outputs: [], runPolicy: RUN, joinPolicy: 'all', label: 'Fin' },
    ],
    edges: [
      { from: 'tick', port: 'out', to: 'wait' },
      { from: 'wait', port: 'out', to: 'done' },
    ],
    layout: { tick: { x: 80, y: 120 }, wait: { x: 360, y: 120 }, done: { x: 640, y: 120 } },
  } as FlowDef,
  {
    // Lanzador cada noche a las 02:30, sin habilitar: sus horarios no disparan hasta habilitarlo.
    version: 1,
    id: 'nightly-report',
    name: 'Informe nocturno',
    revision: 1,
    inputs: [{ name: 'order', target: 'order', type: { type: 'obj', fields: {} } }],
    outputs: [],
    nodes: [
      {
        id: 'launch', kind: 'core.cron', params: { expression: '30 2 * * *', timezone: 'Europe/Madrid', misfire: 'skip', inputs: { order: { customer: 'ACME' } } },
        outputs: [], runPolicy: RUN, joinPolicy: 'all', label: 'Cada noche 02:30',
      },
      {
        id: 'publish', kind: 'queue.publish',
        params: { connection: 'kafka-prod', destination: 'reports.nightly', payload: 'order', key: 'order.customer', format: 'json' },
        outputs: [], runPolicy: RUN, joinPolicy: 'all', label: 'Publicar informe',
      },
    ],
    edges: [{ from: 'launch', port: 'out', to: 'publish' }],
    layout: { launch: { x: 80, y: 120 }, publish: { x: 360, y: 120 } },
  } as FlowDef,
];

export const seedTriggers: Trigger[] = [
  {
    id: 'orders-http', flowId: 'order-approval', kind: 'http', method: 'POST', path: 'orders',
    response: 'sync', timeoutMs: 5000,
    auth: { scheme: 'apiKey', secret: 'partner-api.token', header: 'X-Api-Key' },
    inputs: { order: '$body.order', requestId: '$headers.x-request-id' } as Trigger['inputs'],
  },
  {
    id: 'orders-queue', flowId: 'order-approval', kind: 'queue', connection: 'kafka-prod', queue: 'orders.new', group: 'flow-orders',
    inputs: { order: '$body' } as Trigger['inputs'],
  },
  {
    id: 'digest-cron', flowId: 'heartbeat', kind: 'cron', cron: '0 0 9 * * 1-5', timezone: 'Europe/Madrid', inputs: {},
  },
];

// ---- Streams -------------------------------------------------------------
// `orders-report`: lista -> map -> filter -> aggregate -> collect; un `any` sobre el mismo stream (broadcast) da el
// booleano del If. Los pedidos de ejemplo se pasan como entrada `orders`.

const node = (id: string, kind: string, params: Record<string, JsonValue>, outputs: NodeDef['outputs'] = [], label?: string): NodeDef => ({
  id, kind, params, outputs, runPolicy: { ...RUN }, joinPolicy: 'all', ...(label ? { label } : {}),
});

const listAny = { type: 'list' as const, element: { type: 'any' as const } };

const ordersReport: FlowDef = {
  version: 1,
  id: 'orders-report',
  name: 'Informe de pedidos (stream)',
  revision: 2,
  inputs: [{ name: 'orders', target: 'orders', type: listAny }],
  outputs: [{ name: 'report', target: 'report', type: listAny }],
  nodes: [
    node('orders', 'stream.source.list', { path: 'orders' }, [], 'Pedidos'),
    node('prep', 'stream.map', { set: { total: { mul: [{ path: 'price' }, { path: 'qty' }] }, big: { gt: [{ mul: [{ path: 'price' }, { path: 'qty' }] }, 100] } } }, [], 'Calcular total'),
    node('large', 'stream.filter', { where: { path: 'big' } }, [], 'Solo grandes'),
    node('byCustomer', 'stream.aggregate', { aggregations: { orders: { op: 'count' }, revenue: { op: 'sum', path: 'total' } }, key: 'customer', emit: 'end' }, [], 'Por cliente'),
    node('report', 'stream.sink.collect', { limit: 100 }, [{ name: 'result', target: 'report', type: listAny }], 'Informe'),
    node('hasLarge', 'stream.sink.reduce', { op: 'any', path: 'big' }, [{ name: 'result', target: 'hasLarge', type: { type: 'bool' } }], 'Hay grandes'),
    node('alert', 'core.if', { path: 'hasLarge' }, [], 'Si hay pedidos grandes'),
    node('notify', 'queue.publish', { connection: 'kafka-prod', destination: 'orders.large', payload: 'report' }, [], 'Avisar'),
  ],
  edges: [
    { from: 'orders', port: 'out', to: 'prep' },
    { from: 'prep', port: 'out', to: 'large' },
    { from: 'prep', port: 'out', to: 'hasLarge' },
    { from: 'large', port: 'out', to: 'byCustomer' },
    { from: 'byCustomer', port: 'out', to: 'report' },
    { from: 'report', port: 'out', to: 'alert' },
    { from: 'hasLarge', port: 'out', to: 'alert' },
    { from: 'alert', port: 'then', to: 'notify' },
  ],
  layout: {
    orders: { x: 60, y: 120 }, prep: { x: 300, y: 120 }, large: { x: 540, y: 60 }, hasLarge: { x: 540, y: 220 },
    byCustomer: { x: 780, y: 60 }, report: { x: 1020, y: 60 }, alert: { x: 1260, y: 140 }, notify: { x: 1500, y: 140 },
  },
};

/** Fuente sin fin: un cron sin `maxTicks` nunca termina; sirve para probar Cancelar. */
const ticker: FlowDef = {
  version: 1,
  id: 'stream-ticker',
  name: 'Latido por segundo (stream sin fin)',
  revision: 1,
  inputs: [],
  outputs: [],
  nodes: [
    node('ticks', 'stream.source.cron', { expression: '* * * * * *' }, [], 'Cada segundo'),
    node('count', 'stream.sink.reduce', { op: 'count' }, [{ name: 'result', target: 'ticks', type: { type: 'num' } }], 'Contar'),
  ],
  edges: [{ from: 'ticks', port: 'out', to: 'count' }],
  layout: { ticks: { x: 80, y: 120 }, count: { x: 340, y: 120 } },
};

seedFlows.push(ordersReport, ticker);
