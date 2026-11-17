# flow/ui

Editor web del módulo de flujos (`modules/flow`): Vite + React + TypeScript + `@xyflow/react`, con [daisyUI 5](https://daisyui.com/) sobre Tailwind CSS v4. Contrato de la API y
de las formas JSON: [`../DESIGN.md`](../DESIGN.md) y los codecs de `../model`, `../engine` y `../http` (Scala). Este
README dice qué hace la UI y en qué se ha comprobado contra el backend real.

## Arrancar

```bash
cd modules/flow/ui
npm install
npm run dev          # http://localhost:5173 (con la API simulada en mock/)
```

Con el mock hay flujos de ejemplo: `order-approval` (`core.if`, dos `core.subflow`, tres revisiones), `notify-customer`,
`heartbeat` (lanzador cron y temporizador, habilitado), `nightly-report` (lanzador cron y `queue.publish`, deshabilitado).
Triggers: `orders-http` (http con `apiKey`, sync), `orders-queue` (cola Kafka) y `digest-cron` (cron). Conexiones:
`kafka-prod`, `artemis-dev`, `redis-cache`, `partner-api`.

## Build y comprobaciones

```bash
npm run build        # tsc --noEmit + vite build -> dist/
npx tsc --noEmit     # solo tipos
npm run preview      # sirve dist/ (sin API: la mayoría de vistas mostrarán errores de red)
```

`dist/` usa `base: './'`, así que el servidor puede servirlo desde el classpath en cualquier ruta. La navegación es
por hash (`#/`, `#/flow/{id}`, `#/flow/{id}?rev={n}`, `#/admin`, `#/instances`, `#/instance/{id}`), sin router del
servidor. Las llamadas van a `/api/...` absoluto (base path `/api`).

## Qué hay

- **Flujos** (`#/`): listar con su estado (habilitado / deshabilitado) e interruptor, crear, abrir, eliminar.
- **Cabecera del editor**: Habilitar/Deshabilitar (`POST /api/flows/{id}/enable|disable`), guardar, ejecutar, test, eliminar.
- **Programación** (pestaña del editor): lanzadores cron del flujo (`GET /api/flows/{id}/schedule`) con su expresión, zona,
  misfire y próxima ejecución (con cuenta atrás). Avisa si hay lanzadores y el flujo no está habilitado. Los horarios son los
  de la última revisión guardada.
- **Lienzo**: paleta agrupada por `category` (desde `GET /api/catalog`), arrastrar o usar «Añadir». Los puertos salen de
  `POST /api/catalog/{kind}/describe`. Conectar un nodo consigo mismo o cerrar un ciclo se rechaza en el cliente.
  El arrastre no recrea los datos de los nodos (`sameData` en `Canvas.tsx`).
- **Nodo `core.cron`**: es un *lanzador* si no tiene aristas de entrada, o un *temporizador* si las tiene (la etiqueta
  del nodo lo dice). Un temporizador en espera muestra «esperando hasta HH:MM» con cuenta atrás (`fireAt` de la instancia).
  Su formulario trae:
  - `expression`: ayuda, validación y vista previa de las próximas 5 ejecuciones. El cálculo es del cliente
    (`src/graph/cron.ts`, sin dependencias, misma gramática que `CronExpression.scala`). Es aproximado en los cambios de hora.
  - `timezone`: IANA con autocompletado de `Intl.supportedValuesOf('timeZone')`.
  - `misfire` (`fire-once` | `skip`), `maxWait` (`90s`, `30m`, `2h`, `1d`) e `inputs` (JSON).
- **`queue.publish`**: aparece con su `ConfigSchema`. `connection` sugiere las conexiones `kafka`, `activemq` y `redis`
  (el schema no marca los campos de conexión, así que se decide por el nodo y el nombre del campo). `payload` y `key`
  autocompletan las rutas del flujo.
- **Propiedades del nodo**: formulario generado desde `ConfigSchema` por `SpecForm` (ver «Especificación de UI»), salidas
  (`OutputDecl`), `runPolicy`, `joinPolicy`. Algunos nodos conservan un editor propio para su configuración compuesta: el
  cron (`expression` con vista previa y `timezone`), las expresiones de `stream.filter`/`stream.map`/`stream.aggregate`, los
  `inputs` de `core.subflow` (con el flujo llamado) y la conexión de `queue.publish`.
- **Campos de código**: `code` de `script.js` y `stream.script` (JavaScript) y `sql` de `db.*` (SQL) se editan en un
  `CodeEditor` sin dependencias: numeración de líneas, Tab y Mayús+Tab indentan, Enter mantiene la indentación y Esc saca el
  foco. El lenguaje es solo informativo (no hay resaltado de sintaxis). El texto de ayuda ya no muestra el sufijo `editor:...`.
- **Streams** (`src/graph/stream.ts`, `Canvas.tsx`): el lienzo distingue puertos de token y de stream (con el tipo de sus
  elementos), y las aristas de stream se dibujan gruesas, con su tipo al pasar el cursor. Conectar token con stream, o
  streams de tipos que no encajan, se rechaza en el cliente con el mismo criterio que el validador (`stream-token-mix`,
  `stream-type-mismatch`, `unknown-input`). Un segmento (`stream.segment`) se muestra como un paso de la auditoría: sus
  miembros toman el estado del anclaje, con sus métricas (`in`, `out`, `dropped`, `failed`, `latencyMs`) y los elementos
  descartados (*dead letters*) en el detalle del paso. El panel de ejecución muestra los contadores en vivo y permite cancelar
  (`POST /api/instances/{id}/cancel`), que sirve para un stream sin fin. En test, una fuente de stream recibe su lista de
  elementos como mock (`values.elements`).
- **Propiedades del flujo**: nombre, `inputs` y `outputs`.
- **Revisiones** (pestaña del editor): la API no tiene lista de revisiones (`GET /api/flows/{id}/revisions` no existe), así
  que se piden `GET /api/flows/{id}?revision=n` desde la última hacia atrás (hasta 30). Abrir una revisión antigua la muestra
  en solo lectura; «Restaurar» la guarda como una revisión nueva con `PUT /api/flows/{id}`.
- **Validación**: automática (con debounce) y al guardar. Los `FlowError` se pintan en el nodo y en el panel.
- **Ejecutar**: lanza `POST /api/flows/{id}/instances` sobre la revisión guardada y sigue `GET /api/instances/{id}/events`
  (SSE). Un reintento vuelve a correr la **misma** instancia (mismo `instanceId`): la UI vuelve a suscribirse. Si la
  instancia aún corre, el servidor responde 409 y la UI muestra su motivo.
- **Test**: entradas, mocks por nodo, `POST /api/flows/{id}/test` con el borrador y la traza paso a paso.
- **Triggers** (`#/admin`): edición completa de lo que la API admite. http: método, ruta, respuesta `async`/`sync`,
  `timeoutMs`, autenticación (`apiKey`, `bearer`, `hmac`; el secreto es `conexión.campo`, con autocompletado) y el mapeo de
  `inputs` como referencia (`$body.x`, `$query.x`, `$headers.x`, `$method`, `$path`), texto o JSON. queue: conexión, cola,
  grupo y tipo de destino. cron: expresión y zona, con vista previa de la próxima ejecución. manual: solo datos. Los 422 del
  servidor se muestran con su mensaje. La API no expone el estado de los triggers, así que la UI no lo muestra.
- **Conexiones** (`#/admin`): CRUD con el formulario del tipo elegido (`GET /api/connection-kinds`), también por `SpecForm`.
  Los secretos solo se escriben: si ya hay un valor guardado, el campo dice «guardado (no se muestra; vacío = conservar)» y
  dejarlo vacío lo conserva. Tipos incluidos en el mock: `postgres`, `mariadb`, `sqlite`, `claude`, `openai`, `codex`, además
  de las de triggers y colas.
- **Ejecuciones** (`#/instances`): historial con filtros (flujo, estado, origen, rango de fechas) y paginación por cursor.
- **Auditoría de una instancia** (`#/instance/{id}`): línea de tiempo, detalle de cada entrada (lecturas, escrituras con
  valor previo → nuevo, memoria, puerto, error, decisión del motor, `info` del nodo, p. ej. el mensaje que `queue.publish`
  habría enviado), y el grafo en el paso elegido. Un temporizador aparece como «esperando al horario». Los valores
  redactados se muestran como «••• redacted». Mientras la instancia corre se sigue por SSE.
  - **Tokens de `ai.agent`**: `node-finished.info.usage` (`inputTokens`, `outputTokens`) sale en el resumen de la línea de
    tiempo («N tokens»), en el detalle con el modelo, y como total de la instancia cuando hay varios.
  - **Logs de `script.js` y `stream.script`**: `node-finished.info.logs` sale en la línea de tiempo («N logs») y en el
    detalle como lista. El resto de `info` se sigue mostrando como JSON, sin repetir esas dos claves.
- Tema claro/oscuro (automático, o forzado con el botón de la cabecera; clase `.dark` en `<html>`), layout de una columna en pantallas pequeñas.

## Contrato con el backend (comprobado contra `flow-server.jar`)

- Tiempos: **números (epoch ms)** en instancias, auditoría, `nextFireAt` y `fireAt`. `toMillis` (`src/graph/time.ts`) también
  acepta texto ISO.
- Estados de nodo: el backend manda `dead`; la UI lo muestra como `skipped` (`getInstance` en `client.ts`).
- Espera de temporizador: `GET /api/instances/{id}` trae `nodes[id].fireAt`; la auditoría trae `node-waiting` con
  `decision: {reason: "scheduled", fireAt}`.
- Auditoría (`src/graph/audit.ts`, `normalizeEntry`): `node-finished` trae `outputs` (con `name`, `path`, `previous`, `value`),
  `status`, `info`, `simulation`; `node-started` trae `memory`, `params` y `reads`; `state-changed` trae `changes`;
  `skipped-by-run-policy` trae `rule`, `port`, `outputs`; `skipped-dead-branch` trae `causes` (`from`, `port`, `why`,
  `took`, `decidedWith`); `waiting-join` trae `pending`. La UI normaliza esas formas a las suyas y también acepta las del
  mock antiguo (`writes`, `memoryBefore`, `deadEdges`...).
- Eventos SSE: `type` y `kind` son la entrada; el nodo va en `node` (no `nodeId`).
- El stream SSE de una instancia **se cierra al terminar** (en el último `instance-finished`) y un reintento lo reabre con
  el replay completo (las entradas anteriores incluidas). `RunPanel` cierra su suscripción cuando `GET /api/instances/{id}`
  deja de decir `running`, y mientras corre relee el estado cada 2 s; no depende solo del SSE.
- Origen: `{type: manual|trigger|subflow|retry|cron, ...}` y la etiqueta corta (`trigger:<id>`, `retry:<id>`, `subflow:<id>`,
  `cron:<flowId>/<nodeId>`).
- Errores: `{errors: [{code, node, message}]}` (422, 404, 409). `errorMessage` (`client.ts`) los muestra.
- `layout` de `FlowDef` sí se guarda y vuelve en la respuesta del `PUT`.

## Especificación de UI (`ui` del ConfigSchema)

Cada campo de un `ConfigSchema` (nodos y tipos de conexión) puede traer una `ui` opcional. Dice cómo se dibuja, no qué valida.
El intérprete es un único componente, `SpecForm` (`src/components/SpecForm.tsx`), que renderiza cualquier lista de campos
con sus valores y devuelve el objeto de valores entero. Lo usan el panel de propiedades de los nodos (vía `ConfigForm`, que solo
añade los widgets compuestos de algunos nodos) y el formulario de conexiones. Los tipos están en `src/api/uiSpec.ts`.

```json
{"name": "sql", "type": {"type": "str"}, "description": "The statement, with ? for each parameter.",
 "ui": {"widget": "code", "language": "sql", "rows": 8, "help": "Con ? por cada parámetro"}}
```

| Campo de `ui` | Significado |
|---|---|
| `widget` | `text`, `textarea`, `code`, `password`, `number`, `checkbox`, `select`, `json`, `list`, `map`, `group` |
| `language` | Solo `code`: `javascript`, `sql` o `json`. Informativo; no hay resaltado |
| `rows` | Filas visibles de `textarea`, `code` y `json` |
| `placeholder` | Texto de ayuda dentro del campo vacío |
| `help` | Texto bajo el campo; sustituye a `description` |
| `options` | `[{value, label}]` para `select`; sin ellas, `oneOf` o sí/no en `bool` |
| `visibleWhen` | `{field, equals}`: el campo se muestra solo si `field` vale `equals` (el valor oculto se conserva) |
| `group` | Nombre de sección: los campos con el mismo `group` se agrupan bajo un encabezado, en el orden de aparición |
| `fields` | Subcampos: de un `group` (un objeto) o de cada elemento de un `list` (una lista de objetos) |
| `order` | Orden dentro del formulario (menor primero; empate, el orden del schema) |

- `list` sin `fields` es una lista de textos; con `fields`, cada elemento es un subformulario. `map` es clave → texto.
- **Sin `ui`** se infiere: `secret` → contraseña; `oneOf` → select; `bool` → sí/no; `num` → número; `str` → texto (con
  autocompletado de rutas si el nombre es de ruta); `list`, `obj` y `any` → JSON. Así los schemas antiguos siguen funcionando.
- **Fallback heredado**: si la descripción lleva `editor:javascript` o `editor:sql` (y se quiere igual que `widget: code`), se
  trata como `code` con ese lenguaje; `editor:text` es un `textarea`. El sufijo se quita del texto de ayuda.
- Secretos: un campo `secret` nunca muestra su valor. En conexiones, `secretsSet` marca los que ya están guardados.
- Lo que la `ui` no sabe dibujar (cron, expresiones de stream, subflujos) se resuelve con un `override` en `ConfigForm`.

## Estructura

```
src/api/types.ts        formas JSON (espejo de los codecs Scala)
src/api/client.ts       cliente REST, errores legibles y suscripción SSE
src/graph/audit.ts      normalización de la auditoría, decisiones y resúmenes
src/graph/cron.ts       parser de cron y próximas ejecuciones (cliente)
src/graph/time.ts       tiempos (epoch ms o ISO), cuenta atrás, zonas IANA
src/graph/graph.ts      ciclos, ids, plantillas de entrada, rutas en el estado
src/hooks/usePorts.ts   puertos por nodo (describe con caché)
src/api/uiSpec.ts       tipos de la `ui` de un campo, inferencia del widget y fallback `editor:...`
src/components/ui/      componentes propios sobre daisyUI: Button, Badge, Input/Textarea/Checkbox/Switch, FormField, SearchSelect
                        (desplegable con búsqueda, sustituye a todo <select>), Card, Alert, Tabs, Modal, Table, toast/Toaster
src/components/         lienzo, paleta, SpecForm (intérprete), CodeEditor, ConfigForm, TriggerForm, ScheduleFields, paneles
src/views/              lista, editor, administración, historial y auditoría
src/index.css           Tailwind, daisyUI y sus dos temas (`flow` y `flow-dark`, por `data-theme` en <html>)
src/styles.css          solo lo propio sin equivalente en daisyUI: lienzo, disposición del editor, auditoría, editor de código
mock/                   API simulada (solo `bun run dev` / `npm run dev`; no entra en dist/)
```

## Limitaciones conocidas

- Sin deshacer (undo) en el lienzo. En el editor de código, Ctrl+Z deshace las inserciones de Tab y Enter, pero no las
  indentaciones de varias líneas.
- El editor de código no resalta sintaxis (`language` solo es informativo).
- Los horarios de la pestaña Programación son los de la última revisión guardada.
- La vista previa de cron es aproximada en los cambios de hora (una hora que se repite cuenta una vez).
- `core.noop` («Unir ramas») existe solo en el mock, para tener un join; no es un nodo de `nodes-core`.
- Los nodos de tipo `read` (configurables en test) no existen todavía en `core`.
- `POST /api/catalog/{kind}/describe` también devuelve `problems` de la configuración; la UI todavía no los muestra.
- Restaurar una revisión conserva los secretos de cada nodo por id (`FlowSecrets.keep`): un nodo que ya no existe en la
  última revisión pierde sus secretos al restaurarla.

## Mock (`mock/`)

Reproduce las formas reales del backend (ver el apartado de contrato). Trae `ui` en los nodos `script.js`, `db.query`,
`db.execute` y `ai.agent` (en `mock/data.ts`) y en los tipos de conexión `postgres`, `mariadb`, `sqlite`, `claude`, `openai`
y `codex`; las conexiones de ejemplo `orders-db` (con contraseña guardada) y `claude-main` (con clave guardada) ilustran el
caso de «secreto guardado». Las ejecuciones de ejemplo de `order-approval`
(`inst-1`) muestran un fallo por timeout y dos reintentos sobre la misma instancia (uno que vuelve a llamar al subflujo y
otro que reutiliza su resultado por RunPolicy). Las ejecuciones nuevas se revelan entrada a entrada por SSE; un temporizador
espera de verdad (con un tope de 25 s por espera en el mock). `queue.publish` no conecta: en una ejecución registra en
`info` lo que se enviaría, y en un test, el mensaje completo.
