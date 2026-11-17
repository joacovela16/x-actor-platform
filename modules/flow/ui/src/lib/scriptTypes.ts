// Tipos para los editores de scripts, a partir de lo que declara el flujo: las entradas y las salidas de los nodos (con las
// entidades que nombran). JavaScript recibe un .d.ts para el servicio de TypeScript de Monaco; Scala, sugerencias ya hechas.
import type { DataType, EntityDef, JsonValue, OutputDecl } from '../api/types';

/** Lo que el editor de un script sabe del nodo y del flujo. */
export interface ScriptEnv {
  /** Tipo del nodo (`script.js`, `script.scala`, `stream.script`). */
  kind: string;
  /** Entradas del flujo y salidas de todos los nodos. */
  decls: OutputDecl[];
  /** Las salidas de este nodo (lo que su código debe escribir). */
  outputs: OutputDecl[];
  /** Puertos por los que puede salir. */
  ports: string[];
  /** `args` del nodo: lo que el código ve como `params`. */
  args: JsonValue | undefined;
  entities: Record<string, EntityDef>;
}

const IDENT = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
const key = (k: string) => (IDENT.test(k) ? k : JSON.stringify(k));
const entityName = (n: string) => `Entity_${n.replace(/[^A-Za-z0-9_]/g, '_')}`;

/** Un DataType como tipo de TypeScript. Las entidades van por nombre (se declaran aparte). */
export function tsType(t: DataType, entities: Record<string, EntityDef>): string {
  switch (t.type) {
    case 'bool': return 'boolean';
    case 'num': return 'number';
    case 'str': return 'string';
    case 'any': return 'any';
    case 'list': return `Array<${tsType(t.element, entities)}>`;
    case 'obj': return `{ ${Object.entries(t.fields).map(([k, v]) => `${key(k)}: ${tsType(v, entities)};`).join(' ')} }`;
    case 'entity': return entities[t.name] ? entityName(t.name) : 'any';
    case 'ref': {
      const e = entities[t.entity];
      const k = e?.fields.find((f) => f.name === e.key);
      return k ? tsType(k.type, entities) : 'any';
    }
  }
}

/** El tipo de un valor JSON de ejemplo (los `args`). */
function tsOfJson(v: JsonValue | undefined): string {
  if (v === null || v === undefined) return 'any';
  if (Array.isArray(v)) return v.length ? `Array<${tsOfJson(v[0])}>` : 'any[]';
  switch (typeof v) {
    case 'boolean': return 'boolean';
    case 'number': return 'number';
    case 'string': return 'string';
    default: return `{ ${Object.entries(v).map(([k, x]) => `${key(k)}: ${tsOfJson(x)};`).join(' ')} }`;
  }
}

interface Tree { type?: DataType; children: Map<string, Tree> }

function treeOf(decls: OutputDecl[]): Tree {
  const root: Tree = { children: new Map() };
  for (const d of decls) {
    let node = root;
    for (const seg of d.target.split('.').filter(Boolean)) {
      if (!node.children.has(seg)) node.children.set(seg, { children: new Map() });
      node = node.children.get(seg)!;
    }
    node.type = d.type;
  }
  return root;
}

/** Un nivel que solo se conoce por las rutas que lo cruzan (`order` de `order.total`) queda abierto: puede tener más campos. */
function tsOfTree(t: Tree, entities: Record<string, EntityDef>): string {
  const fields = [...t.children].map(([k, c]) => `${key(k)}: ${tsOfTree(c, entities)};`);
  if (!t.type) fields.push('[key: string]: any;');
  const obj = `{ ${fields.join(' ')} }`;
  if (!t.type) return obj;
  const own = tsType(t.type, entities);
  return t.children.size ? `${own} & ${obj}` : own;
}

function entityDecls(entities: Record<string, EntityDef>): string {
  return Object.values(entities)
    .map((e) => {
      const fields = e.fields.map((f) => {
        const doc = f.description || f.label ? `  /** ${(f.description ?? f.label ?? '').replace(/\*\//g, '* /')} */\n` : '';
        return `${doc}  ${key(f.name)}${f.required === false ? '?' : ''}: ${tsType(f.type, entities)};`;
      });
      const doc = e.description ? `/** ${e.description.replace(/\*\//g, '* /')} */\n` : `/** La entidad ${e.name}. */\n`;
      return `${doc}interface ${entityName(e.name)} {\n${fields.join('\n')}\n}`;
    })
    .join('\n\n');
}

/** El .d.ts de un script de JavaScript: `state`, `memory`, `params`, `log` y lo que puede devolver. */
export function jsDeclarations(env: ScriptEnv): string {
  const { entities } = env;
  const ports = env.ports.length ? env.ports.map((p) => JSON.stringify(p)).join(' | ') : '"out"';
  const outputs = env.outputs.map((o) => `  ${key(o.name)}: ${tsType(o.type, entities)};`).join('\n');
  const common = `${entityDecls(entities)}

/** Escribe una línea en el registro de la ejecución. */
declare function log(...values: any[]): void;

/** Los argumentos del nodo (su campo args). */
declare const params: ${tsOfJson(env.args)};
`;
  if (env.kind === 'stream.script') return `${common}\n/** El elemento del stream. */\ndeclare const element: any;\n`;
  return `${common}
/** El estado del flujo: sus entradas y lo que escriben los nodos. Solo lectura. */
interface FlowState ${tsOfTree(treeOf(env.decls), entities)}
declare const state: Readonly<FlowState>;

/** Lo que este nodo guardó en su ejecución anterior. Solo lectura: devuelve la nueva en memory. */
declare const memory: Readonly<Record<string, any>>;

/** Las salidas que declara este nodo. */
interface NodeValues {
${outputs}
}

/** Lo que puede devolver el código (o nada). */
interface NodeResult {
  values?: NodeValues;
  port?: ${ports};
  memory?: any;
}
`;
}

/** Una sugerencia del editor de Scala. `insert` es un snippet de Monaco. */
export interface ScalaItem {
  label: string;
  kind: 'path' | 'fn' | 'value' | 'snippet';
  insert: string;
  detail?: string;
  doc?: string;
}

/** Tipo de Scala para leer un valor de ese tipo con circe, y lo que vale si falta. */
function scalaRead(t: DataType, entities: Record<string, EntityDef>): [string, string] {
  switch (t.type) {
    case 'bool': return ['Boolean', 'false'];
    case 'num': return ['Double', '0d'];
    case 'str': return ['String', '""'];
    case 'list': return ['List[Json]', 'Nil'];
    case 'ref': {
      const e = entities[t.entity];
      const k = e?.fields.find((f) => f.name === e.key);
      return k ? scalaRead(k.type, entities) : ['Json', 'Json.Null'];
    }
    default: return ['Json', 'Json.Null'];
  }
}

function fieldsOf(t: DataType, entities: Record<string, EntityDef>): Record<string, DataType> | null {
  if (t.type === 'obj') return t.fields;
  if (t.type === 'entity') {
    const e = entities[t.name];
    return e ? Object.fromEntries(e.fields.map((f) => [f.name, f.type])) : null;
  }
  return null;
}

/** Las rutas con su tipo dentro de un valor, hasta `depth` niveles. */
function typedPaths(base: string, t: DataType, entities: Record<string, EntityDef>, depth = 4, inside: string[] = []): Array<[string, DataType]> {
  const own: Array<[string, DataType]> = [[base, t]];
  if (depth <= 0 || (t.type === 'entity' && inside.includes(t.name))) return own;
  const fields = fieldsOf(t, entities);
  if (!fields) return own;
  const next = t.type === 'entity' ? [...inside, t.name] : inside;
  return [...own, ...Object.entries(fields).flatMap(([n, ft]) => typedPaths(`${base}.${n}`, ft, entities, depth - 1, next))];
}

const q = (s: string) => JSON.stringify(s).replace(/\$/g, '\\$');

/** Las sugerencias de Scala: el API del script, las rutas del estado con su lectura tipada y `out` con las salidas del nodo. */
export function scalaCompletions(env: ScriptEnv): ScalaItem[] {
  const { entities } = env;
  const items: ScalaItem[] = [
    { label: 'state', kind: 'value', insert: 'state', detail: 'Json', doc: 'El estado del flujo.' },
    { label: 'memory', kind: 'value', insert: 'memory', detail: 'Json', doc: 'Lo que este nodo guardó la vez anterior.' },
    { label: 'params', kind: 'value', insert: 'params', detail: 'Json', doc: 'Los args del nodo.' },
    { label: 'log', kind: 'fn', insert: 'log(s"$0")', detail: 'String => Unit', doc: 'Escribe una línea en el registro.' },
    { label: 'via', kind: 'fn', insert: `via(\${1|${(env.ports.length ? env.ports : ['out']).map((p) => q(p).replace(/[,|]/g, '\\$&')).join(',')}|}, $0)`, detail: '(port, values*) => Json', doc: 'Sale por ese puerto escribiendo esos valores.' },
  ];
  const outs = env.outputs.map((o, i) => `${q(o.name)} -> \${${i + 1}:${scalaRead(o.type, entities)[1]}}.asJson`);
  items.push({
    label: 'out',
    kind: 'fn',
    insert: outs.length ? `out(${outs.join(', ')})` : 'out($0)',
    detail: '(values*) => Json',
    doc: outs.length ? `Sale por out escribiendo ${env.outputs.map((o) => o.name).join(', ')}.` : 'Sale por out escribiendo esos valores.',
  });
  const seen = new Set<string>();
  for (const d of env.decls) {
    for (const [path, t] of typedPaths(d.target, d.type, entities)) {
      if (seen.has(path)) continue;
      seen.add(path);
      const segs = path.split('.');
      const [tpe, dflt] = scalaRead(t, entities);
      const down = segs.slice(0, -1).map((s) => `.downField(${q(s)})`).join('');
      items.push({
        label: `state.${path}`,
        kind: 'path',
        insert: `state.hcursor${down}.get[${tpe}](${q(segs[segs.length - 1])}).getOrElse(\${1:${dflt}})`,
        detail: tpe,
        doc: `Lee state.${path} (${t.type}).`,
      });
    }
  }
  items.push(
    { label: 'hcursor.get', kind: 'snippet', insert: 'hcursor.get[${1:String}](${2:"field"})', detail: 'Decoder.Result[A]', doc: 'Lee un campo del objeto.' },
    { label: 'asJson', kind: 'snippet', insert: 'asJson', detail: 'Json', doc: 'Convierte un valor en Json (io.circe.syntax).' },
    { label: 'Json.obj', kind: 'snippet', insert: 'Json.obj(${1:"key"} -> ${2:value}.asJson)', detail: 'Json' },
  );
  return items;
}
