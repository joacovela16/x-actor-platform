// Editor de expresiones de los nodos de stream (`Expr`: JSON, no código). Trabaja sobre el JSON guardado: no hay una
// forma intermedia que pueda perder algo al editar. Modo árbol (por defecto) y modo JSON avanzado, con validación con
// el mismo parser que el servidor (graph/expr.ts) y vista previa sobre un elemento de ejemplo que pega el usuario.
import { useMemo, useState } from 'react';
import type { JsonValue } from '../api/types';
import { BINARY, COLLECTION, UNARY, VARIADIC, evalExpr, filterElement, parseExpr, parseMapParams, transformElement } from '../graph/expr';
import { Button } from '@/components/ui/button';
import { FormField } from '@/components/ui/form-field';
import { SearchSelect } from '@/components/ui/search-select';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/input';
import { Checkbox } from '@/components/ui/input';
import { parseRelaxed } from '@/lib/relaxedJson';

type Json = JsonValue;
type Kind = 'path' | 'const' | 'exists' | 'op' | 'if' | 'over' | 'raw';

const isObj = (j: Json | undefined): j is { [k: string]: Json } => typeof j === 'object' && j !== null && !Array.isArray(j);

const OP_GROUPS: Array<{ label: string; ops: string[] }> = [
  { label: 'comparación', ops: [...BINARY] },
  { label: 'lógica', ops: ['and', 'or', 'not'] },
  { label: 'aritmética', ops: ['add', 'sub', 'mul', 'div', 'round'] },
  { label: 'texto y longitud', ops: ['concat', 'len', 'matches'] },
];

const OVER_LABELS: Record<string, string> = {
  sum: 'suma', count: 'cuántos', min: 'mínimo', max: 'máximo', all: 'todos cumplen', any: 'alguno cumple',
};

/** Qué forma tiene la expresión: campo, valor, operación, condicional, existencia o algo que el editor no entiende (JSON). */
export function kindOf(v: Json): Kind {
  if (!isObj(v)) return 'const';
  const keys = Object.keys(v);
  if (keys.length === 1 && keys[0] === 'const') return 'const';
  if (keys[0] === 'path' && keys.every((k, i) => i === 0 || k === 'default')) return 'path';
  if (keys.length === 1 && keys[0] === 'exists') return 'exists';
  if (keys.length === 1 && keys[0] === 'if') return 'if';
  if (keys.length === 1 && (COLLECTION as readonly string[]).includes(keys[0])) return 'over';
  if (keys.length === 1 && ((UNARY as readonly string[]).includes(keys[0]) || (BINARY as readonly string[]).includes(keys[0]) || (VARIADIC as readonly string[]).includes(keys[0]))) return 'op';
  return 'raw';
}

/** Valor de un nodo de tipo `const`: un valor suelto (no objeto) o `{const: x}`. Los objetos siempre van envueltos. */
function constValue(v: Json): Json {
  return isObj(v) && Object.keys(v)[0] === 'const' ? v.const : v;
}

function wrapConst(v: Json): Json {
  return isObj(v) ? { const: v } : v;
}

function opName(v: Json): string {
  return isObj(v) ? Object.keys(v)[0] : '';
}

function opArgs(v: Json): Json[] {
  const a = isObj(v) ? v[opName(v)] : undefined;
  return Array.isArray(a) ? a : [];
}

/** Árbol de una expresión, editable. Se llama a `onChange` con el JSON nuevo. */
export function ExprTree({ value, onChange, depth = 0 }: { value: Json; onChange: (v: Json) => void; depth?: number }) {
  const kind = kindOf(value);
  const setKind = (k: Kind) => {
    switch (k) {
      case 'path': return onChange({ path: 'campo' });
      case 'const': return onChange(0);
      case 'exists': return onChange({ exists: 'campo' });
      case 'if': return onChange({ if: [{ const: true }, 0, 0] });
      case 'op': return onChange({ eq: [0, 0] });
      case 'over': return onChange({ sum: ['lista', { path: 'campo' }] });
      default: return onChange(value);
    }
  };
  return (
    <div className="expr" style={{ marginLeft: depth ? 12 : 0 }}>
      <SearchSelect<Kind>
        className="expr-kind"
        aria-label="Tipo de expresión"
        value={kind}
        onChange={setKind}
        options={[
          { value: 'path', label: 'campo del elemento' },
          { value: 'const', label: 'valor' },
          { value: 'op', label: 'operación' },
          { value: 'if', label: 'si (condición)' },
          { value: 'exists', label: '¿existe el campo?' },
          { value: 'over', label: 'sobre los ítems de una lista' },
          ...(kind === 'raw' ? [{ value: 'raw' as Kind, label: 'JSON (no se edita en el árbol)' }] : []),
        ]}
      />
      {kind === 'path' && <PathExpr value={value} onChange={onChange} />}
      {kind === 'const' && <ConstExpr value={value} onChange={onChange} />}
      {kind === 'exists' && (
        <Input className="font-mono" aria-label="Campo que debe existir" value={String(isObj(value) ? value.exists ?? '' : '')} placeholder="campo.anidado"
          onChange={(e) => onChange({ exists: e.target.value })} />
      )}
      {kind === 'if' && <IfExpr value={value} onChange={onChange} depth={depth} />}
      {kind === 'op' && <OpExpr value={value} onChange={onChange} depth={depth} />}
      {kind === 'over' && <OverExpr value={value} onChange={onChange} depth={depth} />}
      {kind === 'raw' && <p className="text-xs text-base-content/60">Expresión que el árbol no representa: edítala en modo JSON.</p>}
    </div>
  );
}

function PathExpr({ value, onChange }: { value: Json; onChange: (v: Json) => void }) {
  const obj = isObj(value) ? value : {};
  const hasDefault = 'default' in obj;
  return (
    <div className="expr-line">
      <Input className="font-mono" aria-label="Campo del elemento" value={String(obj.path ?? '')} placeholder="order.total"
        onChange={(e) => onChange(hasDefault ? { path: e.target.value, default: obj.default } : { path: e.target.value })} />
      <label className="text-xs text-base-content/60 expr-default">
        <Checkbox checked={hasDefault} onCheckedChange={(c) => onChange(c === true ? { path: obj.path ?? '', default: null } : { path: obj.path ?? '' })} />
        por defecto si falta
      </label>
      {hasDefault && <ConstInline value={obj.default ?? null} onChange={(d) => onChange({ path: obj.path ?? '', default: d })} />}
    </div>
  );
}

/** Texto que se lee como JSON si lo es, o como texto. Lo que se muestra es lo que el usuario escribe. */
function ConstInline({ value, onChange }: { value: Json; onChange: (v: Json) => void }) {
  const [text, setText] = useState(() => (typeof value === 'string' ? value : JSON.stringify(value)));
  return (
    <Input className="font-mono w-40" aria-label="Valor" value={text}
      onChange={(e) => {
        setText(e.target.value);
        onChange(parseConst(e.target.value));
      }} />
  );
}

function ConstExpr({ value, onChange }: { value: Json; onChange: (v: Json) => void }) {
  const current = constValue(value);
  const [text, setText] = useState(() => (typeof current === 'string' ? current : JSON.stringify(current)));
  return (
    <div className="expr-line">
      <Input className="font-mono" aria-label="Valor constante" value={text}
        onChange={(e) => {
          setText(e.target.value);
          onChange(wrapConst(parseConst(e.target.value)));
        }} />
      <span className="text-xs text-base-content/60">número, true/false, null, [listas], {'{objetos}'} o texto sin comillas</span>
    </div>
  );
}

/** Lo que escribe el usuario como valor: JSON si se puede leer, si no, texto. */
export function parseConst(text: string): Json {
  try {
    return parseRelaxed(text) as Json;
  } catch {
    return text;
  }
}

function IfExpr({ value, onChange, depth }: { value: Json; onChange: (v: Json) => void; depth: number }) {
  const args = isObj(value) && Array.isArray(value.if) && value.if.length === 3 ? value.if : [{ const: true }, 0, 0];
  const set = (i: number, v: Json) => onChange({ if: args.map((a, j) => (j === i ? v : a)) });
  return (
    <div className="expr-args">
      <span className="text-xs text-base-content/60">si</span><ExprTree value={args[0]} onChange={(v) => set(0, v)} depth={depth + 1} />
      <span className="text-xs text-base-content/60">entonces</span><ExprTree value={args[1]} onChange={(v) => set(1, v)} depth={depth + 1} />
      <span className="text-xs text-base-content/60">si no</span><ExprTree value={args[2]} onChange={(v) => set(2, v)} depth={depth + 1} />
    </div>
  );
}

/** `{"sum": ["lines", <expresión sobre cada ítem>]}`: la ruta de la lista y, opcional, lo que se calcula de cada ítem. */
function OverExpr({ value, onChange, depth }: { value: Json; onChange: (v: Json) => void; depth: number }) {
  const name = opName(value);
  const args = opArgs(value);
  const list = typeof args[0] === 'string' ? args[0] : '';
  const hasBody = args.length > 1;
  const write = (n: string, l: string, body: Json | undefined) => onChange({ [n]: body === undefined ? [l] : [l, body] });
  return (
    <div className="expr-args">
      <div className="expr-line">
        <SearchSelect
          className="expr-op"
          aria-label="Operación sobre la lista"
          value={name}
          onChange={(n) => write(n, list, hasBody ? args[1] : undefined)}
          options={COLLECTION.map((o) => ({ value: o, label: `${o} (${OVER_LABELS[o]})` }))}
        />
        <Input className="font-mono" aria-label="Lista" value={list} placeholder="lines" onChange={(e) => write(name, e.target.value, hasBody ? args[1] : undefined)} />
      </div>
      <label className="text-xs text-base-content/60 expr-default">
        <Checkbox checked={hasBody} onCheckedChange={(c) => write(name, list, c === true ? { path: 'campo' } : undefined)} />
        {name === 'count' || name === 'all' || name === 'any' ? 'condición sobre cada ítem' : 'valor de cada ítem'}
      </label>
      {hasBody && <ExprTree value={args[1]} onChange={(v) => write(name, list, v)} depth={depth + 1} />}
    </div>
  );
}

function OpExpr({ value, onChange, depth }: { value: Json; onChange: (v: Json) => void; depth: number }) {
  const name = opName(value);
  const args = opArgs(value);
  const unary = (UNARY as readonly string[]).includes(name);
  const binary = (BINARY as readonly string[]).includes(name);
  const setName = (n: string) => {
    const count = (UNARY as readonly string[]).includes(n) ? 1 : (BINARY as readonly string[]).includes(n) ? 2 : Math.max(2, args.length);
    const next = Array.from({ length: count }, (_, i) => args[i] ?? 0);
    onChange({ [n]: (UNARY as readonly string[]).includes(n) ? next[0] : next });
  };
  return (
    <div className="expr-args">
      <SearchSelect
        className="expr-op"
        aria-label="Operación"
        value={name}
        onChange={setName}
        options={OP_GROUPS.flatMap((g) => g.ops.map((o) => ({ value: o, label: o, group: g.label })))}
      />
      {unary
        ? <ExprTree value={isObj(value) ? (value[name] as Json) : 0} onChange={(v) => onChange({ [name]: v })} depth={depth + 1} />
        : (
          <>
            {args.map((a, i) => (
              <div className="expr-arg" key={i}>
                <ExprTree value={a} depth={depth + 1} onChange={(v) => onChange({ [name]: args.map((x, j) => (j === i ? v : x)) })} />
                {!binary && args.length > 2 && (
                  <Button variant="ghost" size="xs" className="text-error hover:text-error" type="button" onClick={() => onChange({ [name]: args.filter((_, j) => j !== i) })}>quitar</Button>
                )}
              </div>
            ))}
            {!binary && <Button variant="ghost" size="xs" type="button" onClick={() => onChange({ [name]: [...args, 0] })}>+ valor</Button>}
          </>
        )}
    </div>
  );
}

/** Resultado de una expresión sobre un elemento: el valor, o por qué no se puede calcular. */
export function previewValue(expr: Json, sample: Json): { ok: true; text: string } | { ok: false; error: string } {
  const parsed = parseExpr(expr);
  if (!parsed.ok) return { ok: false, error: parsed.error };
  const r = evalExpr(parsed.expr, sample);
  return r.ok ? { ok: true, text: JSON.stringify(r.v) } : { ok: false, error: r.error };
}

/** Texto del elemento de ejemplo: JSON válido o un aviso. */
function useSample(initial: string) {
  const [text, setText] = useState(initial);
  const parsed = useMemo(() => {
    try {
      return { ok: true as const, value: parseRelaxed(text) as Json };
    } catch (e) {
      return { ok: false as const, error: (e as Error).message };
    }
  }, [text]);
  return { text, setText, parsed };
}

export const SAMPLE_ELEMENT = '{\n  "price": 25,\n  "qty": 4,\n  "customer": { "name": "Ana" }\n}';

function SampleBox({ sample }: { sample: ReturnType<typeof useSample> }) {
  return (
    <FormField label="Elemento de ejemplo (JSON)">
      <Textarea className={`font-mono ${sample.parsed.ok ? '' : 'border-error'}`} rows={4} spellCheck={false} aria-label="Elemento de ejemplo"
        value={sample.text} onChange={(e) => sample.setText(e.target.value)} />
      {!sample.parsed.ok && <span className="my-0.5 text-xs text-error">JSON no válido: {sample.parsed.error}</span>}
    </FormField>
  );
}

/**
 * Campo `Expr` completo: árbol o JSON avanzado, y vista previa sobre un elemento de ejemplo. `preview` dice qué
 * significa el resultado (p. ej. para un `where`: pasa o se descarta).
 */
export function ExprField({ value, onChange, label, preview, sampleText, sampleLabel = 'Probar con un elemento', optional = false }: {
  value: Json | undefined;
  onChange: (v: Json | undefined) => void;
  label: string;
  /** `filter`: pasa o se descarta (stream); `condition`: se cumple o no (reglas y guardas). */
  preview: 'value' | 'filter' | 'condition';
  /** Ejemplo inicial de la vista previa (JSON). */
  sampleText?: string;
  sampleLabel?: string;
  /** Se puede quitar (queda sin expresión). */
  optional?: boolean;
}) {
  const [advanced, setAdvanced] = useState(false);
  const [jsonText, setJsonText] = useState(() => (value === undefined ? '' : JSON.stringify(value, null, 2)));
  const [jsonError, setJsonError] = useState<string | null>(null);
  const sample = useSample(sampleText ?? SAMPLE_ELEMENT);
  const current: Json = value ?? 0;
  const exprError = value === undefined ? 'falta la expresión' : parseExpr(value).ok ? null : (parseExpr(value) as { error: string }).error;

  let result: string | null = null;
  if (sample.parsed.ok && value !== undefined && !exprError) {
    const parsed = parseExpr(value);
    if (parsed.ok) {
      const r = evalExpr(parsed.expr, sample.parsed.value);
      if (!r.ok) result = `error: ${r.error}`;
      else if (preview === 'filter') {
        const f = filterElement(parsed.expr, sample.parsed.value);
        result = f.ok ? (f.pass ? 'pasa el filtro' : 'se descarta') : `error: ${f.error}`;
      } else if (preview === 'condition') {
        result = typeof r.v === 'boolean' ? (r.v ? 'se cumple' : 'no se cumple') : `error: da ${JSON.stringify(r.v)}, no un booleano`;
      } else result = `→ ${JSON.stringify(r.v)}`;
    }
  }

  return (
    <div className="expr-field">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="flex items-center gap-1.5 text-xs font-normal text-base-content/60">{label}</span>
        <div className="flex flex-wrap items-center gap-2">
          <Button variant="ghost" size="xs" type="button"  onClick={() => setAdvanced(false)}>editor</Button>
          <Button variant="ghost" size="xs" type="button"  onClick={() => { setJsonText(value === undefined ? '' : JSON.stringify(value, null, 2)); setJsonError(null); setAdvanced(true); }}>JSON</Button>
          {optional && value !== undefined && (
            <Button variant="ghost" size="xs" type="button" onClick={() => onChange(undefined)}>quitar</Button>
          )}
        </div>
      </div>
      {!advanced && (
        value === undefined
          ? <Button variant="ghost" size="xs" type="button" onClick={() => onChange(current)}>+ escribir expresión</Button>
          : <ExprTree value={value} onChange={(v) => onChange(v)} />
      )}
      {advanced && (
        <>
          <Textarea
            className={`font-mono ${jsonError || exprError ? 'border-error' : ''}`}
            rows={5}
            spellCheck={false}
            aria-label={`${label} (JSON)`}
            value={jsonText}
            onChange={(e) => {
              setJsonText(e.target.value);
              try {
                const j = parseRelaxed(e.target.value) as Json;
                const p = parseExpr(j);
                if (!p.ok) {
                  setJsonError(p.error);
                  return;
                }
                setJsonError(null);
                onChange(j);
              } catch (err) {
                setJsonError((err as Error).message);
              }
            }}
          />
          {jsonError && <span className="my-0.5 text-xs text-error">{jsonError}</span>}
        </>
      )}
      {!advanced && exprError && value !== undefined && <span className="my-0.5 text-xs text-error">{exprError}</span>}
      <details className="expr-preview">
        <summary className="text-xs text-base-content/60">{sampleLabel}</summary>
        <SampleBox sample={sample} />
        {value !== undefined && result && <p className="font-mono">{result}</p>}
      </details>
    </div>
  );
}

/** `set` de `stream.map`: filas campo (ruta del elemento) -> expresión, con vista previa del elemento resultante. */
export function MapSetEditor({ value, onChange }: { value: Json; onChange: (v: Json) => void }) {
  const obj: { [k: string]: Json } = isObj(value) ? value : {};
  const entries = Object.entries(obj);
  const sample = useSample(SAMPLE_ELEMENT);
  const setRows = (rows: Array<[string, Json]>) => onChange(Object.fromEntries(rows));
  const rows = entries;
  const [advanced, setAdvanced] = useState(false);
  const [jsonText, setJsonText] = useState(() => JSON.stringify(obj, null, 2));
  const [jsonError, setJsonError] = useState<string | null>(null);

  const parsedAll = parseMapParams({ set: obj });
  let preview: string | null = null;
  if (sample.parsed.ok && parsedAll.ok) {
    const t = transformElement(parsedAll.p, sample.parsed.value);
    preview = t.ok ? JSON.stringify(t.v) : `error: ${t.error}`;
  }

  return (
    <div className="map-set">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="flex items-center gap-1.5 text-xs font-normal text-base-content/60">Campos calculados</span>
        <div className="flex flex-wrap items-center gap-2">
          <Button variant="ghost" size="xs" type="button"  onClick={() => setAdvanced(false)}>filas</Button>
          <Button variant="ghost" size="xs" type="button"  onClick={() => { setJsonText(JSON.stringify(obj, null, 2)); setJsonError(null); setAdvanced(true); }}>JSON</Button>
        </div>
      </div>
      {advanced && (
        <>
          <Textarea className={`font-mono ${jsonError ? 'border-error' : ''}`} rows={5} spellCheck={false} aria-label="Campos calculados (JSON)" value={jsonText}
            onChange={(e) => {
              setJsonText(e.target.value);
              try {
                const j = parseRelaxed(e.target.value) as Json;
                if (!isObj(j)) return setJsonError('debe ser un objeto: campo → expresión');
                const p = parseMapParams({ set: j });
                if (!p.ok) return setJsonError(p.error);
                setJsonError(null);
                onChange(j);
              } catch (err) {
                setJsonError((err as Error).message);
              }
            }} />
          {jsonError && <span className="my-0.5 text-xs text-error">{jsonError}</span>}
        </>
      )}
      {!advanced && (
        <>
          {rows.length === 0 && <p className="text-xs text-base-content/60">Sin campos calculados: el elemento pasa tal cual (o con lo que diga «quitar» y «reemplazar»).</p>}
          {rows.map(([field, expr], i) => (
            <div className="map-row" key={i}>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <Input className="font-mono" aria-label="Campo que se calcula" value={field} placeholder="total"
                  onChange={(e) => setRows(rows.map((r, j): [string, Json] => (j === i ? [e.target.value, r[1]] : r)))} />
                <Button variant="ghost" size="xs" className="text-error hover:text-error" type="button" onClick={() => setRows(rows.filter((_, j) => j !== i))}>quitar</Button>
              </div>
              <ExprTree value={expr} onChange={(v) => setRows(rows.map((r, j): [string, Json] => (j === i ? [r[0], v] : r)))} />
            </div>
          ))}
          <Button variant="ghost" size="xs" type="button" onClick={() => setRows([...rows, [uniqueField(obj), { path: 'campo' }]])}>+ campo</Button>
        </>
      )}
      <details className="expr-preview">
        <summary className="text-xs text-base-content/60">Probar con un elemento</summary>
        <SampleBox sample={sample} />
        {preview && <p className="font-mono">→ {preview}</p>}
      </details>
    </div>
  );
}

function uniqueField(obj: { [k: string]: Json }): string {
  let i = 1;
  while (`campo${i}` in obj) i++;
  return `campo${i}`;
}

const AGG_OPS = ['count', 'sum', 'avg', 'min', 'max', 'first', 'last', 'collect'] as const;

/** `aggregations` de `stream.aggregate`: nombre -> {op, path}. `count` no lleva ruta. */
export function AggregationsEditor({ value, onChange }: { value: Json | undefined; onChange: (v: Json | undefined) => void }) {
  const obj: { [k: string]: Json } = isObj(value) ? value : {};
  const rows = Object.entries(obj);
  const set = (next: Array<[string, Json]>) => onChange(next.length ? Object.fromEntries(next) : undefined);
  return (
    <div className="aggregations">
      {rows.length === 0 && <p className="text-xs text-base-content/60">Sin agregados: hay que declarar al menos uno.</p>}
      {rows.map(([name, spec], i) => {
        const s: { [k: string]: Json } = isObj(spec) ? spec : {};
        const op = typeof s.op === 'string' ? s.op : 'count';
        return (
          <div className="agg-row" key={i}>
            <Input className="font-mono" aria-label="Nombre del agregado" value={name}
              onChange={(e) => set(rows.map((r, j): [string, Json] => (j === i ? [e.target.value, spec] : r)))} />
            <SearchSelect
              aria-label="Operación del agregado"
              value={op}
              onChange={(v) => set(rows.map((r, j): [string, Json] => (j === i ? [r[0], v === 'count' ? { op: 'count' } : { op: v, path: typeof s.path === 'string' ? s.path : 'campo' }] : r)))}
              options={AGG_OPS.map((o) => ({ value: o, label: o }))}
            />
            {op !== 'count' && (
              <Input className="font-mono" aria-label="Campo del agregado" value={typeof s.path === 'string' ? s.path : ''} placeholder="campo del elemento"
                onChange={(e) => set(rows.map((r, j): [string, Json] => (j === i ? [r[0], { op, path: e.target.value }] : r)))} />
            )}
            <Button variant="ghost" size="xs" className="text-error hover:text-error" type="button" onClick={() => set(rows.filter((_, j) => j !== i))}>quitar</Button>
          </div>
        );
      })}
      <Button variant="ghost" size="xs" type="button" onClick={() => set([...rows, [`total${rows.length + 1}`, { op: 'count' } as Json]])}>+ agregado</Button>
    </div>
  );
}
