import { lazy, Suspense, type ReactNode } from 'react';
import type { ConfigField, JsonValue } from '../api/types';
import { resolveUi, sameValue, type ResolvedUi } from '../api/uiSpec';
import { CodeEditor } from './CodeEditor';
import { readFiles } from './scriptFiles';
import { JsonBox } from './JsonBox';
import { useEntities } from '../hooks/useEntities';
import { RefText } from './RefComplete';
import { Button } from '@/components/ui/button';
import { SearchSelect } from '@/components/ui/search-select';
import { Input } from '@/components/ui/input';
import { ComboInput } from '@/components/ui/combo-input';
import { Badge } from '@/components/ui/badge';
import { Checkbox } from '@/components/ui/input';

/**
 * Un widget específico de un nodo (p. ej. el editor de expresiones de `stream.filter`), que el intérprete no sabe dibujar
 * desde la `ui` sola. Devuelve `null` para dejar el campo al intérprete.
 */
export type Override = (
  field: ConfigField,
  ctx: { value: JsonValue | undefined; set: (v: JsonValue | undefined) => void; values: Record<string, JsonValue> },
) => ReactNode | null;

/** Lo que se pasa tal cual a los subcampos (`group`, `list` y `map` anidados). */
export interface SpecContext {
  /** Rutas del flujo, para autocompletar los campos que son rutas del estado. */
  paths: string[];
  /** Qué campos son rutas del estado (los que autocompletan). */
  isPathField?: (f: ConfigField) => boolean;
  /** Las referencias `${...}` que se pueden usar en los campos de texto (autocompletado al escribir `${`). */
  refs?: string[];
  /** Secretos ya guardados (conexiones): se indica que existen, nunca su valor. */
  secretsSet?: string[];
  overrides?: Override;
}

export interface SpecFormProps extends SpecContext {
  fields: ConfigField[];
  values: Record<string, JsonValue>;
  /** Recibe el objeto completo de valores tras cada cambio. */
  onChange: (values: Record<string, JsonValue>) => void;
  emptyText?: string;
}

// Monaco pesa: se carga la primera vez que se abre un nodo de código.
const ScriptEditor = lazy(() => import('./ScriptEditor'));

const isObj = (v: unknown): v is Record<string, JsonValue> => typeof v === 'object' && v !== null && !Array.isArray(v);

/** `visibleWhen`: el campo se muestra solo si otro campo vale lo que se pide. Los valores ocultos se conservan. */
function isVisible(f: ConfigField, values: Record<string, JsonValue>): boolean {
  const w = f.ui?.visibleWhen;
  return !w || sameValue(values[w.field], w.equals);
}

/** Orden por `ui.order` (estable) y secciones por `ui.group`, en el orden en que aparecen. */
function sections(fields: ConfigField[]): Array<{ name: string; fields: ConfigField[] }> {
  const ordered = fields
    .map((f, i) => ({ f, i }))
    .sort((a, b) => (a.f.ui?.order ?? 0) - (b.f.ui?.order ?? 0) || a.i - b.i)
    .map((x) => x.f);
  const byName = new Map<string, ConfigField[]>();
  for (const f of ordered) {
    const name = f.ui?.group ?? '';
    byName.set(name, [...(byName.get(name) ?? []), f]);
  }
  return [...byName].map(([name, list]) => ({ name, fields: list }));
}

/**
 * Intérprete de `ConfigSchema`: dibuja cada campo con el widget de su `ui` (o el inferido) y devuelve el objeto de
 * valores entero. Lo usan el panel de propiedades de los nodos y el formulario de conexiones.
 */
export function SpecForm(props: SpecFormProps) {
  const { fields, values, onChange, emptyText, ...ctx } = props;
  if (fields.length === 0) return <p className="text-xs text-base-content/60">{emptyText ?? 'Sin campos.'}</p>;
  // Los campos de archivos se editan como pestañas de su `code`.
  const filesFields = new Set(fields.flatMap((f) => (f.ui?.filesField ? [f.ui.filesField] : [])));
  const visible = fields.filter((f) => isVisible(f, values) && !filesFields.has(f.name));
  const setField = (name: string, v: JsonValue | undefined) => {
    const next = { ...values };
    if (v === undefined) delete next[name];
    else next[name] = v;
    onChange(next);
  };
  return (
    <div className="cfg">
      {sections(visible).map((s) => {
        const rows = s.fields.map((f) => (
          <FieldRow
            key={f.name}
            field={f}
            value={values[f.name]}
            values={values}
            onSet={(v) => setField(f.name, v)}
            onSetOther={setField}
            ctx={ctx}
          />
        ));
        return s.name ? (
          <section className="spec-group" key={`g-${s.name}`}>
            <h4>{s.name}</h4>
            {rows}
          </section>
        ) : (
          <div key="g-">{rows}</div>
        );
      })}
    </div>
  );
}

function FieldRow({
  field, value, values, onSet, onSetOther, ctx,
}: {
  field: ConfigField;
  value: JsonValue | undefined;
  values: Record<string, JsonValue>;
  onSet: (v: JsonValue | undefined) => void;
  /** Cambia otro campo (los archivos de un `code`). */
  onSetOther: (name: string, v: JsonValue | undefined) => void;
  ctx: SpecContext;
}) {
  const pathField = ctx.isPathField?.(field) ?? false;
  const ui = resolveUi(field, pathField);
  const override = ctx.overrides?.(field, { value, set: onSet, values });
  const required = field.required !== false;
  const hasDefault = field.default !== undefined;
  const isSet = value !== undefined;
  const canClear = isSet && (!required || hasDefault);
  const saved = ctx.secretsSet?.includes(field.name) ?? false;

  return (
    <div className="cfg-row">
      <div className="cfg-head">
        <span className="flex items-center gap-1.5 text-xs font-normal text-base-content/60 font-mono">
          {field.name}
          {required && !hasDefault && <span className="text-error" title="obligatorio"> *</span>}
        </span>
        <span className="text-xs text-base-content/60">{field.type.type}</span>
        {field.secret && <Badge variant="outline">secreto</Badge>}
        {saved && <Badge variant="success">guardado</Badge>}
        {canClear && <Button variant="ghost" size="xs" type="button" onClick={() => onSet(undefined)}>quitar</Button>}
      </div>
      {override ?? <Widget field={field} ui={ui} value={value} values={values} onSet={onSet} onSetOther={onSetOther} ctx={ctx} saved={saved} />}
      {ui.help && <span className="text-xs text-base-content/60">{ui.help}</span>}
      {hasDefault && <span className="text-xs text-base-content/60">por defecto: {JSON.stringify(field.default)}</span>}
    </div>
  );
}

function Widget({
  field, ui, value, values, onSet, onSetOther, ctx, saved,
}: {
  field: ConfigField;
  ui: ResolvedUi;
  value: JsonValue | undefined;
  values: Record<string, JsonValue>;
  onSet: (v: JsonValue | undefined) => void;
  onSetOther: (name: string, v: JsonValue | undefined) => void;
  ctx: SpecContext;
  saved: boolean;
}) {
  const { entities } = useEntities();
  const hasDefault = field.default !== undefined;
  const isSet = value !== undefined;
  const text = typeof value === 'string' ? value : '';
  const refs = ctx.refs ?? [];

  switch (ui.widget) {
    case 'text':
      if (ui.pathField)
        return (
          <ComboInput
            className="font-mono"
            aria-label={field.name}
            value={text}
            options={ctx.paths}
            placeholder={ui.placeholder ?? (hasDefault ? String(field.default) : undefined)}
            onChange={(t) => onSet(t === '' ? undefined : t)}
          />
        );
      return (
        <>
          <RefText
            aria-label={field.name}
            value={text}
            options={refs}
            placeholder={ui.placeholder ?? (hasDefault ? String(field.default) : '')}
            onChange={(t) => onSet(t === '' ? undefined : t)}
          />
        </>
      );

    case 'password':
      return (
        <Input
          type="password"
          
          autoComplete="new-password"
          aria-label={field.name}
          value={text}
          placeholder={
            saved && !isSet ? 'guardado (no se muestra; vacío = conservar)' : isSet ? 'valor definido (no se muestra)' : 'sin definir'
          }
          onChange={(e) => onSet(e.target.value === '' ? undefined : e.target.value)}
        />
      );

    case 'textarea':
      return (
        <RefText
          aria-label={field.name}
          rows={ui.rows ?? 4}
          value={text}
          options={refs}
          placeholder={ui.placeholder}
          onChange={(t) => onSet(t === '' ? undefined : t)}
        />
      );

    case 'files':
      return null;

    case 'code':
      if (ui.language === 'javascript' || ui.language === 'scala') {
        const filesField = ui.filesField;
        return (
          <Suspense fallback={<div className="skeleton h-40 w-full" aria-label="Cargando el editor" />}>
            <ScriptEditor
              language={ui.language}
              code={text}
              onCode={(t) => onSet(t === '' ? undefined : t)}
              files={filesField ? readFiles(values[filesField]) : undefined}
              onFiles={filesField ? (fs) => onSetOther(filesField, fs.length ? fs.map((f) => ({ name: f.name, code: f.code })) : undefined) : undefined}
              template={ui.template}
              rows={ui.rows}
              label={field.name}
            />
          </Suspense>
        );
      }
      return (
        <CodeEditor
          value={text}
          language={ui.language}
          rows={ui.rows}
          label={field.name}
          placeholder={ui.placeholder}
          refs={refs}
          onChange={(t) => onSet(t === '' ? undefined : t)}
        />
      );

    case 'number':
      return (
        // Texto y no `type="number"`, para admitir también una referencia a una variable de entorno o a un valor del flujo: `${env.DB_PORT}`, `${state.qty}`.
        <RefText
          inputMode="decimal"
          aria-label={field.name}
          value={typeof value === 'number' ? String(value) : typeof value === 'string' ? value : ''}
          options={refs}
          placeholder={hasDefault ? String(field.default) : ui.placeholder ?? ''}
          onChange={(t) => onSet(t.trim() === '' ? undefined : /^-?\d+(\.\d+)?$/.test(t.trim()) ? Number(t) : t)}
        />
      );

    case 'checkbox':
      return (
        <Checkbox
          aria-label={field.name}
          checked={value === true}
          onCheckedChange={(c) => onSet(c === true)}
        />
      );

    case 'select': {
      const options = ui.options
        ?? field.oneOf?.map((v) => ({ value: v, label: typeof v === 'string' ? v : JSON.stringify(v) }))
        ?? (field.type.type === 'bool' ? [{ value: true, label: 'sí' }, { value: false, label: 'no' }] : []);
      const idx = options.findIndex((o) => sameValue(o.value, value));
      const empty = field.type.type === 'bool' ? '— sin valor —' : '— elegir —';
      return (
        <SearchSelect
          aria-label={field.name}
          value={idx >= 0 ? String(idx) : ''}
          onChange={(v) => onSet(v === '' ? undefined : options[Number(v)].value)}
          options={[
            { value: '', label: hasDefault ? `(por defecto: ${JSON.stringify(field.default)})` : empty },
            ...options.map((o, i) => ({ value: String(i), label: o.label })),
          ]}
        />
      );
    }

    case 'json':
      return <JsonBox value={value} onChange={onSet} rows={ui.rows ?? 3} />;

    case 'entity': {
      // El nombre de una entidad del modelo de negocio. Una que ya no existe se sigue mostrando, marcada.
      const names = entities.map((e) => e.name);
      const missing = text && !names.includes(text) ? [{ value: text, label: `${text} (no existe)` }] : [];
      return (
        <SearchSelect
          aria-label={field.name}
          value={text}
          onChange={(v) => onSet(v === '' ? undefined : v)}
          options={[{ value: '', label: '— elegir entidad —' }, ...missing, ...names.map((n) => ({ value: n, label: n }))]}
        />
      );
    }

    case 'group': {
      const obj = isObj(value) ? value : {};
      return (
        <div className="spec-nested">
          <SpecForm
            fields={field.ui?.fields ?? []}
            values={obj}
            onChange={(v) => onSet(v)}
            paths={ctx.paths}
            isPathField={ctx.isPathField}
            secretsSet={ctx.secretsSet}
            overrides={ctx.overrides}
            emptyText="Sin subcampos."
          />
        </div>
      );
    }

    case 'list':
      return <ListWidget field={field} value={value} onSet={onSet} ctx={ctx} />;

    case 'map':
      return <MapWidget value={value} onSet={onSet} />;
  }
}

/** Lista: de objetos (si hay `ui.fields`, cada uno con su SpecForm) o de textos. */
function ListWidget({
  field, value, onSet, ctx,
}: {
  field: ConfigField;
  value: JsonValue | undefined;
  onSet: (v: JsonValue | undefined) => void;
  ctx: SpecContext;
}) {
  const items: JsonValue[] = Array.isArray(value) ? value : [];
  const itemFields = field.ui?.fields;
  const setItems = (next: JsonValue[]) => onSet(next);
  return (
    <div className="spec-list">
      {items.map((item, i) => (
        <div className="spec-item" key={i}>
          {itemFields ? (
            <SpecForm
              fields={itemFields}
              values={isObj(item) ? item : {}}
              onChange={(v) => setItems(items.map((x, j) => (j === i ? v : x)))}
              paths={ctx.paths}
              isPathField={ctx.isPathField}
              secretsSet={ctx.secretsSet}
              overrides={ctx.overrides}
            />
          ) : (
            <Input
              className="font-mono"
              aria-label={`${field.name} ${i + 1}`}
              value={typeof item === 'string' ? item : JSON.stringify(item)}
              onChange={(e) => setItems(items.map((x, j) => (j === i ? e.target.value : x)))}
            />
          )}
          <Button variant="ghost" size="xs" className="text-error hover:text-error" type="button" onClick={() => setItems(items.filter((_, j) => j !== i))}>
            quitar
          </Button>
        </div>
      ))}
      <Button variant="outline" size="xs" type="button" onClick={() => setItems([...items, itemFields ? {} : ''])}>
        + añadir
      </Button>
    </div>
  );
}

/** Mapa clave → texto. Renombrar una clave rehace el par en el mismo sitio. */
function MapWidget({ value, onSet }: { value: JsonValue | undefined; onSet: (v: JsonValue | undefined) => void }) {
  const entries: Array<[string, JsonValue]> = Object.entries(isObj(value) ? value : {});
  const setEntries = (rows: Array<[string, JsonValue]>) => onSet(Object.fromEntries(rows));
  const asText = (v: JsonValue) => (typeof v === 'string' ? v : JSON.stringify(v));
  return (
    <div className="spec-list">
      {entries.map(([k, v], i) => (
        <div className="spec-item" key={i}>
          <Input
            className="font-mono"
            aria-label="clave"
            value={k}
            onChange={(e) => setEntries(entries.map((x, j) => (j === i ? [e.target.value, x[1]] : x)))}
          />
          <Input
            
            aria-label="valor"
            value={asText(v)}
            onChange={(e) => setEntries(entries.map((x, j) => (j === i ? [x[0], e.target.value] : x)))}
          />
          <Button variant="ghost" size="xs" className="text-error hover:text-error" type="button" onClick={() => setEntries(entries.filter((_, j) => j !== i))}>
            quitar
          </Button>
        </div>
      ))}
      <Button variant="outline" size="xs" type="button" onClick={() => setEntries([...entries, ['', '']])}>
        + añadir
      </Button>
    </div>
  );
}
