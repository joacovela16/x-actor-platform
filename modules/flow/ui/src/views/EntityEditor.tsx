import { useContext, useEffect, useMemo, useState } from 'react';
import { ArrowDown, ArrowLeft, ArrowUp, Plus, Save, Trash2 } from 'lucide-react';
import { ApiError, api, errorMessage } from '../api/client';
import type {
  DataType, EntityCheck, EntityConstraints, EntityDef, EntityError, EntityField, EntityKind, EntityRule, EntityStateMachine,
  EntityTransition, FlowError, JsonValue,
} from '../api/types';
import { AppContext } from '../App';
import { DataTypeEditor } from '../components/DataTypeEditor';
import { ExprField } from '../components/ExprEditor';
import { exampleFor } from '../graph/example';
import { renderType, templateFor } from '../graph/graph';
import { reloadEntities, useEntities } from '../hooks/useEntities';
import { KindBadge, SourceBadge } from './EntitiesView';
import { Alert } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardBody } from '@/components/ui/card';
import { FormField } from '@/components/ui/form-field';
import { Checkbox, Input, Textarea } from '@/components/ui/input';
import { SearchSelect } from '@/components/ui/search-select';
import { Tabs } from '@/components/ui/tabs';
import { parseRelaxed } from '@/lib/relaxedJson';

type Tab = 'fields' | 'rules' | 'states' | 'test' | 'json';

function newEntity(name: string, kind: EntityKind): EntityDef {
  return kind === 'root'
    ? { name, kind, key: 'id', fields: [{ name: 'id', type: { type: 'str' } }] }
    : { name, kind, fields: [{ name: 'campo', type: { type: 'str' } }] };
}

/** La entidad tal como se envía: sin lo que agrega el servidor ni listas vacías. */
function clean(e: EntityDef): EntityDef {
  const out: EntityDef = { ...e };
  if (out.kind === 'value') delete out.key;
  if (!out.rules?.length) delete out.rules;
  if (!out.label) delete out.label;
  if (!out.description) delete out.description;
  return out;
}

const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

/** Editor de una entidad: datos generales y pestañas para campos, reglas, estados, una prueba con un valor y el JSON. Las de
 *  código o de un plugin se ven pero no se cambian. */
export function EntityEditor({ name, isNew }: { name: string; isNew?: EntityKind }) {
  const { toast, navigate, setDirty } = useContext(AppContext);
  const { byName } = useEntities();
  const [doc, setDoc] = useState<EntityDef | null>(null);
  const [saved, setSaved] = useState<EntityDef | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [errors, setErrors] = useState<EntityError[]>([]);
  const [affected, setAffected] = useState<Array<{ flowId: string; errors: FlowError[] }>>([]);
  const [tab, setTab] = useState<Tab>('fields');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.getEntity(name).then(
      (e) => {
        setDoc(e);
        setSaved(e);
      },
      (e) => {
        if (e instanceof ApiError && e.status === 404 && isNew) setDoc(newEntity(name, isNew));
        else setLoadError(errorMessage(e));
      },
    );
  }, [name, isNew]);

  const readOnly = !!doc?.source && doc.source !== 'store';
  const dirty = !!doc && !readOnly && !same(clean(doc), saved ? clean(saved) : null);
  useEffect(() => setDirty(dirty), [dirty, setDirty]);
  useEffect(() => () => setDirty(false), [setDirty]);

  // Los problemas de la definición, mientras se edita.
  useEffect(() => {
    if (!doc || readOnly) return;
    const t = window.setTimeout(() => {
      api.validateEntity(clean(doc)).then(setErrors, () => {});
    }, 400);
    return () => window.clearTimeout(t);
  }, [doc, readOnly]);

  // Las entidades que ve el editor: las guardadas con esta en su versión de ahora.
  const entities = useMemo(() => (doc ? { ...byName, [doc.name]: doc } : byName), [byName, doc]);
  const sample = useMemo(() => (doc ? JSON.stringify(templateFor({ type: 'entity', name: doc.name }, entities), null, 2) : '{}'), [doc, entities]);

  if (loadError) return <section className="mx-auto w-full max-w-5xl p-6"><Alert variant="error">{loadError}</Alert></section>;
  if (!doc) return <section className="mx-auto w-full max-w-5xl p-6 text-sm text-base-content/60">Cargando…</section>;

  const update = (patch: Partial<EntityDef>) => setDoc((d) => (d ? { ...d, ...patch } : d));
  const keyCandidates = doc.fields.filter((f) => (f.type.type === 'str' || f.type.type === 'num') && f.computed === undefined);

  const save = async () => {
    setBusy(true);
    try {
      const r = await api.saveEntity(clean(doc));
      if (!r.ok) {
        setErrors(r.errors);
        toast('La entidad tiene problemas: no se guardó.', 'error');
        return;
      }
      setDoc(r.entity);
      setSaved(r.entity);
      setAffected(r.affected);
      setErrors([]);
      await reloadEntities();
      toast(`Guardada la revisión ${r.entity.revision}`, 'ok');
      if (isNew) window.history.replaceState(null, '', `#/entity/${encodeURIComponent(r.entity.name)}`);
    } catch (e) {
      toast(`No se pudo guardar: ${errorMessage(e)}`, 'error');
    } finally {
      setBusy(false);
    }
  };

  const remove = async () => {
    if (!window.confirm(`¿Eliminar la entidad '${doc.name}' y sus revisiones?`)) return;
    try {
      await api.deleteEntity(doc.name);
      await reloadEntities();
      setDirty(false);
      toast('Entidad eliminada', 'ok');
      navigate('#/entities');
    } catch (e) {
      toast(`No se pudo eliminar: ${errorMessage(e)}`, 'error');
    }
  };

  const general = errors.filter((e) => !e.field);
  const byField = (f: string) => errors.filter((e) => e.field === f);

  return (
    <section className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <Button variant="ghost" size="icon-sm" aria-label="Volver a las entidades" onClick={() => navigate('#/entities')}><ArrowLeft /></Button>
          <h1 className="font-mono text-xl font-semibold">{doc.name}</h1>
          <KindBadge kind={doc.kind} />
          <SourceBadge source={doc.source} />
          {doc.revision !== undefined && <span className="text-xs text-base-content/60">revisión {doc.revision}</span>}
          {dirty && <Badge variant="warning">sin guardar</Badge>}
        </div>
        {!readOnly && (
          <div className="flex items-center gap-2">
            {saved && <Button variant="ghost" onClick={remove}><Trash2 /> Eliminar</Button>}
            <Button onClick={save} disabled={busy || !dirty}><Save /> Guardar</Button>
          </div>
        )}
      </div>

      {readOnly && (
        <Alert variant="info" className="mb-3">
          Esta entidad está definida en {doc.source === 'code' ? 'el código del servidor' : `un plugin (${doc.source?.replace(/^plugin:/, '')})`}: se usa
          como las demás pero no se cambia aquí.
        </Alert>
      )}
      {general.length > 0 && (
        <Alert variant="error" className="mb-3">
          <ul className="list-disc pl-4">{general.map((e, i) => <li key={i}>{e.message}</li>)}</ul>
        </Alert>
      )}
      {affected.length > 0 && (
        <Alert variant="warning" className="mb-3">
          <div>
            Con este cambio dejan de validar estos flujos (siguen publicados, pero fallarán al correr lo que ya no encaja):
            <ul className="list-disc pl-4">
              {affected.map((a) => (
                <li key={a.flowId}>
                  <a className="link font-mono" href={`#/flow/${encodeURIComponent(a.flowId)}`}>{a.flowId}</a>: {a.errors.map((e) => e.message).join('; ')}
                </li>
              ))}
            </ul>
          </div>
        </Alert>
      )}

      <Card className="mb-4">
        <CardBody className="grid gap-3 sm:grid-cols-2">
          <FormField label="Etiqueta">
            <Input value={doc.label ?? ''} disabled={readOnly} placeholder="Pedido" onChange={(e) => update({ label: e.target.value })} />
          </FormField>
          <FormField label="Tipo">
            <SearchSelect<EntityKind>
              aria-label="Tipo de entidad"
              value={doc.kind}
              disabled={readOnly}
              onChange={(kind) => update(kind === 'root' ? { kind, key: doc.key ?? keyCandidates[0]?.name } : { kind, key: undefined })}
              options={[{ value: 'root', label: 'raíz (con clave)' }, { value: 'value', label: 'valor (sin identidad)' }]}
            />
          </FormField>
          <FormField label="Descripción" className="sm:col-span-1">
            <Input value={doc.description ?? ''} disabled={readOnly} onChange={(e) => update({ description: e.target.value })} />
          </FormField>
          {doc.kind === 'root' && (
            <FormField label="Clave" hint="El campo (texto o número, obligatorio) que identifica cada valor. Una referencia guarda su valor.">
              <SearchSelect
                aria-label="Campo clave"
                value={doc.key ?? ''}
                disabled={readOnly}
                onChange={(key) => update({ key })}
                options={[{ value: '', label: '— elegir —' }, ...keyCandidates.map((f) => ({ value: f.name, label: f.name }))]}
              />
            </FormField>
          )}
        </CardBody>
      </Card>

      <Tabs<Tab>
        className="mb-3"
        value={tab}
        onChange={setTab}
        items={[
          { value: 'fields', label: `Campos (${doc.fields.length})` },
          { value: 'rules', label: `Reglas (${doc.rules?.length ?? 0})` },
          { value: 'states', label: doc.states ? `Estados (${doc.states.states.length})` : 'Estados' },
          { value: 'test', label: 'Probar' },
          { value: 'json', label: 'JSON' },
        ]}
      />

      <fieldset disabled={readOnly} className="min-w-0">
        {tab === 'fields' && (
          <FieldsTab fields={doc.fields} keyField={doc.kind === 'root' ? doc.key : undefined} errorsOf={byField} sample={sample}
            onChange={(fields) => update({ fields })} />
        )}
        {tab === 'rules' && <RulesTab rules={doc.rules ?? []} sample={sample} onChange={(rules) => update({ rules })} />}
        {tab === 'states' && <StatesTab entity={doc} sample={sample} onChange={(states) => update({ states })} />}
      </fieldset>
      {tab === 'test' && <TestTab entity={doc} entities={entities} sample={sample} />}
      {tab === 'json' && <JsonTab entity={doc} readOnly={readOnly} onChange={(e) => setDoc({ ...e, name: doc.name, source: doc.source, revision: doc.revision })} />}
    </section>
  );
}

// ---- campos ----

function FieldsTab({ fields, keyField, errorsOf, sample, onChange }: {
  fields: EntityField[];
  keyField?: string;
  errorsOf: (field: string) => EntityError[];
  sample: string;
  onChange: (fields: EntityField[]) => void;
}) {
  const set = (i: number, f: EntityField) => onChange(fields.map((x, j) => (j === i ? f : x)));
  const move = (i: number, by: number) => {
    const next = [...fields];
    const [f] = next.splice(i, 1);
    next.splice(i + by, 0, f);
    onChange(next);
  };
  return (
    <div className="flex flex-col gap-3">
      {fields.map((f, i) => (
        <FieldCard key={i} field={f} isKey={f.name === keyField} errors={errorsOf(f.name)} sample={sample}
          onChange={(nf) => set(i, nf)}
          onRemove={() => onChange(fields.filter((_, j) => j !== i))}
          onUp={i > 0 ? () => move(i, -1) : undefined}
          onDown={i < fields.length - 1 ? () => move(i, 1) : undefined} />
      ))}
      <div>
        <Button variant="outline" type="button" onClick={() => onChange([...fields, { name: uniqueName(fields.map((f) => f.name), 'campo'), type: { type: 'str' } }])}>
          <Plus /> Campo
        </Button>
      </div>
    </div>
  );
}

function FieldCard({ field, isKey, errors, sample, onChange, onRemove, onUp, onDown }: {
  field: EntityField;
  isKey: boolean;
  errors: EntityError[];
  sample: string;
  onChange: (f: EntityField) => void;
  onRemove: () => void;
  onUp?: () => void;
  onDown?: () => void;
}) {
  const computed = field.computed !== undefined;
  const k = field.constraints ?? {};
  const setConstraint = (patch: Partial<EntityConstraints>) => {
    const next: EntityConstraints = { ...k, ...patch };
    for (const key of Object.keys(next) as Array<keyof EntityConstraints>) if (next[key] === undefined || next[key] === '') delete next[key];
    onChange({ ...field, constraints: Object.keys(next).length ? next : undefined });
  };
  const setType = (type: DataType) => {
    // Las restricciones que ya no aplican al tipo nuevo se quitan.
    const kept: EntityConstraints = { oneOf: k.oneOf };
    if (type.type === 'num') Object.assign(kept, { min: k.min, max: k.max });
    if (type.type === 'str') Object.assign(kept, { minLength: k.minLength, maxLength: k.maxLength, pattern: k.pattern });
    if (type.type === 'list') Object.assign(kept, { minItems: k.minItems, maxItems: k.maxItems });
    const clean = Object.fromEntries(Object.entries(kept).filter(([, v]) => v !== undefined));
    onChange({ ...field, type, constraints: Object.keys(clean).length ? clean : undefined });
  };
  return (
    <Card>
      <CardBody className="flex flex-col gap-3">
        <div className="flex flex-wrap items-start gap-3">
          <FormField label="Nombre" className="w-44">
            <Input className="font-mono" value={field.name} onChange={(e) => onChange({ ...field, name: e.target.value })} />
          </FormField>
          <FormField label="Tipo" className="min-w-56 flex-1">
            <DataTypeEditor value={field.type} onChange={setType} />
          </FormField>
          <div className="flex flex-col gap-1 pt-6 text-sm">
            <label className="flex items-center gap-2">
              <Checkbox checked={field.required !== false} disabled={computed} onCheckedChange={(c) => onChange({ ...field, required: c === true ? undefined : false })} />
              obligatorio
            </label>
            <label className="flex items-center gap-2">
              <Checkbox checked={computed} disabled={isKey}
                onCheckedChange={(c) => onChange(c === true ? { ...field, computed: { path: 'campo' }, default: undefined, required: undefined } : { ...field, computed: undefined })} />
              calculado
            </label>
          </div>
          <div className="ml-auto flex items-center gap-1 pt-5">
            {isKey && <Badge variant="info">clave</Badge>}
            <Button variant="ghost" size="icon-sm" type="button" aria-label="Subir" disabled={!onUp} onClick={onUp}><ArrowUp /></Button>
            <Button variant="ghost" size="icon-sm" type="button" aria-label="Bajar" disabled={!onDown} onClick={onDown}><ArrowDown /></Button>
            <Button variant="ghost" size="icon-sm" type="button" className="hover:text-error" aria-label={`Quitar ${field.name}`} onClick={onRemove}><Trash2 /></Button>
          </div>
        </div>

        {computed ? (
          <ExprField label={`Cómo se calcula (${renderType(field.type)})`} preview="value" value={field.computed} sampleText={sample}
            sampleLabel="Probar con un valor de la entidad" onChange={(v) => onChange({ ...field, computed: v ?? { path: 'campo' } })} />
        ) : (
          <div className="grid gap-3 sm:grid-cols-3">
            {field.type.type === 'num' && (
              <>
                <NumberField label="Mínimo" value={k.min} onChange={(min) => setConstraint({ min })} />
                <NumberField label="Máximo" value={k.max} onChange={(max) => setConstraint({ max })} />
              </>
            )}
            {field.type.type === 'str' && (
              <>
                <NumberField label="Largo mínimo" value={k.minLength} onChange={(minLength) => setConstraint({ minLength })} />
                <NumberField label="Largo máximo" value={k.maxLength} onChange={(maxLength) => setConstraint({ maxLength })} />
                <FormField label="Patrón (regex)" hint="Ánclalo con ^…$ para el texto entero.">
                  <Input className="font-mono" value={k.pattern ?? ''} placeholder="ninguno" onChange={(e) => setConstraint({ pattern: e.target.value || undefined })} />
                </FormField>
              </>
            )}
            {field.type.type === 'list' && (
              <>
                <NumberField label="Mínimo de ítems" value={k.minItems} onChange={(minItems) => setConstraint({ minItems })} />
                <NumberField label="Máximo de ítems" value={k.maxItems} onChange={(maxItems) => setConstraint({ maxItems })} />
              </>
            )}
            {['str', 'num'].includes(field.type.type) && (
              <FormField label="Valores permitidos" hint="Separados por coma.">
                <Input value={(k.oneOf ?? []).map((v) => (typeof v === 'string' ? v : JSON.stringify(v))).join(', ')}
                  onChange={(e) => {
                    const items = e.target.value.split(',').map((x) => x.trim()).filter(Boolean);
                    setConstraint({ oneOf: items.length ? items.map((x) => (field.type.type === 'num' && !Number.isNaN(Number(x)) ? Number(x) : x)) : undefined });
                  }} />
              </FormField>
            )}
            <FormField label="Por defecto" hint="Texto, número, true/false o JSON.">
              <JsonInput value={field.default} onChange={(v) => onChange({ ...field, default: v })} />
            </FormField>
          </div>
        )}
        {errors.length > 0 && (
          <ul className="list-disc pl-4 text-xs text-error">{errors.map((e, i) => <li key={i}>{e.message}</li>)}</ul>
        )}
      </CardBody>
    </Card>
  );
}

/** Un valor en una línea: se lee como JSON si se puede (`3`, `true`, `[1,2]`) y si no como texto. Vacío es sin valor. */
function JsonInput({ value, onChange }: { value: JsonValue | undefined; onChange: (v: JsonValue | undefined) => void }) {
  const show = (v: JsonValue | undefined) => (v === undefined ? '' : typeof v === 'string' ? v : JSON.stringify(v));
  const [text, setText] = useState(() => show(value));
  useEffect(() => {
    setText((t) => (same(readJson(t), value) ? t : show(value)));
  }, [value]);
  return (
    <Input className="font-mono" value={text} placeholder="sin valor"
      onChange={(e) => {
        setText(e.target.value);
        onChange(readJson(e.target.value));
      }} />
  );
}

function readJson(text: string): JsonValue | undefined {
  if (text.trim() === '') return undefined;
  try {
    return parseRelaxed(text) as JsonValue;
  } catch {
    return text;
  }
}

function NumberField({ label, value, onChange }: { label: string; value?: number; onChange: (v: number | undefined) => void }) {
  return (
    <FormField label={label}>
      <Input type="number" value={value ?? ''} onChange={(e) => onChange(e.target.value === '' ? undefined : Number(e.target.value))} />
    </FormField>
  );
}

// ---- reglas ----

function RulesTab({ rules, sample, onChange }: { rules: EntityRule[]; sample: string; onChange: (rules: EntityRule[]) => void }) {
  const set = (i: number, r: EntityRule) => onChange(rules.map((x, j) => (j === i ? r : x)));
  return (
    <div className="flex flex-col gap-3">
      <p className="text-sm text-base-content/60">
        Condiciones entre campos que cumple todo valor. «Cuándo» es opcional: sin él la regla se aplica siempre.
      </p>
      {rules.map((r, i) => (
        <Card key={i}>
          <CardBody className="flex flex-col gap-3">
            <div className="flex flex-wrap items-end gap-3">
              <FormField label="Nombre" className="w-56">
                <Input className="font-mono" value={r.name} onChange={(e) => set(i, { ...r, name: e.target.value })} />
              </FormField>
              <FormField label="Mensaje si no se cumple" className="min-w-64 flex-1">
                <Input value={r.message ?? ''} onChange={(e) => set(i, { ...r, message: e.target.value || undefined })} />
              </FormField>
              <Button variant="ghost" size="icon-sm" type="button" className="hover:text-error" aria-label={`Quitar ${r.name}`}
                onClick={() => onChange(rules.filter((_, j) => j !== i))}><Trash2 /></Button>
            </div>
            <ExprField label="Cuándo se aplica (opcional)" preview="condition" optional value={r.when} sampleText={sample}
              sampleLabel="Probar con un valor de la entidad" onChange={(when) => set(i, { ...r, when })} />
            <ExprField label="Qué debe cumplirse" preview="condition" value={r.check} sampleText={sample}
              sampleLabel="Probar con un valor de la entidad" onChange={(check) => set(i, { ...r, check: check ?? true })} />
          </CardBody>
        </Card>
      ))}
      <div>
        <Button variant="outline" type="button"
          onClick={() => onChange([...rules, { name: uniqueName(rules.map((r) => r.name), 'regla'), check: { gt: [{ path: 'campo' }, 0] } }])}>
          <Plus /> Regla
        </Button>
      </div>
    </div>
  );
}

// ---- estados ----

function StatesTab({ entity, sample, onChange }: { entity: EntityDef; sample: string; onChange: (s: EntityStateMachine | undefined) => void }) {
  const m = entity.states;
  const textFields = entity.fields.filter((f) => f.type.type === 'str' && f.computed === undefined);
  if (!m) {
    return (
      <div className="rounded-box border border-dashed border-base-300 p-8 text-center text-sm text-base-content/60">
        <p className="mb-3">Sin estados. Con ellos, un campo de texto guarda el estado de cada valor y el nodo «Entity transition» lo mueve
          solo por las transiciones que se definan aquí.</p>
        <Button type="button" variant="outline" disabled={textFields.length === 0}
          onClick={() => onChange({ field: textFields[0]?.name ?? 'status', states: ['draft', 'done'], initial: 'draft', transitions: [{ name: 'finish', from: ['draft'], to: 'done' }] })}>
          <Plus /> Agregar estados
        </Button>
        {textFields.length === 0 && <p className="mt-2 text-xs">Hace falta un campo de texto que no sea calculado.</p>}
      </div>
    );
  }
  const states = m.states;
  const setT = (i: number, t: EntityTransition) => onChange({ ...m, transitions: m.transitions.map((x, j) => (j === i ? t : x)) });
  return (
    <div className="flex flex-col gap-3">
      <Card>
        <CardBody className="grid gap-3 sm:grid-cols-3">
          <FormField label="Campo del estado">
            <SearchSelect aria-label="Campo del estado" value={m.field} onChange={(field) => onChange({ ...m, field })}
              options={textFields.map((f) => ({ value: f.name, label: f.name }))} />
          </FormField>
          <FormField label="Estados" hint="Separados por coma.">
            <Input className="font-mono" defaultValue={states.join(', ')}
              onBlur={(e) => onChange({ ...m, states: e.target.value.split(',').map((x) => x.trim()).filter(Boolean) })} />
          </FormField>
          <FormField label="Inicial" hint="El de un valor que llega sin estado.">
            <SearchSelect aria-label="Estado inicial" value={m.initial} onChange={(initial) => onChange({ ...m, initial })}
              options={states.map((s) => ({ value: s, label: s }))} />
          </FormField>
        </CardBody>
      </Card>
      {m.transitions.map((t, i) => (
        <Card key={i}>
          <CardBody className="flex flex-col gap-3">
            <div className="flex flex-wrap items-end gap-3">
              <FormField label="Transición" className="w-44">
                <Input className="font-mono" value={t.name} onChange={(e) => setT(i, { ...t, name: e.target.value })} />
              </FormField>
              <FormField label="Desde" className="min-w-56 flex-1">
                <div className="flex flex-wrap gap-3 pt-1 text-sm">
                  {states.map((s) => (
                    <label key={s} className="flex items-center gap-1.5">
                      <Checkbox checked={t.from.includes(s)}
                        onCheckedChange={(c) => setT(i, { ...t, from: c === true ? [...t.from, s] : t.from.filter((x) => x !== s) })} />
                      <span className="font-mono">{s}</span>
                    </label>
                  ))}
                </div>
              </FormField>
              <FormField label="Hacia" className="w-40">
                <SearchSelect aria-label="Estado de destino" value={t.to} onChange={(to) => setT(i, { ...t, to })}
                  options={states.map((s) => ({ value: s, label: s }))} />
              </FormField>
              <Button variant="ghost" size="icon-sm" type="button" className="hover:text-error" aria-label={`Quitar ${t.name}`}
                onClick={() => onChange({ ...m, transitions: m.transitions.filter((_, j) => j !== i) })}><Trash2 /></Button>
            </div>
            <ExprField label="Condición (opcional)" preview="condition" optional value={t.guard} sampleText={sample}
              sampleLabel="Probar con un valor de la entidad" onChange={(guard) => setT(i, { ...t, guard })} />
            {t.guard !== undefined && (
              <FormField label="Mensaje si no se cumple">
                <Input value={t.message ?? ''} onChange={(e) => setT(i, { ...t, message: e.target.value || undefined })} />
              </FormField>
            )}
          </CardBody>
        </Card>
      ))}
      <div className="flex gap-2">
        <Button variant="outline" type="button"
          onClick={() => onChange({ ...m, transitions: [...m.transitions, { name: uniqueName(m.transitions.map((t) => t.name), 'transicion'), from: [m.initial], to: m.initial }] })}>
          <Plus /> Transición
        </Button>
        <Button variant="ghost" type="button" onClick={() => onChange(undefined)}>Quitar los estados</Button>
      </div>
    </div>
  );
}

// ---- probar ----

/** Un valor de ejemplo comprobado contra la entidad tal como está en el editor (sin guardar): el valor que resulta (con defaults y
 *  campos calculados) y cada problema; y el mismo valor movido por cada transición. */
function TestTab({ entity, entities, sample }: { entity: EntityDef; entities: Record<string, EntityDef>; sample: string }) {
  const generate = () => JSON.stringify(exampleFor(entity.name, entities), null, 2);
  const [text, setText] = useState(generate);
  // Un ejemplo que la entidad acepte: se prueban unos cuantos (las reglas pueden pedir campos opcionales que el azar dejó afuera).
  const generateValid = async () => {
    setResult(null);
    let last = generate();
    for (let n = 0; n < 8; n++) {
      try {
        const check = await api.checkEntity(entity.name, { value: JSON.parse(last) as JsonValue, entity: clean(entity) });
        if (check.ok) break;
      } catch {
        break;
      }
      last = generate();
    }
    setText(last);
  };
  const [result, setResult] = useState<{ title: string; check: EntityCheck } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const run = async (transition?: string) => {
    let value: JsonValue;
    try {
      value = parseRelaxed(text) as JsonValue;
    } catch (e) {
      return setError(`JSON no válido: ${(e as Error).message}`);
    }
    setError(null);
    try {
      const check = await api.checkEntity(entity.name, { value, transition, entity: clean(entity) });
      setResult({ title: transition ? `Transición «${transition}»` : 'Comprobación', check });
    } catch (e) {
      setError(errorMessage(e));
    }
  };

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <div className="flex flex-col gap-2">
        <FormField label="Valor de ejemplo (JSON)">
          <Textarea className="font-mono" rows={14} spellCheck={false} value={text} onChange={(e) => setText(e.target.value)} />
        </FormField>
        <div className="flex flex-wrap gap-2">
          <Button type="button" onClick={() => run()}>Comprobar</Button>
          {entity.states?.transitions.map((t) => (
            <Button key={t.name} type="button" variant="outline" onClick={() => run(t.name)}>{t.name}</Button>
          ))}
          <Button type="button" variant="ghost" onClick={() => void generateValid()}>Generar ejemplo</Button>
          <Button type="button" variant="ghost" onClick={() => { setText(sample); setResult(null); }}>Plantilla vacía</Button>
        </div>
        {error && <Alert variant="error">{error}</Alert>}
      </div>
      <div className="flex flex-col gap-2">
        {!result && <p className="text-sm text-base-content/60">Comprueba el valor para ver cómo lo deja la entidad y qué reglas rompe.</p>}
        {result && (
          <>
            <div className="flex items-center gap-2 text-sm font-medium">
              {result.title}
              {result.check.ok ? <Badge variant="success">válido</Badge> : <Badge variant="destructive">{result.check.problems.length} problema(s)</Badge>}
            </div>
            {result.check.problems.length > 0 && (
              <ul className="flex flex-col gap-1 text-sm">
                {result.check.problems.map((p, i) => (
                  <li key={i}>
                    <span className="font-mono">{p.path || '(valor)'}</span> <Badge variant="outline">{p.rule}</Badge> {p.message}
                  </li>
                ))}
              </ul>
            )}
            <FormField label={result.check.ok ? 'Valor resultante' : 'Valor (hasta donde se pudo)'}>
              <pre className="max-h-96 overflow-auto rounded-box bg-base-200 p-3 font-mono text-xs">{JSON.stringify(result.check.value, null, 2)}</pre>
            </FormField>
          </>
        )}
      </div>
    </div>
  );
}

// ---- JSON ----

function JsonTab({ entity, readOnly, onChange }: { entity: EntityDef; readOnly: boolean; onChange: (e: EntityDef) => void }) {
  const shown = useMemo(() => {
    const { source: _source, ...rest } = entity;
    return JSON.stringify(rest, null, 2);
  }, [entity]);
  const [text, setText] = useState(shown);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => setText(shown), [shown]);
  return (
    <div className="flex flex-col gap-2">
      <p className="text-sm text-base-content/60">La entidad tal como se guarda. Es lo mismo que escribe el DSL de Scala (<span className="font-mono">xplat.flow.entity.dsl</span>).</p>
      <Textarea className="font-mono" rows={24} spellCheck={false} value={text} readOnly={readOnly} onChange={(e) => setText(e.target.value)} />
      {error && <Alert variant="error">{error}</Alert>}
      {!readOnly && (
        <div>
          <Button type="button" variant="outline" disabled={text === shown} onClick={() => {
            try {
              const parsed = parseRelaxed(text) as unknown as EntityDef;
              if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.fields)) throw new Error('falta "fields"');
              setError(null);
              onChange(parsed);
            } catch (e) {
              setError(`No se pudo aplicar: ${(e as Error).message}`);
            }
          }}>Aplicar</Button>
        </div>
      )}
    </div>
  );
}

function uniqueName(taken: string[], base: string): string {
  let i = 1;
  while (taken.includes(`${base}${i}`)) i++;
  return `${base}${i}`;
}
