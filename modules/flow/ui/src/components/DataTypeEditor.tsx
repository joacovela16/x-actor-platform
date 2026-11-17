import type { DataType, DataTypeTag } from '../api/types';
import { defaultType } from '../graph/graph';
import { useEntities } from '../hooks/useEntities';
import { Button } from '@/components/ui/button';
import { SearchSelect } from '@/components/ui/search-select';
import { Input } from '@/components/ui/input';

const TAGS: Array<{ value: DataTypeTag; label: string }> = [
  { value: 'bool', label: 'bool' },
  { value: 'num', label: 'num' },
  { value: 'str', label: 'str' },
  { value: 'any', label: 'any' },
  { value: 'list', label: 'list' },
  { value: 'obj', label: 'obj' },
  { value: 'entity', label: 'entidad' },
  { value: 'ref', label: 'referencia a entidad' },
];

/** Editor recursivo de `DataType` (list y obj anidan otro tipo; entity y ref eligen una entidad del modelo de negocio). */
export function DataTypeEditor({ value, onChange, depth = 0 }: { value: DataType; onChange: (t: DataType) => void; depth?: number }) {
  const { entities } = useEntities();
  const roots = entities.filter((e) => e.kind === 'root');
  return (
    <div className="dtype" style={{ marginLeft: depth ? 12 : 0 }}>
      <SearchSelect
        aria-label="Tipo"
        value={value.type}
        onChange={(t) => onChange(defaultType(t, (t === 'ref' ? roots : entities)[0]?.name ?? ''))}
        options={TAGS}
      />
      {value.type === 'entity' && (
        <SearchSelect
          aria-label="Entidad"
          value={value.name}
          onChange={(name) => onChange({ type: 'entity', name })}
          options={entityOptions(entities.map((e) => e.name), value.name)}
        />
      )}
      {value.type === 'ref' && (
        <SearchSelect
          aria-label="Entidad referenciada"
          value={value.entity}
          onChange={(entity) => onChange({ type: 'ref', entity })}
          options={entityOptions(roots.map((e) => e.name), value.entity)}
        />
      )}
      {value.type === 'list' && (
        <div className="dtype-nested">
          <span className="text-xs text-base-content/60">elementos:</span>
          <DataTypeEditor
            value={value.element}
            depth={depth + 1}
            onChange={(element) => onChange({ type: 'list', element })}
          />
        </div>
      )}
      {value.type === 'obj' && (
        <ObjFields value={value.fields} depth={depth} onChange={(fields) => onChange({ type: 'obj', fields })} />
      )}
    </div>
  );
}

function ObjFields({ value, onChange, depth }: { value: Record<string, DataType>; onChange: (f: Record<string, DataType>) => void; depth: number }) {
  const entries = Object.entries(value);
  const rename = (oldName: string, newName: string) => {
    const next: Record<string, DataType> = {};
    for (const [k, v] of entries) next[k === oldName ? newName : k] = v;
    onChange(next);
  };
  return (
    <div className="dtype-nested">
      {entries.map(([name, t], i) => (
        <div className="dtype-field" key={i}>
          <Input
            className="w-40"
            aria-label="Nombre del campo"
            value={name}
            placeholder="campo"
            onChange={(e) => rename(name, e.target.value)}
          />
          <DataTypeEditor value={t} depth={depth + 1} onChange={(nt) => onChange({ ...value, [name]: nt })} />
          <Button variant="ghost" className="text-error hover:text-error" type="button"
            onClick={() => {
              const next = { ...value };
              delete next[name];
              onChange(next);
            }}
          >
            quitar
          </Button>
        </div>
      ))}
      <Button variant="ghost" size="xs" type="button"
        onClick={() => onChange({ ...value, [uniqueField(value)]: { type: 'any' } })}
      >
        + campo
      </Button>
    </div>
  );
}

/** Las entidades a elegir; una que ya no existe se sigue mostrando, marcada. */
function entityOptions(names: string[], current: string) {
  const options = names.map((n) => ({ value: n, label: n }));
  if (current && !names.includes(current)) options.unshift({ value: current, label: `${current} (no existe)` });
  if (!current) options.unshift({ value: '', label: '— elegir —' });
  return options;
}

function uniqueField(obj: Record<string, unknown>): string {
  let i = 1;
  while (`campo${i}` in obj) i++;
  return `campo${i}`;
}
