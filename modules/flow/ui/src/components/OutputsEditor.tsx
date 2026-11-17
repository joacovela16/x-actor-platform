import type { OutputDecl } from '../api/types';
import { isValidStatePath } from '../graph/graph';
import { DataTypeEditor } from './DataTypeEditor';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface Props {
  decls: OutputDecl[];
  onChange: (decls: OutputDecl[]) => void;
  /** Texto del nombre de cada fila (salidas: "lo que escribe el nodo"; entradas: "dato que recibe"). */
  nameLabel: string;
  paths?: string[];
  emptyText: string;
}

/** Editor de `OutputDecl` (nombre, target en el estado, tipo). Sirve para salidas de nodo y para inputs/outputs del flujo. */
export function OutputsEditor({ decls, onChange, nameLabel, paths = [], emptyText }: Props) {
  const update = (i: number, next: OutputDecl) => onChange(decls.map((d, j) => (j === i ? next : d)));
  const remove = (i: number) => onChange(decls.filter((_, j) => j !== i));
  const add = () => onChange([...decls, { name: uniqueName(decls), target: '', type: { type: 'any' } }]);
  const listId = `paths-${nameLabel.replace(/\s/g, '')}`;

  return (
    <div className="outputs-editor">
      {decls.length === 0 && <p className="text-xs text-base-content/60">{emptyText}</p>}
      {decls.map((d, i) => {
        const dupName = decls.filter((x) => x.name === d.name).length > 1;
        const badTarget = !isValidStatePath(d.target);
        return (
          <div className="decl-row" key={i}>
            <div className="decl-line">
              <label className="min-w-35 flex-1">
                <span className="text-xs text-base-content/60">{nameLabel}</span>
                <Input  value={d.name} onChange={(e) => update(i, { ...d, name: e.target.value })} />
              </label>
              <label className="min-w-35 flex-1">
                <span className="text-xs text-base-content/60">ruta en el estado</span>
                <Input
                  className={`font-mono ${badTarget ? 'border-error' : ''}`}
                  list={listId}
                  value={d.target}
                  placeholder="order.total"
                  onChange={(e) => update(i, { ...d, target: e.target.value })}
                />
              </label>
              <Button variant="ghost" className="text-error hover:text-error" type="button" onClick={() => remove(i)}>quitar</Button>
            </div>
            <div className="decl-line">
              <span className="text-xs text-base-content/60">tipo</span>
              <DataTypeEditor value={d.type} onChange={(type) => update(i, { ...d, type })} />
            </div>
            {dupName && <p className="my-0.5 text-xs text-error">Nombre repetido.</p>}
            {badTarget && <p className="my-0.5 text-xs text-error">Ruta no válida: segmentos separados por puntos, sin vacíos.</p>}
          </div>
        );
      })}
      <datalist id={listId}>
        {paths.map((p) => <option key={p} value={p} />)}
      </datalist>
      <Button variant="ghost" type="button" onClick={add}>+ añadir</Button>
    </div>
  );
}

function uniqueName(decls: OutputDecl[]): string {
  let i = 1;
  while (decls.some((d) => d.name === `valor${i}`)) i++;
  return `valor${i}`;
}
