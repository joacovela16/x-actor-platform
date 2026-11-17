import { latestMajors } from '@/lib/plugins';
import { categoryClass } from '../lib/category';
import { useMemo, useState } from 'react';
import type { CatalogEntry } from '../api/types';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';

interface Props {
  catalog: CatalogEntry[];
  onAdd: (kind: string) => void;
}

/** Paleta de nodos agrupada por `category`. Se arrastra al lienzo, o se añade con el botón (útil en pantallas táctiles). */
export function Palette({ catalog, onAdd }: Props) {
  const [q, setQ] = useState('');
  const groups = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const map = new Map<string, CatalogEntry[]>();
    for (const c of latestMajors(catalog)) {
      if (needle && !`${c.label} ${c.kind} ${c.category} ${c.description ?? ''}`.toLowerCase().includes(needle)) continue;
      map.set(c.category, [...(map.get(c.category) ?? []), c]);
    }
    return [...map.entries()].sort(([a], [b]) => a.localeCompare(b));
  }, [catalog, q]);

  return (
    <div className="palette">
      {/* La búsqueda queda fija arriba al desplazar la lista (el panel tiene 8px de padding: se compensa para cubrirlo). */}
      <div className="sticky -top-2 z-10 -mx-2 border-b bg-base-100 px-2 py-2">
        <Input placeholder="Buscar nodo…" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Buscar nodo" />
      </div>
      {groups.length === 0 && <p className="text-xs text-base-content/60">Ningún nodo coincide.</p>}
      {groups.map(([category, items]) => (
        <div className="palette-group" key={category}>
          <h3>{category}</h3>
          {items.map((c) => (
            <div
              key={c.kind}
              className={`palette-item ${categoryClass(c.kind)}`}
              title={c.description || undefined}
              draggable
              onDragStart={(e) => {
                e.dataTransfer.setData('application/x-flow-kind', c.kind);
                e.dataTransfer.effectAllowed = 'copy';
              }}
            >
              <div>
                <div className="palette-label">
                  {c.label}
                  {c.role && c.role !== 'node' && <Badge variant="secondary" className="h-4 px-1.5 text-[10px]">{c.role === 'source' ? 'fuente' : c.role === 'sink' ? 'sumidero' : 'operador'}</Badge>}
                </div>
                <div className="text-xs text-base-content/60 font-mono">{c.kind}</div>
              </div>
              <Button variant="ghost" size="xs" type="button" onClick={() => onAdd(c.kind)} aria-label={`Añadir ${c.label}`}>
                Añadir
              </Button>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}
