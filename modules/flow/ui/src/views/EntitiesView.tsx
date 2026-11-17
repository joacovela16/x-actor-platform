import { useContext, useState } from 'react';
import { Plus } from 'lucide-react';
import type { EntityKind } from '../api/types';
import { AppContext } from '../App';
import { useEntities } from '../hooks/useEntities';
import { Alert } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { FormField } from '@/components/ui/form-field';
import { Input } from '@/components/ui/input';
import { Modal } from '@/components/ui/modal';
import { SearchSelect } from '@/components/ui/search-select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

const NAME_RE = /^[A-Za-z][A-Za-z0-9_]*$/;

/** Cómo se muestra de dónde viene una entidad. Solo las de `store` se editan aquí. */
export function SourceBadge({ source }: { source?: string }) {
  if (!source || source === 'store') return <Badge variant="secondary">editable</Badge>;
  if (source === 'code') return <Badge variant="outline" title="Definida en el código del servidor">código</Badge>;
  return <Badge variant="outline" title="La trae un plugin">{source.replace(/^plugin:/, 'plugin ')}</Badge>;
}

export function KindBadge({ kind }: { kind: EntityKind }) {
  return kind === 'root'
    ? <Badge variant="info" title="Tiene identidad (clave) y se puede referenciar">raíz</Badge>
    : <Badge variant="ghost" title="Sin identidad: solo vive dentro de otras">valor</Badge>;
}

/** Las entidades del modelo de negocio: las que se editan aquí y las que vienen de código o de plugins. */
export function EntitiesView() {
  const { navigate, toast } = useContext(AppContext);
  const { entities, loaded, error } = useEntities();
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState('');
  const [kind, setKind] = useState<EntityKind>('root');

  const create = (e: React.FormEvent) => {
    e.preventDefault();
    if (!NAME_RE.test(name)) return toast('El nombre empieza con una letra y sigue con letras, números o _.', 'error');
    if (entities.some((x) => x.name === name)) return toast(`Ya hay una entidad '${name}'.`, 'error');
    setCreating(false);
    navigate(`#/entity/${encodeURIComponent(name)}?new=${kind}`);
  };

  return (
    <section className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-4 flex items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold">Entidades</h1>
          <p className="text-sm text-base-content/60">
            El modelo de negocio: campos, valores anidados, referencias, reglas, campos calculados y estados. Los flujos las usan como tipos.
          </p>
        </div>
        <Button onClick={() => setCreating(true)}><Plus /> Nueva entidad</Button>
        <Modal
          open={creating}
          onClose={() => setCreating(false)}
          title="Nueva entidad"
          description="El nombre no se puede cambiar después. Una raíz tiene clave y se puede referenciar; un valor solo vive dentro de otras."
          onSubmit={create}
          actions={<Button type="submit" disabled={!name}>Crear</Button>}
        >
          <div className="flex flex-col gap-4">
            <FormField label="Nombre" hint="Por ejemplo Order, Customer, Address">
              <Input autoFocus className="font-mono" value={name} onChange={(e) => setName(e.target.value)} placeholder="Order" required />
            </FormField>
            <FormField label="Tipo">
              <SearchSelect<EntityKind>
                aria-label="Tipo de entidad"
                value={kind}
                onChange={setKind}
                options={[
                  { value: 'root', label: 'raíz (con clave)' },
                  { value: 'value', label: 'valor (sin identidad)' },
                ]}
              />
            </FormField>
          </div>
        </Modal>
      </div>

      {error && <Alert variant="error" className="mb-3">No se pudieron cargar las entidades: {error}</Alert>}
      {!loaded && <p className="text-sm text-base-content/60">Cargando…</p>}
      {loaded && entities.length === 0 && (
        <div className="rounded-box border border-dashed border-base-300 p-10 text-center text-sm text-base-content/60">
          Todavía no hay entidades. Crea la primera con «Nueva entidad».
        </div>
      )}
      {entities.length > 0 && (
        <div className="overflow-hidden rounded-box border border-base-300 bg-base-100">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Entidad</TableHead>
                <TableHead className="w-24">Tipo</TableHead>
                <TableHead>Campos</TableHead>
                <TableHead className="w-28">Origen</TableHead>
                <TableHead className="w-24 text-right">Revisión</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {entities.map((e) => (
                <TableRow key={e.name} className="cursor-pointer" onClick={() => navigate(`#/entity/${encodeURIComponent(e.name)}`)}>
                  <TableCell>
                    <div className="font-medium">{e.label || e.name}</div>
                    {e.label && <div className="font-mono text-xs text-base-content/60">{e.name}</div>}
                  </TableCell>
                  <TableCell><KindBadge kind={e.kind} /></TableCell>
                  <TableCell className="text-xs text-base-content/70">
                    <span className="font-mono">{e.fields.map((f) => f.name).join(', ')}</span>
                    {(e.rules?.length ?? 0) > 0 && <span> · {e.rules!.length} regla{e.rules!.length === 1 ? '' : 's'}</span>}
                    {e.states && <span> · estados</span>}
                  </TableCell>
                  <TableCell><SourceBadge source={e.source} /></TableCell>
                  <TableCell className="text-right tabular-nums">{e.revision ?? '—'}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </section>
  );
}
