import { useEffect, useSyncExternalStore } from 'react';
import { api } from '../api/client';
import type { EntityDef } from '../api/types';

// Las entidades del modelo de negocio, compartidas por toda la app: el editor de tipos, el autocompletado de rutas, el formulario
// de los nodos y la vista de entidades piden la misma lista, que se carga una vez y se recarga cuando se guarda o borra una.

type Snapshot = { entities: EntityDef[]; byName: Record<string, EntityDef>; loaded: boolean; error: string | null };

let snapshot: Snapshot = { entities: [], byName: {}, loaded: false, error: null };
let loading: Promise<void> | null = null;
const listeners = new Set<() => void>();

function publish(next: Snapshot) {
  snapshot = next;
  for (const l of listeners) l();
}

/** Vuelve a pedir las entidades al servidor. */
export function reloadEntities(): Promise<void> {
  loading = api.listEntities().then(
    (entities) => publish({ entities, byName: Object.fromEntries(entities.map((e) => [e.name, e])), loaded: true, error: null }),
    (e: Error) => publish({ ...snapshot, loaded: true, error: e.message }),
  );
  return loading;
}

function subscribe(l: () => void) {
  listeners.add(l);
  return () => listeners.delete(l);
}

/** Las entidades (y por nombre). La primera vez que se usa las carga. */
export function useEntities(): Snapshot {
  const s = useSyncExternalStore(subscribe, () => snapshot);
  useEffect(() => {
    if (!loading) void reloadEntities();
  }, []);
  return s;
}
