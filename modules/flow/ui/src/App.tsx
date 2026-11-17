import { createContext, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { MoonIcon, MonitorIcon, SunIcon } from 'lucide-react';
import { api } from './api/client';
import type { CatalogEntry, EntityKind } from './api/types';
import { ConnectionsView, TriggersView } from './views/AdminView';
import { PluginsView } from './views/PluginsView';
import { BundleView } from './views/BundleView';
import { HealthView } from './views/HealthView';
import { EntitiesView } from './views/EntitiesView';
import { EntityEditor } from './views/EntityEditor';
import { FlowEditor } from './views/FlowEditor';
import { FlowList } from './views/FlowList';
import { InstanceAudit } from './views/InstanceAudit';
import { InstanceHistory } from './views/InstanceHistory';
import { Alert } from '@/components/ui/alert';
import { Button } from '@/components/ui/button';
import { Toaster, toast as notify } from '@/components/ui/toast';
import { cn } from '@/lib/utils';

type Route =
  | { name: 'flows' }
  | { name: 'flow'; id: string; rev?: number }
  | { name: 'triggers' }
  | { name: 'connections' }
  | { name: 'plugins' }
  | { name: 'bundle' }
  | { name: 'health' }
  | { name: 'entities' }
  | { name: 'entity'; id: string; isNew?: EntityKind }
  | { name: 'instances' }
  | { name: 'instance'; id: string };
type Theme = 'auto' | 'light' | 'dark';

interface AppCtx {
  catalog: CatalogEntry[];
  toast: (message: string, kind?: 'info' | 'error' | 'ok') => void;
  setDirty: (dirty: boolean) => void;
  navigate: (hash: string) => void;
}

export const AppContext = createContext<AppCtx>({
  catalog: [],
  toast: () => {},
  setDirty: () => {},
  navigate: () => {},
});

function parseRoute(hash: string): Route {
  const [h, query = ''] = hash.replace(/^#\/?/, '').split('?');
  if (h.startsWith('flow/')) {
    // `#/flow/{id}` es la última revisión; `#/flow/{id}?rev=n` una revisión antigua, en solo lectura.
    const rev = Number(new URLSearchParams(query).get('rev'));
    return {
      name: 'flow',
      id: decodeURIComponent(h.slice('flow/'.length)),
      rev: Number.isInteger(rev) && rev > 0 ? rev : undefined,
    };
  }
  // `#/admin` era la vista de triggers y conexiones juntos.
  if (h === 'triggers' || h === 'admin') return { name: 'triggers' };
  if (h === 'connections') return { name: 'connections' };
  if (h === 'plugins') return { name: 'plugins' };
  if (h === 'bundle') return { name: 'bundle' };
  if (h === 'health') return { name: 'health' };
  if (h === 'entities') return { name: 'entities' };
  if (h.startsWith('entity/')) {
    // `#/entity/{nombre}?new=root|value` abre una entidad nueva sin guardar.
    const kind = new URLSearchParams(query).get('new');
    return { name: 'entity', id: decodeURIComponent(h.slice('entity/'.length)), isNew: kind === 'root' || kind === 'value' ? kind : undefined };
  }
  if (h === 'instances') return { name: 'instances' };
  if (h.startsWith('instance/')) return { name: 'instance', id: decodeURIComponent(h.slice('instance/'.length)) };
  return { name: 'flows' };
}

function readTheme(): Theme {
  try {
    const t = localStorage.getItem('flow-ui-theme');
    if (t === 'light' || t === 'dark') return t;
  } catch {
    /* almacenamiento bloqueado: se usa el automático */
  }
  return 'auto';
}

export default function App() {
  const [route, setRoute] = useState<Route>(() => parseRoute(window.location.hash));
  const [catalog, setCatalog] = useState<CatalogEntry[]>([]);
  const [catalogError, setCatalogError] = useState<string | null>(null);
  const [theme, setTheme] = useState<Theme>(readTheme);
  const dirtyRef = useRef(false);

  useEffect(() => {
    const onHash = () => setRoute(parseRoute(window.location.hash));
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  useEffect(() => {
    api
      .catalog()
      .then(setCatalog)
      .catch((e: Error) => setCatalogError(`No se pudo cargar el catálogo de nodos: ${e.message}`));
  }, []);

  useEffect(() => {
    const onBeforeUnload = (e: BeforeUnloadEvent) => {
      if (dirtyRef.current) e.preventDefault();
    };
    window.addEventListener('beforeunload', onBeforeUnload);
    return () => window.removeEventListener('beforeunload', onBeforeUnload);
  }, []);

  useEffect(() => {
    const root = document.documentElement;
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const apply = () => {
      const dark = theme === 'dark' || (theme === 'auto' && media.matches);
      root.dataset.theme = dark ? 'flow-dark' : 'flow';
    };
    apply();
    media.addEventListener('change', apply);
    try {
      if (theme === 'auto') localStorage.removeItem('flow-ui-theme');
      else localStorage.setItem('flow-ui-theme', theme);
    } catch {
      /* sin almacenamiento el tema vale solo para esta vista */
    }
    return () => media.removeEventListener('change', apply);
  }, [theme]);

  const toast = useCallback((text: string, kind: 'info' | 'error' | 'ok' = 'info') => notify(text, kind), []);

  const setDirty = useCallback((d: boolean) => {
    dirtyRef.current = d;
  }, []);

  const navigate = useCallback((hash: string) => {
    if (dirtyRef.current && !window.confirm('Hay cambios sin guardar. ¿Salir igualmente?')) return;
    dirtyRef.current = false;
    window.location.hash = hash;
  }, []);

  const ctx = useMemo(() => ({ catalog, toast, setDirty, navigate }), [catalog, toast, setDirty, navigate]);

  const cycleTheme = () => setTheme((t) => (t === 'auto' ? 'dark' : t === 'dark' ? 'light' : 'auto'));

  const nav: { hash: string; label: string; active: boolean }[] = [
    { hash: '#/', label: 'Flujos', active: route.name === 'flows' || route.name === 'flow' },
    { hash: '#/entities', label: 'Entidades', active: route.name === 'entities' || route.name === 'entity' },
    { hash: '#/instances', label: 'Ejecuciones', active: route.name === 'instances' || route.name === 'instance' },
    { hash: '#/health', label: 'Salud', active: route.name === 'health' },
    { hash: '#/triggers', label: 'Triggers', active: route.name === 'triggers' },
    { hash: '#/connections', label: 'Conexiones', active: route.name === 'connections' },
    { hash: '#/plugins', label: 'Plugins', active: route.name === 'plugins' },
    { hash: '#/bundle', label: 'Mover', active: route.name === 'bundle' },
  ];
  const ThemeIcon = theme === 'auto' ? MonitorIcon : theme === 'dark' ? MoonIcon : SunIcon;
  const themeLabel = theme === 'auto' ? 'auto' : theme === 'dark' ? 'oscuro' : 'claro';

  return (
    <AppContext.Provider value={ctx}>
        <div className="flex min-h-full flex-col bg-base-200 text-base-content">
          <header className="sticky top-0 z-10 flex h-13 shrink-0 items-center gap-2 border-b border-base-300 bg-base-100 px-3 sm:px-4">
            <nav className="flex flex-1 flex-wrap items-center gap-1">
              {nav.map((n) => (
                <Button
                  key={n.hash}
                  href={n.hash}
                  variant="ghost"
                  size="sm"
                  className={cn('font-semibold', n.active ? 'bg-base-200 text-base-content' : 'text-base-content/60')}
                  aria-current={n.active ? 'page' : undefined}
                  onClick={(e) => {
                    e.preventDefault();
                    navigate(n.hash);
                  }}
                >
                  {n.label}
                </Button>
              ))}
            </nav>
            <Button type="button" variant="ghost" size="sm" onClick={cycleTheme} title="Tema">
              <ThemeIcon />
              Tema: {themeLabel}
            </Button>
          </header>

          {catalogError && (
            <Alert variant="error" className="mx-4 my-2 w-auto">
              {catalogError}
            </Alert>
          )}

          <main className="flex min-h-0 flex-1 flex-col">
            {route.name === 'flows' && <FlowList />}
            {route.name === 'flow' && <FlowEditor key={`${route.id}@${route.rev ?? 'last'}`} flowId={route.id} rev={route.rev} />}
            {route.name === 'triggers' && <TriggersView />}
            {route.name === 'connections' && <ConnectionsView />}
            {route.name === 'plugins' && <PluginsView />}
            {route.name === 'bundle' && <BundleView />}
            {route.name === 'health' && <HealthView />}
            {route.name === 'entities' && <EntitiesView />}
            {route.name === 'entity' && <EntityEditor key={`${route.id}:${route.isNew ?? ''}`} name={route.id} isNew={route.isNew} />}
            {route.name === 'instances' && <InstanceHistory />}
            {route.name === 'instance' && <InstanceAudit key={route.id} id={route.id} />}
          </main>

          <Toaster />
        </div>
    </AppContext.Provider>
  );
}
