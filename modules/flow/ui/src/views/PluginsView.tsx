import { useEffect, useState } from 'react';
import { api, errorMessage } from '../api/client';
import type { PluginStatus } from '../api/types';
import { Alert } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Card, CardBody } from '@/components/ui/card';

/** Los plugins del servidor: todo tipo de nodo viene de uno. Primero los que trae el servidor; luego los que encontró en el
 *  directorio de plugins al arrancar, los cargados con sus nodos y los que fallaron con el motivo. Instalar o quitar uno de
 *  esos es copiar o borrar su directorio y reiniciar el servidor.
 */
export function PluginsView() {
  const [plugins, setPlugins] = useState<PluginStatus[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.plugins().then(setPlugins, (e) => setError(errorMessage(e)));
  }, []);

  return (
    <section className="mx-auto w-full max-w-4xl p-4">
      <h1 className="mb-1 text-xl font-semibold">Plugins</h1>
      <p className="mb-3 text-sm text-base-content/60">
        Todos los nodos vienen de plugins. Los del servidor vienen con él; los demás son directorios en el de plugins
        (<span className="font-mono">--plugins-dir</span>) y se cargan al arrancar.
      </p>
      {error && <Alert variant="error">{error}</Alert>}
      {plugins && plugins.length === 0 && <p className="text-sm text-base-content/60">No hay plugins.</p>}
      <div className="flex flex-col gap-3">
        {plugins?.map((p) => (
          <Card
            key={p.dir}
            title={
              <span className="flex flex-wrap items-center gap-2">
                <span>{p.manifest?.name ?? p.dir}</span>
                {p.source === 'builtin' ? (
                  <Badge variant="outline">del servidor</Badge>
                ) : (
                  <>
                    {p.manifest && <span className="font-mono text-sm font-normal text-base-content/60">{p.manifest.version}</span>}
                    {p.manifest && <Badge variant="outline">{p.manifest.transport}</Badge>}
                  </>
                )}
                <Badge variant={p.state === 'loaded' ? 'success' : 'destructive'}>{p.state === 'loaded' ? 'cargado' : 'falló'}</Badge>
              </span>
            }
          >
            <CardBody className="pt-0 text-sm">
              {p.source !== 'builtin' && (
              <div className="text-xs text-base-content/60">
                Directorio <span className="font-mono">{p.dir}</span>
                {p.manifest?.transport === 'process' && (
                  <>
                    {' · '}comando <span className="font-mono">{p.manifest.command.join(' ')}</span>
                  </>
                )}
              </div>
              )}
              {p.error && <Alert variant="error">{p.error}</Alert>}
              {p.kinds && p.kinds.length > 0 && (
                <div>
                  <div className="mb-1 text-xs font-medium">Nodos</div>
                  <div className="flex flex-wrap gap-1">
                    {p.kinds.map((k) => (
                      <Badge key={k} variant="secondary" className="font-mono">{k}</Badge>
                    ))}
                  </div>
                </div>
              )}
              {p.connectionKinds && p.connectionKinds.length > 0 && (
                <div>
                  <div className="mb-1 text-xs font-medium">Tipos de conexión</div>
                  <div className="flex flex-wrap gap-1">
                    {p.connectionKinds.map((k) => (
                      <Badge key={k} variant="outline" className="font-mono">{k}</Badge>
                    ))}
                  </div>
                </div>
              )}
              {p.triggers && p.triggers.length > 0 && (
                <div>
                  <div className="mb-1 text-xs font-medium">Triggers</div>
                  <div className="flex flex-wrap gap-1">
                    {p.triggers.map((t) => (
                      <Badge key={t} variant="outline" className="font-mono">{t}</Badge>
                    ))}
                  </div>
                </div>
              )}
              {p.entities && p.entities.length > 0 && (
                <div>
                  <div className="mb-1 text-xs font-medium">Entidades</div>
                  <div className="flex flex-wrap gap-1">
                    {p.entities.map((e) => (
                      <a key={e} href={`#/entity/${encodeURIComponent(e)}`}>
                        <Badge variant="outline" className="font-mono">{e}</Badge>
                      </a>
                    ))}
                  </div>
                </div>
              )}
            </CardBody>
          </Card>
        ))}
      </div>
    </section>
  );
}
