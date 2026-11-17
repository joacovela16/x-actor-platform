import { newerMajor } from '@/lib/plugins';
import { Alert } from '@/components/ui/alert';
import { useContext, useMemo, useState } from 'react';
import { api } from '../api/client';
import type { CatalogEntry, FlowDef, FlowError, JsonValue, NodeDef, NodeRole, OutputDecl, RunAction } from '../api/types';
import { AppContext } from '../App';
import { ConfigForm } from './ConfigForm';
import { ScriptEnvContext } from './scriptFiles';
import { useEntities } from '../hooks/useEntities';
import { OutputsEditor } from './OutputsEditor';
import { Button } from '@/components/ui/button';
import { FormField } from '@/components/ui/form-field';
import { SearchSelect, type SelectOption } from '@/components/ui/search-select';
import { Input } from '@/components/ui/input';

interface Props {
  node: NodeDef;
  entry: CatalogEntry | undefined;
  flow: FlowDef;
  ports: string[];
  role: NodeRole;
  errors: FlowError[];
  paths: string[];
  /** Campos del elemento que entra por cada entrada de stream (`in`, `left`, `right`), para sugerir. */
  elementPaths?: Record<string, string[]>;
  onChange: (patch: Partial<NodeDef>) => void;
  onRename: (newId: string) => string | null;
  onDelete: () => void;
}

/** Propiedades de un nodo: identidad, configuración (desde ConfigSchema), salidas y políticas. */

const RUN_ACTIONS: SelectOption<RunAction>[] = [
  { value: 'run', label: 'ejecutar otra vez' },
  { value: 'skip', label: 'saltar y reutilizar resultado' },
];

export function NodeProperties({ node, entry, flow, ports, role, errors, paths, elementPaths, onChange, onRename, onDelete }: Props) {
  const { toast, catalog } = useContext(AppContext);
  const [idDraft, setIdDraft] = useState(node.id);
  const [idError, setIdError] = useState<string | null>(null);
  const [copying, setCopying] = useState(false);
  const { byName: entities } = useEntities();

  // Para los editores de código: los tipos del estado del flujo, las salidas y los puertos de este nodo.
  const scriptEnv = useMemo(
    () => ({
      nodeId: node.id,
      kind: node.kind,
      decls: [...flow.inputs, ...flow.nodes.flatMap((n) => n.outputs)],
      outputs: node.outputs,
      ports: ports.filter((p) => p !== 'error'),
      args: (node.params as Record<string, JsonValue>).args,
      entities,
    }),
    [node.id, node.kind, node.outputs, node.params, flow.inputs, flow.nodes, ports, entities],
  );

  // Si cambia el nodo seleccionado, el borrador del id se reinicia (el componente se monta con key=node.id).
  const commitId = () => {
    if (idDraft === node.id) return;
    const err = onRename(idDraft);
    setIdError(err);
    if (err) setIdDraft(node.id);
  };

  const copySubflowOutputs = async () => {
    const flowId = typeof node.params.flowId === 'string' ? node.params.flowId : '';
    if (!flowId) return toast('Elige primero el flujo a ejecutar.', 'error');
    setCopying(true);
    try {
      const called = await api.getFlow(flowId);
      const outputs: OutputDecl[] = called.outputs.map((o) => ({ name: o.name, target: o.name, type: o.type }));
      onChange({ outputs });
      toast(`${outputs.length} salida(s) copiadas de '${flowId}'`, 'ok');
    } catch (e) {
      toast(`No se pudo leer '${flowId}': ${(e as Error).message}`, 'error');
    } finally {
      setCopying(false);
    }
  };

  const newer = newerMajor(node.kind, catalog);
  const outgoing = flow.edges.filter((e) => e.from === node.id);

  return (
    <div className="props">
      <div className="props-head">
        <div>
          <div className="text-xs text-base-content/60">{entry?.category ?? ''}</div>
          <h2>{entry?.label ?? node.kind}</h2>
          <div className="font-mono text-xs text-base-content/60">{node.kind}</div>
          {entry?.description && <p className="mt-1.5 max-w-prose text-sm text-base-content/80">{entry.description}</p>}
        </div>
        <Button variant="ghost" className="text-error hover:text-error" type="button" onClick={onDelete}>Eliminar nodo</Button>
      </div>

      {newer && (
        <Alert variant="warning" className="mb-2 text-xs">
          Este nodo usa un major viejo del plugin. Hay uno más nuevo, <span className="font-mono">{newer}</span>: puede cambiar su configuración, así que se migra a mano (agregá el nodo nuevo y copiá lo que haga falta).
        </Alert>
      )}

      {errors.map((e, i) => (
        <p className="my-0.5 text-xs text-error" key={i}>{e.message}</p>
      ))}

      <section>
        <FormField label="Id del nodo">
          <Input
            className={`font-mono ${idError ? 'border-error' : ''}`}
            value={idDraft}
            onChange={(e) => setIdDraft(e.target.value)}
            onBlur={commitId}
            onKeyDown={(e) => e.key === 'Enter' && commitId()}
          />
          {idError && <span className="my-0.5 text-xs text-error">{idError}</span>}
        </FormField>
        <FormField label="Etiqueta">
          <Input  value={node.label ?? ''} onChange={(e) => onChange({ label: e.target.value || undefined })} />
        </FormField>
      </section>

      <section>
        <h3>Configuración</h3>
        {entry ? (
          <ScriptEnvContext.Provider value={scriptEnv}>
            <ConfigForm
              kind={node.kind}
              schema={entry.config}
              params={node.params as Record<string, JsonValue>}
              onChange={(params) => onChange({ params })}
              paths={paths}
              elementPaths={elementPaths}
            />
          </ScriptEnvContext.Provider>
        ) : (
          <p className="my-0.5 text-xs text-error">El tipo '{node.kind}' no existe en el catálogo.</p>
        )}
      </section>

      <section>
        <h3>Puertos</h3>
        <p className="text-xs text-base-content/60">{ports.join(', ')}</p>
        <p className="text-xs text-base-content/60">Aristas salientes: {outgoing.length}</p>
      </section>

      {node.kind === 'core.subflow' && (
        <section>
          <Button variant="outline" type="button" onClick={copySubflowOutputs} disabled={copying}>
            Copiar salidas del flujo llamado
          </Button>
        </section>
      )}

      <section>
        <h3>Salidas (escribe en el estado)</h3>
        {role === 'sink' && (
          <p className="text-xs text-base-content/60">
            En el segmento de stream la salida se llama <span className="font-mono">{node.id}.{node.outputs[0]?.name ?? 'result'}</span> (así aparece en la auditoría).
            {node.outputs.length > 1 && <span className="my-0.5 text-xs text-error"> Solo la primera salida recibe el valor; las demás quedan sin escribir.</span>}
          </p>
        )}
        <OutputsEditor
          decls={node.outputs}
          onChange={(outputs) => onChange({ outputs })}
          nameLabel="nombre de la salida"
          paths={paths}
          emptyText="Este nodo no escribe nada en el estado."
        />
      </section>

      <section>
        <h3>Políticas</h3>
        <FormField label="Si la última ejecución fue correcta">
          <SearchSelect
            value={node.runPolicy.whenSucceeded}
            onChange={(whenSucceeded) => onChange({ runPolicy: { ...node.runPolicy, whenSucceeded } })}
            options={RUN_ACTIONS}
          />
        </FormField>
        <FormField label="Si la última ejecución falló">
          <SearchSelect
            value={node.runPolicy.whenFailed}
            onChange={(whenFailed) => onChange({ runPolicy: { ...node.runPolicy, whenFailed } })}
            options={RUN_ACTIONS}
          />
        </FormField>
        <FormField label="Entradas (join)">
          <SearchSelect
            value={node.joinPolicy}
            onChange={(joinPolicy) => onChange({ joinPolicy })}
            options={[
              { value: 'all', label: 'all: espera a todas las aristas' },
              { value: 'any', label: 'any: corre con la primera' },
            ]}
          />
        </FormField>
        <p className="text-xs text-base-content/60">Una arista que no se tomará (rama no elegida) cuenta como llegada.</p>
      </section>
    </div>
  );
}
