import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { api, ApiError, errorMessage } from '../api/client';
import type { Edge, FlowDef, FlowError, NodeDef, ScheduleView, TraceStep } from '../api/types';
import { Canvas, type LiveCounters, type Trace } from '../components/Canvas';
import { ErrorsPanel } from '../components/ErrorsPanel';
import { FlowSettings } from '../components/FlowSettings';
import { NodeProperties } from '../components/NodeProperties';
import { Palette } from '../components/Palette';
import { RevisionsPanel } from '../components/RevisionsPanel';
import { RunPanel } from '../components/RunPanel';
import { SchedulePanel } from '../components/SchedulePanel';
import { TestPanel, type TraceView } from '../components/TestPanel';
import { useEntities } from '../hooks/useEntities';
import { usePorts } from '../hooks/usePorts';
import { AppContext } from '../App';
import { elementPaths, inferElementTypes } from '../graph/elementTypes';
import { declaredPaths, edgeKey, fallbackPosition, newNodeDef, wouldCreateCycle } from '../graph/graph';
import { edgeProblem, findSegments, type EdgeProblem, type NodeIO } from '../graph/stream';
import type { RunStatus } from '../components/RunPanel';
import { Button } from '@/components/ui/button';
import { Alert } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { ResizeHandle, usePanelWidth } from '../components/ResizeHandle';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { Tabs } from '@/components/ui/tabs';

const NO_ERRORS_BY_NODE: Record<string, string[]> = {};
const NO_EDGE_PROBLEMS: Record<string, string> = {};

type BottomTab = 'errors' | 'run' | 'test';
type RightTab = 'node' | 'flow' | 'schedule' | 'revisions';

/**
 * Editor de un flujo: lienzo, paleta, propiedades, validación, programación, revisiones, ejecución y modo test.
 * Con `rev` abre esa revisión antigua en solo lectura; «Restaurar» la guarda como una revisión nueva.
 */
export function FlowEditor({ flowId, rev }: { flowId: string; rev?: number }) {
  const { catalog, toast, setDirty, navigate } = useContext(AppContext);
  const readOnly = rev !== undefined;
  const [doc, setDoc] = useState<FlowDef | null>(null);
  const [saved, setSaved] = useState('');
  const [loadError, setLoadError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [rightTab, setRightTab] = useState<RightTab>('flow');
  const [bottomTab, setBottomTab] = useState<BottomTab>('errors');
  // El panel inferior empieza plegado para dejar el lienzo entero; se abre al elegir una pestaña o al fallar el guardado.
  const [bottomOpen, setBottomOpen] = useState(false);
  const paletteWidth = usePanelWidth('palette', 220, 160, 420);
  const propsWidth = usePanelWidth('props', 320, 260, 720);
  const showBottom = (t: BottomTab) => {
    setBottomTab(t);
    setBottomOpen(true);
  };
  const [errors, setErrors] = useState<FlowError[]>([]);
  const [validating, setValidating] = useState(false);
  const [nodeStatus, setNodeStatus] = useState<Record<string, string>>({});
  const [fireAt, setFireAt] = useState<Record<string, number>>({});
  const [live, setLive] = useState<Record<string, LiveCounters>>({});
  const [running, setRunning] = useState(false);
  const [traceView, setTraceView] = useState<TraceView | null>(null);
  const [saving, setSaving] = useState(false);
  /** Hay un borrador guardado sin publicar (lo que se edita es él). */
  const [hasDraft, setHasDraft] = useState(false);
  /** Existe al menos una revisión publicada (un flujo nuevo puede ser solo borrador). */
  const [published, setPublished] = useState(true);
  const [schedule, setSchedule] = useState<ScheduleView | null>(null);
  const [scheduleError, setScheduleError] = useState<string | null>(null);
  const [toggling, setToggling] = useState(false);
  /** Número de la última revisión guardada (la que devuelve `GET /api/flows/{id}`). */
  const [latest, setLatest] = useState<number | null>(null);
  const validSeq = useRef(0);

  const dirty = !readOnly && doc !== null && JSON.stringify(doc) !== saved;
  useEffect(() => setDirty(dirty), [dirty, setDirty]);
  useEffect(() => () => setDirty(false), [setDirty]);

  const loadSchedule = useCallback(() => {
    api
      .getFlow(flowId)
      .then((f) => setLatest(f.revision))
      .catch(() => setLatest(null));
    api
      .schedule(flowId)
      .then((s) => {
        setSchedule(s);
        setScheduleError(null);
      })
      .catch((e: unknown) => setScheduleError(errorMessage(e)));
  }, [flowId]);

  useEffect(() => {
    loadSchedule();
  }, [loadSchedule]);

  useEffect(() => {
    let alive = true;
    setDoc(null);
    setLoadError(null);
    // La última revisión se edita a partir de su borrador, si lo tiene; una revisión anterior se mira tal cual.
    const notFound = (e: unknown) => {
      if (e instanceof ApiError && e.status === 404) return null;
      throw e;
    };
    Promise.all([rev === undefined ? api.getDraft(flowId) : Promise.resolve(null), api.getFlow(flowId, rev).catch(rev === undefined ? notFound : (e) => Promise.reject(e))])
      .then(([draft, flow]) => {
        if (!alive) return;
        const f = draft?.flow ?? flow;
        if (!f) throw new ApiError(404, null);
        setDoc(f);
        setSaved(JSON.stringify(f));
        setHasDraft(!!draft);
        setPublished(!!flow);
      })
      .catch((e: unknown) => {
        if (!alive) return;
        setLoadError(rev !== undefined ? `No existe la revisión ${rev}: ${errorMessage(e)}` : `No se pudo abrir el flujo: ${errorMessage(e)}`);
      });
    return () => {
      alive = false;
    };
  }, [flowId, rev]);

  // Validación automática (con debounce) cada vez que cambia el documento. Solo cuenta la última respuesta.
  const docJson = doc ? JSON.stringify(doc) : '';
  useEffect(() => {
    if (!doc || readOnly) return;
    const seq = ++validSeq.current;
    setValidating(true);
    const t = setTimeout(() => {
      api
        .validate(doc)
        .then((errs) => seq === validSeq.current && setErrors(errs))
        .catch((e: unknown) => seq === validSeq.current && toast(`Validación: ${errorMessage(e)}`, 'error'))
        .finally(() => seq === validSeq.current && setValidating(false));
    }, 500);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [docJson, readOnly]);

  const shapes = usePorts(doc?.nodes ?? [], catalog);

  // Aristas que el cliente ve mal (token/stream, tipo, entrada que no existe). Se calcula con las formas de los nodos.
  const edgeProblems = useMemo(() => {
    const m: Record<string, EdgeProblem> = {};
    if (!doc) return m;
    for (const e of doc.edges) {
      const p = edgeProblem(e, shapes as Record<string, NodeIO>);
      if (p) m[edgeKey(e)] = p;
    }
    return m;
  }, [doc, shapes]);
  const edgeMessages = useMemo(() => Object.fromEntries(Object.entries(edgeProblems).map(([k, p]) => [k, p.message])), [edgeProblems]);

  const segments = useMemo(() => (doc ? findSegments(doc.nodes, doc.edges, shapes as Record<string, NodeIO>) : []), [doc, shapes]);

  const update = (fn: (d: FlowDef) => FlowDef) => setDoc((d) => (d ? fn(d) : d));

  const selectedNode: NodeDef | undefined = doc?.nodes.find((n) => n.id === selectedId);

  const select = (id: string | null) => {
    setSelectedId(id);
    if (id) setRightTab('node');
  };

  // Errores del validador en cada nodo que nombran (`node`, o `nodes` si son varios), más la configuración que el nodo
  // dice que está mal (describe.problems: expresiones, campos), sin repetir lo que ya trae el validador.
  const errorsByNode = useMemo(() => {
    const m: Record<string, string[]> = {};
    const add = (id: string, msg: string) => {
      const list = (m[id] ??= []);
      // El validador antepone «node 'x' is badly configured: » al mismo problema que da el nodo: cuenta como uno.
      if (!list.some((l) => l === msg || l.endsWith(`: ${msg}`) || msg.endsWith(`: ${l}`))) list.push(msg);
    };
    for (const e of errors) {
      for (const id of e.nodes ?? (e.node ? [e.node] : [])) add(id, e.message);
    }
    for (const n of doc?.nodes ?? []) for (const msg of shapes[n.id]?.problems ?? []) add(n.id, msg);
    return m;
  }, [errors, doc, shapes]);

  // Lo que se lista en el panel de validación: el validador del servidor más los problemas que solo ve el cliente.
  const allErrors = useMemo<FlowError[]>(() => {
    const extra: FlowError[] = Object.entries(edgeProblems).map(([key, p]) => {
      const edge = doc?.edges.find((e) => edgeKey(e) === key);
      return { code: p.code, node: edge?.to ?? null, message: p.message };
    });
    return [...errors, ...extra];
  }, [errors, edgeProblems, doc]);

  // Las rutas declaradas, con los campos de los objetos y las entidades que declaran (`${state.order.customer}`).
  const { byName: entities } = useEntities();
  const paths = useMemo(() => (doc ? declaredPaths(doc, entities) : []), [doc, entities]);
  // El tipo de los elementos de cada stream, inferido de lo que el flujo declara.
  const elementTypes = useMemo(() => (doc ? inferElementTypes(doc, shapes as Record<string, NodeIO>, entities) : undefined), [doc, shapes, entities]);

  // ---- mutaciones -------------------------------------------------------

  const updateNode = (id: string, patch: Partial<NodeDef>) =>
    update((d) => ({ ...d, nodes: d.nodes.map((n) => (n.id === id ? { ...n, ...patch } : n)) }));

  const renameNode = (oldId: string, newId: string): string | null => {
    if (!doc || newId === oldId) return null;
    if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/.test(newId)) return 'Id no válido: letras, números, guion, punto o guion bajo.';
    if (doc.nodes.some((n) => n.id === newId)) return `Ya hay un nodo '${newId}'.`;
    update((d) => ({
      ...d,
      nodes: d.nodes.map((n) => (n.id === oldId ? { ...n, id: newId } : n)),
      edges: d.edges.map((e) => ({ ...e, from: e.from === oldId ? newId : e.from, to: e.to === oldId ? newId : e.to })),
      layout: d.layout && oldId in d.layout
        ? Object.fromEntries(Object.entries(d.layout).map(([k, v]) => [k === oldId ? newId : k, v]))
        : d.layout,
    }));
    setSelectedId(newId);
    return null;
  };

  const addNode = (kind: string, at?: { x: number; y: number }) => {
    if (!doc) return;
    const entry = catalog.find((c) => c.kind === kind);
    if (!entry) return;
    const node = newNodeDef(entry, doc.nodes);
    const pos = at ?? fallbackPosition(doc.nodes.length);
    update((d) => ({ ...d, nodes: [...d.nodes, node], layout: { ...(d.layout ?? {}), [node.id]: pos } }));
    select(node.id);
  };

  const removeNodes = (ids: string[]) => {
    const gone = new Set(ids);
    update((d) => ({
      ...d,
      nodes: d.nodes.filter((n) => !gone.has(n.id)),
      edges: d.edges.filter((e) => !gone.has(e.from) && !gone.has(e.to)),
      layout: d.layout && Object.fromEntries(Object.entries(d.layout).filter(([k]) => !gone.has(k))),
    }));
    if (selectedId && gone.has(selectedId)) setSelectedId(null);
  };

  const removeEdges = (keys: string[]) => {
    const gone = new Set(keys);
    update((d) => ({ ...d, edges: d.edges.filter((e) => !gone.has(edgeKey(e))) }));
  };

  const moveNodes = (positions: Record<string, { x: number; y: number }>) =>
    update((d) => ({ ...d, layout: { ...(d.layout ?? {}), ...positions } }));

  /** Conecta `from.port` con la entrada `input` de `to`. Valida en el cliente lo que el servidor también valida (stream con token, tipo, entrada). */
  const connect = (from: string, port: string, to: string, input: string): string | null => {
    if (!doc) return null;
    const edge: Edge = input && input !== 'in' ? { from, port, to, input } : { from, port, to };
    if (doc.edges.some((e) => edgeKey(e) === edgeKey(edge))) return null;
    if (wouldCreateCycle(doc.edges, from, to)) return 'Esa conexión crearía un ciclo.';
    const problem = edgeProblem(edge, shapes as Record<string, NodeIO>);
    if (problem) return problem.message;
    update((d) => ({ ...d, edges: [...d.edges, edge] }));
    return null;
  };

  // ---- guardar / restaurar / borrar / habilitar ------------------------

  /** Guarda lo que se edita como borrador, sin validar: lo que no valida todavía no se pierde. */
  const saveDraft = async (): Promise<boolean> => {
    if (!doc) return false;
    setSaving(true);
    try {
      const res = await api.saveDraft(doc);
      const merged: FlowDef = { ...res.flow, layout: res.flow.layout ?? doc.layout };
      setDoc(merged);
      setSaved(JSON.stringify(merged));
      setHasDraft(true);
      setErrors(res.errors);
      return true;
    } catch (e) {
      toast(`No se pudo guardar el borrador: ${errorMessage(e)}`, 'error');
      return false;
    } finally {
      setSaving(false);
    }
  };

  const save = async () => {
    if (await saveDraft()) toast('Borrador guardado', 'ok');
  };

  /** Publica: guarda el borrador (si hay cambios), lo valida en el servidor y lo convierte en una revisión nueva. */
  const publish = async () => {
    if (!doc) return;
    if (dirty || !hasDraft) {
      if (!(await saveDraft())) return;
    }
    setSaving(true);
    try {
      const res = await api.publish(doc.id);
      if (res.ok) {
        const merged: FlowDef = { ...res.flow, layout: res.flow.layout ?? doc.layout };
        setDoc(merged);
        setSaved(JSON.stringify(merged));
        setHasDraft(false);
        setPublished(true);
        setErrors([]);
        toast(`Publicado (revisión ${res.flow.revision})`, 'ok');
        loadSchedule();
      } else {
        setErrors(res.errors);
        showBottom('errors');
        toast(`No se publicó: ${res.errors.length} error(es). Queda como borrador.`, 'error');
      }
    } catch (e) {
      toast(`No se pudo publicar: ${errorMessage(e)}`, 'error');
    } finally {
      setSaving(false);
    }
  };

  /** Tira el borrador y vuelve a la última revisión publicada; un flujo que solo era borrador desaparece. */
  const discardDraft = async () => {
    const what = published ? 'Se vuelve a la última revisión publicada.' : 'El flujo no tiene revisiones publicadas: desaparece.';
    if (!window.confirm(`¿Descartar el borrador? ${what}`)) return;
    try {
      await api.discardDraft(flowId);
      setHasDraft(false);
      if (!published) {
        setDirty(false);
        navigate('#/');
        return;
      }
      const f = await api.getFlow(flowId);
      setDoc(f);
      setSaved(JSON.stringify(f));
      toast('Borrador descartado', 'ok');
    } catch (e) {
      toast(`No se pudo descartar: ${errorMessage(e)}`, 'error');
    }
  };

  /** Guarda el contenido de la revisión que se mira como una revisión nueva (la última pasa a ser esa). */
  const restore = async () => {
    if (!doc || rev === undefined) return;
    if (!window.confirm(`¿Restaurar la revisión ${rev}? Se guarda como una revisión nueva; la actual queda en el historial.`)) return;
    try {
      const res = await api.saveFlow(doc);
      if (!res.ok) return toast(`No se restauró: ${res.errors.map((x) => x.message).join('; ')}`, 'error');
      toast(`Restaurada la revisión ${rev} (ahora es la revisión ${res.flow.revision})`, 'ok');
      navigate(`#/flow/${encodeURIComponent(flowId)}`);
    } catch (e) {
      toast(`No se pudo restaurar: ${errorMessage(e)}`, 'error');
    }
  };

  const deleteFlow = async () => {
    if (!window.confirm(`¿Eliminar el flujo '${doc?.name}'?`)) return;
    try {
      setDirty(false);
      await api.deleteFlow(flowId);
      navigate('#/');
    } catch (e) {
      toast(`No se pudo eliminar: ${errorMessage(e)}`, 'error');
    }
  };

  const toggleEnabled = async () => {
    if (!schedule) return;
    setToggling(true);
    try {
      const next = schedule.enabled ? await api.disableFlow(flowId) : await api.enableFlow(flowId);
      setSchedule(next);
      toast(next.enabled ? 'Flujo habilitado: sus lanzadores cron se disparan' : 'Flujo deshabilitado', 'ok');
    } catch (e) {
      toast(`No se pudo cambiar: ${errorMessage(e)}`, 'error');
    } finally {
      setToggling(false);
    }
  };

  const onNodeStatus = useCallback((st: RunStatus) => {
    setNodeStatus(st.statuses);
    setFireAt(st.fireAt);
    setLive(st.live);
    setRunning(st.running);
  }, []);

  // ---- traza (modo test) -----------------------------------------------

  // Un paso de un segmento de stream es el anclaje, pero sus miembros también se han recorrido: se marcan todos. Sus
  // salidas son las de sus sumideros (`out`) o las de cualquier miembro (`error`); las aristas que entran en sus fuentes
  // también cuentan como recorridas.
  const trace: Trace | null = useMemo(() => {
    if (!traceView || traceView.steps.length === 0 || !doc) return null;
    const upto: TraceStep[] = traceView.steps.slice(0, traceView.index + 1);
    const visited = new Set<string>();
    const active = new Set<string>();
    for (const s of upto) {
      const seg = segments.find((x) => x.anchor === s.nodeId);
      const members = seg ? seg.members : [s.nodeId];
      members.forEach((id) => visited.add(id));
      for (const e of doc.edges) {
        const fromMember = members.includes(e.from);
        const leaves = seg
          ? s.port === 'error' ? fromMember && e.port === 'error' : seg.sinks.includes(e.from) && e.port === (s.port ?? 'out')
          : e.from === s.nodeId && e.port === s.port;
        const enters = !!seg && seg.sources.includes(e.to) && !members.includes(e.from) && upto.some((x) => x.nodeId === e.from);
        if (leaves || enters) active.add(edgeKey(e));
      }
    }
    return { current: traceView.steps[traceView.index].nodeId, visited, activeEdges: active };
  }, [traceView, doc, segments]);

  // ---- render ----------------------------------------------------------

  if (loadError) {
    return (
      <div className="mx-auto w-full max-w-6xl p-4">
        <Alert variant="error">{loadError}</Alert>
      </div>
    );
  }
  if (!doc) return <div className="mx-auto w-full max-w-6xl p-4"><p className="text-xs text-base-content/60">Cargando flujo…</p></div>;

  const errCount = allErrors.length;
  const enabled = schedule?.enabled;
  const launchers = schedule?.schedules.length ?? 0;

  return (
    <div
      className={`editor ${bottomOpen ? '' : 'bottom-closed'}`}
      style={{ '--palette-w': `${paletteWidth.width}px`, '--props-w': `${propsWidth.width}px` } as React.CSSProperties}
    >
      <div className="editor-toolbar">
        <Button variant="ghost" type="button" onClick={() => navigate('#/')}>← Flujos</Button>
        <div className="toolbar-title">
          <strong>{doc.name}</strong>{' '}
          <span className="text-xs text-base-content/60 font-mono">
            {doc.id} · {readOnly ? `revisión ${rev} (solo lectura)` : published ? `rev ${latest ?? doc.revision}` : 'sin publicar'}
          </span>
          {!readOnly && hasDraft && (
            <Badge variant="info" title="Hay cambios guardados que todavía no se publicaron; lo que corre es la última revisión publicada">
              borrador
            </Badge>
          )}
          {dirty && <Badge variant="warning">sin guardar</Badge>}
          {schedule && (
            <Badge variant={enabled ? 'success' : 'outline'} title="Los lanzadores cron solo disparan con el flujo habilitado">
              {enabled ? 'habilitado' : 'deshabilitado'}
            </Badge>
          )}
        </div>
        <div className="toolbar-actions">
          {schedule && (
            <Button variant="outline" type="button" onClick={toggleEnabled} disabled={toggling}>
              {enabled ? 'Deshabilitar' : 'Habilitar'}
            </Button>
          )}
          {readOnly ? (
            <>
              <Button type="button" onClick={restore}>Restaurar</Button>
              <Button variant="ghost" href={`#/flow/${encodeURIComponent(flowId)}`}>Ver la última</Button>
            </>
          ) : (
            <>
              <Button variant="outline" type="button" onClick={save} disabled={saving || !dirty} title="Guarda sin validar; no cambia lo que corre">
                Guardar borrador
              </Button>
              <Button
                type="button"
                onClick={publish}
                disabled={saving || errCount > 0 || (!dirty && !hasDraft)}
                title={errCount > 0 ? `Resuelve ${errCount} error(es) para publicar` : 'Valida y guarda una revisión nueva, que es la que corre'}
              >
                Publicar
              </Button>
              {hasDraft && (
                <Button variant="ghost" type="button" onClick={discardDraft} disabled={saving}>Descartar borrador</Button>
              )}
              <Button variant="outline" type="button" onClick={() => showBottom('run')}>Ejecutar</Button>
              <Button variant="outline" type="button" onClick={() => showBottom('test')}>Test</Button>
              <Button variant="ghost" className="text-error hover:text-error" type="button" onClick={deleteFlow}>Eliminar</Button>
            </>
          )}
        </div>
      </div>

      {readOnly && (
        <Alert variant="warning" className="readonly-banner rounded-none">
          Estás viendo la revisión {rev}, en solo lectura. «Restaurar» la guarda como una revisión nueva.
        </Alert>
      )}

      <aside className="editor-palette">
        <details open={window.matchMedia('(min-width: 900px)').matches}>
          <summary className="cursor-pointer py-1 font-semibold">Nodos</summary>
          <Palette catalog={catalog} onAdd={(kind) => addNode(kind)} />
        </details>
      </aside>

      <div className="editor-canvas">
        <ResizeHandle side="left" label="Ancho de la paleta" panel={paletteWidth} />
        <ResizeHandle side="right" label="Ancho del panel de propiedades" panel={propsWidth} />
        <Canvas
          flow={doc}
          catalog={catalog}
          shapes={shapes}
          selectedId={selectedId}
          errorsByNode={readOnly ? NO_ERRORS_BY_NODE : errorsByNode}
          edgeProblems={readOnly ? NO_EDGE_PROBLEMS : edgeMessages}
          nodeStatus={nodeStatus}
          fireAt={fireAt}
          live={readOnly ? undefined : live}
          running={running}
          trace={trace}
          readOnly={readOnly}
          elementTypes={elementTypes}
          onSelect={select}
          onMove={moveNodes}
          onRemoveNodes={removeNodes}
          onRemoveEdges={removeEdges}
          onConnect={connect}
          onAddNode={addNode}
          onMessage={(m) => toast(m, 'error')}
        />
      </div>

      <aside className="editor-props">
        <Tabs
          className="border-b border-base-300 px-2 pt-1"
          value={rightTab}
          onChange={setRightTab}
          items={[
            { value: 'node', label: 'Nodo' },
            { value: 'flow', label: 'Flujo' },
            { value: 'schedule', label: <>Programación {launchers > 0 && <Badge variant="outline">{launchers}</Badge>}</> },
            { value: 'revisions', label: 'Revisiones' },
          ]}
        />
        {rightTab === 'node' && selectedNode && (
          <fieldset className="readonly-set" disabled={readOnly}>
            <NodeProperties
              key={selectedNode.id}
              node={selectedNode}
              entry={catalog.find((c) => c.kind === selectedNode.kind)}
              flow={doc}
              ports={(shapes[selectedNode.id]?.ports ?? []).map((p) => p.name)}
              role={shapes[selectedNode.id]?.role ?? 'node'}
              errors={[
                ...allErrors.filter((e) => e.node === selectedNode.id || e.nodes?.includes(selectedNode.id)),
                // La configuración que el nodo dice estar mal, sin repetir lo que ya trae el validador.
                ...(shapes[selectedNode.id]?.problems ?? [])
                  .filter((m) => !allErrors.some((e) => e.message === m || e.message.endsWith(`: ${m}`)))
                  .map((message): FlowError => ({ code: 'invalid-params', node: selectedNode.id, message })),
              ]}
              paths={paths}
              elementPaths={Object.fromEntries(Object.entries(elementTypes?.inputs[selectedNode.id] ?? {}).map(([input, t]) => [input, elementPaths(t, entities)]))}
              onChange={(patch) => updateNode(selectedNode.id, patch)}
              onRename={(newId) => renameNode(selectedNode.id, newId)}
              onDelete={() => removeNodes([selectedNode.id])}
            />
          </fieldset>
        )}
        {rightTab === 'node' && !selectedNode && <p className="text-xs text-base-content/60">Selecciona un nodo en el lienzo para editar sus propiedades.</p>}
        {rightTab === 'flow' && (
          <fieldset className="readonly-set" disabled={readOnly}>
            <FlowSettings flow={doc} paths={paths} onChange={(patch) => update((d) => ({ ...d, ...patch }))} />
          </fieldset>
        )}
        {rightTab === 'schedule' && (
          <SchedulePanel
            schedule={schedule}
            error={scheduleError}
            dirty={dirty}
            toggling={toggling}
            onToggle={toggleEnabled}
          />
        )}
        {rightTab === 'revisions' && (
          latest !== null
            ? <RevisionsPanel flowId={flowId} latest={latest} current={rev} />
            : <p className="text-xs text-base-content/60">Cargando revisiones…</p>
        )}
      </aside>

      <section className="editor-bottom">
        <div className="flex items-center justify-between border-b border-base-300 px-2 pt-1">
          <Tabs
            value={bottomOpen ? bottomTab : ''}
            onChange={showBottom}
            items={[
              { value: 'errors' as BottomTab, label: <>Validación {errCount > 0 && <Badge variant="destructive">{errCount}</Badge>}</> },
              ...(readOnly ? [] : [{ value: 'run' as BottomTab, label: 'Ejecutar' }, { value: 'test' as BottomTab, label: 'Test' }]),
            ]}
          />
          <Button
            variant="ghost"
            size="icon-sm"
            type="button"
            aria-label={bottomOpen ? 'Plegar panel' : 'Desplegar panel'}
            onClick={() => setBottomOpen((o) => !o)}
          >
            {bottomOpen ? <ChevronDown /> : <ChevronUp />}
          </Button>
        </div>
        {bottomOpen && <div className="bottom-body">
          {bottomTab === 'errors' && (
            <ErrorsPanel errors={readOnly ? [] : allErrors} validating={validating} onFocus={select} />
          )}
          {bottomTab === 'run' && !readOnly && (
            <RunPanel key={doc.id} flow={doc} dirty={dirty} onNodeStatus={onNodeStatus} />
          )}
          {bottomTab === 'test' && !readOnly && (
            <TestPanel
              key={doc.id}
              flow={doc}
              catalog={catalog}
              shapes={shapes}
              trace={traceView}
              onTrace={setTraceView}
            />
          )}
        </div>}
      </section>
    </div>
  );
}
