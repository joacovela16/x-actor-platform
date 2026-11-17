import { newerMajor } from '@/lib/plugins';
import { categoryClass } from '../lib/category';
import {
  BaseEdge,
  Background,
  Controls,
  EdgeLabelRenderer,
  getBezierPath,
  Handle,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useNodesState,
  useReactFlow,
  useNodesInitialized,
  useStore,
  useUpdateNodeInternals,
  type Edge,
  type EdgeChange,
  type EdgeProps,
  type Node,
  type NodeChange,
  type NodeProps,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { memo, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import type { CatalogEntry, FlowDef, NodeRole, SideEffects } from '../api/types';
import type { NodeShape } from '../hooks/usePorts';
import { ERROR_PORT, edgeKey } from '../graph/graph';
import { fmtClock, fmtCountdown } from '../graph/time';
import { findSegments, inputOf, isStreamEdge, renderDataType, segmentIndexOf, streamOf } from '../graph/stream';
import { Badge } from '@/components/ui/badge';

/** Un puerto como lo pinta el nodo: sin el tipo completo (solo si es de stream y cuál es su tipo de elemento). */
export interface PortView {
  name: string;
  label?: string;
  /** Tipo de los elementos si el puerto lleva un stream. */
  stream?: string;
}

/** Contadores en vivo de un nodo de stream (se actualizan cada pocos segundos, no por cada elemento). */
export interface LiveCounters {
  in: number;
  out: number;
  failed: number;
  /** Ha habido actividad desde la lectura anterior. */
  active: boolean;
}

/**
 * Datos de un nodo en el lienzo. Son pocos y estables: nada de muestras ni listas grandes. `sameData` compara por valor,
 * así que un nodo cuyo contenido no cambia no se re-renderiza al arrastrar ni al validar.
 */
export interface FlowNodeData extends Record<string, unknown> {
  label: string;
  kind: string;
  sideEffects: SideEffects;
  /** El tipo del major más nuevo, si el nodo usa uno viejo de un plugin. */
  newer?: string;
  role: NodeRole;
  outs: PortView[];
  ins: PortView[];
  status?: string;
  errors: string[];
  trace?: 'current' | 'visited';
  /** Solo `core.cron`: lanzador (sin aristas de entrada) o temporizador (con entrada). */
  cronMode?: 'launcher' | 'timer';
  /** Solo un temporizador que espera: epoch ms en que sigue. */
  fireAt?: number;
  /** Índice del segmento de stream al que pertenece (color). */
  seg?: number;
  live?: LiveCounters;
}

interface EdgeData extends Record<string, unknown> {
  /** Tipo de los elementos, si la arista es de stream. */
  streamType?: string;
  problem?: string;
  live?: boolean;
  trace?: boolean;
  /** Borra la arista (no está si el flujo es de solo lectura). */
  onDelete?: () => void;
}

type FNode = Node<FlowNodeData, 'flow'>;
type FEdge = Edge<EdgeData>;

const CRON_KIND = 'core.cron';
const ROW = 24;
// Cabecera: padding 6 + título 22 + tipo 16 + fila de errores 14 + 2 de margen. La fila de errores siempre existe
// (vacía si no hay errores), así el alto del nodo no cambia al validar ni al seleccionar.
const HEAD = 60;
const SEGMENT_COLORS = 6;

const NO_ERRORS: string[] = [];
const NO_PORTS: PortView[] = [];

/** Hora en que sigue un temporizador, refrescada cada segundo solo en ese nodo. */
function useCountdown(fireAt: number | undefined): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (fireAt === undefined) return;
    setNow(Date.now());
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [fireAt]);
  return now;
}

const FlowNodeView = memo(function FlowNodeView({ id, data, selected, dragging }: NodeProps<FNode>) {
  const updateNodeInternals = useUpdateNodeInternals();
  const launcher = data.cronMode === 'launcher';
  const insKey = data.ins.map((p) => p.name).join(',');
  const outsKey = data.outs.map((p) => p.name).join(',');
  // Cuando cambian los puertos (p. ej. al llegar la descripción) hay que remedir los handles.
  useEffect(() => {
    updateNodeInternals(id);
  }, [id, insKey, outsKey, data.cronMode, updateNodeInternals]);

  const now = useCountdown(data.fireAt);
  const outs = data.outs.some((p) => p.name === ERROR_PORT) ? data.outs : [...data.outs, { name: ERROR_PORT, label: 'error' }];
  const ins = launcher ? NO_PORTS : data.ins;
  const rows = Math.max(outs.length, ins.length, 1);
  const height = HEAD + rows * ROW + 8;
  const cls = [
    'flow-node',
    categoryClass(data.kind),
    data.status ? `st-${data.status}` : '',
    data.trace ? `trace-${data.trace}` : '',
    selected ? 'is-selected' : '',
    data.errors.length ? 'has-errors' : '',
    data.seg !== undefined ? `seg-${data.seg}` : '',
    data.role !== 'node' ? `role-${data.role}` : '',
    data.live?.active ? 'is-active' : '',
  ]
    .filter(Boolean)
    .join(' ');
  let errLine = ' ';
  if (data.errors.length > 0) errLine = `${data.errors.length} error(es)`;
  else if (data.fireAt !== undefined) errLine = `esperando hasta ${fmtClock(data.fireAt)} · ${fmtCountdown(data.fireAt, now)}`;
  else if (data.live) errLine = `${data.live.in} → ${data.live.out}${data.live.failed ? ` · ${data.live.failed} fallos` : ''}`;
  return (
    // El tooltip nativo no se muestra durante el arrastre (aparecía bajo el cursor mientras el nodo se movía).
    <div className={cls} style={{ height }} title={dragging ? undefined : data.errors.join('\n') || undefined}>
      {ins.map((p, i) => (
        <div key={`in-${p.name}`}>
          <span className="port-label is-in" style={{ top: HEAD + i * ROW + ROW / 2 - 9 }}>{p.label ?? p.name}</span>
          <Handle
            type="target"
            position={Position.Left}
            id={p.name}
            className={`handle-in ${p.stream ? 'handle-stream' : ''}`}
            style={{ top: HEAD + i * ROW + ROW / 2 }}
          />
        </div>
      ))}
      <div className="node-title">{data.label}</div>
      <div className="node-kind font-mono">
        {data.kind}
        {data.sideEffects !== 'pure' && <Badge variant="outline" className="h-4 px-1.5 text-[10px]">{data.sideEffects === 'write' ? 'escribe' : 'lee'}</Badge>}
        {data.newer && <Badge variant="warning" className="h-4 px-1.5 text-[10px]" title={`Hay una versión más nueva: ${data.newer}`}>major viejo</Badge>}
        {data.role !== 'node' && <Badge variant="secondary" className="h-4 px-1.5 text-[10px]">{roleLabel(data.role)}</Badge>}
        {data.cronMode && (
          <Badge variant="outline" className={`h-4 px-1.5 text-[10px] ${launcher ? 'border-primary text-primary' : 'border-warning text-warning'}`}>{launcher ? 'lanzador' : 'temporizador'}</Badge>
        )}
      </div>
      <div className={`node-err ${data.fireAt !== undefined && data.errors.length === 0 ? 'is-waiting' : ''} ${data.live && data.errors.length === 0 ? 'is-live' : ''}`}>
        {errLine}
        {data.live?.active && <span className="activity-dot" aria-hidden="true" />}
      </div>
      {outs.map((p, i) => {
        const top = HEAD + i * ROW + ROW / 2;
        return (
          <div key={`out-${p.name}`}>
            <span className={`port-label ${p.name === ERROR_PORT ? 'is-error' : ''} ${p.stream ? 'is-stream' : ''}`} style={{ top: top - 9 }}>
              {p.label ?? p.name}
            </span>
            <Handle
              type="source"
              position={Position.Right}
              id={p.name}
              className={`${p.name === ERROR_PORT ? 'handle-error' : ''} ${p.stream ? 'handle-stream' : ''}`}
              style={{ top }}
            />
          </div>
        );
      })}
    </div>
  );
});

function roleLabel(role: NodeRole): string {
  return role === 'source' ? 'fuente' : role === 'sink' ? 'sumidero' : 'operador';
}

const nodeTypes = { flow: FlowNodeView };

/** Botón para borrar la arista seleccionada, en su punto medio. */
function EdgeDeleteButton({ x, y, onDelete }: { x: number; y: number; onDelete: () => void }) {
  return (
    <EdgeLabelRenderer>
      <button
        type="button"
        className="edge-delete nodrag nopan"
        style={{ transform: `translate(-50%, -50%) translate(${x}px, ${y}px)` }}
        title="Borrar conexión (Supr)"
        aria-label="Borrar conexión"
        onClick={(e) => {
          e.stopPropagation();
          onDelete();
        }}
      >
        ×
      </button>
    </EdgeLabelRenderer>
  );
}

/** Arista normal (de token): curva con su etiqueta de puerto y, si está seleccionada, el botón de borrar. */
const TokenEdgeView = memo(function TokenEdgeView(props: EdgeProps<FEdge>) {
  const [path, labelX, labelY] = getBezierPath(props);
  const onDelete = props.selected ? props.data?.onDelete : undefined;
  return (
    <>
      <BaseEdge
        id={props.id}
        path={path}
        markerEnd={props.markerEnd}
        style={props.style}
        interactionWidth={props.interactionWidth}
        label={onDelete ? undefined : props.label}
        labelX={labelX}
        labelY={labelY}
        labelStyle={props.labelStyle}
        labelShowBg={props.labelShowBg}
        labelBgStyle={props.labelBgStyle}
        labelBgPadding={props.labelBgPadding}
        labelBgBorderRadius={props.labelBgBorderRadius}
      />
      {onDelete && <EdgeDeleteButton x={labelX} y={labelY} onDelete={onDelete} />}
    </>
  );
});

/** Arista de stream: trazo grueso con flujo (solo mientras corre el segmento) y el tipo de elemento al pasar el cursor. */
const StreamEdgeView = memo(function StreamEdgeView(props: EdgeProps<FEdge>) {
  const [path, labelX, labelY] = getBezierPath(props);
  const [hover, setHover] = useState(false);
  const data = props.data;
  const cls = ['edge-stream', data?.live ? 'is-live' : '', data?.problem ? 'is-invalid' : '', props.selected ? 'is-selected' : ''].filter(Boolean).join(' ');
  return (
    <>
      <BaseEdge id={props.id} path={path} className={cls} markerEnd={props.markerEnd} />
      {/* Trazo invisible y ancho para que el cursor la encuentre sin apuntar al milímetro. */}
      <path
        d={path}
        fill="none"
        stroke="transparent"
        strokeWidth={14}
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => setHover(false)}
      />
      {props.selected && data?.onDelete && <EdgeDeleteButton x={labelX} y={labelY} onDelete={data.onDelete} />}
      {hover && !props.selected && (data?.streamType || data?.problem) && (
        <EdgeLabelRenderer>
          <div className={`edge-tip ${data?.problem ? 'is-invalid' : ''}`} style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }}>
            {data?.problem ?? `stream de ${data?.streamType}`}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
});

const edgeTypes = { stream: StreamEdgeView, token: TokenEdgeView };

/** Igualdad de contenido de `data` (arrays y objetos por valor). Los arrays se recrean en cada validación. */
function sameData(a: FlowNodeData, b: FlowNodeData): boolean {
  const keys = Object.keys(a) as (keyof FlowNodeData)[];
  if (keys.length !== Object.keys(b).length) return false;
  return keys.every((k) => {
    const x = a[k];
    const y = b[k];
    if (x === y) return true;
    return typeof x === 'object' && x !== null && JSON.stringify(x) === JSON.stringify(y);
  });
}

export interface Trace {
  current: string | null;
  visited: Set<string>;
  activeEdges: Set<string>;
}

interface Props {
  flow: FlowDef;
  catalog: CatalogEntry[];
  /** Forma de cada nodo: salidas, entradas, papel y problemas de configuración. */
  shapes: Record<string, NodeShape>;
  selectedId: string | null;
  errorsByNode: Record<string, string[]>;
  /** Aristas que el cliente ve mal (token/stream, tipo, entrada inexistente): clave -> mensaje. */
  edgeProblems: Record<string, string>;
  nodeStatus: Record<string, string>;
  /** Temporizadores en espera: id del nodo -> epoch ms en que sigue. */
  fireAt?: Record<string, number>;
  /** Contadores en vivo de los nodos de stream. */
  live?: Record<string, LiveCounters>;
  /** La instancia corre: las aristas de stream se animan. */
  running?: boolean;
  trace: Trace | null;
  onSelect: (id: string | null) => void;
  onMove: (positions: Record<string, { x: number; y: number }>) => void;
  onRemoveNodes: (ids: string[]) => void;
  onRemoveEdges: (keys: string[]) => void;
  /** Devuelve un mensaje de error si la conexión no se puede añadir. `input` es la entrada del destino. */
  onConnect: (from: string, port: string, to: string, input: string) => string | null;
  onAddNode: (kind: string, at: { x: number; y: number }) => void;
  onMessage: (msg: string) => void;
  /** Solo lectura (vista de auditoría): no se mueven, conectan ni borran nodos. */
  readOnly?: boolean;
  /** Clase extra por clave de arista (p. ej. edge-fired / edge-dead). */
  edgeClasses?: Record<string, string>;
  onNodeClick?: (id: string) => void;
}

export function Canvas(props: Props) {
  return (
    <ReactFlowProvider>
      <Board {...props} />
    </ReactFlowProvider>
  );
}

function Board(p: Props) {
  const { screenToFlowPosition } = useReactFlow();

  // Posiciones: durante el arrastre React Flow es la fuente de verdad (estado interno). El modelo (`layout`) se
  // actualiza solo al soltar (onNodeDragStop). Así, cada paso del arrastre no reconstruye el flujo ni re-renderiza
  // el editor. El layout se lee por ref: cambiar la posición no invalida los datos de los nodos.
  // Aristas seleccionadas: las aristas son controladas, así que la selección se guarda aquí.
  const [selectedEdges, setSelectedEdges] = useState<Set<string>>(() => new Set());
  const removeEdgesRef = useRef(p.onRemoveEdges);
  removeEdgesRef.current = p.onRemoveEdges;

  const layoutRef = useRef(p.flow.layout);
  layoutRef.current = p.flow.layout;

  // Nodos con alguna arista de entrada: un `core.cron` con entrada es un temporizador, sin ella, un lanzador.
  const withInput = useMemo(() => new Set(p.flow.edges.map((e) => e.to)), [p.flow.edges]);

  // Segmentos de stream: dependen de la estructura (nodos, aristas, puertos), nunca de la posición ni del arrastre.
  const segments = useMemo(() => findSegments(p.flow.nodes, p.flow.edges, p.shapes), [p.flow.nodes, p.flow.edges, p.shapes]);
  const segOf = useMemo(() => segmentIndexOf(segments), [segments]);

  const derived: FNode[] = useMemo(() => {
    return p.flow.nodes.map((n, i) => {
      const entry = p.catalog.find((c) => c.kind === n.kind);
      const shape = p.shapes[n.id];
      const pos = layoutRef.current?.[n.id] ?? { x: 80 + (i % 4) * 240, y: 80 + Math.floor(i / 4) * 160 };
      const traced = p.trace?.current === n.id ? 'current' : p.trace?.visited.has(n.id) ? 'visited' : undefined;
      const toView = (ps: NonNullable<typeof shape>['outs']): PortView[] =>
        ps.map((x) => ({ name: x.name, label: x.label, stream: x.stream ? renderDataType(x.stream) : undefined }));
      const data: FlowNodeData = {
        label: n.label || n.id,
        kind: n.kind,
        sideEffects: entry?.sideEffects ?? 'pure',
        newer: newerMajor(n.kind, p.catalog),
        role: shape?.role ?? entry?.role ?? 'node',
        outs: shape ? toView(shape.outs) : NO_PORTS,
        ins: shape ? toView(shape.ins) : NO_PORTS,
        status: p.nodeStatus[n.id],
        errors: p.errorsByNode[n.id] ?? NO_ERRORS,
        trace: traced,
      };
      if (n.kind === CRON_KIND) data.cronMode = withInput.has(n.id) ? 'timer' : 'launcher';
      const fire = p.fireAt?.[n.id];
      if (fire !== undefined) data.fireAt = fire;
      const seg = segOf.get(n.id);
      if (seg !== undefined) data.seg = seg % SEGMENT_COLORS;
      const live = p.live?.[n.id];
      if (live) data.live = live;
      return { id: n.id, type: 'flow', position: pos, selected: p.selectedId === n.id, data };
    });
    // Sin `layoutRef` en las dependencias: mover un nodo no reconstruye los datos.
  }, [p.flow.nodes, p.catalog, p.shapes, p.selectedId, p.errorsByNode, p.nodeStatus, p.fireAt, p.live, p.trace, withInput, segOf]);

  const [nodes, setNodes, applyNodeChanges] = useNodesState<FNode>([]);

  // Al cambiar la estructura o los datos se reconstruyen los nodos, pero se conserva lo que React Flow ya tiene
  // (posición en curso, medidas, arrastre). Un nodo cuyo contenido visible no cambió conserva su mismo objeto: así
  // seleccionar un nodo (al empezar a arrastrarlo) o validar (al soltar) no re-renderiza todo el lienzo.
  useLayoutEffect(() => {
    setNodes((prev) => {
      const byId = new Map(prev.map((n) => [n.id, n]));
      return derived.map((d) => {
        const old = byId.get(d.id);
        if (!old) return d;
        if (old.selected === d.selected && old.type === d.type && sameData(old.data, d.data)) return old;
        return { ...old, ...d, position: old.position };
      });
    });
  }, [derived, setNodes]);

  const edges: FEdge[] = useMemo(
    () =>
      p.flow.edges.map((e) => {
        const key = edgeKey(e);
        const active = p.trace?.activeEdges.has(key);
        const isStream = isStreamEdge(e, p.shapes);
        const out = streamOf(p.shapes[e.from], e.port);
        const extra = p.edgeClasses?.[key] ?? '';
        const common = {
          id: key,
          source: e.from,
          sourceHandle: e.port,
          target: e.to,
          targetHandle: inputOf(e),
          animated: !!active && !isStream,
          className: [e.port === ERROR_PORT ? 'edge-error' : '', active ? 'edge-trace' : '', extra].join(' ').trim(),
          selected: selectedEdges.has(key),
        };
        const onDelete = p.readOnly ? undefined : () => removeEdgesRef.current([key]);
        if (isStream) {
          return {
            ...common,
            type: 'stream',
            data: {
              streamType: out ? renderDataType(out) : undefined,
              problem: p.edgeProblems[key],
              live: !!p.running,
              trace: !!active,
              onDelete,
            },
          } as FEdge;
        }
        return {
          ...common,
          type: 'token',
          label: e.port === 'out' ? (inputOf(e) !== 'in' ? inputOf(e) : undefined) : e.port,
          data: { problem: p.edgeProblems[key], onDelete },
        } as FEdge;
      }),
    [p.flow.edges, p.trace, p.edgeClasses, p.shapes, p.edgeProblems, p.running, p.readOnly, selectedEdges],
  );

  const onNodesChange = (changes: NodeChange<FNode>[]) => {
    applyNodeChanges(changes); // posiciones, medidas y selección viven aquí durante el arrastre
    const removed: string[] = [];
    for (const c of changes) {
      if (c.type === 'remove') removed.push(c.id);
      if (c.type === 'select' && c.selected) p.onSelect(c.id);
    }
    if (removed.length) p.onRemoveNodes(removed);
  };

  // Al soltar se confirma la posición en el modelo (una sola actualización del flujo por arrastre).
  const onNodeDragStop = (_: unknown, __: FNode, dragged: FNode[]) => {
    const moved: Record<string, { x: number; y: number }> = {};
    for (const n of dragged) moved[n.id] = { x: n.position.x, y: n.position.y };
    if (Object.keys(moved).length) p.onMove(moved);
  };

  const onEdgesChange = (changes: EdgeChange<FEdge>[]) => {
    const selects = changes.filter((c) => c.type === 'select');
    if (selects.length)
      setSelectedEdges((prev) => {
        const next = new Set(prev);
        for (const c of selects) if (c.type === 'select') (c.selected ? next.add(c.id) : next.delete(c.id));
        return next;
      });
    const removed = changes.flatMap((c) => (c.type === 'remove' ? [c.id] : []));
    if (removed.length) p.onRemoveEdges(removed);
  };

  return (
    <div
      className="canvas"
      onDragOver={(e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'copy';
      }}
      onDrop={(e) => {
        e.preventDefault();
        const kind = e.dataTransfer.getData('application/x-flow-kind');
        if (!kind) return;
        p.onAddNode(kind, screenToFlowPosition({ x: e.clientX, y: e.clientY }));
      }}
    >
      <ReactFlow<FNode, FEdge>
        nodes={nodes}
        onNodeDragStop={onNodeDragStop}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={(c) => {
          if (!c.source || !c.target) return;
          const port = c.sourceHandle ?? 'out';
          const input = c.targetHandle ?? 'in';
          if (c.source === c.target) return p.onMessage('Un nodo no puede conectarse consigo mismo.');
          const err = p.onConnect(c.source, port, c.target, input);
          if (err) p.onMessage(err);
        }}
        onPaneClick={() => p.onSelect(null)}
        deleteKeyCode={p.readOnly ? null : ['Backspace', 'Delete']}
        nodesDraggable={!p.readOnly}
        nodesConnectable={!p.readOnly}
        onNodeClick={(_, node) => p.onNodeClick?.(node.id)}
        fitView
        fitViewOptions={{ padding: 0.2, maxZoom: 1 }}
        minZoom={0.3}
        proOptions={{ hideAttribution: false }}
      >
        <Background gap={20} size={1} />
        <Controls showInteractive={false} />
        <AutoFit flowId={p.flow.id} />
      </ReactFlow>
      {p.flow.nodes.length === 0 && (
        <div className="canvas-empty text-xs text-base-content/60">Arrastra un nodo desde la paleta o usa “Añadir”.</div>
      )}
    </div>
  );
}

const FIT = { padding: 0.2, maxZoom: 1 };

/**
 * Centra el diagrama al abrir un flujo: cuando los nodos ya están medidos y cada vez que el lienzo cambia de tamaño
 * (la rejilla del editor se asienta después del primer render). Deja de hacerlo en cuanto el usuario mueve la vista.
 */
function AutoFit({ flowId }: { flowId: string }) {
  const { fitView } = useReactFlow();
  const ready = useNodesInitialized();
  const touched = useRef(false);
  const el = useStore((st) => st.domNode);

  useEffect(() => {
    touched.current = false;
  }, [flowId]);

  useEffect(() => {
    if (ready && !touched.current) requestAnimationFrame(() => fitView(FIT));
  }, [ready, flowId, fitView]);

  useEffect(() => {
    if (!el) return;
    // Rueda o arrastre sobre el lienzo: el usuario eligió su vista y ya no se recentra solo.
    const pane = el.querySelector('.react-flow__pane');
    const mark = () => (touched.current = true);
    pane?.addEventListener('wheel', mark, { passive: true });
    pane?.addEventListener('pointerdown', mark);
    const ro = new ResizeObserver(() => {
      if (!touched.current) fitView(FIT);
    });
    ro.observe(el);
    return () => {
      ro.disconnect();
      pane?.removeEventListener('wheel', mark);
      pane?.removeEventListener('pointerdown', mark);
    };
  }, [el, fitView]);

  return null;
}
