import { useContext, useState } from 'react';
import { api } from '../api/client';
import type { CatalogEntry, FlowDef, JsonValue, Mock, NodeMetrics, Trace, TraceStep } from '../api/types';
import type { NodeShape } from '../hooks/usePorts';
import { templateFromDecls } from '../graph/graph';
import { useEntities } from '../hooks/useEntities';
import { AppContext } from '../App';
import { JsonBox } from './JsonBox';
import { ElementsEditor } from './ElementsEditor';
import { Button } from '@/components/ui/button';
import { SearchSelect } from '@/components/ui/search-select';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

export interface TraceView {
  steps: TraceStep[];
  index: number;
}

interface MockDraft {
  mode: 'none' | 'output' | 'error';
  port: string;
  values: JsonValue;
  /** Fuentes de stream: la lista de elementos que emite (`values.elements`). */
  elements: JsonValue[];
  error: string;
}

interface Props {
  flow: FlowDef;
  catalog: CatalogEntry[];
  shapes: Record<string, NodeShape>;
  trace: TraceView | null;
  onTrace: (t: TraceView | null) => void;
}

const EMPTY_DRAFT: MockDraft = { mode: 'none', port: 'out', values: {}, elements: [], error: '' };

/** Modo test: entradas, mocks por nodo y traza paso a paso (se pinta sobre el grafo). Usa el borrador, no hace falta guardar. */
export function TestPanel({ flow, catalog, shapes, trace, onTrace }: Props) {
  const { toast } = useContext(AppContext);
  const { byName: entities } = useEntities();
  const [inputs, setInputs] = useState<JsonValue | undefined>(() => templateFromDecls(flow.inputs, entities));
  const [valid, setValid] = useState(true);
  const [mocks, setMocks] = useState<Record<string, MockDraft>>({});
  const [busy, setBusy] = useState(false);
  const [raw, setRaw] = useState<Trace | null>(null);

  const draftOf = (id: string): MockDraft => mocks[id] ?? EMPTY_DRAFT;
  const setDraft = (id: string, patch: Partial<MockDraft>) => setMocks((m) => ({ ...m, [id]: { ...draftOf(id), ...patch } }));

  const run = async () => {
    const body: Record<string, Mock> = {};
    for (const n of flow.nodes) {
      const d = draftOf(n.id);
      if (d.mode === 'error') body[n.id] = { type: 'fail', error: d.error || 'fallo simulado' };
      if (d.mode === 'output') {
        const isSource = shapes[n.id]?.role === 'source';
        // Una fuente de stream recibe su lista de elementos; cualquier otro nodo, sus valores de salida.
        const values = isSource ? { elements: d.elements } : ((d.values ?? {}) as Record<string, JsonValue>);
        body[n.id] = { type: 'output', port: d.port, values };
      }
    }
    setBusy(true);
    try {
      const t = await api.simulate(flow.id, { flow, inputs: inputs ?? {}, mocks: body });
      setRaw(t);
      onTrace({ steps: t.steps ?? [], index: 0 });
    } catch (e) {
      toast(`La simulación falló: ${(e as Error).message}`, 'error');
    } finally {
      setBusy(false);
    }
  };

  const step = trace?.steps[trace.index];

  return (
    <div className="test-panel">
      <div className="run-col">
        <JsonBox label="Entradas de prueba" value={inputs} onChange={setInputs} onValid={setValid} rows={5} />
        <h3>Mocks por nodo</h3>
        <p className="text-xs text-base-content/60">Sin mock, el nodo corre según su tipo: “escribe” se simula siempre; “puro” se ejecuta de verdad.</p>
        {flow.nodes.length === 0 && <p className="text-xs text-base-content/60">El flujo no tiene nodos.</p>}
        {flow.nodes.map((n) => {
          const d = draftOf(n.id);
          const kind = catalog.find((c) => c.kind === n.kind);
          const shape = shapes[n.id];
          const portNames = (shape?.ports ?? []).map((p) => p.name);
          const isSource = shape?.role === 'source';
          return (
            <div className="mock-row" key={n.id}>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="font-mono">{n.id}</span>
                <Badge variant="outline">
                  {isSource ? 'fuente de stream' : kind?.sideEffects === 'write' ? 'simulado' : kind?.sideEffects === 'read' ? 'lectura' : 'real'}
                </Badge>
              </div>
              <SearchSelect
                aria-label={`Mock de ${n.id}`}
                value={d.mode}
                onChange={(mode) => setDraft(n.id, { mode })}
                options={[
                  { value: 'none', label: 'sin mock' },
                  { value: 'output', label: isSource ? 'elementos fijos' : 'salida fija' },
                  { value: 'error', label: 'error' },
                ]}
              />
              {d.mode === 'output' && isSource && (
                <ElementsEditor
                  label={`Elementos que emite ${n.id}`}
                  value={d.elements}
                  onChange={(elements) => setDraft(n.id, { elements })}
                />
              )}
              {d.mode === 'output' && !isSource && (
                <>
                  <SearchSelect
                    aria-label={`Puerto de ${n.id}`}
                    value={d.port}
                    onChange={(port) => setDraft(n.id, { port })}
                    options={portNames.filter((p) => p !== 'error').map((p) => ({ value: p, label: p }))}
                  />
                  <JsonBox label="Valores (nombre de salida → valor)" value={d.values} onChange={(v) => setDraft(n.id, { values: v ?? {} })} rows={3} />
                </>
              )}
              {d.mode === 'error' && (
                <Input  placeholder="mensaje del error" value={d.error} onChange={(e) => setDraft(n.id, { error: e.target.value })} />
              )}
            </div>
          );
        })}
        <Button type="button" disabled={!valid || busy} onClick={run}>Ejecutar prueba</Button>
      </div>

      <div className="run-col">
        <h3>Traza</h3>
        {!trace && <p className="text-xs text-base-content/60">Ejecuta una prueba para ver el recorrido.</p>}
        {trace && (
          <>
            <div className="flex flex-wrap items-center gap-2">
              <Button variant="outline" type="button" disabled={trace.index <= 0} onClick={() => onTrace({ ...trace, index: trace.index - 1 })}>Anterior</Button>
              <Button variant="outline" type="button" disabled={trace.index>= trace.steps.length - 1} onClick={() => onTrace({ ...trace, index: trace.index + 1 })}>Siguiente</Button>
              <span className="text-xs text-base-content/60">paso {trace.steps.length ? trace.index + 1 : 0} de {trace.steps.length}</span>
            </div>
            {trace.steps.length > 0 && (
              <input
                type="range"
                min={0}
                max={trace.steps.length - 1}
                value={trace.index}
                aria-label="Paso de la traza"
                onChange={(e) => onTrace({ ...trace, index: Number(e.target.value) })}
              />
            )}
            {step && (
              <div className="flex flex-col gap-1 rounded-md bg-base-200 p-2">
                <div><span className="font-mono">{step.nodeId}</span> {step.kind && <span className="text-xs text-base-content/60">{step.kind}</span>}</div>
                <div>puerto: <span className="font-mono">{step.port ?? '—'}</span> {step.mocked && <Badge variant="outline">mock</Badge>} {step.outcome && <Badge variant="outline">{step.outcome}</Badge>}</div>
                {step.error && <div className="my-0.5 text-xs text-error">{step.error}</div>}
                {step.kind === 'stream.segment' && <SegmentDetail step={step} />}
              </div>
            )}
            <ol className="trace-list">
              {trace.steps.map((s, i) => (
                <li key={i}>
                  <Button type="button" variant="link" size="xs" className={`h-auto p-0 ${i === trace.index ? 'font-semibold' : 'text-base-content'}`} onClick={() => onTrace({ ...trace, index: i })}>
                    {i + 1}. <span className="font-mono">{s.nodeId}</span>{s.kind === 'stream.segment' ? ' (stream)' : ''} → <span className="font-mono">{s.port ?? '—'}</span>
                  </Button>
                </li>
              ))}
            </ol>
            {raw && (
              <>
                <h4>Estado final</h4>
                <pre className="max-h-60 overflow-auto rounded-md bg-base-200 p-2 text-xs">{JSON.stringify(raw.state ?? step?.state ?? null, null, 2)}</pre>
                <details>
                  <summary className="text-xs text-base-content/60">Respuesta completa</summary>
                  <pre className="max-h-60 overflow-auto rounded-md bg-base-200 p-2 text-xs">{JSON.stringify(raw, null, 2)}</pre>
                </details>
              </>
            )}
          </>
        )}
      </div>
    </div>
  );
}

/** Un paso de segmento: las métricas de cada nodo del segmento y los elementos descartados. */
function SegmentDetail({ step }: { step: TraceStep }) {
  const metrics: Record<string, NodeMetrics> = (step.info?.metrics ?? {}) as Record<string, NodeMetrics>;
  const dead = step.info?.deadLetters ?? [];
  const outs = Object.entries(step.values ?? {});
  return (
    <div className="segment-detail">
      <Table className="text-xs">
        <TableHeader>
          <TableRow><TableHead>nodo</TableHead><TableHead>entran</TableHead><TableHead>salen</TableHead><TableHead>fallos</TableHead><TableHead>ms</TableHead></TableRow>
        </TableHeader>
        <TableBody>
          {Object.entries(metrics).map(([id, m]) => (
            <TableRow key={id}>
              <TableCell className="font-mono">{id}</TableCell>
              <TableCell className="num">{m.in}</TableCell>
              <TableCell className="num">{m.out}</TableCell>
              <TableCell className="num">{m.failed}</TableCell>
              <TableCell className="num">{m.latencyMs ?? ''}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      {outs.map(([name, value]) => (
        <div key={name}>
          <span className="font-mono">{name}</span> <span className="text-xs text-base-content/60">{Array.isArray(value) ? `${value.length} elemento(s)` : ''}</span>
          <pre className="max-h-60 overflow-auto rounded-md bg-base-200 p-2 text-xs">{JSON.stringify(value, null, 2)}</pre>
        </div>
      ))}
      {dead.length > 0 && (
        <p className="my-0.5 text-xs text-error">{dead.length} elemento(s) descartado(s): {dead.map((d) => `${d.node}: ${d.error}`).join('; ')}</p>
      )}
    </div>
  );
}
