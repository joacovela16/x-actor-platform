import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { api, subscribeInstance } from '../api/client';
import type { AuditEntry, AuditTrail, DeadLetter, FlowDef, JsonValue, NodeMetrics } from '../api/types';
import { Canvas, type Trace } from '../components/Canvas';
import type { NodeShape } from '../hooks/usePorts';
import { AppContext } from '../App';
import {
  fmtDateTime, fmtDuration, fmtTime, kindLabel, kindTone, lastEntryOfNode, logsOf, originText,
  REASON_LABEL, renderValue, segmentView, snapshotAt, summaryOf, usageOf,
} from '../graph/audit';
import { portsOf } from '../graph/graph';
import { fmtClock, fmtCountdown, toMillis } from '../graph/time';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { StatusBadge } from '@/components/StatusBadge';
import { Checkbox } from '@/components/ui/input';
import { Alert } from '@/components/ui/alert';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';

const NOOP = () => {};
const EMPTY_TRACE: Trace = { current: null, visited: new Set(), activeEdges: new Set() };

/** Auditoría de una instancia: línea de tiempo, detalle de cada paso y recorrido sobre el grafo (en vivo por SSE). */
export function InstanceAudit({ id }: { id: string }) {
  const { catalog, toast } = useContext(AppContext);
  const [trail, setTrail] = useState<AuditTrail | null>(null);
  const [flow, setFlow] = useState<FlowDef | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [sel, setSel] = useState(-1);
  const [follow, setFollow] = useState(false);
  const firstLoad = useRef(true);
  const selRowRef = useRef<HTMLLIElement | null>(null);

  const load = useCallback(() => {
    return api
      .getAudit(id)
      .then((t) => {
        setTrail(t);
        setError(null);
      })
      .catch((e: Error) => setError(`No se pudo cargar la auditoría: ${e.message}`));
  }, [id]);

  useEffect(() => {
    firstLoad.current = true;
    setTrail(null);
    setSel(-1);
    void load();
  }, [load]);

  const entries: AuditEntry[] = useMemo(() => trail?.entries ?? [], [trail]);
  const running = trail?.instance.status === 'running';
  const flowId = trail?.instance.flowId;
  const revision = trail?.instance.revision;

  // Definición del flujo en la revisión de la instancia (si no existe, la última).
  useEffect(() => {
    if (!flowId) return;
    let alive = true;
    api
      .getFlow(flowId, revision)
      .catch(() => api.getFlow(flowId))
      .then((f) => alive && setFlow(f))
      .catch(() => alive && setFlow(null));
    return () => {
      alive = false;
    };
  }, [flowId, revision]);

  // En vivo: mientras corre, cada evento SSE relee la auditoría (con un pequeño margen para agrupar eventos).
  useEffect(() => {
    if (!running) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const close = subscribeInstance(
      id,
      () => {
        clearTimeout(timer);
        timer = setTimeout(() => void load(), 250);
      },
      () => {
        /* EventSource reintenta solo */
      },
    );
    return () => {
      clearTimeout(timer);
      close();
    };
  }, [running, id, load]);

  // Selección inicial: la última entrada; "seguir en vivo" activo si la instancia corre.
  useEffect(() => {
    if (!trail) return;
    if (firstLoad.current) {
      firstLoad.current = false;
      setFollow(running === true);
      setSel(entries.length - 1);
    } else if (follow || sel < 0) {
      setSel(entries.length - 1);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [entries.length, trail, follow]);

  useEffect(() => {
    selRowRef.current?.scrollIntoView({ block: 'nearest' });
  }, [sel]);

  const choose = (i: number, manual = true) => {
    if (manual) setFollow(false);
    setSel(i);
  };

  const snap = useMemo(() => snapshotAt(entries, sel, flow), [entries, sel, flow]);
  const cur: AuditEntry | undefined = entries[sel];

  // Formas de los nodos para el lienzo de solo lectura: las del catálogo (la auditoría no pide describe).
  const shapes = useMemo(() => {
    const m: Record<string, NodeShape> = {};
    for (const n of flow?.nodes ?? []) {
      const entry = catalog.find((c) => c.kind === n.kind);
      const ports = portsOf(entry?.ports);
      m[n.id] = { ports, outs: ports, ins: entry?.inputs ?? [{ name: 'in' }], role: entry?.role ?? 'node', problems: [], described: false };
    }
    return m;
  }, [flow, catalog]);

  const trace: Trace = useMemo(
    () => ({ ...EMPTY_TRACE, current: snap.current, visited: snap.visited }),
    [snap],
  );

  // Tokens de todos los `ai.agent` de la instancia (solo cuentan las ejecuciones reales, no las reutilizadas).
  const tokenTotals = useMemo(() => {
    let input = 0;
    let output = 0;
    let any = false;
    for (const e of entries) {
      const u = e.kind === 'node-finished' ? usageOf(e.info) : undefined;
      if (u) {
        any = true;
        input += u.inputTokens;
        output += u.outputTokens;
      }
    }
    return any ? { input, output } : null;
  }, [entries]);

  if (error) {
    return (
      <section className="mx-auto w-full max-w-6xl p-4">
        <Alert variant="error">{error}</Alert>
      </section>
    );
  }
  if (!trail) return <section className="mx-auto w-full max-w-6xl p-4"><p className="text-xs text-base-content/60">Cargando auditoría…</p></section>;

  const inst = trail.instance;
  const origin = typeof inst.origin === 'object' && inst.origin !== null ? (inst.origin as Record<string, unknown>) : null;
  const children = [...new Set(entries.flatMap((e) => (e.childInstanceId ? [e.childInstanceId] : [])))];
  const started = entries.find((e) => e.kind === 'instance-started');
  const startedAt = inst.startedAt ?? started?.at ?? null;
  const endedAt = inst.endedAt ?? null;
  const t0 = toMillis(startedAt);
  const t1 = toMillis(endedAt);
  const dur = t0 !== null && t1 !== null ? t1 - t0 : null;

  return (
    <section className="mx-auto w-full max-w-6xl space-y-3 p-4 audit-page">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <h1 className="text-xl font-semibold">Ejecución <span className="font-mono">{inst.id}</span></h1>
          <p className="text-xs text-base-content/60">
            <a href="#/instances">← Ejecuciones</a> · flujo <a href={`#/flow/${encodeURIComponent(inst.flowId)}`}>{inst.flowId}</a>
            {inst.revision !== undefined && <> · revisión {inst.revision}</>}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <StatusBadge status={inst.status ?? ''}>{running ? 'en curso' : inst.status}</StatusBadge>
          {running && (
            <label className="flex flex-wrap items-center gap-2 text-xs text-base-content/60">
              <Checkbox checked={follow} onCheckedChange={(c) => setFollow(c === true)} /> seguir en vivo
            </label>
          )}
        </div>
      </div>

      <Card className="audit-head flex flex-col gap-2 p-3">
        <div><span className="text-xs text-base-content/60">origen</span> {originText(inst.origin)}</div>
        {origin?.kind === 'retry' && typeof origin.retryOf === 'string' && origin.retryOf !== inst.id && (
          <div>instancia original: <a className="font-mono" href={`#/instance/${encodeURIComponent(origin.retryOf)}`}>{origin.retryOf}</a></div>
        )}
        {origin?.kind === 'subflow' && typeof origin.parentInstanceId === 'string' && (
          <div>instancia padre: <a className="font-mono" href={`#/instance/${encodeURIComponent(origin.parentInstanceId)}`}>{origin.parentInstanceId}</a>
            {origin.parentNode ? <> · nodo <span className="font-mono">{String(origin.parentNode)}</span></> : null}
          </div>
        )}
        {children.length > 0 && (
          <div>subflujos: {children.map((c) => (
            <a key={c} className="font-mono chip" href={`#/instance/${encodeURIComponent(c)}`}>{c}</a>
          ))}</div>
        )}
        <div><span className="text-xs text-base-content/60">inicio</span> {fmtDateTime(startedAt)} · <span className="text-xs text-base-content/60">duración</span> {fmtDuration(dur)} · {entries.length} entradas</div>
        {tokenTotals && (
          <div><span className="text-xs text-base-content/60">tokens (ai.agent)</span> {tokenTotals.input} entrada · {tokenTotals.output} salida</div>
        )}
      </Card>

      <div className="audit-body">
        <div className="audit-timeline rounded-xl border bg-base-100 p-3 text-base-content">
          <h3>Línea de tiempo</h3>
          <ol className="timeline">
            {entries.map((e, i) => (
              <li
                key={e.seq}
                ref={i === sel ? selRowRef : undefined}
                className={`tl-item tone-${kindTone(e)} ${i === sel ? 'is-selected' : ''}`}
              >
                <button type="button" className="tl-btn" onClick={() => choose(i)} aria-current={i === sel}>
                  <span className="tl-time font-mono">{fmtTime(e.at)}</span>
                  <span className="tl-main">
                    <span className="tl-head">
                      {e.node ? <span className="font-mono">{e.node}</span> : <span className="text-xs text-base-content/60">instancia</span>}
                      {e.attempt !== undefined && <Badge variant="outline">intento {e.attempt}</Badge>}
                      {e.durationMs !== undefined && <span className="text-xs text-base-content/60">{fmtDuration(e.durationMs)}</span>}
                    </span>
                    <span className="tl-kind">{entryLabel(e)}{e.mocked ? ' · mock' : ''}{e.simulated ? ' · simulación' : ''}</span>
                    <span className="tl-sum text-xs text-base-content/60">{summaryOf(e)}</span>
                  </span>
                </button>
              </li>
            ))}
          </ol>
          <label className="flex flex-wrap items-center gap-2 text-xs text-base-content/60">
            <Checkbox checked={follow} onCheckedChange={(c) => setFollow(c === true)} /> seguir la última entrada
          </label>
        </div>

        <div className="audit-right">
          <div className="audit-graph rounded-xl border bg-base-100 p-3 text-base-content">
            {flow ? (
              <div className="audit-canvas">
                <Canvas
                  flow={flow}
                  catalog={catalog}
                  shapes={shapes}
                  selectedId={cur?.node ?? null}
                  errorsByNode={{}}
                  edgeProblems={{}}
                  nodeStatus={snap.nodeStatus}
                  fireAt={snap.fireAt}
                  trace={trace}
                  readOnly
                  edgeClasses={snap.edgeClasses}
                  onSelect={NOOP}
                  onMove={NOOP}
                  onRemoveNodes={NOOP}
                  onRemoveEdges={NOOP}
                  onConnect={() => null}
                  onAddNode={NOOP}
                  onMessage={(m) => toast(m, 'info')}
                  onNodeClick={(nodeId) => {
                    const i = lastEntryOfNode(entries, nodeId);
                    if (i >= 0) choose(i);
                  }}
                />
              </div>
            ) : (
              <p className="text-xs text-base-content/60">No se encuentra la definición del flujo en la revisión {inst.revision ?? '?'}; el grafo no se puede dibujar.</p>
            )}
            <div className="legend text-xs text-base-content/60">
              <span className="lg lg-ok" /> ejecutado <span className="lg lg-err" /> fallido <span className="lg lg-skip" /> omitido
              <span className="lg lg-reuse" /> reutilizado <span className="lg lg-wait" /> esperando <span className="lg lg-fired" /> arista disparada <span className="lg lg-dead" /> arista muerta
            </div>
          </div>

          <Card className="audit-detail flex flex-col gap-2 p-3">
            {cur ? <EntryDetail entry={cur} entries={entries} instanceId={inst.id} onJump={(i) => choose(i)} /> : <p className="text-xs text-base-content/60">Sin entradas.</p>}
          </Card>
        </div>
      </div>
    </section>
  );
}

function Link({ id }: { id: string }) {
  return <a className="font-mono" href={`#/instance/${encodeURIComponent(id)}`}>{id}</a>;
}

/** Detalle de una entrada: lecturas, escrituras (diff), memoria, puerto, error y decisión del motor. */
function EntryDetail({ entry: e, entries, instanceId, onJump }: { entry: AuditEntry; entries: AuditEntry[]; instanceId: string; onJump: (i: number) => void }) {
  const indexOf = (pred: (x: AuditEntry) => boolean) => entries.findIndex(pred);
  const d = e.decision;
  return (
    <div className="detail">
      <h3>{entryLabel(e)}{e.mocked ? ' · mock' : ''}{e.simulated ? ' · simulación' : ''}</h3>
      <p className="text-xs text-base-content/60">
        {e.node && <span className="font-mono">{e.node}</span>} {e.nodeKind && <span className="font-mono">· {e.nodeKind}</span>}
        {e.attempt !== undefined && <> · intento {e.attempt}</>}
        {e.at && <> · {fmtDateTime(e.at)}</>}
        {e.durationMs !== undefined && <> · {fmtDuration(e.durationMs)}</>}
      </p>

      {e.kind === 'node-finished' && (
        <Field label="Puerto tomado"><span className="font-mono">{e.port ?? '—'}</span></Field>
      )}
      {e.error && <Field label="Error"><span className="my-0.5 text-xs text-error">{e.error}</span></Field>}
      {e.kind === 'node-started' && e.nodeKind === 'stream.segment' && (() => {
        const seg = segmentView(e.params);
        return seg ? (
          <Field label={`Segmento de stream (${seg.members.length} nodos)`}>
            <p className="text-xs text-base-content/60">Un solo paso: sus fuentes son <span className="font-mono">{seg.sources.join(', ') || '—'}</span>; sus sumideros, <span className="font-mono">{seg.sinks.join(', ') || '—'}</span>.</p>
            <ul className="member-list">
              {seg.members.map((id) => <li key={id} className="font-mono">{id}</li>)}
            </ul>
          </Field>
        ) : null;
      })()}
      {e.kind === 'node-finished' && e.nodeKind === 'stream.segment' && e.info && (
        <SegmentMetricsView info={e.info} />
      )}
      {e.kind === 'node-finished' && usageOf(e.info) && (() => {
        const u = usageOf(e.info)!;
        const model = typeof e.info?.model === 'string' ? e.info.model : undefined;
        return (
          <Field label="Tokens (ai.agent)">
            <span className="font-mono">{u.inputTokens}</span> entrada · <span className="font-mono">{u.outputTokens}</span> salida
            {model && <span className="text-xs text-base-content/60"> · modelo {model}</span>}
          </Field>
        );
      })()}
      {e.kind === 'node-finished' && logsOf(e.info).length > 0 && (
        <Field label={`Logs del script (${logsOf(e.info).length})`}>
          <ol className="script-logs">
            {logsOf(e.info).map((line, i) => (
              <li key={i} className="font-mono">{line}</li>
            ))}
          </ol>
        </Field>
      )}
      {e.kind === 'node-finished' && e.info && Object.keys(rawInfo(e.info)).length > 0 && (
        <Field label="Información del nodo">
          <Pre value={rawInfo(e.info)} />
        </Field>
      )}

      {e.kind === 'instance-started' && <Field label="Entradas iniciales"><Pre value={e.inputs} /></Field>}
      {e.kind === 'instance-finished' && <Field label="Estado final"><span className="font-mono">{String(e.status ?? '—')}</span></Field>}
      {e.kind === 'retry-requested' && (
        <Field label="Reintento">
          {e.retryOf === instanceId ? <span className="text-xs text-base-content/60">de esta misma instancia (vuelve a correr en el mismo id)</span> : <>de <Link id={String(e.retryOf)} /></>}
          {e.reason && <span className="text-xs text-base-content/60"> · {String(e.reason)}</span>}
        </Field>
      )}
      {(e.kind === 'edge-fired' || e.kind === 'edge-dead') && e.edge && (
        <Field label={e.kind === 'edge-fired' ? 'Arista disparada' : 'Arista muerta'}>
          <span className="font-mono">{e.edge.from}.{e.edge.port} → {e.edge.to}</span>
        </Field>
      )}

      {e.reads && e.reads.length > 0 && (
        <Field label="Lecturas del estado">
          <ul className="kv">
            {e.reads.map((r, i) => (
              <li key={i}><span className="font-mono">{r.path}</span> = <Pre value={r.value} inline /></li>
            ))}
          </ul>
        </Field>
      )}

      {e.writes && e.writes.length > 0 && (
        <Field label="Escrituras en el estado (diff)">
          <Table className="text-xs">
            <TableHeader><TableRow><TableHead>ruta</TableHead><TableHead>previo</TableHead><TableHead></TableHead><TableHead>nuevo</TableHead></TableRow></TableHeader>
            <TableBody>
              {e.writes.map((w, i) => (
                <TableRow key={i}>
                  <TableCell className="font-mono">{w.path}</TableCell>
                  <TableCell><Pre value={w.previous} inline removed /></TableCell>
                  <TableCell className="text-xs text-base-content/60">→</TableCell>
                  <TableCell><Pre value={w.value} inline added /></TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Field>
      )}

      {(e.memoryBefore !== undefined || e.memoryAfter !== undefined) && (
        <Field label="Memoria del nodo">
          <div className="mem-pair">
            <div><span className="text-xs text-base-content/60">antes</span><Pre value={e.memoryBefore} /></div>
            <div><span className="text-xs text-base-content/60">después</span><Pre value={e.memoryAfter} /></div>
          </div>
        </Field>
      )}

      {d && <DecisionView decision={d} onJump={onJump} indexOf={indexOf} />}

      {e.childInstanceId && (
        <Field label="Subflujo">
          <Link id={e.childInstanceId} />
        </Field>
      )}
    </div>
  );
}

/** Texto de una entrada; una espera de temporizador se nombra por su horario. */
/** Métricas por nodo y elementos descartados de un segmento (`node-finished.info`). */
function SegmentMetricsView({ info }: { info: Record<string, unknown> }) {
  const metrics = (isRecord(info.metrics) ? info.metrics : {}) as Record<string, Partial<NodeMetrics>>;
  const dead = Array.isArray(info.deadLetters) ? (info.deadLetters as DeadLetter[]) : [];
  return (
    <>
      <Field label="Métricas por nodo">
        <Table className="text-xs">
          <TableHeader><TableRow><TableHead>nodo</TableHead><TableHead>entran</TableHead><TableHead>salen</TableHead><TableHead>descartados</TableHead><TableHead>fallos</TableHead><TableHead>ms</TableHead></TableRow></TableHeader>
          <TableBody>
            {Object.entries(metrics).map(([id, m]) => (
              <TableRow key={id}>
                <TableCell className="font-mono">{id}</TableCell>
                <TableCell className="num">{m.in ?? ''}</TableCell>
                <TableCell className="num">{m.out ?? ''}</TableCell>
                <TableCell className="num">{m.dropped ?? ''}</TableCell>
                <TableCell className="num">{m.failed ?? ''}</TableCell>
                <TableCell className="num">{m.latencyMs ?? ''}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Field>
      <Field label={`Elementos descartados por error (${dead.length})`}>
        {dead.length === 0 && <span className="text-xs text-base-content/60">Ninguno.</span>}
        <ul className="dead-letters">
          {dead.map((d, i) => (
            <li key={i} className="dead-letter"><span className="font-mono">{d.node}</span> <span className="my-0.5 text-xs text-error">{d.error}</span><Pre value={d.element} /></li>
          ))}
        </ul>
      </Field>
    </>
  );
}

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/** `info` sin `usage` ni `logs`, que tienen su propia vista. */
function rawInfo(info: Record<string, unknown>): Record<string, unknown> {
  const rest = { ...info };
  delete rest.usage;
  delete rest.logs;
  return rest;
}

function entryLabel(e: AuditEntry): string {
  if (e.kind === 'node-waiting' && e.decision?.reason === 'scheduled') return 'esperando al horario';
  return kindLabel(e.kind);
}

/** Espera de un temporizador: hasta cuándo, con cuenta atrás. */
function WaitView({ fireAt }: { fireAt: number }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  return (
    <span>
      hasta las <span className="font-mono">{fmtClock(fireAt)}</span> <span className="text-xs text-base-content/60">({fmtCountdown(fireAt, now)})</span>
    </span>
  );
}

function DecisionView({
  decision: d, onJump, indexOf,
}: {
  decision: NonNullable<AuditEntry['decision']>;
  onJump: (i: number) => void;
  indexOf: (p: (x: AuditEntry) => boolean) => number;
}) {
  const nodeFinishedIdx = (node: string) => indexOf((x) => x.kind === 'node-finished' && x.node === node);
  return (
    <Field label={`Decisión: ${REASON_LABEL[d.reason ?? ''] ?? d.reason ?? '—'}`}>
      {d.reason === 'skipped-dead-branch' && (
        <>
          <ul className="kv">
            {(d.deadEdges ?? []).map((ed, i) => (
              <li key={i}>arista muerta <span className="font-mono">{ed.from}.{ed.port} → {ed.to}</span></li>
            ))}
          </ul>
          <p className="text-xs text-base-content/60">Decidido por:</p>
          <ul className="kv">
            {(d.decidedBy ?? []).map((b, i) => {
              const at = b.node ? nodeFinishedIdx(b.node) : -1;
              return (
                <li key={i}>
                  <span className="font-mono">{b.node}</span>
                  {b.path !== undefined
                    ? <> leyó <span className="font-mono">{b.path}</span> = <Pre value={b.value} inline /> y tomó <span className="font-mono">{b.portTaken}</span></>
                    : <> terminó por <span className="font-mono">{b.portTaken ?? '—'}</span></>}
                  {at >= 0 && <Button type="button" variant="link" size="xs" className="h-auto p-0" onClick={() => onJump(at)}>ver paso</Button>}
                </li>
              );
            })}
          </ul>
        </>
      )}
      {d.reason === 'scheduled' && d.fireAt !== undefined && (
        <p>Temporizador: sigue <WaitView fireAt={d.fireAt} /></p>
      )}
      {d.reason === 'waiting-join' && (
        <ul className="kv">
          {(d.pendingEdges ?? []).map((ed, i) => (
            <li key={i}>espera <span className="font-mono">{ed.from}.{ed.port} → {ed.to}</span></li>
          ))}
        </ul>
      )}
      {d.reason === 'skipped-by-run-policy' && (
        <>
          <p className="text-xs text-base-content/60">
            Última ejecución: {String(d.previousStatus ?? '—')}. Se reutiliza su resultado según RunPolicy
            (puerto <span className="font-mono">{String(d.reused?.port ?? '—')}</span>).
          </p>
          {d.reused?.childInstanceId && <p>subflujo reutilizado: <Link id={String(d.reused.childInstanceId)} /></p>}
          {d.reused?.writes && d.reused.writes.length > 0 && (
            <Table className="text-xs">
              <TableHeader><TableRow><TableHead>ruta</TableHead><TableHead>previo</TableHead><TableHead></TableHead><TableHead>reaplicado</TableHead></TableRow></TableHeader>
              <TableBody>
                {d.reused.writes.map((w, i) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono">{w.path}</TableCell>
                    <TableCell><Pre value={w.previous} inline removed /></TableCell>
                    <TableCell className="text-xs text-base-content/60">→</TableCell>
                    <TableCell><Pre value={w.value} inline added /></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </>
      )}
    </Field>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="audit-field">
      <div className="text-xs text-base-content/60 audit-label">{label}</div>
      <div>{children}</div>
    </div>
  );
}

function Pre({ value, inline, removed, added }: { value: unknown; inline?: boolean; removed?: boolean; added?: boolean }) {
  const text = renderValue(value as JsonValue | undefined);
  const redacted = text === '••• redacted';
  const cls = [redacted ? 'redacted' : '', removed ? 'diff-old' : '', added ? 'diff-new' : ''].filter(Boolean).join(' ');
  if (inline) return <code className={`inline-val ${cls}`}>{text.replace(/\s+/g, ' ')}</code>;
  return <pre className={`max-h-60 overflow-auto rounded-md bg-base-200 p-2 text-xs ${cls}`}>{text}</pre>;
}
