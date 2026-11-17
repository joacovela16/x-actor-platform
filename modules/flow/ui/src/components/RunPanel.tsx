import { useContext, useEffect, useMemo, useRef, useState } from 'react';
import { api, errorMessage, subscribeInstance } from '../api/client';
import type { DeadLetter, FlowDef, Instance, InstanceEvent, JsonValue, NodeMetrics } from '../api/types';
import type { LiveCounters } from './Canvas';
import { templateFromDecls } from '../graph/graph';
import { useEntities } from '../hooks/useEntities';
import { fmtClock, fmtCountdown } from '../graph/time';
import { AppContext } from '../App';
import { JsonBox } from './JsonBox';
import { Button } from '@/components/ui/button';
import { FormField } from '@/components/ui/form-field';
import { SearchSelect } from '@/components/ui/search-select';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { StatusBadge, ToneBadge } from '@/components/StatusBadge';

/** Lo que el editor necesita para pintar el lienzo mientras se ejecuta. */
export interface RunStatus {
  statuses: Record<string, string>;
  fireAt: Record<string, number>;
  live: Record<string, LiveCounters>;
  running: boolean;
}

const EMPTY_STATUS: RunStatus = { statuses: {}, fireAt: {}, live: {}, running: false };

interface Props {
  flow: FlowDef;
  dirty: boolean;
  onNodeStatus: (st: RunStatus) => void;
}

/** Métricas de un nodo que está en un segmento de stream: `in`/`out` acumulados, con un indicador de actividad. */
function liveOf(m: NodeMetrics, prev: { in: number; out: number } | undefined): LiveCounters {
  return { in: m.in, out: m.out, failed: m.failed, active: !!prev && (prev.in !== m.in || prev.out !== m.out) };
}

/** Lanza una instancia del flujo guardado y sigue su estado por SSE (los eventos disparan una lectura del estado). */
export function RunPanel({ flow, dirty, onNodeStatus }: Props) {
  const { toast } = useContext(AppContext);
  const { byName: entities } = useEntities();
  const [inputs, setInputs] = useState<JsonValue | undefined>(() => templateFromDecls(flow.inputs, entities));
  const [valid, setValid] = useState(true);
  const [instanceId, setInstanceId] = useState<string | null>(null);
  // Cambia en cada lanzamiento o reintento: un reintento vuelve a correr la MISMA instancia (mismo id), así que
  // sin este contador no se volvería a suscribir a sus eventos.
  const [epoch, setEpoch] = useState(0);
  const [instance, setInstance] = useState<Instance | null>(null);
  const [log, setLog] = useState<InstanceEvent[]>([]);
  const [openId, setOpenId] = useState('');
  const [busy, setBusy] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const [sampleOf, setSampleOf] = useState<string | null>(null);
  // Última lectura de contadores por nodo, para saber si hubo actividad desde la anterior.
  const prevCounts = useRef(new Map<string, { in: number; out: number }>());

  // Quien pinta el panel es `getInstance`: el SSE solo avisa de que hay algo nuevo. Tras cada aviso y cada pocos
  // segundos mientras corre se relee el estado. Cuando ya no corre se cierra el stream: el servidor lo cierra al
  // terminar y el navegador lo reabriría sin fin (replay de todas las entradas). Un reintento cambia `epoch` y se
  // vuelve a suscribir: el replay trae también el `instance-finished` antiguo, así que el cierre se decide por el
  // estado y no por el evento.
  useEffect(() => {
    if (!instanceId) return;
    let alive = true;
    let closeStream: (() => void) | null = null;
    let poll: ReturnType<typeof setInterval> | undefined;
    const stop = () => {
      clearInterval(poll);
      closeStream?.();
      closeStream = null;
    };
    const refresh = () =>
      api
        .getInstance(instanceId)
        .then((i) => {
          if (!alive) return;
          setInstance(i);
          const statuses: Record<string, string> = {};
          const fires: Record<string, number> = {};
          const live: Record<string, LiveCounters> = {};
          for (const [k, v] of Object.entries(i.nodes)) {
            statuses[k] = v.fireAt != null ? 'waiting' : v.status;
            if (v.fireAt != null) fires[k] = v.fireAt;
            if (v.metrics) {
              live[k] = liveOf(v.metrics, prevCounts.current.get(k));
              prevCounts.current.set(k, { in: v.metrics.in, out: v.metrics.out });
            }
          }
          onNodeStatus({ statuses, fireAt: fires, live, running: i.status === 'running' });
          if (i.status !== 'running') stop();
        })
        .catch((e: unknown) => alive && toast(`Estado de la instancia: ${errorMessage(e)}`, 'error'));
    refresh();
    closeStream = subscribeInstance(
      instanceId,
      (ev) => {
        if (!alive) return;
        setLog((l) => [...l.slice(-199), ev]);
        refresh();
      },
      () => {
        /* EventSource reintenta solo mientras la instancia corre */
      },
    );
    poll = setInterval(refresh, 2000);
    return () => {
      alive = false;
      stop();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [instanceId, epoch]);

  // Al salir del panel no quedan cuentas atrás ni contadores pintados en el lienzo.
  const onStatusRef = useRef(onNodeStatus);
  onStatusRef.current = onNodeStatus;
  useEffect(() => () => onStatusRef.current(EMPTY_STATUS), []);

  // Cuenta atrás de los temporizadores en espera (un solo reloj para la tabla).
  const anyWaiting = instance ? Object.values(instance.nodes).some((n) => n.fireAt != null) : false;
  useEffect(() => {
    if (!anyWaiting) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [anyWaiting]);

  const resetView = () => {
    setLog([]);
    setInstance(null);
    prevCounts.current.clear();
    onNodeStatus(EMPTY_STATUS);
  };

  const launch = async () => {
    setBusy(true);
    try {
      const res = await api.startInstance(flow.id, { inputs: inputs ?? {}, revision: flow.revision });
      resetView();
      setInstanceId(res.instanceId);
      setEpoch((n) => n + 1);
    } catch (e) {
      toast(`No se pudo lanzar: ${errorMessage(e)}`, 'error');
    } finally {
      setBusy(false);
    }
  };

  // El retry responde el mismo id de la instancia: se vuelve a seguir desde cero. Si aún no ha terminado, el
  // servidor responde 409 y se muestra su motivo.
  const retry = async () => {
    if (!instanceId) return;
    try {
      const r = await api.retryInstance(instanceId);
      resetView();
      setInstanceId(r.instanceId);
      setEpoch((n) => n + 1);
    } catch (e) {
      toast(`No se pudo reintentar: ${errorMessage(e)}`, 'error');
    }
  };

  // Cancelar: sirve para un stream sin fin. El servidor responde 202 y el estado de la instancia pasa a `failed`
  // (los nodos en curso quedan `cancelled`); el sondeo de `refresh` lo recoge.
  const cancel = async () => {
    if (!instanceId) return;
    try {
      await api.cancelInstance(instanceId);
      toast('Cancelación pedida: la instancia se cerrará en un momento.', 'ok');
      setEpoch((n) => n + 1);
    } catch (e) {
      toast(`No se pudo cancelar: ${errorMessage(e)}`, 'error');
    }
  };

  const nodeRows = useMemo(() => (instance ? Object.entries(instance.nodes) : []), [instance]);
  const deadLetters: DeadLetter[] = instance?.deadLetters ?? [];
  const running = instance?.status === 'running';
  const sampled = sampleOf && instance?.nodes[sampleOf]?.metrics ? instance.nodes[sampleOf].metrics! : null;

  return (
    <div className="run-panel">
      <div className="run-col">
        <h3>Lanzar</h3>
        {dirty && <p className="my-0.5 text-xs text-error">Hay cambios sin guardar: se ejecuta la última revisión guardada.</p>}
        <JsonBox label="Entradas" value={inputs} onChange={setInputs} onValid={setValid} rows={6} hint="Según las entradas declaradas en el flujo." />
        <Button type="button" disabled={dirty || !valid || busy} onClick={launch}>
          Lanzar instancia
        </Button>
        <div className="flex flex-wrap items-center gap-2">
          <Input className="font-mono" placeholder="id de instancia" value={openId} onChange={(e) => setOpenId(e.target.value)} aria-label="Abrir instancia" />
          <Button variant="outline" type="button"
            disabled={!openId}
            onClick={() => {
              prevCounts.current.clear();
              setInstanceId(openId.trim());
              setEpoch((n) => n + 1);
            }}
          >
            Abrir
          </Button>
        </div>
      </div>

      <div className="run-col">
        {!instanceId && <p className="text-xs text-base-content/60">Sin instancia seleccionada.</p>}
        {instanceId && (
          <>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <h3>Instancia <span className="font-mono">{instanceId}</span></h3>
                <StatusBadge status={instance?.status ?? 'running'}>{instance?.status ?? 'cargando'}</StatusBadge>
                {instance && <span className="text-xs text-base-content/60"> revisión {instance.revision}</span>}
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <Button variant="ghost" href={`#/instance/${encodeURIComponent(instanceId)}`}>Ver auditoría</Button>
                {running && <Button variant="outline" className="text-error hover:text-error" type="button" onClick={cancel}>Cancelar</Button>}
                <Button variant="ghost" type="button" onClick={retry} disabled={running}>Reintentar</Button>
              </div>
            </div>
            <Table>
              <TableHeader>
                <TableRow><TableHead>nodo</TableHead><TableHead>estado</TableHead><TableHead>puerto</TableHead><TableHead>error</TableHead><TableHead>entran</TableHead><TableHead>salen</TableHead><TableHead>fallos</TableHead><TableHead>ms</TableHead></TableRow>
              </TableHeader>
              <TableBody>
                {nodeRows.map(([id, st]) => (
                  <TableRow key={id}>
                    <TableCell className="font-mono">
                      {id}
                      {st.metrics && <> <ToneBadge tone="info">stream</ToneBadge></>}
                    </TableCell>
                    <TableCell>
                      {st.fireAt != null ? (
                        <ToneBadge tone="warning">esperando hasta {fmtClock(st.fireAt)} · {fmtCountdown(st.fireAt, now)}</ToneBadge>
                      ) : (
                        <StatusBadge status={st.status} />
                      )}
                    </TableCell>
                    <TableCell className="font-mono">{st.port ?? ''}</TableCell>
                    <TableCell className="my-0.5 text-xs text-error">{st.error ?? ''}</TableCell>
                    <TableCell className="num">{st.metrics?.in ?? ''}</TableCell>
                    <TableCell className="num">{st.metrics?.out ?? ''}</TableCell>
                    <TableCell className="num">{st.metrics ? st.metrics.failed : ""}</TableCell>
                    <TableCell className="num">{st.metrics?.latencyMs ?? ''}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <p className="text-xs text-base-content/60">Un nodo de un segmento muestra el estado del paso (el segmento es un solo paso); sus contadores son los suyos.</p>

            {instance && Object.values(instance.nodes).some((n) => n.metrics) && (
              <>
                <h4>Muestras por nodo</h4>
                <FormField label="Nodo">
                  <SearchSelect
                    aria-label="Nodo de las muestras"
                    value={sampleOf ?? ''}
                    onChange={(v) => setSampleOf(v || null)}
                    options={nodeRows.filter(([, st]) => st.metrics).map(([id]) => ({ value: id, label: id }))}
                  />
                </FormField>
                {sampled && (
                  <div className="samples">
                    <div>
                      <span className="text-xs text-base-content/60">entrada (primeras 5)</span>
                      <pre className="max-h-60 overflow-auto rounded-md bg-base-200 p-2 text-xs">{JSON.stringify(sampled.sampleIn, null, 2)}</pre>
                    </div>
                    <div>
                      <span className="text-xs text-base-content/60">salida (primeras 5)</span>
                      <pre className="max-h-60 overflow-auto rounded-md bg-base-200 p-2 text-xs">{JSON.stringify(sampled.sampleOut, null, 2)}</pre>
                    </div>
                  </div>
                )}
              </>
            )}

            <h4>Elementos descartados por error ({deadLetters.length})</h4>
            {deadLetters.length === 0 && <p className="text-xs text-base-content/60">Ninguno. Un elemento que un nodo no puede tratar no para el stream: queda aquí.</p>}
            {deadLetters.length > 0 && (
              <ul className="dead-letters">
                {deadLetters.map((d, i) => (
                  <li key={i} className="dead-letter">
                    <span className="font-mono">{d.node}</span> <span className="my-0.5 text-xs text-error">{d.error}</span>
                    <pre className="max-h-60 overflow-auto rounded-md bg-base-200 p-2 text-xs">{JSON.stringify(d.element, null, 2)}</pre>
                  </li>
                ))}
              </ul>
            )}

            <h4>Estado global</h4>
            <pre className="max-h-60 overflow-auto rounded-md bg-base-200 p-2 text-xs">{instance ? JSON.stringify(instance.state, null, 2) : '…'}</pre>
            <h4>Eventos</h4>
            <ol className="event-log font-mono">
              {log.slice(-50).map((ev, i) => (
                <li key={i}>{JSON.stringify(ev)}</li>
              ))}
            </ol>
          </>
        )}
      </div>
    </div>
  );
}
