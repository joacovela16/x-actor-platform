import { useEffect, useState } from 'react';
import { api, errorMessage } from '../api/client';
import type { Health } from '../api/types';
import { Alert } from '@/components/ui/alert';
import { Badge, type BadgeVariant } from '@/components/ui/badge';
import { Card, CardBody } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

const STATE: Record<Health['triggers'][number]['state'], { label: string; variant: BadgeVariant }> = {
  running: { label: 'leyendo', variant: 'success' },
  waiting: { label: 'espera su conexión', variant: 'warning' },
  unserved: { label: 'ningún plugin lo lee', variant: 'destructive' },
};

function ms(v: number | null): string {
  if (v === null) return '—';
  return v < 1000 ? `${v} ms` : `${(v / 1000).toFixed(v < 10_000 ? 1 : 0)} s`;
}

function when(at: number): string {
  return new Date(at).toLocaleString();
}

/** Cómo van los flujos (últimas 24 h, en este servidor) y las fuentes de triggers. Se actualiza sola cada 10 s. Para series
 *  largas y alertas, el mismo dato está en `/metrics` para Prometheus.
 */
export function HealthView() {
  const [health, setHealth] = useState<Health | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const load = () =>
      api.health().then(
        (h) => alive && (setHealth(h), setError(null)),
        (e) => alive && setError(errorMessage(e)),
      );
    load();
    const timer = setInterval(load, 10_000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  const flows = [...(health?.flows ?? [])].sort((a, b) => b.failed - a.failed || a.flowId.localeCompare(b.flowId));
  const failures = flows.flatMap((f) => f.lastFailures.map((x) => ({ ...x, flowId: f.flowId }))).sort((a, b) => b.at - a.at).slice(0, 10);

  return (
    <section className="mx-auto flex w-full max-w-5xl flex-col gap-4 p-4">
      <div>
        <h1 className="mb-1 text-xl font-semibold">Salud</h1>
        <p className="text-sm text-base-content/60">
          Las ejecuciones de las últimas 24 h en este servidor. Para alertas e historia larga, Prometheus lee lo mismo en{' '}
          <a className="link font-mono" href="/metrics">/metrics</a>.
        </p>
      </div>
      {error && <Alert variant="error">{error}</Alert>}

      <Card title="Flujos">
        <CardBody className="pt-0">
          {health && flows.length === 0 ? (
            <p className="text-sm text-base-content/60">Ninguna ejecución todavía.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Flujo</TableHead>
                  <TableHead className="text-right">En curso</TableHead>
                  <TableHead className="text-right">Bien</TableHead>
                  <TableHead className="text-right">Fallaron</TableHead>
                  <TableHead className="text-right">Éxito</TableHead>
                  <TableHead className="text-right">p50</TableHead>
                  <TableHead className="text-right">p95</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {flows.map((f) => {
                  const total = f.succeeded + f.failed;
                  const rate = total === 0 ? null : f.succeeded / total;
                  return (
                    <TableRow key={f.flowId}>
                      <TableCell>
                        <a className="link link-hover font-mono" href={`#/flow/${encodeURIComponent(f.flowId)}`}>{f.flowId}</a>
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{f.running}</TableCell>
                      <TableCell className="text-right tabular-nums">{f.succeeded}</TableCell>
                      <TableCell className={`text-right tabular-nums ${f.failed > 0 ? 'font-semibold text-error' : ''}`}>{f.failed}</TableCell>
                      <TableCell className="text-right tabular-nums">{rate === null ? '—' : `${Math.round(rate * 100)}%`}</TableCell>
                      <TableCell className="text-right tabular-nums">{ms(f.p50Ms)}</TableCell>
                      <TableCell className="text-right tabular-nums">{ms(f.p95Ms)}</TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardBody>
      </Card>

      {failures.length > 0 && (
        <Card title="Últimos fallos">
          <CardBody className="pt-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Cuándo</TableHead>
                  <TableHead>Flujo</TableHead>
                  <TableHead>Error</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {failures.map((x) => (
                  <TableRow key={x.instanceId}>
                    <TableCell className="whitespace-nowrap">
                      <a className="link link-hover" href={`#/instance/${encodeURIComponent(x.instanceId)}`}>{when(x.at)}</a>
                    </TableCell>
                    <TableCell className="font-mono">{x.flowId}</TableCell>
                    <TableCell className="text-base-content/80">{x.error ?? 'falló'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardBody>
        </Card>
      )}

      <Card title="Fuentes de triggers">
        <CardBody className="pt-0">
          {health && health.triggers.length === 0 ? (
            <p className="text-sm text-base-content/60">No hay triggers que lean de colas u otras fuentes.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Trigger</TableHead>
                  <TableHead>Tipo</TableHead>
                  <TableHead>Conexión</TableHead>
                  <TableHead>Estado</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {health?.triggers.map((t) => (
                  <TableRow key={t.triggerId}>
                    <TableCell className="font-mono">{t.triggerId}</TableCell>
                    <TableCell>{t.kind}</TableCell>
                    <TableCell className="font-mono">{t.connection ?? '—'}</TableCell>
                    <TableCell>
                      <Badge variant={STATE[t.state].variant}>{STATE[t.state].label}</Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardBody>
      </Card>
    </section>
  );
}
