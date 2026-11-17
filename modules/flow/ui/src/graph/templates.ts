// Plantillas para empezar un flujo: grafos pequeños de casos frecuentes. Los nombres de conexión son de ejemplo: el flujo se crea
// como borrador, así que se puede guardar aunque todavía no valide, y se completa en el editor.
import type { FlowDef, JsonValue, NodeDef, OutputDecl } from '../api/types';

const node = (id: string, kind: string, params: Record<string, JsonValue>, outputs: OutputDecl[] = []): NodeDef => ({
  id,
  kind,
  params,
  outputs,
  runPolicy: { whenSucceeded: 'run', whenFailed: 'run' },
  joinPolicy: 'all',
});

export interface FlowTemplate {
  key: string;
  label: string;
  description: string;
  build: (id: string, name: string) => FlowDef;
}

const base = (id: string, name: string): FlowDef => ({ version: 1, id, name, revision: 1, inputs: [], outputs: [], nodes: [], edges: [] });

export const TEMPLATES: FlowTemplate[] = [
  {
    key: 'approval',
    label: 'API: decidir y responder',
    description: 'Recibe una petición, decide con un If y responde 201 o 409.',
    build: (id, name) => ({
      ...base(id, name),
      inputs: [
        { name: 'approved', target: 'approved', type: { type: 'bool' } },
        { name: 'order', target: 'order', type: { type: 'any' } },
      ],
      nodes: [
        node('check', 'core.if', { path: 'approved' }),
        node('ok', 'core.respond', { status: 201, body: 'order' }),
        node('rejected', 'core.respond', { status: 409 }),
      ],
      edges: [
        { from: 'check', port: 'then', to: 'ok' },
        { from: 'check', port: 'else', to: 'rejected' },
      ],
      layout: { check: { x: 80, y: 120 }, ok: { x: 360, y: 40 }, rejected: { x: 360, y: 220 } },
    }),
  },
  {
    key: 'query',
    label: 'API: consultar y responder',
    description: 'Lee filas de una base de datos con un parámetro de la petición y las devuelve.',
    build: (id, name) => ({
      ...base(id, name),
      inputs: [{ name: 'customerId', target: 'customerId', type: { type: 'str' } }],
      nodes: [
        node('query', 'db.query', { connection: 'main', sql: 'SELECT * FROM orders WHERE customer_id = ?', params: [{ path: 'customerId' }] }, [
          { name: 'rows', target: 'rows', type: { type: 'list', element: { type: 'obj', fields: {} } } },
        ]),
        node('respond', 'core.respond', { status: 200, body: 'rows' }),
      ],
      edges: [{ from: 'query', port: 'out', to: 'respond' }],
      layout: { query: { x: 80, y: 120 }, respond: { x: 380, y: 120 } },
    }),
  },
  {
    key: 'queue-filter-publish',
    label: 'Cola: filtrar y reenviar',
    description: 'Consume una cola, deja pasar los mensajes que cumplen una condición y los publica en otra.',
    build: (id, name) => ({
      ...base(id, name),
      nodes: [
        node('in', 'stream.source.queue', { connection: 'broker', source: 'orders' }),
        node('big', 'stream.filter', { where: { gt: [{ path: 'total' }, 100] } }),
        node('out', 'stream.sink.publish', { connection: 'broker', destination: 'big-orders' }),
      ],
      edges: [
        { from: 'in', port: 'out', to: 'big' },
        { from: 'big', port: 'out', to: 'out' },
      ],
      layout: { in: { x: 60, y: 120 }, big: { x: 320, y: 120 }, out: { x: 580, y: 120 } },
    }),
  },
  {
    key: 'enrich',
    label: 'Stream: recorrer una colección y enriquecer',
    description: 'Lee una colección de MongoDB con cursor, ejecuta otro flujo por documento y reúne el resultado.',
    build: (id, name) => ({
      ...base(id, name),
      nodes: [
        node('docs', 'stream.source.mongo', { connection: 'mongo', collection: 'orders', filter: { status: 'open' } }),
        node('lookup', 'stream.evalMap', { flowId: 'customer-lookup', inputs: { id: 'customerId' }, into: 'customer', parallel: 4 }),
        node('all', 'stream.sink.collect', {}, [{ name: 'result', target: 'enriched', type: { type: 'any' } }]),
      ],
      edges: [
        { from: 'docs', port: 'out', to: 'lookup' },
        { from: 'lookup', port: 'out', to: 'all' },
      ],
      layout: { docs: { x: 60, y: 120 }, lookup: { x: 340, y: 120 }, all: { x: 620, y: 120 } },
    }),
  },
];
