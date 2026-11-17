import { describe, expect, it } from 'vitest';
import type { DataType, FlowDef } from '../api/types';
import { elementPaths, inferElementTypes } from './elementTypes';
import type { NodeIO } from './stream';

const s = (stream: DataType = { type: 'any' }) => ({ name: 'out', stream });
const io = (role: NodeIO['role'], ins: string[], outs: string[]): NodeIO => ({
  role,
  ins: ins.map((name) => ({ name, stream: { type: 'any' } as DataType })),
  outs: outs.map((name) => ({ ...s(), name })),
});

const order: DataType = {
  type: 'obj',
  fields: { id: { type: 'str' }, lines: { type: 'list', element: { type: 'obj', fields: { sku: { type: 'str' }, qty: { type: 'num' } } } } },
};

const flow: FlowDef = {
  id: 'f',
  name: 'F',
  version: 1,
  inputs: [],
  outputs: [],
  nodes: [
    { id: 'find', kind: 'mongo.find', params: {}, outputs: [{ name: 'documents', target: 'documents', type: { type: 'list', element: order } }] },
    { id: 'list', kind: 'stream.source.list', params: { path: 'documents' }, outputs: [] },
    { id: 'flat', kind: 'stream.flatMap', params: { path: 'lines' }, outputs: [] },
    { id: 'idx', kind: 'stream.zipWithIndex', params: {}, outputs: [] },
    { id: 'all', kind: 'stream.sink.collect', params: {}, outputs: [] },
  ],
  edges: [
    { from: 'find', to: 'list', port: 'out' },
    { from: 'list', to: 'flat', port: 'out' },
    { from: 'flat', to: 'idx', port: 'out' },
    { from: 'idx', to: 'all', port: 'out' },
  ],
} as unknown as FlowDef;

const shapes: Record<string, NodeIO> = {
  find: { role: 'node', ins: [{ name: 'in' }], outs: [{ name: 'out' }] },
  list: { role: 'source', ins: [{ name: 'in' }], outs: [s()] },
  flat: io('operator', ['in'], ['out']),
  idx: io('operator', ['in'], ['out']),
  all: io('sink', ['in'], []),
};

describe('inferElementTypes', () => {
  const t = inferElementTypes(flow, shapes, {});

  it('takes the element of a list source from the declared type of the state', () => {
    expect(t.outputs['list.out']).toEqual(order);
    expect(elementPaths(t.inputs.flat.in, {})).toEqual(['id', 'lines']);
  });

  it('follows the operators: flatMap gives the items, zipWithIndex wraps them', () => {
    expect(elementPaths(t.inputs.idx.in, {})).toEqual(['sku', 'qty']);
    expect(elementPaths(t.inputs.all.in, {})).toEqual(['value', 'value.sku', 'value.qty', 'index']);
  });
});
