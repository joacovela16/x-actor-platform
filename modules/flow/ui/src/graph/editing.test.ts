import { describe, expect, it } from 'vitest';
import type { CatalogEntry, FlowDef, JsonValue, NodeDef } from '../api/types';

const n = (id: string, params: Record<string, JsonValue> = {}): NodeDef => ({ id, kind: 'k', params, outputs: [], runPolicy: { whenSucceeded: 'run', whenFailed: 'run' }, joinPolicy: 'all' });
import { History, copy, freeId, paste, splitEdge } from './editing';
import { edgeKey } from './graph';

const flow = (n: number): FlowDef => ({ version: 1, id: 'f', name: `v${n}`, revision: 1, inputs: [], outputs: [], nodes: [], edges: [] });

describe('History', () => {
  it('undoes and redoes, and merges changes that come close together', () => {
    const h = new History(10, 100);
    const a = flow(1);
    const b = flow(2);
    const c = flow(3);
    h.record(a, 0);
    h.record(b, 50); // same change as the one before: typing
    expect(h.undo(c)).toBe(a);
    expect(h.redo(a)).toBe(c);
    h.record(c, 1000);
    expect(h.canRedo).toBe(false);
  });
});

describe('copy and paste', () => {
  const f: FlowDef = {
    ...flow(1),
    nodes: [
      n('a', { x: 1 }),
      n('b'),
      n('c'),
    ],
    edges: [
      { from: 'a', port: 'out', to: 'b' },
      { from: 'b', port: 'out', to: 'c' },
    ],
    layout: { a: { x: 0, y: 0 }, b: { x: 100, y: 0 } },
  };

  it('pastes with new ids, the edges between the copied nodes, and moved', () => {
    const clip = copy(f, ['a', 'b'])!;
    const { flow: out, ids } = paste(f, clip);
    expect(ids).toEqual(['a-2', 'b-2']);
    expect(out.edges.slice(2)).toEqual([{ from: 'a-2', port: 'out', to: 'b-2' }]);
    expect(out.layout?.['b-2']).toEqual({ x: 140, y: 40 });
    expect(out.nodes[3].params).toEqual({ x: 1 });
    expect(out.nodes[3].params).not.toBe(f.nodes[0].params);
  });

  it('finds a free id', () => {
    expect(freeId('a-2', new Set(['a', 'a-2']))).toBe('a-3');
  });
});

describe('splitEdge', () => {
  it('puts the node in the middle of the edge', () => {
    const f: FlowDef = { ...flow(1), nodes: [], edges: [{ from: 'x', port: 'then', to: 'y' }] };
    const entry = { kind: 'stream.branch', ports: [{ name: 'then' }, { name: 'else' }, 'error'], inputs: [{ name: 'in' }] } as unknown as CatalogEntry;
    const out = splitEdge(f, edgeKey(f.edges[0]), { ...n('m'), kind: 'stream.branch' }, entry);
    expect(out.edges).toEqual([
      { from: 'x', port: 'then', to: 'm' },
      { from: 'm', port: 'then', to: 'y' },
    ]);
  });
});
