import { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api/client';
import type { CatalogEntry, Describe, NodeDef, NodeRole, PortInfo } from '../api/types';
import { portsOf } from '../graph/graph';
import type { NodeIO } from '../graph/stream';

/** Lo que el editor sabe de un nodo: sus salidas y entradas (con tipo de stream si lo llevan), su papel y los fallos de configuración. */
export interface NodeShape extends NodeIO {
  /** Salidas, con `error` al final. Igual que `outs`. */
  ports: PortInfo[];
  problems: string[];
  /** Entrada `in` (o las de join/zip) del catálogo cuando no hay describe todavía. */
  described: boolean;
}

const NO_PROBLEMS: string[] = [];

/**
 * Forma de cada nodo. Los puertos pueden depender de los params (`POST /api/catalog/{kind}/describe`), así que se pide la
 * descripción para cada combinación kind+params distinta. Mientras llega, se usan los datos del catálogo.
 */
export function usePorts(nodes: NodeDef[], catalog: CatalogEntry[]): Record<string, NodeShape> {
  const cache = useRef(new Map<string, Describe>());
  const pending = useRef(new Set<string>());
  // La última descripción de cada nodo: se sigue mostrando mientras llega la de los params nuevos, para que el panel no salte en
  // cada tecla (sin ella los problemas desaparecen y vuelven a aparecer).
  const last = useRef(new Map<string, { kind: string; describe: Describe }>());
  const mounted = useRef(true);
  const [version, setVersion] = useState(0);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const catalogByKind = useMemo(() => new Map(catalog.map((c) => [c.kind, c])), [catalog]);
  const keyOf = (n: NodeDef) => `${n.kind}\n${JSON.stringify(n.params)}`;
  const signature = nodes.map((n) => `${n.id}\u0001${keyOf(n)}`).join('\u0002');

  useEffect(() => {
    const ask = () => {
      for (const n of nodes) {
        const key = keyOf(n);
        if (cache.current.has(key) || pending.current.has(key)) continue;
        pending.current.add(key);
        api
          .describe(n.kind, n.params)
          .then((d) => {
            // Se guarda aunque los params ya hayan cambiado: la clave es kind+params, así que sigue siendo cierta.
            cache.current.set(key, d);
            if (mounted.current) setVersion((v) => v + 1);
          })
          .catch(() => {
            // Sin descripción se usan los datos del catálogo; el validador dirá lo que falte.
          })
          .finally(() => pending.current.delete(key));
      }
    };
    // Al cargar se pide enseguida; mientras se escribe, cuando se deja de teclear un momento.
    if (nodes.every((n) => cache.current.has(keyOf(n)))) return;
    if (cache.current.size === 0) {
      ask();
      return;
    }
    const timer = setTimeout(ask, 250);
    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [signature]);

  return useMemo(() => {
    const out: Record<string, NodeShape> = {};
    for (const n of nodes) {
      const fresh = cache.current.get(keyOf(n));
      if (fresh) last.current.set(n.id, { kind: n.kind, describe: fresh });
      const previous = last.current.get(n.id);
      const described = fresh ?? (previous?.kind === n.kind ? previous.describe : undefined);
      const entry = catalogByKind.get(n.kind);
      const ports = portsOf(described ? described.ports : entry?.ports);
      const ins = (described ? described.inputs : entry?.inputs) ?? [{ name: 'in' }];
      const role: NodeRole = (described ? described.role : entry?.role) ?? 'node';
      out[n.id] = {
        ports,
        outs: ports,
        ins,
        role,
        problems: described?.problems ?? NO_PROBLEMS,
        described: !!described,
      };
    }
    return out;
    // version fuerza recalcular cuando llega una descripción nueva
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [signature, version, catalogByKind]);
}
