import type { CatalogEntry } from '../api/types';

/** `<plugin>.<kind>@<major>` → sus partes; `null` para los tipos que no vienen de un plugin. */
export function pluginKind(kind: string): { base: string; major: number } | null {
  const m = /^(.+)@(\d+)$/.exec(kind);
  return m ? { base: m[1], major: Number(m[2]) } : null;
}

/** El tipo del major más nuevo de un nodo de plugin, si hay uno más nuevo que `kind` en el catálogo. */
export function newerMajor(kind: string, catalog: CatalogEntry[]): string | undefined {
  const k = pluginKind(kind);
  if (!k) return undefined;
  let best: { kind: string; major: number } | undefined;
  for (const c of catalog) {
    const o = pluginKind(c.kind);
    if (o && o.base === k.base && o.major > k.major && (!best || o.major > best.major)) best = { kind: c.kind, major: o.major };
  }
  return best?.kind;
}

/** El catálogo para la paleta: de cada nodo de plugin, solo su major más nuevo (los viejos siguen sirviendo a los flujos que los usan). */
export function latestMajors(catalog: CatalogEntry[]): CatalogEntry[] {
  return catalog.filter((c) => newerMajor(c.kind, catalog) === undefined);
}
