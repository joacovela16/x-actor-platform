/** Familias de nodos con color propio (ver `--cat-*` en index.css); el resto usa el de `core`. */
const FAMILIES = new Set(['core', 'db', 'mongo', 'http', 'queue', 'stream', 'script', 'ai']);

/** Clase CSS con el color de la familia del nodo, por el prefijo de su kind: `mongo.find` → `cat-mongo`. */
export function categoryClass(kind: string): string {
  const prefix = kind.split('.')[0];
  return `cat-${FAMILIES.has(prefix) ? prefix : 'core'}`;
}
