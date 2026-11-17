// Lo de los editores de scripts que no necesita Monaco (que se carga aparte, al abrir un nodo de código).
import { createContext } from 'react';
import type { JsonValue } from '../api/types';
import type { ScriptEnv } from '@/lib/scriptTypes';

/** Lo que el editor sabe del nodo que se edita. Lo da el panel de propiedades; sin él no hay tipos del flujo. */
export const ScriptEnvContext = createContext<(ScriptEnv & { nodeId: string }) | null>(null);

/** Un archivo más de un script, además de su código. */
export interface ScriptFile {
  name: string;
  code: string;
}

/** Lee los `files` de un nodo (`[{name, code}]`); lo que no tenga esa forma se ignora. */
export function readFiles(v: JsonValue | undefined): ScriptFile[] {
  if (!Array.isArray(v)) return [];
  return v.flatMap((f) =>
    f && typeof f === 'object' && !Array.isArray(f) && typeof f.name === 'string' ? [{ name: f.name, code: typeof f.code === 'string' ? f.code : '' }] : [],
  );
}
