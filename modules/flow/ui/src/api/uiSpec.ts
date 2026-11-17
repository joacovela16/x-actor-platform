// Especificación de UI de un campo (`ConfigSchema.Field.ui`): cómo se dibuja, no qué valida. La escribe el backend
// (o el mock) junto al schema; si falta, `resolveUi` la infiere del DataType, de `secret` y de `oneOf`, como antes.
// `SpecForm` es el único intérprete: nodos y conexiones pasan por él.
import type { ConfigField, JsonValue } from './types';

export type UiWidget =
  | 'text' | 'textarea' | 'code' | 'password' | 'number' | 'checkbox' | 'select' | 'json' | 'list' | 'map' | 'group' | 'entity'
  /** Los archivos de un `code` (ver `filesField`): no se dibuja solo. */
  | 'files';

/** Lenguaje de un `code`. JavaScript y Scala se editan con Monaco (tipos, resaltado y archivos); el resto, con el editor simple. */
export type UiLanguage = 'javascript' | 'scala' | 'sql' | 'json';

export interface UiOption {
  value: JsonValue;
  label: string;
}

/** El campo solo se muestra cuando `values[field]` es igual a `equals`. */
export interface UiVisibleWhen {
  field: string;
  equals: JsonValue;
}

export interface UiSpec {
  /** Widget a usar. Sin él se infiere (ver `resolveUi`). */
  widget?: UiWidget;
  /** Solo para `code`. */
  language?: UiLanguage;
  /** Filas visibles de `textarea`, `code` y `json`. */
  rows?: number;
  placeholder?: string;
  /** Texto de ayuda. Sustituye a la `description` (que puede llevar un `editor:...` heredado). */
  help?: string;
  /** Opciones explícitas de `select`; sin ellas, `oneOf` o sí/no para `bool`. */
  options?: UiOption[];
  visibleWhen?: UiVisibleWhen;
  /** Sección: los campos con el mismo `group` se agrupan bajo un encabezado. */
  group?: string;
  /** Subcampos de `group` (un objeto) o de `list` (una lista de objetos). */
  fields?: ConfigField[];
  /** Orden dentro del formulario (menor primero; empate, orden del schema). */
  order?: number;
  /** Valor con el que empieza un nodo nuevo (p. ej. código de ejemplo). */
  template?: string;
  /** Solo para `code`: el campo hermano (`[{name, code}]`) con más archivos, que se editan como pestañas. */
  filesField?: string;
}

/** Widget resuelto para un campo: la `ui` explícita o la inferida, con el fallback heredado `editor:...`. */
export interface ResolvedUi {
  widget: UiWidget;
  language?: UiLanguage;
  rows?: number;
  placeholder?: string;
  help?: string;
  options?: UiOption[];
  /** El campo es una ruta del estado: autocompleta con las rutas del flujo. */
  pathField: boolean;
  template?: string;
  filesField?: string;
}

const EDITOR_HINT = /\s*\beditor:[a-z]+\b/gi;

/** La descripción sin el sufijo `editor:...` (texto heredado de los schemas). */
export function stripEditorHint(description: string | undefined): string | undefined {
  if (description === undefined) return undefined;
  const text = description.replace(EDITOR_HINT, '').trim();
  return text === '' ? undefined : text;
}

/** Fallback heredado: `editor:javascript|sql` equivale a `code`; `editor:text` a `textarea`. */
export function legacyEditor(description: string | undefined): { widget: UiWidget; language?: UiLanguage } | undefined {
  if (!description) return undefined;
  if (/\beditor:javascript\b/i.test(description)) return { widget: 'code', language: 'javascript' };
  if (/\beditor:sql\b/i.test(description)) return { widget: 'code', language: 'sql' };
  if (/\beditor:text\b/i.test(description)) return { widget: 'textarea' };
  return undefined;
}

/** Widget de un campo: `ui.widget`, luego el fallback heredado, luego la inferencia por secret, oneOf y tipo. */
export function resolveUi(field: ConfigField, pathField = false): ResolvedUi {
  const ui = field.ui ?? {};
  const legacy = legacyEditor(field.description);
  let widget: UiWidget;
  if (ui.widget) widget = ui.widget;
  else if (legacy) widget = legacy.widget;
  else if (field.secret) widget = 'password';
  else if (field.oneOf && field.oneOf.length > 0) widget = 'select';
  else {
    switch (field.type.type) {
      case 'bool': widget = 'select'; break;
      case 'num': widget = 'number'; break;
      case 'str': widget = 'text'; break;
      default: widget = 'json';
    }
  }
  return {
    widget,
    language: ui.language ?? legacy?.language,
    rows: ui.rows,
    placeholder: ui.placeholder,
    help: ui.help ?? stripEditorHint(field.description),
    options: ui.options,
    pathField,
    template: ui.template,
    filesField: ui.filesField,
  };
}

/** Igualdad de valores JSON (para opciones, `visibleWhen` y el valor seleccionado). */
export function sameValue(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
