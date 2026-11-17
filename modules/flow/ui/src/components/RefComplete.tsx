import { useRef, useState, type ChangeEvent, type KeyboardEvent, type RefObject } from 'react';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/input';

/** El `${...}` que se está escribiendo: desde `${` hasta el cursor, sin `}` en medio. */
function openRef(text: string, caret: number): { start: number; typed: string } | null {
  const before = text.slice(0, caret);
  const at = before.lastIndexOf('${');
  if (at < 0 || (at > 0 && before[at - 1] === '$')) return null;
  const typed = before.slice(at + 2);
  if (/[}\s"'`]/.test(typed)) return null;
  return { start: at + 2, typed };
}

type Field = HTMLInputElement | HTMLTextAreaElement;

/**
 * Autocompletado de referencias `${origen.ruta}` en un campo de texto: al escribir `${` aparece la lista de lo que se puede
 * usar ahí (`state.…`, `context.…`), filtrada por lo escrito. Flechas para elegir, Enter o Tab para poner, Esc para cerrar.
 *
 * `onKeyDown` devuelve true cuando la tecla fue del autocompletado (el campo no debe tratarla).
 */
export function useRefComplete(ref: RefObject<Field | null>, value: string, onChange: (v: string) => void, options: string[]) {
  const [open, setOpen] = useState<{ start: number; typed: string } | null>(null);
  const [active, setActive] = useState(0);

  const matches = open ? options.filter((o) => o.toLowerCase().includes(open.typed.toLowerCase())).slice(0, 12) : [];
  const visible = open !== null && matches.length > 0;

  /** Llamar tras cada cambio o movimiento del cursor. */
  const sync = () => {
    const el = ref.current;
    if (!el || options.length === 0) return setOpen(null);
    const next = openRef(el.value, el.selectionStart ?? el.value.length);
    setOpen(next);
    if (!next || next.typed !== open?.typed) setActive(0);
  };

  const accept = (choice: string) => {
    const el = ref.current;
    if (!el || !open) return;
    const caret = el.selectionStart ?? value.length;
    // Si ya hay un `}` justo después (se estaba corrigiendo una referencia), no se duplica.
    const rest = value.slice(caret);
    const close = rest.startsWith('}') ? '' : '}';
    const next = value.slice(0, open.start) + choice + close + rest;
    const pos = open.start + choice.length + 1;
    onChange(next);
    setOpen(null);
    requestAnimationFrame(() => {
      el.focus();
      el.setSelectionRange(pos, pos);
    });
  };

  const onKeyDown = (e: KeyboardEvent<Field>): boolean => {
    if (!visible) return false;
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      setActive((a) => (a + (e.key === 'ArrowDown' ? 1 : matches.length - 1)) % matches.length);
      return true;
    }
    if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault();
      accept(matches[Math.min(active, matches.length - 1)]);
      return true;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      setOpen(null);
      return true;
    }
    return false;
  };

  const popup = visible ? (
    <ul className="ref-complete" role="listbox" aria-label="referencias">
      {matches.map((m, i) => (
        <li
          key={m}
          role="option"
          aria-selected={i === active}
          className={i === active ? 'is-active' : undefined}
          // mousedown y no click: así el campo no pierde el foco antes de poner la referencia.
          onMouseDown={(e) => {
            e.preventDefault();
            accept(m);
          }}
        >
          <span className="text-base-content/60">{'${'}</span>
          {m}
          <span className="text-base-content/60">{'}'}</span>
        </li>
      ))}
    </ul>
  ) : null;

  return { sync, onKeyDown, popup, close: () => setOpen(null) };
}

/** Lo que un campo de un nodo puede referenciar: las rutas del estado y los datos de la ejecución. */
export function nodeRefOptions(paths: string[]): string[] {
  return [...paths.map((p) => `state.${p}`), 'context.instanceId', 'context.nodeId', 'context.key'];
}

/** Un `Input` (o `Textarea` con `rows`) con el autocompletado de referencias. */
export function RefText({
  value, onChange, options, rows, className, ...rest
}: {
  value: string;
  onChange: (v: string) => void;
  options: string[];
  rows?: number;
  className?: string;
  'aria-label'?: string;
  placeholder?: string;
  inputMode?: 'decimal' | 'text';
  list?: string;
}) {
  const ref = useRef<HTMLInputElement & HTMLTextAreaElement>(null);
  const rc = useRefComplete(ref, value, onChange, options);
  const common = {
    ...rest,
    ref,
    value,
    onChange: (e: ChangeEvent<Field>) => {
      onChange(e.target.value);
      rc.sync();
    },
    onKeyDown: (e: KeyboardEvent<Field>) => void rc.onKeyDown(e),
    onKeyUp: (e: KeyboardEvent<Field>) => {
      if (e.key === 'ArrowLeft' || e.key === 'ArrowRight' || e.key === 'Home' || e.key === 'End') rc.sync();
    },
    onClick: () => rc.sync(),
    onBlur: () => rc.close(),
  };
  return (
    <div className="relative">
      {rows ? <Textarea rows={rows} className={className} {...common} /> : <Input className={className} {...common} />}
      {rc.popup}
    </div>
  );
}
