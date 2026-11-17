import { useLayoutEffect, useRef, type KeyboardEvent } from 'react';
import type { UiLanguage } from '../api/uiSpec';
import { useRefComplete } from './RefComplete';

interface Props {
  value: string;
  onChange: (v: string) => void;
  /** Informativo: se muestra y va en `data-lang`. No hay resaltado de sintaxis. */
  language?: UiLanguage;
  rows?: number;
  label?: string;
  placeholder?: string;
  /** Referencias `${...}` para autocompletar al escribir `${` (vacío: sin autocompletado, p. ej. en JavaScript). */
  refs?: string[];
}

const LANG_LABEL: Record<UiLanguage, string> = { javascript: 'JavaScript', scala: 'Scala', sql: 'SQL', json: 'JSON' };

/**
 * Editor de código sin dependencias: textarea monoespaciada con numeración de líneas. Tab indenta (Mayús+Tab desindenta,
 * también las líneas seleccionadas), Enter mantiene la indentación de la línea y Esc saca el foco (para no quedar atrapado).
 */
export function CodeEditor({ value, onChange, language, rows = 12, label, placeholder, refs = [] }: Props) {
  const taRef = useRef<HTMLTextAreaElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);
  // Selección que hay que poner tras un cambio manual (el DOM la pierde al reescribir `value`).
  const pendingSel = useRef<[number, number] | null>(null);
  const lineCount = value.split('\n').length;
  const rc = useRefComplete(taRef, value, onChange, refs);

  useLayoutEffect(() => {
    const sel = pendingSel.current;
    if (sel && taRef.current) {
      taRef.current.setSelectionRange(sel[0], sel[1]);
      pendingSel.current = null;
    }
  });

  /** Reescribe el texto y deja la selección indicada. */
  const edit = (next: string, start: number, end: number) => {
    pendingSel.current = [start, end];
    onChange(next);
  };

  /** Inserta en la selección. `insertText` conserva el historial de deshacer del navegador; si no está, se reescribe. */
  const insert = (ta: HTMLTextAreaElement, text: string, s: number, e: number) => {
    ta.focus();
    ta.setSelectionRange(s, e);
    const done = typeof document.execCommand === 'function' && document.execCommand('insertText', false, text);
    if (!done) edit(value.slice(0, s) + text + value.slice(e), s + text.length, s + text.length);
  };

  const onKeyDown = (ev: KeyboardEvent<HTMLTextAreaElement>) => {
    if (rc.onKeyDown(ev)) return;
    const ta = ev.currentTarget;
    const s = ta.selectionStart;
    const e = ta.selectionEnd;

    if (ev.key === 'Escape') {
      ta.blur();
      return;
    }

    if (ev.key === 'Tab') {
      ev.preventDefault();
      if (!ev.shiftKey && s === e) {
        insert(ta, '  ', s, e);
        return;
      }
      // Bloque de líneas tocadas por la selección (una sola línea también cuenta).
      const ls = value.lastIndexOf('\n', s - 1) + 1;
      const nl = value.indexOf('\n', e);
      const le = nl === -1 ? value.length : nl;
      const block = value.slice(ls, le).split('\n');
      const next = block.map((l) => (ev.shiftKey ? l.replace(/^(?: {1,2}|\t)/, '') : '  ' + l));
      const removedFirst = block[0].length - next[0].length;
      const newStart = Math.max(ls, ev.shiftKey ? s - removedFirst : s + 2);
      const newEnd = e + (next.join('\n').length - block.join('\n').length);
      edit(value.slice(0, ls) + next.join('\n') + value.slice(le), newStart, newEnd);
      return;
    }

    if (ev.key === 'Enter' && !ev.shiftKey && !ev.ctrlKey && !ev.metaKey && !ev.altKey) {
      ev.preventDefault();
      const ls = value.lastIndexOf('\n', s - 1) + 1;
      const indent = /^[ \t]*/.exec(value.slice(ls, s))?.[0] ?? '';
      insert(ta, '\n' + indent, s, e);
    }
  };

  return (
    <div className="code-wrap">
      <div className="code-editor relative" data-lang={language ?? 'text'}>
        <div className="code-gutter" ref={gutterRef} aria-hidden="true">
          {Array.from({ length: lineCount }, (_, i) => (
            <div key={i}>{i + 1}</div>
          ))}
        </div>
        <textarea
          ref={taRef}
          className="code-text"
          aria-label={label}
          rows={rows}
          spellCheck={false}
          wrap="off"
          value={value}
          placeholder={placeholder}
          onChange={(e) => {
            onChange(e.target.value);
            rc.sync();
          }}
          onKeyDown={onKeyDown}
          onClick={() => rc.sync()}
          onBlur={() => rc.close()}
          onScroll={(e) => {
            if (gutterRef.current) gutterRef.current.scrollTop = e.currentTarget.scrollTop;
          }}
        />
        {rc.popup}
      </div>
      <span className="text-xs text-base-content/60">
        {language ? `${LANG_LABEL[language]} · ` : ''}Tab indenta, Mayús+Tab desindenta, Esc sale del editor
      </span>
    </div>
  );
}
