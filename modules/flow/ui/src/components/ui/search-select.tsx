import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import { createPortal } from 'react-dom';
import { Check, ChevronDown, Search } from 'lucide-react';
import { cn } from '@/lib/utils';

export interface SelectOption<T extends string = string> {
  value: T;
  label: string;
  /** Texto secundario a la derecha (también se busca en él). */
  hint?: string;
  /** Encabezado bajo el que se agrupa; las opciones de un mismo grupo deben ir seguidas. */
  group?: string;
  disabled?: boolean;
}

interface Props<T extends string> {
  value: T;
  onChange: (value: T) => void;
  options: SelectOption<T>[];
  placeholder?: string;
  searchPlaceholder?: string;
  emptyText?: string;
  disabled?: boolean;
  className?: string;
  id?: string;
  'aria-label'?: string;
}

const norm = (s: string) => s.normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase();

const MAX_LIST_H = 288; // px, coincide con max-h-72

/**
 * Desplegable con búsqueda (combobox). Sustituye al <select> nativo en toda la UI.
 * La lista se pinta en un portal con posición fija, así no la recortan los paneles con overflow.
 */
export function SearchSelect<T extends string>({
  value,
  onChange,
  options,
  placeholder = '— elegir —',
  searchPlaceholder = 'Buscar…',
  emptyText = 'Sin resultados',
  disabled,
  className,
  id,
  'aria-label': ariaLabel,
}: Props<T>) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const [pos, setPos] = useState<{ left: number; top?: number; bottom?: number; width: number } | null>(null);
  const [portalTarget, setPortalTarget] = useState<HTMLElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLUListElement>(null);
  const listId = useId();

  const selected = options.find((o) => o.value === value);

  const filtered = useMemo(() => {
    const q = norm(query.trim());
    if (!q) return options;
    return options.filter((o) => norm(`${o.label} ${o.value} ${o.hint ?? ''} ${o.group ?? ''}`).includes(q));
  }, [options, query]);

  const place = () => {
    const r = triggerRef.current?.getBoundingClientRect();
    if (!r) return;
    const width = Math.min(Math.max(r.width, 280), window.innerWidth - 16);
    const left = Math.max(8, Math.min(r.left, window.innerWidth - width - 8));
    const below = window.innerHeight - r.bottom;
    // Arriba solo si abajo no cabe y arriba hay más sitio.
    if (below < MAX_LIST_H + 56 && r.top > below) setPos({ left, bottom: window.innerHeight - r.top + 4, width });
    else setPos({ left, top: r.bottom + 4, width });
  };

  const openList = (initialQuery = '') => {
    if (disabled) return;
    setQuery(initialQuery);
    const idx = initialQuery ? 0 : Math.max(0, options.findIndex((o) => o.value === value));
    setActive(idx);
    // Dentro de un <dialog> modal (top layer) el portal debe ir al propio dialog; en body quedaría debajo.
    setPortalTarget(triggerRef.current?.closest('dialog') ?? document.body);
    place();
    setOpen(true);
  };

  const close = (refocus = true) => {
    setOpen(false);
    if (refocus) triggerRef.current?.focus();
  };

  const pick = (o: SelectOption<T> | undefined) => {
    if (!o || o.disabled) return;
    if (o.value !== value) onChange(o.value);
    close();
  };

  // Mientras está abierto: se recoloca al hacer scroll o cambiar el tamaño, y se cierra al pulsar fuera.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (!popRef.current?.contains(t) && !triggerRef.current?.contains(t)) close(false);
    };
    window.addEventListener('scroll', place, true);
    window.addEventListener('resize', place);
    document.addEventListener('mousedown', onDown);
    return () => {
      window.removeEventListener('scroll', place, true);
      window.removeEventListener('resize', place);
      document.removeEventListener('mousedown', onDown);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // La opción activa siempre a la vista.
  useLayoutEffect(() => {
    if (!open) return;
    listRef.current?.querySelector<HTMLElement>(`[data-idx="${active}"]`)?.scrollIntoView({ block: 'nearest' });
  }, [active, open]);

  const move = (delta: number) => {
    if (filtered.length === 0) return;
    let i = active;
    for (let n = 0; n < filtered.length; n++) {
      i = (i + delta + filtered.length) % filtered.length;
      if (!filtered[i].disabled) break;
    }
    setActive(i);
  };

  const onSearchKey = (e: KeyboardEvent<HTMLInputElement>) => {
    switch (e.key) {
      case 'ArrowDown': e.preventDefault(); move(1); break;
      case 'ArrowUp': e.preventDefault(); move(-1); break;
      case 'Home': e.preventDefault(); setActive(0); break;
      case 'End': e.preventDefault(); setActive(filtered.length - 1); break;
      case 'Enter': e.preventDefault(); pick(filtered[active]); break;
      case 'Escape': e.preventDefault(); e.stopPropagation(); close(); break;
      case 'Tab': close(false); break;
    }
  };

  const onTriggerKey = (e: KeyboardEvent<HTMLButtonElement>) => {
    if (open) return;
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      openList();
    } else if (e.key.length === 1 && !e.ctrlKey && !e.metaKey && !e.altKey && e.key !== ' ') {
      // Escribir sobre el control cerrado abre la búsqueda con esa letra.
      e.preventDefault();
      openList(e.key);
    }
  };

  let lastGroup: string | undefined;

  return (
    <>
      <button
        ref={triggerRef}
        id={id}
        type="button"
        role="combobox"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={open ? listId : undefined}
        aria-label={ariaLabel}
        disabled={disabled}
        className={cn('input input-sm w-full cursor-pointer justify-between gap-2 text-left', className)}
        onClick={() => (open ? close() : openList())}
        onKeyDown={onTriggerKey}
      >
        <span className={cn('min-w-0 flex-1 truncate', !selected && !value && 'text-base-content/50')}>
          {selected ? selected.label : value || placeholder}
        </span>
        <ChevronDown className={cn('size-4 shrink-0 text-base-content/50 transition-transform', open && 'rotate-180')} />
      </button>

      {open &&
        pos &&
        portalTarget &&
        createPortal(
          <div
            ref={popRef}
            className="fixed z-[1000] flex flex-col overflow-hidden rounded-box border border-base-300 bg-base-100 shadow-lg"
            style={{ left: pos.left, top: pos.top, bottom: pos.bottom, width: pos.width }}
          >
            <label className="input input-sm input-ghost m-1 w-auto gap-2 focus-within:outline-none">
              <Search className="size-3.5 shrink-0 opacity-50" />
              <input
                autoFocus
                type="text"
                role="searchbox"
                aria-label="Buscar opción"
                aria-activedescendant={filtered[active] ? `${listId}-${active}` : undefined}
                aria-controls={listId}
                placeholder={searchPlaceholder}
                value={query}
                onChange={(e) => {
                  setQuery(e.target.value);
                  setActive(0);
                }}
                onKeyDown={onSearchKey}
              />
            </label>
            <ul ref={listRef} id={listId} role="listbox" aria-label={ariaLabel} className="max-h-72 overflow-y-auto border-t border-base-300 p-1 text-sm">
              {filtered.length === 0 && <li className="px-2 py-1.5 text-base-content/50">{emptyText}</li>}
              {filtered.map((o, i) => {
                const header = o.group && o.group !== lastGroup ? o.group : null;
                lastGroup = o.group;
                const isSel = o.value === value;
                return (
                  <li key={`${o.group ?? ''}:${o.value}`} role="presentation">
                    {header && <div className="px-2 pt-2 pb-1 text-[11px] font-semibold tracking-wide text-base-content/50 uppercase">{header}</div>}
                    <div
                      id={`${listId}-${i}`}
                      data-idx={i}
                      role="option"
                      aria-selected={isSel}
                      aria-disabled={o.disabled || undefined}
                      className={cn(
                        'flex cursor-pointer items-center gap-2 rounded-field px-2 py-1.5',
                        i === active && 'bg-base-200',
                        o.disabled && 'cursor-not-allowed opacity-50',
                      )}
                      onMouseMove={() => i !== active && setActive(i)}
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => pick(o)}
                    >
                      <Check className={cn('size-3.5 shrink-0 text-primary', !isSel && 'invisible')} />
                      <span className="min-w-0 flex-1 truncate">{o.label}</span>
                      {o.hint && <span className="max-w-[45%] shrink truncate font-mono text-xs text-base-content/50">{o.hint}</span>}
                    </div>
                  </li>
                );
              })}
            </ul>
          </div>,
          portalTarget,
        )}
    </>
  );
}
