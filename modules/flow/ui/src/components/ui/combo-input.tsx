import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import { createPortal } from 'react-dom';
import { ChevronDown } from 'lucide-react';
import { cn } from '@/lib/utils';

interface Props {
  value: string;
  onChange: (value: string) => void;
  /** Sugerencias; se puede escribir cualquier otro texto. */
  options: string[];
  placeholder?: string;
  className?: string;
  'aria-label'?: string;
}

const MAX_LIST_H = 240; // px, coincide con max-h-60

/**
 * Campo de texto libre con un desplegable de sugerencias (combobox editable). A diferencia de `SearchSelect`, el valor no
 * tiene que ser una de las opciones. La flecha muestra todas; al escribir se filtran.
 */
export function ComboInput({ value, onChange, options, placeholder, className, 'aria-label': ariaLabel }: Props) {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState(false);
  const [active, setActive] = useState(-1);
  const [pos, setPos] = useState<{ left: number; top?: number; bottom?: number; width: number } | null>(null);
  const wrapRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const popRef = useRef<HTMLUListElement>(null);
  const listId = useId();

  const shown = useMemo(() => {
    const q = value.trim().toLowerCase();
    const all = [...new Set(options)];
    return filter && q ? all.filter((o) => o.toLowerCase().includes(q)) : all;
  }, [options, value, filter]);

  const place = () => {
    const r = wrapRef.current?.getBoundingClientRect();
    if (!r) return;
    const below = window.innerHeight - r.bottom;
    if (below < MAX_LIST_H + 8 && r.top > below) setPos({ left: r.left, bottom: window.innerHeight - r.top + 4, width: r.width });
    else setPos({ left: r.left, top: r.bottom + 4, width: r.width });
  };

  const show = (filtering: boolean) => {
    setFilter(filtering);
    setActive(-1);
    place();
    setOpen(true);
  };

  const pick = (o: string) => {
    onChange(o);
    setOpen(false);
    inputRef.current?.focus();
  };

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (!popRef.current?.contains(t) && !wrapRef.current?.contains(t)) setOpen(false);
    };
    window.addEventListener('scroll', place, true);
    window.addEventListener('resize', place);
    document.addEventListener('mousedown', onDown);
    return () => {
      window.removeEventListener('scroll', place, true);
      window.removeEventListener('resize', place);
      document.removeEventListener('mousedown', onDown);
    };
  }, [open]);

  useLayoutEffect(() => {
    if (open && active >= 0) popRef.current?.querySelector<HTMLElement>(`[data-idx="${active}"]`)?.scrollIntoView({ block: 'nearest' });
  }, [active, open]);

  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      if (!open) return show(false);
      if (shown.length === 0) return;
      const d = e.key === 'ArrowDown' ? 1 : -1;
      setActive((i) => (i + d + shown.length) % shown.length);
    } else if (e.key === 'Enter' && open && active >= 0 && shown[active] !== undefined) {
      e.preventDefault();
      pick(shown[active]);
    } else if (e.key === 'Escape' && open) {
      e.preventDefault();
      e.stopPropagation();
      setOpen(false);
    } else if (e.key === 'Tab') setOpen(false);
  };

  const hasOptions = options.length > 0;

  return (
    <>
      <div ref={wrapRef} className={cn('input input-sm w-full gap-1 pr-1', className)}>
        <input
          ref={inputRef}
          type="text"
          role="combobox"
          aria-label={ariaLabel}
          aria-expanded={open}
          aria-autocomplete="list"
          aria-controls={open ? listId : undefined}
          aria-activedescendant={open && active >= 0 ? `${listId}-${active}` : undefined}
          className="min-w-0 flex-1"
          value={value}
          placeholder={placeholder ?? (hasOptions ? 'elige o escribe…' : undefined)}
          onChange={(e) => {
            onChange(e.target.value);
            if (hasOptions) show(true);
          }}
          onKeyDown={onKey}
        />
        {hasOptions && (
          <button
            type="button"
            tabIndex={-1}
            aria-label="Mostrar sugerencias"
            className="btn btn-ghost btn-xs btn-square"
            onMouseDown={(e) => e.preventDefault()}
            onClick={() => (open ? setOpen(false) : (show(false), inputRef.current?.focus()))}
          >
            <ChevronDown className={cn('size-4 text-base-content/50 transition-transform', open && 'rotate-180')} />
          </button>
        )}
      </div>

      {open &&
        pos &&
        shown.length > 0 &&
        createPortal(
          <ul
            ref={popRef}
            id={listId}
            role="listbox"
            className="fixed z-[1000] max-h-60 overflow-y-auto rounded-box border border-base-300 bg-base-100 p-1 font-mono text-sm shadow-lg"
            style={{ left: pos.left, top: pos.top, bottom: pos.bottom, width: pos.width }}
          >
            {shown.map((o, i) => (
              <li
                key={o}
                id={`${listId}-${i}`}
                data-idx={i}
                role="option"
                aria-selected={o === value}
                className={cn('cursor-pointer truncate rounded-field px-2 py-1', i === active && 'bg-base-200', o === value && 'text-primary')}
                onMouseMove={() => i !== active && setActive(i)}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => pick(o)}
              >
                {o}
              </li>
            ))}
          </ul>,
          inputRef.current?.closest('dialog') ?? document.body,
        )}
    </>
  );
}
