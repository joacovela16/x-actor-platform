import type { ReactNode } from 'react';
import { cn } from '@/lib/utils';

export interface TabItem<T extends string> {
  value: T;
  label: ReactNode;
}

interface Props<T extends string> {
  items: TabItem<T>[];
  /** Pestaña activa; ninguna si no coincide con ningún item. */
  value: T | '';
  onChange: (value: T) => void;
  className?: string;
}

/** Pestañas de daisyUI (estilo borde). Solo la barra: el contenido lo pinta quien la usa. */
export function Tabs<T extends string>({ items, value, onChange, className }: Props<T>) {
  return (
    <div role="tablist" className={cn('tabs tabs-border tabs-sm flex-nowrap overflow-x-auto', className)}>
      {items.map((t) => (
        <button
          key={t.value}
          type="button"
          role="tab"
          aria-selected={t.value === value}
          className={cn('tab shrink-0 gap-1.5 px-2.5', t.value === value && 'tab-active')}
          onClick={() => onChange(t.value)}
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}
