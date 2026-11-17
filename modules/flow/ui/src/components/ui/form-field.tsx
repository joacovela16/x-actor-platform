import type { ReactNode } from 'react';
import { cn } from '@/lib/utils';

interface Props {
  label?: ReactNode;
  /** Texto de ayuda bajo el control. */
  hint?: ReactNode;
  /** Mensaje de error bajo el control (sustituye a la ayuda). */
  error?: ReactNode;
  className?: string;
  children: ReactNode;
}

/** Etiqueta + control + ayuda. La etiqueta envuelve al control, así no hace falta `htmlFor`. */
export function FormField({ label, hint, error, className, children }: Props) {
  return (
    <label className={cn('flex min-w-0 flex-col gap-1.5', className)}>
      {label && <span className="flex items-center gap-1.5 text-xs text-base-content/60">{label}</span>}
      {children}
      {error ? <span className="text-xs text-error">{error}</span> : hint ? <span className="text-xs text-base-content/60">{hint}</span> : null}
    </label>
  );
}
