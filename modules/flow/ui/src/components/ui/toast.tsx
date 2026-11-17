import { useSyncExternalStore } from 'react';
import { CircleAlert, CircleCheck, Info, X } from 'lucide-react';
import { cn } from '@/lib/utils';

export type ToastKind = 'info' | 'error' | 'ok';

interface Item {
  id: number;
  text: string;
  kind: ToastKind;
}

// Almacén mínimo fuera de React: `toast()` se puede llamar desde cualquier sitio y <Toaster/> lo pinta.
let items: Item[] = [];
let seq = 0;
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());

function dismiss(id: number) {
  items = items.filter((t) => t.id !== id);
  emit();
}

export function toast(text: string, kind: ToastKind = 'info') {
  const id = ++seq;
  items = [...items.slice(-4), { id, text, kind }];
  emit();
  setTimeout(() => dismiss(id), kind === 'error' ? 7000 : 4000);
}

const STYLE: Record<ToastKind, { cls: string; Icon: typeof Info }> = {
  info: { cls: 'alert-info', Icon: Info },
  error: { cls: 'alert-error', Icon: CircleAlert },
  ok: { cls: 'alert-success', Icon: CircleCheck },
};

export function Toaster() {
  const list = useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => items,
  );
  if (list.length === 0) return null;
  return (
    <div className="toast toast-center toast-bottom z-50" aria-live="polite">
      {list.map((t) => {
        const { cls, Icon } = STYLE[t.kind];
        return (
          <div key={t.id} role={t.kind === 'error' ? 'alert' : 'status'} className={cn('alert max-w-md py-2 text-sm shadow-lg', cls)}>
            <Icon className="size-4 shrink-0" />
            <span className="whitespace-normal">{t.text}</span>
            <button type="button" className="btn btn-ghost btn-xs btn-square" aria-label="Cerrar" onClick={() => dismiss(t.id)}>
              <X className="size-3.5" />
            </button>
          </div>
        );
      })}
    </div>
  );
}
