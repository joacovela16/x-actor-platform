import { useCallback, useRef, useState } from 'react';

/** Width of a side panel that the user can change; it is remembered in this browser (best effort). */
export function usePanelWidth(key: string, initial: number, min: number, max: number) {
  const storageKey = `flow.panel.${key}`;
  const clamp = useCallback((w: number) => Math.round(Math.min(max, Math.max(min, w))), [min, max]);
  const [width, setWidthState] = useState(() => {
    try {
      const saved = Number(localStorage.getItem(storageKey));
      return saved ? clamp(saved) : initial;
    } catch {
      return initial;
    }
  });
  const setWidth = useCallback(
    (w: number) => {
      const next = clamp(w);
      setWidthState(next);
      try {
        localStorage.setItem(storageKey, String(next));
      } catch {
        /* sin almacenamiento: el ancho vale solo para esta sesión */
      }
    },
    [clamp, storageKey],
  );
  return { width, setWidth, reset: () => setWidth(initial), min, max };
}

type Props = {
  /** Lado del lienzo en el que está el panel: arrastrar hacia el lienzo lo ensancha. */
  side: 'left' | 'right';
  label: string;
  panel: ReturnType<typeof usePanelWidth>;
};

/** Asa vertical para cambiar el ancho de un panel lateral: arrastrar, flechas del teclado, o doble clic para volver al ancho inicial. */
export function ResizeHandle({ side, label, panel }: Props) {
  const start = useRef<{ x: number; width: number } | null>(null);
  const [dragging, setDragging] = useState(false);
  const sign = side === 'left' ? 1 : -1;

  return (
    <div
      role="separator"
      aria-orientation="vertical"
      aria-label={label}
      aria-valuenow={panel.width}
      aria-valuemin={panel.min}
      aria-valuemax={panel.max}
      tabIndex={0}
      title={`${label} (doble clic: ancho inicial)`}
      className={`resize-handle is-${side} ${dragging ? 'is-dragging' : ''}`}
      onPointerDown={(e) => {
        e.preventDefault();
        e.currentTarget.setPointerCapture(e.pointerId);
        start.current = { x: e.clientX, width: panel.width };
        setDragging(true);
      }}
      onPointerMove={(e) => {
        if (start.current) panel.setWidth(start.current.width + sign * (e.clientX - start.current.x));
      }}
      onPointerUp={() => {
        start.current = null;
        setDragging(false);
      }}
      onPointerCancel={() => {
        start.current = null;
        setDragging(false);
      }}
      onDoubleClick={panel.reset}
      onKeyDown={(e) => {
        const step = e.shiftKey ? 48 : 16;
        if (e.key === 'ArrowLeft') panel.setWidth(panel.width - sign * step);
        else if (e.key === 'ArrowRight') panel.setWidth(panel.width + sign * step);
        else return;
        e.preventDefault();
      }}
    />
  );
}
