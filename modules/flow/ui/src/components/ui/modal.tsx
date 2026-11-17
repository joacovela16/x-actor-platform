import { useEffect, useRef, type ReactNode } from 'react';
import { X } from 'lucide-react';
import { Button } from './button';

interface Props {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  description?: ReactNode;
  /** Botones al pie (a la derecha). */
  actions?: ReactNode;
  children?: ReactNode;
  /** Si se da, el contenido va dentro de un <form> que llama a esto al enviar. */
  onSubmit?: (e: React.FormEvent<HTMLFormElement>) => void;
}

/** Diálogo modal sobre <dialog> nativo: foco atrapado, Escape y clic en el fondo cierran. */
export function Modal({ open, onClose, title, description, actions, children, onSubmit }: Props) {
  const ref = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const d = ref.current;
    if (!d) return;
    if (open && !d.open) d.showModal();
    if (!open && d.open) d.close();
  }, [open]);

  const body = (
    <>
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <h3 className="text-lg font-semibold">{title}</h3>
          {description && <p className="text-sm text-base-content/60">{description}</p>}
        </div>
        <Button variant="ghost" size="icon-sm" aria-label="Cerrar" onClick={onClose}>
          <X />
        </Button>
      </div>
      {children}
      {actions && <div className="modal-action mt-4">{actions}</div>}
    </>
  );

  return (
    <dialog ref={ref} className="modal" onClose={onClose}>
      {onSubmit ? (
        <form className="modal-box" onSubmit={onSubmit}>
          {body}
        </form>
      ) : (
        <div className="modal-box">{body}</div>
      )}
      <form method="dialog" className="modal-backdrop">
        <button type="submit" aria-label="Cerrar">cerrar</button>
      </form>
    </dialog>
  );
}
