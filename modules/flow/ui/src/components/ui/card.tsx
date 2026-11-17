import type { ComponentProps, ReactNode } from 'react';
import { cn } from '@/lib/utils';

/** Panel con borde. Con `title` (y opcionalmente `actions`) pinta una cabecera. */
export function Card({ title, actions, className, children, ...props }: Omit<ComponentProps<'div'>, 'title'> & { title?: ReactNode; actions?: ReactNode }) {
  return (
    <div className={cn('card card-border border-base-300 bg-base-100', className)} {...props}>
      {title !== undefined ? (
        <div className="card-body gap-3 p-4">
          <div className="flex items-center justify-between gap-2">
            <h2 className="card-title text-base">{title}</h2>
            {actions}
          </div>
          {children}
        </div>
      ) : (
        children
      )}
    </div>
  );
}

/** Cuerpo con relleno para una Card sin título. */
export function CardBody({ className, ...props }: ComponentProps<'div'>) {
  return <div className={cn('card-body gap-3 p-4', className)} {...props} />;
}
