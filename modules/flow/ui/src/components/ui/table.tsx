import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils';

/** Tabla de daisyUI con scroll horizontal propio. */
export function Table({ className, ...props }: ComponentProps<'table'>) {
  return (
    <div className="w-full overflow-x-auto">
      <table className={cn('table table-sm', className)} {...props} />
    </div>
  );
}

export const TableHeader = (props: ComponentProps<'thead'>) => <thead {...props} />;
export const TableBody = (props: ComponentProps<'tbody'>) => <tbody {...props} />;

export function TableRow({ className, ...props }: ComponentProps<'tr'>) {
  return <tr className={cn('hover:bg-base-200/60', className)} {...props} />;
}

export function TableHead({ className, ...props }: ComponentProps<'th'>) {
  return <th className={cn('font-medium text-base-content/60', className)} {...props} />;
}

export function TableCell({ className, ...props }: ComponentProps<'td'>) {
  return <td className={cn('align-middle', className)} {...props} />;
}
