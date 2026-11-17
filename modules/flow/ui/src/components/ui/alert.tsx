import type { ComponentProps } from 'react';
import { CircleAlert, Info, TriangleAlert, CircleCheck } from 'lucide-react';
import { cn } from '@/lib/utils';

export type AlertVariant = 'info' | 'error' | 'warning' | 'success';

const STYLE: Record<AlertVariant, { cls: string; Icon: typeof Info }> = {
  info: { cls: 'alert-info', Icon: Info },
  error: { cls: 'alert-error', Icon: CircleAlert },
  warning: { cls: 'alert-warning', Icon: TriangleAlert },
  success: { cls: 'alert-success', Icon: CircleCheck },
};

export function Alert({ variant = 'info', className, children, ...props }: ComponentProps<'div'> & { variant?: AlertVariant }) {
  const { cls, Icon } = STYLE[variant];
  return (
    <div role={variant === 'error' ? 'alert' : 'status'} className={cn('alert alert-soft items-start py-2 text-sm', cls, className)} {...props}>
      <Icon className="mt-0.5 size-4 shrink-0" />
      <div className="min-w-0">{children}</div>
    </div>
  );
}
