import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils';

export type BadgeVariant = 'default' | 'secondary' | 'outline' | 'ghost' | 'destructive' | 'info' | 'success' | 'warning';

const VARIANT: Record<BadgeVariant, string> = {
  default: 'badge-primary',
  secondary: 'badge-neutral badge-soft',
  outline: 'badge-outline border-base-300',
  ghost: 'badge-ghost',
  destructive: 'badge-error badge-soft',
  info: 'badge-info badge-outline',
  success: 'badge-success badge-outline',
  warning: 'badge-warning badge-outline',
};

export function Badge({ variant = 'default', className, ...props }: ComponentProps<'span'> & { variant?: BadgeVariant }) {
  return <span className={cn('badge badge-sm gap-1 font-medium whitespace-nowrap [&>svg]:size-3', VARIANT[variant], className)} {...props} />;
}
