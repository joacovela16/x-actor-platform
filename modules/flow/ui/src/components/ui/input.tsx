import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils';

export function Input({ className, ...props }: ComponentProps<'input'>) {
  return <input className={cn('input input-sm w-full', className)} {...props} />;
}

export function Textarea({ className, ...props }: ComponentProps<'textarea'>) {
  return <textarea className={cn('textarea textarea-sm w-full', className)} {...props} />;
}

type CheckProps = Omit<ComponentProps<'input'>, 'type' | 'onChange'> & { onCheckedChange?: (checked: boolean) => void };

export function Checkbox({ className, onCheckedChange, ...props }: CheckProps) {
  return <input type="checkbox" className={cn('checkbox checkbox-sm checkbox-primary', className)} onChange={(e) => onCheckedChange?.(e.target.checked)} {...props} />;
}

export function Switch({ className, onCheckedChange, ...props }: CheckProps) {
  return <input type="checkbox" role="switch" className={cn('toggle toggle-sm toggle-primary', className)} onChange={(e) => onCheckedChange?.(e.target.checked)} {...props} />;
}
