import type { ComponentProps } from 'react';
import { cn } from '@/lib/utils';

export type ButtonVariant = 'default' | 'outline' | 'secondary' | 'ghost' | 'destructive' | 'link';
export type ButtonSize = 'default' | 'xs' | 'sm' | 'lg' | 'icon' | 'icon-xs' | 'icon-sm' | 'icon-lg';

const VARIANT: Record<ButtonVariant, string> = {
  default: 'btn-primary',
  outline: 'btn-outline border-base-300 hover:border-base-300 hover:bg-base-200 hover:text-base-content',
  secondary: 'btn-secondary',
  ghost: 'btn-ghost',
  destructive: 'btn-error btn-soft',
  link: 'btn-link no-underline hover:underline',
};

const SIZE: Record<ButtonSize, string> = {
  default: 'btn-sm',
  xs: 'btn-xs',
  sm: 'btn-sm h-7 min-h-7',
  lg: 'btn-md',
  icon: 'btn-sm btn-square',
  'icon-xs': 'btn-xs btn-square',
  'icon-sm': 'btn-xs btn-square size-7',
  'icon-lg': 'btn-md btn-square',
};

/** Clases de botón de daisyUI, para dar aspecto de botón a otro elemento. */
export function buttonClass({ variant = 'default', size = 'default', className }: { variant?: ButtonVariant; size?: ButtonSize; className?: string } = {}) {
  return cn('btn flex-nowrap gap-1.5 font-medium [&_svg]:size-4 [&_svg]:shrink-0', VARIANT[variant], SIZE[size], className);
}

interface Common {
  variant?: ButtonVariant;
  size?: ButtonSize;
}

type ButtonProps = Common & ComponentProps<'button'> & { href?: undefined };
type LinkProps = Common & ComponentProps<'a'> & { href: string };

/** Botón; con `href` se pinta como enlace con aspecto de botón. */
export function Button(props: ButtonProps | LinkProps) {
  if (props.href !== undefined) {
    const { variant, size, className, ...rest } = props as LinkProps;
    return <a className={buttonClass({ variant, size, className })} {...rest} />;
  }
  const { variant, size, className, type = 'button', ...rest } = props as ButtonProps;
  return <button type={type} className={buttonClass({ variant, size, className })} {...rest} />;
}
