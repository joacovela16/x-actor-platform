import { Badge, type BadgeVariant } from '@/components/ui/badge';

export type Tone = 'success' | 'danger' | 'warning' | 'info' | 'muted';

const TONE: Record<Tone, BadgeVariant> = {
  success: 'success',
  danger: 'destructive',
  warning: 'warning',
  info: 'info',
  muted: 'ghost',
};

const STATUS_TONE: Record<string, Tone> = {
  succeeded: 'success',
  failed: 'danger',
  running: 'info',
  waiting: 'warning',
  skipped: 'muted',
  pending: 'muted',
};

/** Etiqueta con el color de un estado de instancia o de nodo (`succeeded`, `failed`, `running`...). */
export function StatusBadge({ status, className, children }: { status: string; className?: string; children?: React.ReactNode }) {
  return <ToneBadge tone={STATUS_TONE[status] ?? 'muted'} className={className}>{children ?? status}</ToneBadge>;
}

export function ToneBadge({ tone, className, children }: { tone: Tone; className?: string; children: React.ReactNode }) {
  return (
    <Badge variant={TONE[tone]} className={className}>
      {children}
    </Badge>
  );
}
