import type { FlowError } from '../api/types';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';

interface Props {
  errors: FlowError[];
  validating: boolean;
  onFocus: (nodeId: string) => void;
}

/** Errores del validador (POST /api/flows/validate). Los que traen `node` se enfocan en el lienzo. */
export function ErrorsPanel({ errors, validating, onFocus }: Props) {
  if (errors.length === 0) {
    return <p className="text-success">{validating ? 'Validando…' : 'Sin errores.'}</p>;
  }
  return (
    <ul className="issue-list">
      {errors.map((e, i) => (
        <li key={`${e.code}-${i}`} className="issue">
          <Badge variant="destructive" className="font-mono">{e.code}</Badge>
          <span className="issue-msg">{e.message}</span>
          {e.node && (
            <Button variant="ghost" size="xs" type="button" onClick={() => onFocus(e.node!)}>
              ver nodo {e.node}
            </Button>
          )}
        </li>
      ))}
    </ul>
  );
}
