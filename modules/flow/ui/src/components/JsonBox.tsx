import { useEffect, useRef, useState } from 'react';
import type { JsonValue } from '../api/types';
import { FormField } from '@/components/ui/form-field';
import { Textarea } from '@/components/ui/input';
import { parseRelaxed } from '@/lib/relaxedJson';

interface Props {
  label?: string;
  value: JsonValue | undefined;
  /** Se llama solo con JSON válido. Texto vacío = undefined. */
  onChange: (v: JsonValue | undefined) => void;
  /** Avisa si el texto actual es JSON válido. */
  onValid?: (valid: boolean) => void;
  rows?: number;
  hint?: string;
}

/** Textarea de JSON con validación en vivo. Mantiene el texto mientras se escribe. No exige comillas (ver `parseRelaxed`). */
export function JsonBox({ label, value, onChange, onValid, rows = 5, hint }: Props) {
  const fmt = (v: JsonValue | undefined) => (v === undefined ? '' : JSON.stringify(v, null, 2));
  const [text, setText] = useState(() => fmt(value));
  const [error, setError] = useState<string | null>(null);
  const lastSeen = useRef(JSON.stringify(value ?? null));

  // Si el valor cambia desde fuera (otro nodo, plantilla...), se reescribe el texto.
  useEffect(() => {
    const s = JSON.stringify(value ?? null);
    if (s !== lastSeen.current) {
      lastSeen.current = s;
      setText(fmt(value));
      setError(null);
      onValid?.(true);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);

  const handle = (t: string) => {
    setText(t);
    if (t.trim() === '') {
      setError(null);
      lastSeen.current = 'null';
      onValid?.(true);
      onChange(undefined);
      return;
    }
    try {
      const parsed = parseRelaxed(t);
      setError(null);
      lastSeen.current = JSON.stringify(parsed);
      onValid?.(true);
      onChange(parsed);
    } catch (e) {
      setError((e as Error).message);
      onValid?.(false);
    }
  };

  return (
    <FormField label={label} hint={hint} error={error && `No se puede leer: ${error}`}>
      <Textarea
        className={`font-mono ${error ? 'border-error' : ''}`}
        rows={rows}
        spellCheck={false}
        value={text}
        onChange={(e) => handle(e.target.value)}
      />
    </FormField>
  );
}
