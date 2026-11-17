import { useState } from 'react';
import type { JsonValue } from '../api/types';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/input';
import { parseRelaxed } from '@/lib/relaxedJson';

interface Props {
  label: string;
  value: JsonValue[];
  onChange: (items: JsonValue[]) => void;
}

/** Lista de elementos JSON (el mock de una fuente de stream). Cada elemento se edita como texto; solo se envían los válidos. */
export function ElementsEditor({ label, value, onChange }: Props) {
  const [texts, setTexts] = useState<string[]>(() => value.map((v) => JSON.stringify(v)));
  const [errors, setErrors] = useState<Array<string | null>>(() => value.map(() => null));

  const emit = (next: string[]) => {
    const parsed: JsonValue[] = [];
    const errs: Array<string | null> = next.map((t) => {
      if (t.trim() === '') return null;
      try {
        parsed.push(parseRelaxed(t));
        return null;
      } catch (e) {
        return (e as Error).message;
      }
    });
    setTexts(next);
    setErrors(errs);
    onChange(parsed);
  };

  return (
    <div className="elements-editor">
      <span className="flex items-center gap-1.5 text-xs font-normal text-base-content/60">{label}</span>
      {texts.length === 0 && <p className="text-xs text-base-content/60">Sin elementos: la fuente no emite nada en la prueba.</p>}
      {texts.map((t, i) => (
        <div className="element-row" key={i}>
          <span className="text-xs text-base-content/60 font-mono">{i + 1}</span>
          <Textarea
            className={`font-mono ${errors[i] ? 'border-error' : ''}`}
            rows={2}
            spellCheck={false}
            aria-label={`Elemento ${i + 1}`}
            value={t}
            onChange={(e) => emit(texts.map((x, j) => (j === i ? e.target.value : x)))}
          />
          <Button variant="ghost" size="xs" className="text-error hover:text-error" type="button" onClick={() => emit(texts.filter((_, j) => j !== i))}>quitar</Button>
          {errors[i] && <span className="my-0.5 text-xs text-error">JSON no válido: {errors[i]}</span>}
        </div>
      ))}
      <Button variant="ghost" size="xs" type="button" onClick={() => emit([...texts, '{}'])}>+ elemento</Button>
    </div>
  );
}
