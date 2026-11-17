import type { FlowDef, OutputDecl } from '../api/types';
import { OutputsEditor } from './OutputsEditor';
import { FormField } from '@/components/ui/form-field';
import { Input } from '@/components/ui/input';

interface Props {
  flow: FlowDef;
  paths: string[];
  onChange: (patch: Partial<FlowDef>) => void;
}

/** Propiedades del flujo: nombre, inputs y outputs (con target en el estado propio del flujo). */
export function FlowSettings({ flow, paths, onChange }: Props) {
  return (
    <div className="props">
      <h2>Flujo</h2>
      <p className="text-xs text-base-content/60 font-mono">{flow.id} · revisión {flow.revision}</p>
      <FormField label="Nombre">
        <Input  value={flow.name} onChange={(e) => onChange({ name: e.target.value })} />
      </FormField>

      <section>
        <h3>Entradas</h3>
        <p className="text-xs text-base-content/60">Valores que el estado tiene al empezar. Un trigger o el flujo llamador los rellena.</p>
        <OutputsEditor
          decls={flow.inputs}
          onChange={(inputs: OutputDecl[]) => onChange({ inputs })}
          nameLabel="nombre de la entrada"
          paths={paths}
          emptyText="Sin entradas."
        />
      </section>

      <section>
        <h3>Salidas</h3>
        <p className="text-xs text-base-content/60">Valores del estado que este flujo expone a quien lo llama. El target es su ruta en el estado propio.</p>
        <OutputsEditor
          decls={flow.outputs}
          onChange={(outputs: OutputDecl[]) => onChange({ outputs })}
          nameLabel="nombre de la salida"
          paths={paths}
          emptyText="Sin salidas."
        />
      </section>
    </div>
  );
}
