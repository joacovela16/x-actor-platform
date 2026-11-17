import { nodeRefOptions } from './RefComplete';
import { useEffect, useId, useState } from 'react';
import { api } from '../api/client';
import type { Connection, ConfigField, ConfigSchema, JsonValue } from '../api/types';
import { CronField, ZoneField } from './ScheduleFields';
import { SubflowInputs } from './SubflowInputs';
import { AggregationsEditor, ExprField, MapSetEditor } from './ExprEditor';
import { SpecForm, type Override } from './SpecForm';
import { Input } from '@/components/ui/input';
import { ComboInput } from '@/components/ui/combo-input';

interface Props {
  kind: string;
  schema: ConfigSchema;
  params: Record<string, JsonValue>;
  onChange: (params: Record<string, JsonValue>) => void;
  /** Rutas declaradas en el flujo, para autocompletar. */
  paths: string[];
  /** En un nodo de stream: los campos del elemento que entra por cada entrada. */
  elementPaths?: Record<string, string[]>;
}

/** Nodos de stream cuyo `where` es una condición sobre el elemento. */
const CONDITION_KINDS = ['stream.filter', 'stream.branch', 'stream.takeWhile', 'stream.dropWhile'];

/** Tipos de conexión que usa `queue.publish` (ver nodes-queue, QueueKinds). */
const QUEUE_CONNECTION_KINDS = ['kafka', 'activemq', 'redis'];

/** Campos que son una ruta en el estado: se autocompletan con las rutas declaradas del flujo. */
export function isStatePathField(kind: string, field: ConfigField): boolean {
  // En un nodo de stream, `path` es una ruta dentro de cada elemento; solo la fuente de lista lee el estado.
  if (kind.startsWith('stream.')) return kind === 'stream.source.list' && field.name === 'path';
  if (field.name.toLowerCase().includes('path')) return true;
  if (kind === 'queue.publish' && (field.name === 'payload' || field.name === 'key')) return true;
  return /path in the state/i.test(field.description ?? '');
}

/** Campos de un nodo de stream que son una ruta dentro del elemento (no del estado): se sugieren los campos del elemento. */
export function isElementPathField(kind: string, field: ConfigField): boolean {
  if (!kind.startsWith('stream.') || kind === 'stream.source.list' || field.type.type !== 'str') return false;
  return ['path', 'key', 'leftKey', 'rightKey', 'into'].includes(field.name);
}

/** Conexiones guardadas, una sola petición por montaje del formulario. */
function useConnections(enabled: boolean): Connection[] {
  const [list, setList] = useState<Connection[]>([]);
  useEffect(() => {
    if (!enabled) return;
    let alive = true;
    api.listConnections().then((c) => alive && setList(c)).catch(() => alive && setList([]));
    return () => {
      alive = false;
    };
  }, [enabled]);
  return list;
}

/**
 * Formulario de configuración de un nodo. Todo se dibuja con `SpecForm` desde el `ConfigSchema` (su `ui`); aquí solo
 * quedan los widgets de composición propios de algunos nodos (cron, expresiones, mapas de stream, subflujos) y la
 * lista de conexiones de `queue.publish`.
 */
export function ConfigForm({ kind, schema, params, onChange, paths, elementPaths = {} }: Props) {
  const connections = useConnections(kind === 'queue.publish');
  const listId = useId();
  const overrides: Override = (field, { value, set, values }) => {
    if (isElementPathField(kind, field)) {
      const side = field.name === 'leftKey' ? 'left' : field.name === 'rightKey' ? 'right' : 'in';
      return (
        <ComboInput
          className="font-mono"
          aria-label={field.name}
          value={typeof value === 'string' ? value : ''}
          options={elementPaths[side] ?? []}
          placeholder={(elementPaths[side] ?? []).length ? 'elige un campo del elemento o escribe…' : 'campo del elemento, p. ej. items'}
          onChange={(t) => set(t === '' ? undefined : t)}
        />
      );
    }
    if (kind === 'core.cron' && field.name === 'expression') {
      return (
        <CronField
          value={typeof value === 'string' ? value : ''}
          timezone={typeof values.timezone === 'string' ? values.timezone : 'UTC'}
          onChange={(text) => set(text === '' ? undefined : text)}
        />
      );
    }
    if (kind === 'core.cron' && field.name === 'timezone') {
      return (
        <ZoneField
          value={typeof value === 'string' ? value : ''}
          listId={`${listId}-zones`}
          onChange={(z) => set(z === '' ? undefined : z)}
        />
      );
    }
    if (CONDITION_KINDS.includes(kind) && field.name === 'where') return <ExprField value={value} onChange={set} label="Condición (where)" preview="filter" />;
    if (kind === 'stream.scan' && field.name === 'step') return <ExprField value={value} onChange={set} label="Paso (con acc y elem)" preview="value" sampleText={'{"acc": 0, "elem": {}}'} />;
    if (kind === 'stream.map' && field.name === 'set') return <MapSetEditor value={value ?? {}} onChange={(v) => set(v)} />;
    if (kind === 'stream.map' && field.name === 'replace') return <ExprField value={value} onChange={set} label="Reemplazar el elemento por" preview="value" />;
    if (kind === 'stream.aggregate' && field.name === 'aggregations') return <AggregationsEditor value={value} onChange={set} />;
    if (kind === 'core.subflow' && field.name === 'inputs') {
      return (
        <SubflowInputs
          flowId={typeof values.flowId === 'string' ? values.flowId : undefined}
          paths={paths}
          value={(value && typeof value === 'object' && !Array.isArray(value) ? value : {}) as Record<string, string>}
          onChange={(v) => set(v)}
        />
      );
    }
    if (kind === 'queue.publish' && field.name === 'connection') {
      const text = typeof value === 'string' ? value : '';
      const known = connections.some((c) => c.name === text);
      return (
        <>
          <Input
            
            aria-label={field.name}
            list={listId}
            value={text}
            placeholder="nombre de la conexión"
            onChange={(e) => set(e.target.value === '' ? undefined : e.target.value)}
          />
          <datalist id={listId}>
            {connections
              .filter((c) => QUEUE_CONNECTION_KINDS.includes(c.kind))
              .map((c) => <option key={c.name} value={c.name}>{c.kind}</option>)}
          </datalist>
          {text !== '' && !known && connections.length > 0 && <span className="my-0.5 text-xs text-error">no hay una conexión con ese nombre</span>}
        </>
      );
    }
    return null;
  };

  return (
    <SpecForm
      fields={schema.fields}
      values={params}
      onChange={onChange}
      paths={paths}
      refs={nodeRefOptions(paths)}
      isPathField={(f) => isStatePathField(kind, f)}
      overrides={overrides}
      emptyText="Este nodo no tiene configuración."
    />
  );
}
