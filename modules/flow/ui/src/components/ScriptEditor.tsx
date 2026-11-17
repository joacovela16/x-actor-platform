import { useContext, useEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import { FileCode2, Maximize2, Minimize2, Pencil, Plus, RotateCcw, X } from 'lucide-react';
import { ScriptEnvContext, type ScriptFile } from './scriptFiles';
import { monaco, setScalaEnv } from '@/lib/monaco';
import { jsDeclarations, scalaCompletions } from '@/lib/scriptTypes';
import { cn } from '@/lib/utils';

interface Props {
  language: 'javascript' | 'scala';
  code: string;
  onCode: (code: string) => void;
  /** Sin `onFiles` no hay pestañas: solo el código. */
  files?: ScriptFile[];
  onFiles?: (files: ScriptFile[]) => void;
  /** Código de ejemplo: «Plantilla» lo pone en el código principal. */
  template?: string;
  rows?: number;
  label?: string;
}

const EXT: Record<Props['language'], string> = { javascript: 'js', scala: 'scala' };
const NAME = /^[A-Za-z_][A-Za-z0-9_-]*$/;

/** El tema de Monaco que va con el de daisyUI (`data-theme` en <html>). */
function useMonacoTheme(): string {
  const read = () => (document.documentElement.dataset.theme === 'flow-dark' ? 'vs-dark' : 'vs');
  const [theme, setTheme] = useState(read);
  useEffect(() => {
    const obs = new MutationObserver(() => setTheme(read()));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => obs.disconnect();
  }, []);
  return theme;
}

let instances = 0;

/**
 * Editor de los nodos de código: Monaco con una pestaña para el código y otra por archivo (crear, renombrar y borrar). En
 * JavaScript sugiere y comprueba con los tipos del estado del flujo; en Scala resalta y sugiere el API, las rutas del estado y lo
 * que definen los otros archivos.
 */
export default function ScriptEditor({ language, code, onCode, files = [], onFiles, template, rows = 16, label }: Props) {
  const env = useContext(ScriptEnvContext);
  const ext = EXT[language];
  const main = `main.${ext}`;
  // Cada editor tiene sus modelos bajo su prefijo; al cerrarse los borra para que no se mezclen con los de otro nodo.
  const [instance] = useState(() => ++instances);
  const prefix = `inmemory://script-${instance}/${env?.nodeId ?? 'node'}/`;
  const [active, setActive] = useState(main);
  const [editing, setEditing] = useState<{ from: string | null; text: string; error?: string } | null>(null);
  const [max, setMax] = useState(false);
  const theme = useMonacoTheme();
  const editorRef = useRef<Parameters<OnMount>[0] | null>(null);

  const all: ScriptFile[] = useMemo(() => [{ name: main, code }, ...files], [main, code, files]);
  const current = all.find((f) => f.name === active) ?? all[0];

  // Un modelo por archivo, también los que no se ven: los tipos y nombres de unos se usan en otros.
  useEffect(() => {
    for (const f of all) {
      const uri = monaco.Uri.parse(prefix + f.name);
      const m = monaco.editor.getModel(uri);
      if (!m) monaco.editor.createModel(f.code, language, uri);
      else if (m.getValue() !== f.code && f.name !== current.name) m.setValue(f.code);
    }
    for (const m of monaco.editor.getModels()) {
      const uri = m.uri.toString();
      if (uri.startsWith(prefix) && !all.some((f) => prefix + f.name === uri)) m.dispose();
    }
  }, [all, prefix, language, current.name]);

  useEffect(
    () => () => {
      for (const m of monaco.editor.getModels()) if (m.uri.toString().startsWith(prefix)) m.dispose();
    },
    [prefix],
  );

  // Los tipos del flujo para este nodo. Se rehacen en cada cambio del nodo, pero solo se recargan si cambia el texto.
  const dts = env && language === 'javascript' ? jsDeclarations(env) : null;
  const scalaItems = env && language === 'scala' ? JSON.stringify(scalaCompletions(env)) : null;
  useEffect(() => {
    if (dts === null) return;
    const lib = monaco.languages.typescript.javascriptDefaults.addExtraLib(dts, `${prefix}flow.d.ts`);
    return () => lib.dispose();
  }, [dts, prefix]);
  useEffect(() => {
    if (scalaItems === null) return;
    setScalaEnv(prefix, JSON.parse(scalaItems));
    return () => setScalaEnv('', []);
  }, [scalaItems, prefix]);

  const setFile = (name: string, text: string) => {
    if (name === main) onCode(text);
    else onFiles?.(files.map((f) => (f.name === name ? { ...f, code: text } : f)));
  };

  /** Comprueba el nombre que se escribe; añade la extensión si falta. */
  const checkName = (raw: string, from: string | null): { name?: string; error?: string } => {
    const base = raw.trim().replace(new RegExp(`\\.${ext}$`), '');
    if (!base) return { error: 'escribe un nombre' };
    if (!NAME.test(base)) return { error: 'letras, dígitos, _ y -' };
    const name = `${base}.${ext}`;
    if (name === main) return { error: `${main} es el código del nodo` };
    if (name !== from && files.some((f) => f.name === name)) return { error: 'ya existe' };
    return { name };
  };

  const commitName = () => {
    if (!editing || !onFiles) return;
    const { name, error } = checkName(editing.text, editing.from);
    if (!name) return setEditing({ ...editing, error });
    if (editing.from === null) {
      onFiles([...files, { name, code: language === 'javascript' ? `// Lo que declares aquí se puede usar en ${main}.\n` : `// Lo que definas aquí se puede usar en ${main}.\n` }]);
    } else if (name !== editing.from) {
      onFiles(files.map((f) => (f.name === editing.from ? { ...f, name } : f)));
    }
    setActive(name);
    setEditing(null);
  };

  const remove = (name: string) => {
    if (!onFiles || !window.confirm(`¿Borrar ${name}?`)) return;
    onFiles(files.filter((f) => f.name !== name));
    if (active === name) setActive(main);
  };

  const onNameKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      commitName();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      setEditing(null);
    }
  };

  const resetTemplate = () => {
    if (!template) return;
    if (code.trim() !== '' && code !== template && !window.confirm(`¿Reemplazar ${main} por la plantilla?`)) return;
    setActive(main);
    onCode(template);
  };

  const nameInput = (
    <span className="flex flex-col">
      <input
        autoFocus
        className="input input-xs w-36 font-mono"
        aria-label="Nombre del archivo"
        value={editing?.text ?? ''}
        placeholder={`helpers.${ext}`}
        onChange={(e) => setEditing((s) => s && { ...s, text: e.target.value, error: undefined })}
        onKeyDown={onNameKey}
        onBlur={() => (editing?.text.trim() ? commitName() : setEditing(null))}
      />
      {editing?.error && <span className="text-[11px] text-error">{editing.error}</span>}
    </span>
  );

  const height = max ? '100%' : `${Math.max(rows, 10) * 19}px`;

  return (
    <div className={cn('script-editor', max && 'script-editor-max')}>
      <div className="script-tabs" role="tablist" aria-label={label ?? 'Archivos del script'}>
        {all.map((f) =>
          editing?.from === f.name ? (
            <span key={f.name}>{nameInput}</span>
          ) : (
            <span key={f.name} className={cn('script-tab', f.name === current.name && 'script-tab-active')}>
              <button type="button" role="tab" aria-selected={f.name === current.name} onClick={() => setActive(f.name)} onDoubleClick={() => f.name !== main && setEditing({ from: f.name, text: f.name })}>
                <FileCode2 className="size-3.5 opacity-60" />
                {f.name}
              </button>
              {f.name !== main && onFiles && (
                <>
                  <button type="button" className="script-tab-act" aria-label={`Renombrar ${f.name}`} title="Renombrar" onClick={() => setEditing({ from: f.name, text: f.name })}>
                    <Pencil className="size-3" />
                  </button>
                  <button type="button" className="script-tab-act" aria-label={`Borrar ${f.name}`} title="Borrar" onClick={() => remove(f.name)}>
                    <X className="size-3" />
                  </button>
                </>
              )}
            </span>
          ),
        )}
        {editing?.from === null && nameInput}
        {onFiles && editing === null && (
          <button type="button" className="script-tab-act" aria-label="Nuevo archivo" title="Nuevo archivo" onClick={() => setEditing({ from: null, text: '' })}>
            <Plus className="size-3.5" />
          </button>
        )}
        <span className="flex-1" />
        {template && (
          <button type="button" className="script-tab-act" title="Poner la plantilla en el código" onClick={resetTemplate}>
            <RotateCcw className="size-3.5" /> <span className="text-xs">Plantilla</span>
          </button>
        )}
        <button type="button" className="script-tab-act" aria-label={max ? 'Reducir' : 'Maximizar'} title={max ? 'Reducir (Esc)' : 'Maximizar'} onClick={() => setMax((m) => !m)}>
          {max ? <Minimize2 className="size-3.5" /> : <Maximize2 className="size-3.5" />}
        </button>
      </div>
      <div
        className="script-monaco"
        style={{ height }}
        onKeyDown={(e) => {
          if (max && e.key === 'Escape' && !editorRef.current?.hasTextFocus()) setMax(false);
        }}
      >
        <Editor
          path={prefix + current.name}
          language={language}
          value={current.code}
          theme={theme}
          onMount={(ed) => {
            editorRef.current = ed;
          }}
          onChange={(v) => setFile(current.name, v ?? '')}
          options={{
            minimap: { enabled: max },
            fontSize: 13,
            lineHeight: 19,
            tabSize: 2,
            scrollBeyondLastLine: false,
            automaticLayout: true,
            fixedOverflowWidgets: true,
            wordBasedSuggestions: language === 'scala' ? 'allDocuments' : 'off',
            quickSuggestions: { other: true, comments: false, strings: false },
            renderLineHighlight: 'line',
            ariaLabel: label ?? 'Código',
          }}
        />
      </div>
      <span className="text-xs text-base-content/60">
        {language === 'javascript' ? 'JavaScript' : 'Scala'} · Ctrl+Espacio sugiere
        {onFiles && (language === 'javascript' ? ' · los archivos se cargan antes del código' : ' · los archivos se compilan con el código')}
        {!env && ' · sin tipos del flujo'}
      </span>
    </div>
  );
}
