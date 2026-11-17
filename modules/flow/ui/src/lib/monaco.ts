// Monaco empaquetado con la app (sin CDN): los workers salen de `monaco-editor` vía Vite. Este módulo solo se carga con el
// editor de scripts (import diferido), así el resto de la UI no paga su tamaño.
import * as monaco from 'monaco-editor';
import { loader } from '@monaco-editor/react';
import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker';
import tsWorker from 'monaco-editor/esm/vs/language/typescript/ts.worker?worker';
import jsonWorker from 'monaco-editor/esm/vs/language/json/json.worker?worker';
import { scalaCompletions } from './scriptTypes';

self.MonacoEnvironment = {
  getWorker(_id: string, label: string) {
    if (label === 'typescript' || label === 'javascript') return new tsWorker();
    if (label === 'json') return new jsonWorker();
    return new editorWorker();
  },
};

loader.config({ monaco });

const js = monaco.languages.typescript.javascriptDefaults;
js.setCompilerOptions({
  target: monaco.languages.typescript.ScriptTarget.ES2020,
  lib: ['es2020'],
  allowJs: true,
  checkJs: true,
  allowNonTsExtensions: true,
  noEmit: true,
});
js.setDiagnosticsOptions({
  noSemanticValidation: false,
  noSyntaxValidation: false,
  // El código del nodo es el cuerpo de una función: su `return` de primer nivel es correcto.
  diagnosticCodesToIgnore: [1108],
});
js.setEagerModelSync(true);

/** Lo que el editor de Scala sugiere ahora: lo pone el editor abierto (ver `setScalaEnv`). */
let scalaEnv: { prefix: string; items: ReturnType<typeof scalaCompletions> } = { prefix: '', items: [] };

export function setScalaEnv(prefix: string, items: ReturnType<typeof scalaCompletions>) {
  scalaEnv = { prefix, items };
}

/** Los nombres que define un archivo de Scala, para sugerirlos en los demás del mismo nodo. */
const DEFINITION = /\b(?:def|val|var|lazy val|case class|class|case object|object|trait|type|enum|given)\s+([A-Za-z_][A-Za-z0-9_]*)/g;

monaco.languages.registerCompletionItemProvider('scala', {
  triggerCharacters: ['.'],
  provideCompletionItems(model, position) {
    const word = model.getWordUntilPosition(position);
    const line = model.getLineContent(position.lineNumber).slice(0, position.column - 1);
    // `state.` también vale como inicio de una ruta: se reemplaza desde `state`.
    const dotted = /([A-Za-z_][A-Za-z0-9_.]*)$/.exec(line)?.[1] ?? word.word;
    const range = {
      startLineNumber: position.lineNumber,
      endLineNumber: position.lineNumber,
      startColumn: position.column - dotted.length,
      endColumn: position.column,
    };
    const K = monaco.languages.CompletionItemKind;
    const suggestions: monaco.languages.CompletionItem[] = scalaEnv.items.map((it) => ({
      label: it.label,
      kind: it.kind === 'path' ? K.Field : it.kind === 'fn' ? K.Function : it.kind === 'snippet' ? K.Snippet : K.Variable,
      detail: it.detail,
      documentation: it.doc,
      insertText: it.insert,
      insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
      filterText: it.label,
      sortText: it.kind === 'path' ? `0${it.label}` : `1${it.label}`,
      range,
    }));
    // Lo que definen los otros archivos del mismo nodo.
    if (scalaEnv.prefix && model.uri.toString().startsWith(scalaEnv.prefix)) {
      for (const other of monaco.editor.getModels()) {
        const uri = other.uri.toString();
        if (other === model || !uri.startsWith(scalaEnv.prefix)) continue;
        const file = uri.slice(scalaEnv.prefix.length);
        for (const m of other.getValue().matchAll(DEFINITION)) {
          suggestions.push({
            label: m[1],
            kind: K.Reference,
            detail: file,
            insertText: m[1],
            range: { ...range, startColumn: position.column - word.word.length },
          });
        }
      }
    }
    return { suggestions };
  },
});

export { monaco };
