import type { JsonValue } from '../api/types';

/** Lee JSON sin exigir comillas: lo que escribe una persona, no una máquina.
 *
 *  - claves sin comillas: `{nombre: "Ana"}`;
 *  - textos sin comillas: `{nombre: Ana Pérez, ciudad: ${state.ciudad}}` (lo que no es número, `true`, `false` ni `null` es texto);
 *  - comillas simples, comas al final, comas opcionales entre líneas y comentarios `//` y `/* *\/`.
 *
 *  JSON válido se lee igual que con `JSON.parse`. Lanza un `Error` con la posición cuando no se puede leer.
 */
export function parseRelaxed(text: string): JsonValue {
  try {
    return JSON.parse(text) as JsonValue;
  } catch {
    return new Reader(text).document();
  }
}

/** `parseRelaxed`, o el mensaje de por qué no se pudo leer. */
export function tryRelaxed(text: string): { ok: true; value: JsonValue } | { ok: false; error: string } {
  try {
    return { ok: true, value: parseRelaxed(text) };
  } catch (e) {
    return { ok: false, error: (e as Error).message };
  }
}

const NUMBER = /^-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?$/;

class Reader {
  private i = 0;
  constructor(private readonly s: string) {}

  document(): JsonValue {
    this.space();
    const v = this.value(true);
    this.space();
    if (this.i < this.s.length) this.fail(`sobra '${this.s.slice(this.i, this.i + 10)}'`);
    return v;
  }

  private fail(why: string): never {
    const before = this.s.slice(0, this.i);
    const line = before.split('\n').length;
    const col = this.i - before.lastIndexOf('\n');
    throw new Error(`${why} (línea ${line}, columna ${col})`);
  }

  private space() {
    for (;;) {
      const c = this.s[this.i];
      if (c === ' ' || c === '\t' || c === '\n' || c === '\r') this.i++;
      else if (this.s.startsWith('//', this.i)) {
        const end = this.s.indexOf('\n', this.i);
        this.i = end < 0 ? this.s.length : end;
      } else if (this.s.startsWith('/*', this.i)) {
        const end = this.s.indexOf('*/', this.i + 2);
        if (end < 0) this.fail('comentario sin cerrar');
        this.i = end + 2;
      } else return;
    }
  }

  /** `top`: el documento entero, donde un texto suelto llega hasta el final (puede tener comas). */
  private value(top = false): JsonValue {
    const c = this.s[this.i];
    if (c === undefined) this.fail('falta un valor');
    if (c === '{') return this.object();
    if (c === '[') return this.array();
    if (c === '"' || c === "'") return this.quoted();
    return this.bare(top ? '' : ',}]\n');
  }

  private object(): Record<string, JsonValue> {
    this.i++;
    const out: Record<string, JsonValue> = {};
    for (;;) {
      this.space();
      if (this.s[this.i] === '}') {
        this.i++;
        return out;
      }
      if (this.i >= this.s.length) this.fail("falta '}'");
      const c = this.s[this.i];
      const key = c === '"' || c === "'" ? this.quoted() : this.word(':');
      if (key === '') this.fail('falta el nombre de un campo');
      this.space();
      if (this.s[this.i] !== ':') this.fail(`falta ':' después de '${key}'`);
      this.i++;
      this.space();
      out[key] = this.value();
      this.space();
      if (this.s[this.i] === ',') this.i++;
    }
  }

  private array(): JsonValue[] {
    this.i++;
    const out: JsonValue[] = [];
    for (;;) {
      this.space();
      if (this.s[this.i] === ']') {
        this.i++;
        return out;
      }
      if (this.i >= this.s.length) this.fail("falta ']'");
      out.push(this.value());
      this.space();
      if (this.s[this.i] === ',') this.i++;
    }
  }

  private quoted(): string {
    const q = this.s[this.i++];
    let out = '';
    for (;;) {
      const c = this.s[this.i++];
      if (c === undefined) this.fail('texto sin cerrar');
      if (c === q) return out;
      if (c === '\\') {
        const e = this.s[this.i++];
        const map: Record<string, string> = { n: '\n', t: '\t', r: '\r', b: '\b', f: '\f', '/': '/', '\\': '\\', '"': '"', "'": "'" };
        if (e === 'u') {
          out += String.fromCharCode(parseInt(this.s.slice(this.i, this.i + 4), 16));
          this.i += 4;
        } else out += map[e] ?? e;
      } else out += c;
    }
  }

  /** Texto sin comillas hasta uno de `stops`; un `${...}` va entero aunque tenga esos caracteres. */
  private word(stops: string): string {
    const start = this.i;
    while (this.i < this.s.length) {
      if (this.s.startsWith('${', this.i)) {
        const end = this.s.indexOf('}', this.i);
        this.i = end < 0 ? this.s.length : end + 1;
        continue;
      }
      const c = this.s[this.i];
      // `//` es un comentario al principio o después de un espacio; en `https://...` es parte del texto.
      const comment = this.s.startsWith('//', this.i) && (this.i === start || /\s/.test(this.s[this.i - 1]));
      if (stops.includes(c) || comment) break;
      this.i++;
    }
    return this.s.slice(start, this.i).trim();
  }

  private bare(stops: string): JsonValue {
    const w = this.word(stops);
    if (w === '') this.fail('falta un valor');
    if (w === 'true') return true;
    if (w === 'false') return false;
    if (w === 'null') return null;
    if (NUMBER.test(w)) return Number(w);
    return w;
  }
}
