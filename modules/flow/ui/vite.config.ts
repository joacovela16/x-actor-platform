import path from 'node:path';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { mockApi } from './mock/server.ts';

// base './' para que dist/ funcione servido desde el classpath del servidor.
// El mock de la API solo existe en `npm run dev`; el build no lo incluye.
export default defineConfig({
  base: './',
  plugins: [react(), tailwindcss(), mockApi()],
  resolve: {
    alias: { '@': path.resolve(import.meta.dirname, 'src') },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
});
