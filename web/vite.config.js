import { defineConfig } from 'vite';

// Needed for bundling module Workers that may include dependencies.
// Ensures worker output supports multi-chunk builds.
export default defineConfig({
  worker: {
    format: 'es',
  },
});
