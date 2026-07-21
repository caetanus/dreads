import { defineConfig } from 'vite'
import preact from '@preact/preset-vite'
import { viteSingleFile } from 'vite-plugin-singlefile'

// Build the whole dashboard into ONE self-contained index.html (JS + CSS inlined),
// so dreads embeds it with a single compile-time string import and serves it with
// no asset routing. relative base => works whatever path it's served from.
export default defineConfig({
  base: './',
  plugins: [preact(), viteSingleFile()],
  build: {
    target: 'es2020',
    cssCodeSplit: false,
    assetsInlineLimit: 100000000,
    chunkSizeWarningLimit: 4096,
    reportCompressedSize: false,
  },
})
