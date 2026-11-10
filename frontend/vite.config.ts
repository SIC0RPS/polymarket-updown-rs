import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { execSync } from 'node:child_process'

const FRONTEND_ROOT = path.dirname(fileURLToPath(import.meta.url))

function resolveCommitSha(): string {
  try {
    return execSync('git rev-parse --short HEAD', { stdio: ['ignore', 'pipe', 'ignore'] })
      .toString()
      .trim()
  } catch {
    return 'unknown'
  }
}

export default defineConfig({
  build: { chunkSizeWarningLimit: 1000 },
  define: {
    __APP_COMMIT_SHA__: JSON.stringify(resolveCommitSha()),
  },
  plugins: [react(), tailwindcss()],
  server: {
    host: '127.0.0.1',
    port: 5188,
    proxy: {
      '/api': 'http://127.0.0.1:3000',
      '/kill-level': 'http://127.0.0.1:3000',
    },
    fs: {
      allow: [FRONTEND_ROOT, path.resolve(FRONTEND_ROOT, '..')],
    },
  },
})
