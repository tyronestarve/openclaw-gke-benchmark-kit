/**
 * OpenClaw Zero-Code-Change SQLite PRAGMA Preload Hook (Node.js 22+ `node:sqlite`)
 *
 * Usage:
 *   Mount this file into the OpenClaw container (e.g., via ConfigMap at /etc/openclaw/sqlite-pragma-preload.cjs)
 *   and set environment variable on the Pod/SandboxTemplate:
 *     NODE_OPTIONS="--require /etc/openclaw/sqlite-pragma-preload.cjs"
 *
 *   Default Recommended Mode:
 *     WAL + EXCLUSIVE + NORMAL (Zero `-shm` file on NFS; `wal-index` kept in process heap memory)
 *     IMPORTANT: `PRAGMA locking_mode=EXCLUSIVE;` MUST be executed BEFORE `PRAGMA journal_mode=WAL;`
 *     so SQLite enters WAL mode with exclusive locking and never creates a `-shm` shared-memory file on NFS.
 */
'use strict';

try {
  const sqlite = require('node:sqlite');
  const OrigDatabaseSync = sqlite.DatabaseSync;
  const mode = (process.env.OPENCLAW_SQLITE_MODE || process.env.OPENCLAW_STORAGE_TIER || 'wal_exclusive').toLowerCase();

  const pragmas = (mode === 'mode2' || mode === '2' || mode === 'truncate')
    ? 'PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=TRUNCATE; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-65536; PRAGMA busy_timeout=5000;'
    : 'PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-65536; PRAGMA busy_timeout=5000;';

  function PatchedDatabaseSync(...args) {
    const db = new OrigDatabaseSync(...args);
    try {
      // Only apply file-locking pragmas to file-backed databases (skip ':memory:')
      const location = typeof args[0] === 'string' ? args[0] : '';
      if (location && location !== ':memory:') {
        db.exec(pragmas);
      }
    } catch (err) {
      // Ignore if opened read-only
    }
    return db;
  }

  Object.setPrototypeOf(PatchedDatabaseSync, OrigDatabaseSync);
  PatchedDatabaseSync.prototype = OrigDatabaseSync.prototype;
  sqlite.DatabaseSync = PatchedDatabaseSync;

  // Also patch Module._load so both `require('node:sqlite')` and ESM `import { DatabaseSync } from 'node:sqlite'`
  // (when loaded after CJS preload) receive the patched constructor:
  const Module = require('node:module');
  const origLoad = Module._load;
  Module._load = function(request, parent, isMain) {
    const exp = origLoad.apply(this, arguments);
    if (request === 'node:sqlite' || request === 'sqlite') {
      exp.DatabaseSync = PatchedDatabaseSync;
    }
    return exp;
  };
  if (typeof module.registerHooks === 'function' || typeof Module.syncBuiltinESMExports === 'function') {
    try { Module.syncBuiltinESMExports(); } catch (_) {}
  }
} catch (_) {
  // Fallback silently if node:sqlite is not available in older Node versions
}
