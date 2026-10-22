/**
 * OpenClaw Zero-Code-Change SQLite PRAGMA Preload Hook (Node.js 22+ `node:sqlite`)
 *
 * Usage:
 *   Mount this file into the OpenClaw container (e.g., via ConfigMap at /etc/openclaw/sqlite-pragma-preload.cjs)
 *   and set environment variable:
 *     NODE_OPTIONS="--require /etc/openclaw/sqlite-pragma-preload.cjs"
 *     OPENCLAW_STORAGE_TIER="filestore"   # or "hyperdisk"
 */
'use strict';

try {
  const sqlite = require('node:sqlite');
  const OrigDatabaseSync = sqlite.DatabaseSync;
  const tier = (process.env.OPENCLAW_STORAGE_TIER || 'filestore').toLowerCase();

  const pragmas = tier === 'hyperdisk' || tier === 'ssd'
    ? 'PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;'
    : 'PRAGMA journal_mode=TRUNCATE; PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY; PRAGMA busy_timeout=5000;';

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
