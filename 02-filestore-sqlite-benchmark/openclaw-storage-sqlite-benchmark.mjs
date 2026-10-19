#!/usr/bin/env node
/**
 * bench_suite.mjs
 * OpenClaw on GKE Agent Sandbox: Dual-Tier Storage & SQLite Empirical Benchmark Suite
 *
 * Uses built-in Node.js 22+ modules ONLY:
 *   - node:fs, node:path, node:sqlite (DatabaseSync), process.hrtime.bigint()
 *
 * Usage:
 *   node bench_suite.mjs --pd-dir=/mnt/pd --nfs-dir=/mnt/filestore --empty-dir=/mnt/emptydir
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

// Parse CLI arguments
const args = {};
for (const arg of process.argv.slice(2)) {
  if (arg.startsWith('--')) {
    const eqIdx = arg.indexOf('=');
    if (eqIdx !== -1) {
      args[arg.slice(2, eqIdx)] = arg.slice(eqIdx + 1);
    } else {
      args[arg.slice(2)] = true;
    }
  }
}

const PD_DIR = args['pd-dir'] || '/mnt/pd';
const NFS_DIR = args['nfs-dir'] || '/mnt/filestore';
const EMPTY_DIR = args['empty-dir'] || '/mnt/emptydir';
const FILE_COUNT = Number(args['file-count'] || 1000);
const ROUNDS = Number(args['rounds'] || 5);
const TX_COUNT = Number(args['tx-count'] || 1000);
const MIGRATE_SESSIONS = Number(args['migrate-sessions'] || 2000);

function nowNs() {
  return process.hrtime.bigint();
}

function nsToMs(ns) {
  return Number(ns) / 1e6;
}

function fmtMs(ms, digits = 2) {
  return ms.toFixed(digits);
}

function percentile(sortedArr, p) {
  if (sortedArr.length === 0) return 0;
  const idx = Math.min(sortedArr.length - 1, Math.max(0, Math.ceil((p / 100) * sortedArr.length) - 1));
  return sortedArr[idx];
}

function ensureCleanDir(dirPath) {
  fs.rmSync(dirPath, { recursive: true, force: true });
  fs.mkdirSync(dirPath, { recursive: true });
}

// ============================================================================
// MODULE 1: Customer's exact `node-fs-benchmark` (5 rounds x 1,000 x 1KB files)
// ============================================================================
function runFsBenchOnTarget(label, baseDir, count, rounds) {
  const workDir = path.join(baseDir, '.openclaw-fs-bench');
  ensureCleanDir(workDir);
  const payload1KB = Buffer.alloc(1024, 'a');

  const roundStats = [];

  for (let r = 1; r <= rounds; r++) {
    const roundDir = path.join(workDir, `round_${r}`);
    fs.mkdirSync(roundDir, { recursive: true });

    // 1. writeFileSync (1,000 x 1KB)
    const tWrite0 = nowNs();
    for (let i = 0; i < count; i++) {
      fs.writeFileSync(path.join(roundDir, `file_${i}.dat`), payload1KB);
    }
    const writeMs = nsToMs(nowNs() - tWrite0);

    // 2. readFileSync (1,000 x 1KB)
    const tRead0 = nowNs();
    for (let i = 0; i < count; i++) {
      fs.readFileSync(path.join(roundDir, `file_${i}.dat`));
    }
    const readMs = nsToMs(nowNs() - tRead0);

    // 3. statSync (1,000 files)
    const tStat0 = nowNs();
    for (let i = 0; i < count; i++) {
      fs.statSync(path.join(roundDir, `file_${i}.dat`));
    }
    const statMs = nsToMs(nowNs() - tStat0);

    // 4. readdirSync (1,000 files directory)
    const tReaddir0 = nowNs();
    const entries = fs.readdirSync(roundDir);
    const readdirMs = nsToMs(nowNs() - tReaddir0);
    if (entries.length !== count) {
      throw new Error(`readdirSync count mismatch: expected ${count}, got ${entries.length}`);
    }

    // 5. rmSync (1,000 files)
    const tRm0 = nowNs();
    for (let i = 0; i < count; i++) {
      fs.rmSync(path.join(roundDir, `file_${i}.dat`));
    }
    const rmMs = nsToMs(nowNs() - tRm0);

    fs.rmdirSync(roundDir);
    const totalMs = writeMs + readMs + statMs + readdirMs + rmMs;
    roundStats.push({ round: r, writeMs, readMs, statMs, readdirMs, rmMs, totalMs });
  }

  fs.rmSync(workDir, { recursive: true, force: true });

  const avg = (key) => roundStats.reduce((acc, s) => acc + s[key], 0) / rounds;
  return {
    label,
    baseDir,
    count,
    rounds,
    roundStats,
    averages: {
      writeMs: avg('writeMs'),
      readMs: avg('readMs'),
      statMs: avg('statMs'),
      readdirMs: avg('readdirMs'),
      rmMs: avg('rmMs'),
      totalMs: avg('totalMs'),
    },
  };
}

function runModule1() {
  console.log('\n================================================================================');
  console.log(`MODULE 1: node-fs-benchmark (${ROUNDS} Rounds × ${FILE_COUNT} × 1KB Files)`);
  console.log('================================================================================');

  const pdRes = runFsBenchOnTarget('Pro Tier (pd-balanced Block SSD)', PD_DIR, FILE_COUNT, ROUNDS);
  const nfsRes = runFsBenchOnTarget('Standard Tier (Filestore Zonal SSD NFSv3)', NFS_DIR, FILE_COUNT, ROUNDS);

  for (const res of [pdRes, nfsRes]) {
    console.log(`\n### [${res.label}] Path: ${res.baseDir}`);
    console.log('| Round | writeFileSync (ms) | readFileSync (ms) | statSync (ms) | readdirSync (ms) | rmSync (ms) | Total (ms) |');
    console.log('| :--- | ---: | ---: | ---: | ---: | ---: | ---: |');
    for (const s of res.roundStats) {
      console.log(
        `| Round ${s.round} | ${fmtMs(s.writeMs)} | ${fmtMs(s.readMs)} | ${fmtMs(s.statMs)} | ${fmtMs(s.readdirMs)} | ${fmtMs(s.rmMs)} | ${fmtMs(s.totalMs)} |`
      );
    }
    const a = res.averages;
    console.log(
      `| **Average** | **${fmtMs(a.writeMs)}** | **${fmtMs(a.readMs)}** | **${fmtMs(a.statMs)}** | **${fmtMs(a.readdirMs)}** | **${fmtMs(a.rmMs)}** | **${fmtMs(a.totalMs)}** |`
    );
  }

  console.log('\n### Module 1 Summary Comparison (Average across 5 Rounds of 1,000 × 1KB files)');
  console.log('| Operation (1,000 ops) | Pro Tier (`pd-balanced` SSD) | Standard Tier (`Filestore Zonal SSD` NFSv3) | PD Speedup Factor |');
  console.log('| :--- | ---: | ---: | ---: |');
  const ops = [
    ['writeFileSync', 'writeMs'],
    ['readFileSync', 'readMs'],
    ['statSync', 'statMs'],
    ['readdirSync', 'readdirMs'],
    ['rmSync', 'rmMs'],
    ['**Total (All 5 Ops)**', 'totalMs'],
  ];
  for (const [name, key] of ops) {
    const pVal = pdRes.averages[key];
    const nVal = nfsRes.averages[key];
    const ratio = pVal > 0 ? (nVal / pVal).toFixed(1) + 'x' : 'N/A';
    console.log(`| ${name} | ${fmtMs(pVal)} ms | ${fmtMs(nVal)} ms | ${ratio} |`);
  }

  return { pd: pdRes, nfs: nfsRes };
}

// ============================================================================
// MODULE 2: `node:sqlite` 1,000 Synchronous Write Transactions & Multi-Account Concurrency
// ============================================================================
function runSqliteTxBench(configName, baseDir, pragmas, txCount) {
  const workDir = path.join(baseDir, `.openclaw-sqlite-${ configName.replace(/[^a-zA-Z0-9]/g, '_') }`);
  ensureCleanDir(workDir);
  const dbPath = path.join(workDir, 'openclaw.sqlite');

  const db = new DatabaseSync(dbPath);
  for (const p of pragmas) {
    db.exec(p);
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS session_turns (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      turn_idx INTEGER NOT NULL,
      payload TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS agent_state (
      account_id TEXT PRIMARY KEY,
      last_turn INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      state_blob TEXT NOT NULL
    );
    INSERT OR REPLACE INTO agent_state (account_id, last_turn, updated_at, state_blob)
    VALUES ('feishu_acct_01', 0, 0, '{"status":"init"}');
  `);

  const insertStmt = db.prepare(
    'INSERT INTO session_turns (account_id, turn_idx, payload, created_at) VALUES (?, ?, ?, ?)'
  );
  const updateStmt = db.prepare(
    'UPDATE agent_state SET last_turn = ?, updated_at = ?, state_blob = ? WHERE account_id = ?'
  );

  const payload = JSON.stringify({
    msg: 'OpenClaw user turn message payload for SQLite benchmark verification',
    tokens: 256,
    model: 'gemini-2.5-pro',
    padding: 'x'.repeat(400),
  });

  const latenciesMs = [];
  const tStart = nowNs();

  for (let i = 1; i <= txCount; i++) {
    const t0 = nowNs();
    db.exec('BEGIN IMMEDIATE;');
    insertStmt.run('feishu_acct_01', i, payload, Date.now());
    updateStmt.run(i, Date.now(), `{"turn":${i},"active":true}`, 'feishu_acct_01');
    db.exec('COMMIT;');
    const dtMs = nsToMs(nowNs() - t0);
    latenciesMs.push(dtMs);
  }

  const totalMs = nsToMs(nowNs() - tStart);
  db.close();
  fs.rmSync(workDir, { recursive: true, force: true });

  const sorted = [...latenciesMs].sort((a, b) => a - b);
  const p50 = percentile(sorted, 50);
  const p95 = percentile(sorted, 95);
  const p99 = percentile(sorted, 99);
  const avgMs = totalMs / txCount;
  const tps = (txCount / (totalMs / 1000));

  return {
    configName,
    baseDir,
    pragmas: pragmas.join(' '),
    txCount,
    p50Ms: p50,
    p95Ms: p95,
    p99Ms: p99,
    avgMs,
    totalMs,
    tps,
  };
}

function runFeishuTurnSimulation(configName, baseDir, pragmas, accountsCount) {
  const workDir = path.join(baseDir, `.openclaw-feishu-${configName.replace(/[^a-zA-Z0-9]/g, '_')}-${accountsCount}`);
  ensureCleanDir(workDir);
  const locksDir = path.join(workDir, 'session-locks');
  const sessionsDir = path.join(workDir, 'sessions');
  fs.mkdirSync(locksDir, { recursive: true });
  fs.mkdirSync(sessionsDir, { recursive: true });

  const dbPath = path.join(workDir, 'openclaw.sqlite');
  const db = new DatabaseSync(dbPath);
  for (const p of pragmas) {
    db.exec(p);
  }

  db.exec(`
    CREATE TABLE IF NOT EXISTS session_turns (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      turn_idx INTEGER NOT NULL,
      payload TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS agent_state (
      account_id TEXT PRIMARY KEY,
      last_turn INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      state_blob TEXT NOT NULL
    );
  `);

  for (let a = 1; a <= accountsCount; a++) {
    db.exec(`INSERT OR REPLACE INTO agent_state VALUES ('acct_${a}', 0, 0, '{"ready":true}');`);
  }

  const insertStmt = db.prepare(
    'INSERT INTO session_turns (account_id, turn_idx, payload, created_at) VALUES (?, ?, ?, ?)'
  );
  const updateStmt = db.prepare(
    'UPDATE agent_state SET last_turn = ?, updated_at = ?, state_blob = ? WHERE account_id = ?'
  );

  const turnPayload = '{"role":"assistant","content":"Feishu turn response payload ' + 'y'.repeat(300) + '"}\n';

  // Measure single turn total synchronous event-loop blocking time across all active Feishu accounts
  // Each account performs: 30 SQLite write txs + 5 lock file creates/unlinks + 10 jsonl appends per turn
  const t0 = nowNs();
  for (let a = 1; a <= accountsCount; a++) {
    const acctId = `acct_${a}`;
    const jsonlPath = path.join(sessionsDir, `${acctId}.jsonl`);

    // 5 lock file creates/unlinks + readdir scan
    for (let l = 0; l < 5; l++) {
      const lockFile = path.join(locksDir, `${acctId}_${l}.lock`);
      fs.writeFileSync(lockFile, `${process.pid}:${Date.now()}`);
      fs.readdirSync(locksDir);
      fs.rmSync(lockFile);
    }

    // 10 jsonl appends
    for (let j = 0; j < 10; j++) {
      fs.appendFileSync(jsonlPath, turnPayload);
    }

    // 30 SQLite write transactions
    for (let t = 1; t <= 30; t++) {
      db.exec('BEGIN IMMEDIATE;');
      insertStmt.run(acctId, t, turnPayload, Date.now());
      updateStmt.run(t, Date.now(), `{"turn":${t}}`, acctId);
      db.exec('COMMIT;');
    }
  }
  const blockingMs = nsToMs(nowNs() - t0);

  db.close();
  fs.rmSync(workDir, { recursive: true, force: true });

  return {
    configName,
    accountsCount,
    totalTxs: accountsCount * 30,
    totalLocks: accountsCount * 5,
    totalJsonlAppends: accountsCount * 10,
    blockingMs,
  };
}

function runModule2() {
  console.log('\n================================================================================');
  console.log(`MODULE 2: node:sqlite ${TX_COUNT} Synchronous Write Transactions & Multi-Account Concurrency`);
  console.log('================================================================================');

  const configs = [
    {
      id: 'Config A (Pro Tier - PD Block Storage)',
      dir: PD_DIR,
      pragmas: ['PRAGMA journal_mode=WAL;', 'PRAGMA synchronous=NORMAL;'],
    },
    {
      id: 'Config B (Standard Tier Untuned - Filestore NFSv3)',
      dir: NFS_DIR,
      pragmas: ['PRAGMA journal_mode=DELETE;', 'PRAGMA locking_mode=NORMAL;', 'PRAGMA synchronous=FULL;'],
    },
    {
      id: 'Config C (Standard Tier Tuned - Filestore NFSv3)',
      dir: NFS_DIR,
      pragmas: ['PRAGMA journal_mode=TRUNCATE;', 'PRAGMA locking_mode=EXCLUSIVE;', 'PRAGMA synchronous=NORMAL;'],
    },
    {
      id: 'Config D (Local SSD Accelerator - emptyDir)',
      dir: EMPTY_DIR,
      pragmas: ['PRAGMA journal_mode=WAL;', 'PRAGMA synchronous=NORMAL;'],
    },
  ];

  const txResults = [];
  for (const c of configs) {
    const res = runSqliteTxBench(c.id, c.dir, c.pragmas, TX_COUNT);
    txResults.push(res);
  }

  console.log(`\n### 2.1 SQLite ${TX_COUNT} Synchronous Write Transactions Latency (P50 / P95 / P99) & TPS`);
  console.log('| Configuration | PRAGMA Settings | P50 (ms) | P95 (ms) | P99 (ms) | Avg (ms) | Total Time (ms) | Throughput (TPS) |');
  console.log('| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const r of txResults) {
    console.log(
      `| **${r.configName}** | \`${r.pragmas}\` | ${fmtMs(r.p50Ms, 3)} | ${fmtMs(r.p95Ms, 3)} | ${fmtMs(r.p99Ms, 3)} | ${fmtMs(r.avgMs, 3)} | ${fmtMs(r.totalMs, 1)} | **${r.tps.toFixed(1)}** |`
    );
  }

  // Multi-account simulation (1, 5, 17 Feishu accounts across Config A, B, C)
  const accountTiers = [1, 5, 17];
  const concurrencyResults = [];
  for (const c of configs.slice(0, 3)) {
    const row = { configName: c.id, byAccount: {} };
    for (const acct of accountTiers) {
      const sim = runFeishuTurnSimulation(c.id, c.dir, c.pragmas, acct);
      row.byAccount[acct] = sim.blockingMs;
    }
    concurrencyResults.push(row);
  }

  console.log('\n### 2.2 Multi-Account Feishu Turn Concurrency Simulation (Event-Loop Blocking Time per Turn)');
  console.log('> Per account workload: 30 SQLite write txs + 5 lock file create/readdir/unlink cycles + 10 `.jsonl` appends');
  console.log('| Configuration | 1 Account (30 txs) | 5 Accounts (150 txs) | 17 Accounts (510 txs) |');
  console.log('| :--- | ---: | ---: | ---: |');
  for (const r of concurrencyResults) {
    console.log(
      `| **${r.configName}** | ${fmtMs(r.byAccount[1], 1)} ms | ${fmtMs(r.byAccount[5], 1)} ms | **${fmtMs(r.byAccount[17], 1)} ms** |`
    );
  }

  return { txResults, concurrencyResults };
}

// ============================================================================
// MODULE 3: `openclaw doctor --fix` Session Migration Accelerator
// ============================================================================
function runModule3() {
  console.log('\n================================================================================');
  console.log(`MODULE 3: openclaw doctor --fix (${MIGRATE_SESSIONS} Session Migration Accelerator)`);
  console.log('================================================================================');

  const nfsSourceDir = path.join(NFS_DIR, '.openclaw-doctor-src');
  const nfsDirectDbDir = path.join(NFS_DIR, '.openclaw-doctor-direct');
  const emptyWorkDir = path.join(EMPTY_DIR, '.openclaw-doctor-local');
  const nfsReturnDbDir = path.join(NFS_DIR, '.openclaw-doctor-returned');

  ensureCleanDir(nfsSourceDir);
  ensureCleanDir(nfsDirectDbDir);
  ensureCleanDir(emptyWorkDir);
  ensureCleanDir(nfsReturnDbDir);

  // 1. Pre-generate 2,000 simulated historical .jsonl session files + .lock files on --nfs-dir
  const tGen0 = nowNs();
  const sampleLine = JSON.stringify({
    sessionId: 'sess_hist',
    role: 'user',
    content: 'Historical OpenClaw conversation log entry for migration test ' + 'z'.repeat(250),
    ts: Date.now(),
  }) + '\n';
  const fileBody = sampleLine.repeat(4); // ~1.4KB per session file

  for (let i = 0; i < MIGRATE_SESSIONS; i++) {
    fs.writeFileSync(path.join(nfsSourceDir, `session_${i}.jsonl`), fileBody);
    if (i % 4 === 0) {
      fs.writeFileSync(path.join(nfsSourceDir, `session_${i}.lock`), `stale-lock-${i}`);
    }
  }
  const genMs = nsToMs(nowNs() - tGen0);
  console.log(`Generated ${MIGRATE_SESSIONS} .jsonl files + ${Math.ceil(MIGRATE_SESSIONS / 4)} .lock files on NFS in ${fmtMs(genMs, 1)} ms`);

  // Path A: Direct migration on --nfs-dir reading 2,000 .jsonl files and inserting into SQLite on --nfs-dir in default mode
  // Note: In default SQLite mode on NFS, each file migration does a transaction (or batch) + removes stale lock
  const tPathA0 = nowNs();
  const dbPathA = path.join(nfsDirectDbDir, 'migrated_openclaw.sqlite');
  const dbA = new DatabaseSync(dbPathA);
  dbA.exec('PRAGMA journal_mode=DELETE; PRAGMA locking_mode=NORMAL; PRAGMA synchronous=FULL;');
  dbA.exec(`
    CREATE TABLE migrated_sessions (
      session_id TEXT PRIMARY KEY,
      turn_count INTEGER NOT NULL,
      raw_jsonl TEXT NOT NULL,
      migrated_at INTEGER NOT NULL
    );
  `);
  const insertA = dbA.prepare('INSERT INTO migrated_sessions VALUES (?, ?, ?, ?)');

  const nfsEntriesA = fs.readdirSync(nfsSourceDir);
  // Perform per-session transaction (simulating openclaw doctor --fix migrating session-by-session on NFS)
  // To avoid waiting 20+ minutes on 2000 individual FULL-sync NFS transactions while still measuring exact NFS direct migration behavior,
  // we commit every 4 sessions (500 transactions across 2,000 files) OR individual transactions if fast enough.
  // Wait: let's check how fast 2,000 transactions on NFS is: at ~2-4ms/tx on Filestore Zonal SSD, 2,000 txs takes ~4-8 seconds!
  // Let's run all 2,000 individual transactions directly so row count == 2000 and it's 100% authentic!
  for (const name of nfsEntriesA) {
    if (!name.endsWith('.jsonl')) continue;
    const raw = fs.readFileSync(path.join(nfsSourceDir, name), 'utf8');
    dbA.exec('BEGIN IMMEDIATE;');
    insertA.run(name, 4, raw, Date.now());
    dbA.exec('COMMIT;');
  }
  dbA.close();
  const pathAMs = nsToMs(nowNs() - tPathA0);

  // Path B: initContainer + emptyDir pattern:
  // Step B1: copy 2,000 files from --nfs-dir to --empty-dir
  const tStepB1_0 = nowNs();
  const nfsEntriesB = fs.readdirSync(nfsSourceDir);
  for (const name of nfsEntriesB) {
    fs.copyFileSync(path.join(nfsSourceDir, name), path.join(emptyWorkDir, name));
  }
  const stepB1CopyToLocalMs = nsToMs(nowNs() - tStepB1_0);

  // Step B2: run SQLite migration on --empty-dir in WAL mode
  const tStepB2_0 = nowNs();
  const localDbPath = path.join(emptyWorkDir, 'migrated_openclaw.sqlite');
  const dbB = new DatabaseSync(localDbPath);
  dbB.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;');
  dbB.exec(`
    CREATE TABLE migrated_sessions (
      session_id TEXT PRIMARY KEY,
      turn_count INTEGER NOT NULL,
      raw_jsonl TEXT NOT NULL,
      migrated_at INTEGER NOT NULL
    );
  `);
  const insertB = dbB.prepare('INSERT INTO migrated_sessions VALUES (?, ?, ?, ?)');
  const localEntries = fs.readdirSync(emptyWorkDir);
  dbB.exec('BEGIN IMMEDIATE;');
  for (const name of localEntries) {
    if (!name.endsWith('.jsonl')) continue;
    const raw = fs.readFileSync(path.join(emptyWorkDir, name), 'utf8');
    insertB.run(name, 4, raw, Date.now());
  }
  dbB.exec('COMMIT;');
  // Checkpoint WAL into single .sqlite file before copying back to NFS
  dbB.exec('PRAGMA wal_checkpoint(TRUNCATE);');
  dbB.exec('PRAGMA journal_mode=DELETE;');
  dbB.close();
  const stepB2LocalSqliteMs = nsToMs(nowNs() - tStepB2_0);

  // Step B3: copy resulting .sqlite file back to --nfs-dir
  const tStepB3_0 = nowNs();
  const finalNfsDbPath = path.join(nfsReturnDbDir, 'migrated_openclaw.sqlite');
  fs.copyFileSync(localDbPath, finalNfsDbPath);
  const stepB3CopyBackMs = nsToMs(nowNs() - tStepB3_0);

  const pathBTotalMs = stepB1CopyToLocalMs + stepB2LocalSqliteMs + stepB3CopyBackMs;
  const speedupFactor = pathAMs / pathBTotalMs;

  // Verify the .sqlite file copied back to --nfs-dir passes PRAGMA integrity_check and row count == 2000
  const verifyDb = new DatabaseSync(finalNfsDbPath);
  const integrityRow = verifyDb.prepare('PRAGMA integrity_check;').get();
  const countRow = verifyDb.prepare('SELECT COUNT(*) AS cnt FROM migrated_sessions;').get();
  verifyDb.close();

  const integrityStatus = integrityRow ? Object.values(integrityRow)[0] : 'unknown';
  const finalRowCount = countRow ? Number(countRow.cnt) : -1;

  if (integrityStatus !== 'ok' || finalRowCount !== MIGRATE_SESSIONS) {
    throw new Error(
      `Module 3 verification failed: integrity_check=${integrityStatus}, row_count=${finalRowCount} (expected ${MIGRATE_SESSIONS})`
    );
  }

  console.log('\n### Module 3 Migration Path Comparison');
  console.log('| Migration Path / Step | Storage Target & Mode | Duration (ms) | Notes / Verification |');
  console.log('| :--- | :--- | ---: | :--- |');
  console.log(`| **Path A: Direct on Filestore NFSv3** | \`--nfs-dir\` (Default \`DELETE\` journal, per-file tx) | **${fmtMs(pathAMs, 1)} ms** | High RPC lock & journal overhead |`);
  console.log(`| **Path B (Step 1): Stage Files NFS -> emptyDir** | \`--nfs-dir\` -> \`--empty-dir\` (${MIGRATE_SESSIONS} sessions) | ${fmtMs(stepB1CopyToLocalMs, 1)} ms | Sequential file staging |`);
  console.log(`| **Path B (Step 2): Local SQLite Migration** | \`--empty-dir\` (\`WAL\` + batched migration + checkpoint) | ${fmtMs(stepB2LocalSqliteMs, 1)} ms | Zero network lock RPCs |`);
  console.log(`| **Path B (Step 3): Copy \`.sqlite\` back to NFS** | \`--empty-dir\` -> \`--nfs-dir\` (single DB file) | ${fmtMs(stepB3CopyBackMs, 1)} ms | Streamed single-file write |`);
  console.log(`| **Path B: Total (initContainer + emptyDir)** | End-to-End Accelerated Migration | **${fmtMs(pathBTotalMs, 1)} ms** | **${speedupFactor.toFixed(2)}x speedup** vs. Path A |`);
  console.log(`| **Post-Migration Verification on NFS** | \`PRAGMA integrity_check\` & \`COUNT(*)\` | **PASS** | \`integrity_check=${integrityStatus}\`, \`rows=${finalRowCount}\` |`);

  // Cleanup Module 3 temp dirs
  fs.rmSync(nfsSourceDir, { recursive: true, force: true });
  fs.rmSync(nfsDirectDbDir, { recursive: true, force: true });
  fs.rmSync(emptyWorkDir, { recursive: true, force: true });
  fs.rmSync(nfsReturnDbDir, { recursive: true, force: true });

  return {
    sessionsCount: MIGRATE_SESSIONS,
    pathAMs,
    stepB1CopyToLocalMs,
    stepB2LocalSqliteMs,
    stepB3CopyBackMs,
    pathBTotalMs,
    speedupFactor,
    integrityStatus,
    finalRowCount,
  };
}

// ============================================================================
// MODULE 4: Tier 1 Filestore -> Tier 2 PD Live Upgrade Migration (50MB .openclaw dir)
// ============================================================================
function runModule4() {
  console.log('\n================================================================================');
  console.log('MODULE 4: Tier 1 (Filestore) -> Tier 2 (PD Block SSD) Live Upgrade Migration (50MB .openclaw)');
  console.log('================================================================================');

  const nfsUserDir = path.join(NFS_DIR, '.openclaw-tier1-user');
  const pdUserDir = path.join(PD_DIR, '.openclaw-tier2-user');
  ensureCleanDir(nfsUserDir);
  ensureCleanDir(pdUserDir);

  const nfsStateDir = path.join(nfsUserDir, 'state');
  const nfsSessionsDir = path.join(nfsUserDir, 'sessions');
  fs.mkdirSync(nfsStateDir, { recursive: true });
  fs.mkdirSync(nfsSessionsDir, { recursive: true });

  // Populate ~50MB realistic .openclaw directory on --nfs-dir:
  // - 1 SQLite DB (~30MB)
  // - 1,000 .jsonl session files (~20KB each = ~20MB)
  const tPop0 = nowNs();
  const dbPathNfs = path.join(nfsStateDir, 'openclaw.sqlite');
  const db = new DatabaseSync(dbPathNfs);
  db.exec('PRAGMA journal_mode=TRUNCATE; PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=NORMAL;');
  db.exec(`
    CREATE TABLE memory_chunks (
      chunk_id INTEGER PRIMARY KEY,
      agent_id TEXT NOT NULL,
      embedding_blob BLOB NOT NULL,
      content TEXT NOT NULL
    );
  `);
  const insertChunk = db.prepare('INSERT INTO memory_chunks VALUES (?, ?, ?, ?)');
  const blob30KB = Buffer.alloc(30 * 1024, 'm');
  const textContent = 'OpenClaw agent long-term memory vector & context chunk '.repeat(10);

  db.exec('BEGIN IMMEDIATE;');
  for (let i = 1; i <= 1000; i++) {
    insertChunk.run(i, 'agent_main', blob30KB, textContent);
  }
  db.exec('COMMIT;');
  db.close();

  // 1,000 .jsonl session files (~20KB each = ~20MB)
  const sessionPayload20KB = Buffer.alloc(20 * 1024, 's');
  for (let i = 0; i < 1000; i++) {
    fs.writeFileSync(path.join(nfsSessionsDir, `session_${i}.jsonl`), sessionPayload20KB);
  }
  const popMs = nsToMs(nowNs() - tPop0);

  // Calculate total bytes populated
  const dbSizeBytes = fs.statSync(dbPathNfs).size;
  const totalBytes = dbSizeBytes + 1000 * 20 * 1024;
  const totalMB = totalBytes / (1024 * 1024);

  console.log(`Populated ${totalMB.toFixed(2)} MB .openclaw user directory on Filestore (1 SQLite DB [${(dbSizeBytes / 1048576).toFixed(2)} MB] + 1,000 .jsonl files [19.53 MB]) in ${fmtMs(popMs, 1)} ms`);

  // Measure live upgrade sync/copy time from --nfs-dir to --pd-dir
  const tSync0 = nowNs();
  const pdStateDir = path.join(pdUserDir, 'state');
  const pdSessionsDir = path.join(pdUserDir, 'sessions');
  fs.mkdirSync(pdStateDir, { recursive: true });
  fs.mkdirSync(pdSessionsDir, { recursive: true });

  // Copy SQLite DB
  const tDbCopy0 = nowNs();
  const dbPathPd = path.join(pdStateDir, 'openclaw.sqlite');
  fs.copyFileSync(dbPathNfs, dbPathPd);
  const dbCopyMs = nsToMs(nowNs() - tDbCopy0);

  // Copy 1,000 session files
  const tSessCopy0 = nowNs();
  const sessFiles = fs.readdirSync(nfsSessionsDir);
  for (const f of sessFiles) {
    fs.copyFileSync(path.join(nfsSessionsDir, f), path.join(pdSessionsDir, f));
  }
  const sessCopyMs = nsToMs(nowNs() - tSessCopy0);
  const totalSyncMs = nsToMs(nowNs() - tSync0);

  // Verify SQLite integrity & row count on --pd-dir and switch to WAL mode
  const tVerify0 = nowNs();
  const pdDb = new DatabaseSync(dbPathPd);
  pdDb.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;');
  const integrityRow = pdDb.prepare('PRAGMA integrity_check;').get();
  const countRow = pdDb.prepare('SELECT COUNT(*) AS cnt FROM memory_chunks;').get();
  pdDb.close();
  const verifyMs = nsToMs(nowNs() - tVerify0);

  const integrityStatus = integrityRow ? Object.values(integrityRow)[0] : 'unknown';
  const chunkCount = countRow ? Number(countRow.cnt) : -1;
  const pdSessCount = fs.readdirSync(pdSessionsDir).length;

  if (integrityStatus !== 'ok' || chunkCount !== 1000 || pdSessCount !== 1000) {
    throw new Error(
      `Module 4 verification failed: integrity=${integrityStatus}, chunks=${chunkCount}, files=${pdSessCount}`
    );
  }

  const throughputMBps = totalMB / (totalSyncMs / 1000);

  console.log('\n### Module 4 Live Tier Upgrade Results (Standard Tier Filestore -> Pro Tier PD SSD)');
  console.log('| Upgrade Phase | Metric / Result | Notes |');
  console.log('| :--- | ---: | :--- |');
  console.log(`| **Source Footprint (`.concat('`.openclaw`').concat(`)** | **${totalMB.toFixed(2)} MB** | 1 SQLite DB (${(dbSizeBytes / 1048576).toFixed(2)} MB) + 1,000 \`.jsonl\` files |`));
  console.log(`| **Step 1: SQLite DB Stream Copy** | ${fmtMs(dbCopyMs, 1)} ms | Sequential copy \`state/openclaw.sqlite\` |`);
  console.log(`| **Step 2: 1,000 Session Files Copy** | ${fmtMs(sessCopyMs, 1)} ms | Copy \`sessions/*.jsonl\` (1,000 files) |`);
  console.log(`| **Total Cutover Sync Window** | **${fmtMs(totalSyncMs, 1)} ms (${(totalSyncMs / 1000).toFixed(2)} s)** | Effective throughput: **${throughputMBps.toFixed(1)} MB/s** |`);
  console.log(`| **Target PD SQLite Verification** | **PASS (${fmtMs(verifyMs, 1)} ms)** | \`integrity_check=${integrityStatus}\`, \`chunks=${chunkCount}\`, \`files=${pdSessCount}\`, switched to \`WAL\` |`);

  fs.rmSync(nfsUserDir, { recursive: true, force: true });
  fs.rmSync(pdUserDir, { recursive: true, force: true });

  return {
    totalMB,
    dbCopyMs,
    sessCopyMs,
    totalSyncMs,
    verifyMs,
    throughputMBps,
    integrityStatus,
    chunkCount,
    pdSessCount,
  };
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================
async function main() {
  console.log('================================================================================');
  console.log('OpenClaw GKE Agent Sandbox Storage & SQLite Benchmark Suite');
  console.log(`Node.js Version : ${process.version}`);
  console.log(`Timestamp       : ${new Date().toISOString()}`);
  console.log(`PD Directory    : ${PD_DIR}`);
  console.log(`NFS Directory   : ${NFS_DIR}`);
  console.log(`EmptyDir        : ${EMPTY_DIR}`);
  console.log('================================================================================');

  fs.mkdirSync(PD_DIR, { recursive: true });
  fs.mkdirSync(NFS_DIR, { recursive: true });
  fs.mkdirSync(EMPTY_DIR, { recursive: true });

  const m1 = runModule1();
  const m2 = runModule2();
  const m3 = runModule3();
  const m4 = runModule4();

  const summaryJson = {
    metadata: {
      nodeVersion: process.version,
      timestamp: new Date().toISOString(),
      pdDir: PD_DIR,
      nfsDir: NFS_DIR,
      emptyDir: EMPTY_DIR,
    },
    module1: m1,
    module2: m2,
    module3: m3,
    module4: m4,
  };

  console.log('\n================================================================================');
  console.log('BENCHMARK_SUITE_JSON_START');
  console.log(JSON.stringify(summaryJson, null, 2));
  console.log('BENCHMARK_SUITE_JSON_END');
  console.log('================================================================================');
}

main().catch((err) => {
  console.error('FATAL BENCHMARK ERROR:', err);
  process.exit(1);
});
