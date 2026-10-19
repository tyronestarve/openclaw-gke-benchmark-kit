#!/usr/bin/env node
/**
 * hd_bench_suite.mjs
 * OpenClaw on GKE Agent Sandbox: Hyperdisk Balanced (C3/N4 gVisor-Only) Benchmark Suite
 *
 * Uses built-in Node.js 22+ modules ONLY:
 *   - node:fs, node:path, node:os, node:sqlite (DatabaseSync), process.hrtime.bigint()
 *
 * Usage:
 *   node hd_bench_suite.mjs --hd-dir=/mnt/hd --empty-dir=/mnt/emptydir
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import { DatabaseSync } from 'node:sqlite';

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

const HD_DIR = args['hd-dir'] || '/mnt/hd';
const EMPTY_DIR = args['empty-dir'] || '/mnt/emptydir';
const FILE_COUNT = Number(args['file-count'] || 1000);
const ROUNDS = Number(args['rounds'] || 5);
const TX_COUNT = Number(args['tx-count'] || 1000);

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
// TEST 1A: Customer's exact `node-fs-benchmark` (5 rounds x 1,000 x 1KB files)
// ============================================================================
function runFsBenchOnTarget(label, baseDir, count, rounds) {
  const workDir = path.join(baseDir, '.openclaw-hd-fs-bench');
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

// ============================================================================
// TEST 1B: 1,000 x 1KB `open + write + fsync + close` (Hardware Disk Commit)
// ============================================================================
function runFsyncCommitBench(label, baseDir, count) {
  const workDir = path.join(baseDir, '.openclaw-hd-fsync-bench');
  ensureCleanDir(workDir);
  const payload1KB = Buffer.alloc(1024, 'f');

  const latenciesMs = [];
  const tStart = nowNs();

  for (let i = 0; i < count; i++) {
    const filePath = path.join(workDir, `sync_${i}.dat`);
    const t0 = nowNs();
    const fd = fs.openSync(filePath, 'w', 0o644);
    fs.writeSync(fd, payload1KB, 0, payload1KB.length, 0);
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    const dtMs = nsToMs(nowNs() - t0);
    latenciesMs.push(dtMs);
  }

  const totalMs = nsToMs(nowNs() - tStart);
  fs.rmSync(workDir, { recursive: true, force: true });

  const sorted = [...latenciesMs].sort((a, b) => a - b);
  const p50Ms = percentile(sorted, 50);
  const p95Ms = percentile(sorted, 95);
  const p99Ms = percentile(sorted, 99);
  const avgMs = totalMs / count;
  const iops = count / (totalMs / 1000);

  return {
    label,
    baseDir,
    count,
    p50Ms,
    p95Ms,
    p99Ms,
    avgMs,
    totalMs,
    iops,
  };
}

// ============================================================================
// TEST 2A: `node:sqlite` 1,000 Synchronous Write Transactions
// ============================================================================
function runSqliteTxBench(configName, baseDir, pragmas, txCount) {
  const workDir = path.join(baseDir, `.openclaw-sqlite-${configName.replace(/[^a-zA-Z0-9]/g, '_')}`);
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
  const tps = txCount / (totalMs / 1000);

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

// ============================================================================
// TEST 2B: Multi-Account Feishu Turn Concurrency (1, 5, 17 Accounts)
// ============================================================================
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

async function main() {
  const cpuModel = os.cpus()[0]?.model || 'unknown';
  const kernelRelease = os.release();
  console.log('================================================================================');
  console.log('OpenClaw GKE Agent Sandbox: Hyperdisk Balanced (gVisor-Only) Benchmark Suite');
  console.log(`Node.js Version : ${process.version}`);
  console.log(`OS Kernel       : ${os.type()} ${kernelRelease} (${os.arch()})`);
  console.log(`CPU Model       : ${cpuModel} (${os.cpus().length} vCPUs)`);
  console.log(`Timestamp       : ${new Date().toISOString()}`);
  console.log(`Hyperdisk Dir   : ${HD_DIR}`);
  console.log(`EmptyDir        : ${EMPTY_DIR}`);
  console.log('================================================================================');

  fs.mkdirSync(HD_DIR, { recursive: true });
  fs.mkdirSync(EMPTY_DIR, { recursive: true });

  console.log('\n================================================================================');
  console.log(`TEST 1A: node-fs-benchmark (${ROUNDS} Rounds × ${FILE_COUNT} × 1KB Files on Hyperdisk Balanced)`);
  console.log('================================================================================');
  const hdFsRes = runFsBenchOnTarget('Hyperdisk Balanced (10GiB, 3,000 IOPS / 140 MB/s on C3 gVisor)', HD_DIR, FILE_COUNT, ROUNDS);
  console.log('| Round | writeFileSync (ms) | readFileSync (ms) | statSync (ms) | readdirSync (ms) | rmSync (ms) | Total (ms) |');
  console.log('| :--- | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const s of hdFsRes.roundStats) {
    console.log(
      `| Round ${s.round} | ${fmtMs(s.writeMs)} | ${fmtMs(s.readMs)} | ${fmtMs(s.statMs)} | ${fmtMs(s.readdirMs)} | ${fmtMs(s.rmMs)} | ${fmtMs(s.totalMs)} |`
    );
  }
  const a = hdFsRes.averages;
  console.log(
    `| **Average** | **${fmtMs(a.writeMs)}** | **${fmtMs(a.readMs)}** | **${fmtMs(a.statMs)}** | **${fmtMs(a.readdirMs)}** | **${fmtMs(a.rmMs)}** | **${fmtMs(a.totalMs)}** |`
  );

  console.log('\n================================================================================');
  console.log(`TEST 1B: 1,000 × 1KB open + write + fsync + close (True Hardware Disk Commit Latency)`);
  console.log('================================================================================');
  const hdFsyncRes = runFsyncCommitBench('Hyperdisk Balanced (/mnt/hd)', HD_DIR, FILE_COUNT);
  const emptyFsyncRes = runFsyncCommitBench('Local emptyDir (/mnt/emptydir)', EMPTY_DIR, FILE_COUNT);
  console.log('| Target Volume | P50 (ms) | P95 (ms) | P99 (ms) | Avg (ms) | Total Time (ms) | Effective Commit IOPS |');
  console.log('| :--- | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const r of [hdFsyncRes, emptyFsyncRes]) {
    console.log(
      `| **${r.label}** | ${fmtMs(r.p50Ms, 3)} | ${fmtMs(r.p95Ms, 3)} | ${fmtMs(r.p99Ms, 3)} | ${fmtMs(r.avgMs, 3)} | ${fmtMs(r.totalMs, 1)} | **${r.iops.toFixed(1)} IOPS** |`
    );
  }

  console.log('\n================================================================================');
  console.log(`TEST 2: node:sqlite ${TX_COUNT} Synchronous Write Transactions & Multi-Account Concurrency`);
  console.log('================================================================================');
  const sqliteConfigs = [
    {
      id: 'Hyperdisk Balanced (WAL + synchronous=NORMAL)',
      dir: HD_DIR,
      pragmas: ['PRAGMA journal_mode=WAL;', 'PRAGMA synchronous=NORMAL;'],
    },
    {
      id: 'Hyperdisk Balanced (WAL + synchronous=FULL)',
      dir: HD_DIR,
      pragmas: ['PRAGMA journal_mode=WAL;', 'PRAGMA synchronous=FULL;'],
    },
    {
      id: 'C3 Local emptyDir (WAL + synchronous=NORMAL)',
      dir: EMPTY_DIR,
      pragmas: ['PRAGMA journal_mode=WAL;', 'PRAGMA synchronous=NORMAL;'],
    },
  ];

  const txResults = [];
  for (const c of sqliteConfigs) {
    txResults.push(runSqliteTxBench(c.id, c.dir, c.pragmas, TX_COUNT));
  }

  console.log('| Configuration | PRAGMA Settings | P50 (ms) | P95 (ms) | P99 (ms) | Avg (ms) | Total Time (ms) | Throughput (TPS) |');
  console.log('| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const r of txResults) {
    console.log(
      `| **${r.configName}** | \`${r.pragmas}\` | ${fmtMs(r.p50Ms, 3)} | ${fmtMs(r.p95Ms, 3)} | ${fmtMs(r.p99Ms, 3)} | ${fmtMs(r.avgMs, 3)} | ${fmtMs(r.totalMs, 1)} | **${r.tps.toFixed(1)}** |`
    );
  }

  const accountTiers = [1, 5, 17];
  const concurrencyResults = [];
  for (const c of sqliteConfigs.slice(0, 2)) {
    const row = { configName: c.id, byAccount: {} };
    for (const acct of accountTiers) {
      const sim = runFeishuTurnSimulation(c.id, c.dir, c.pragmas, acct);
      row.byAccount[acct] = sim.blockingMs;
    }
    concurrencyResults.push(row);
  }

  console.log('\n### Multi-Account Feishu Turn Concurrency Simulation (Event-Loop Blocking Time per Turn)');
  console.log('| Configuration | 1 Account (30 txs) | 5 Accounts (150 txs) | 17 Accounts (510 txs) |');
  console.log('| :--- | ---: | ---: | ---: |');
  for (const r of concurrencyResults) {
    console.log(
      `| **${r.configName}** | ${fmtMs(r.byAccount[1], 1)} ms | ${fmtMs(r.byAccount[5], 1)} ms | **${fmtMs(r.byAccount[17], 1)} ms** |`
    );
  }

  const summaryJson = {
    metadata: {
      nodeVersion: process.version,
      kernelRelease,
      cpuModel,
      vcpus: os.cpus().length,
      timestamp: new Date().toISOString(),
      hdDir: HD_DIR,
      emptyDir: EMPTY_DIR,
    },
    test1: {
      fsBench: hdFsRes,
      fsyncCommit: {
        hyperdisk: hdFsyncRes,
        emptyDir: emptyFsyncRes,
      },
    },
    test2: {
      txResults,
      concurrencyResults,
    },
  };

  console.log('\n================================================================================');
  console.log('HD_BENCHMARK_JSON_START');
  console.log(JSON.stringify(summaryJson, null, 2));
  console.log('HD_BENCHMARK_JSON_END');
  console.log('================================================================================');
}

main().catch((err) => {
  console.error('FATAL BENCHMARK ERROR:', err);
  process.exit(1);
});
