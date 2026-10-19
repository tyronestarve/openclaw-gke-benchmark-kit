#!/bin/bash
# ==============================================================================
# 18-openclaw-sqlite-tiered-bench.sh
# OpenClaw on GKE Agent Sandbox: 双层分级存储 (PD vs. Filestore vs. emptyDir) 实盘基准测试套件
#
# 全自包含执行 4 大实测模块，自带 trap cleanup EXIT 异常保护与防漏删硬性兜底：
#   - Module 1: 1:1 复刻客户 node-fs-benchmark (1,000 × 1KB 文件 × 5 轮)
#   - Module 2: node:sqlite 1,000 笔同步写事务 P50/P95/P99 & 1/5/17 飞书账号并发压测
#   - Module 3: openclaw doctor --fix (2,000 会话迁移) NFS 直跑 vs. initContainer + emptyDir 本地加速
#   - Module 4: Tier 1 (Filestore) -> Tier 2 (pd-balanced) 50MB .openclaw 在线升配热迁移
#
# 用法:
#   ./18-openclaw-sqlite-tiered-bench.sh [all|local|cleanup]
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID=${PROJECT_ID:-your-gcp-project-id}
ZONE=${ZONE:-us-central1-a}
CLUSTER=${CLUSTER:-ai-sandbox-cluster}
NODE_POOL=${NODE_POOL:-gvisor-nodepool}
VPC_NETWORK=${VPC_NETWORK:-sandbox-network}
FS_INSTANCE=${FS_INSTANCE:-openclaw-bench-fs}
FS_SHARE=${FS_SHARE:-vol1}
NS=${NAMESPACE:-default}
PD_PVC=${PD_PVC:-bench-pd-pvc}
PD_SC=${PD_SC:-standard-rwo}
BENCH_POD=${BENCH_POD:-openclaw-bench-pod}
MODE=$(echo "${1:-all}" | tr '[:upper:]' '[:lower:]')

BENCH_MJS_SRC="${BENCH_MJS_SRC:-${SCRIPT_DIR}/openclaw-storage-sqlite-benchmark.mjs}"
REPORT_OUT="${REPORT_OUT:-${SCRIPT_DIR}/live_bench_report.md}"

# 写入内嵌的 bench_suite.mjs 生成函数，确保单脚本在任何环境 100% 零外部依赖独立可运行
write_embedded_bench_mjs() {
  local target_file="$1"
  cat <<'EOF_MJS' > "$target_file"
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

    const tWrite0 = nowNs();
    for (let i = 0; i < count; i++) {
      fs.writeFileSync(path.join(roundDir, `file_${i}.dat`), payload1KB);
    }
    const writeMs = nsToMs(nowNs() - tWrite0);

    const tRead0 = nowNs();
    for (let i = 0; i < count; i++) {
      fs.readFileSync(path.join(roundDir, `file_${i}.dat`));
    }
    const readMs = nsToMs(nowNs() - tRead0);

    const tStat0 = nowNs();
    for (let i = 0; i < count; i++) {
      fs.statSync(path.join(roundDir, `file_${i}.dat`));
    }
    const statMs = nsToMs(nowNs() - tStat0);

    const tReaddir0 = nowNs();
    const entries = fs.readdirSync(roundDir);
    const readdirMs = nsToMs(nowNs() - tReaddir0);
    if (entries.length !== count) {
      throw new Error(`readdirSync count mismatch: expected ${count}, got ${entries.length}`);
    }

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

    for (let l = 0; l < 5; l++) {
      const lockFile = path.join(locksDir, `${acctId}_${l}.lock`);
      fs.writeFileSync(lockFile, `${process.pid}:${Date.now()}`);
      fs.readdirSync(locksDir);
      fs.rmSync(lockFile);
    }

    for (let j = 0; j < 10; j++) {
      fs.appendFileSync(jsonlPath, turnPayload);
    }

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

  const tGen0 = nowNs();
  const sampleLine = JSON.stringify({
    sessionId: 'sess_hist',
    role: 'user',
    content: 'Historical OpenClaw conversation log entry for migration test ' + 'z'.repeat(250),
    ts: Date.now(),
  }) + '\n';
  const fileBody = sampleLine.repeat(4);

  for (let i = 0; i < MIGRATE_SESSIONS; i++) {
    fs.writeFileSync(path.join(nfsSourceDir, `session_${i}.jsonl`), fileBody);
    if (i % 4 === 0) {
      fs.writeFileSync(path.join(nfsSourceDir, `session_${i}.lock`), `stale-lock-${i}`);
    }
  }
  const genMs = nsToMs(nowNs() - tGen0);
  console.log(`Generated ${MIGRATE_SESSIONS} .jsonl files + ${Math.ceil(MIGRATE_SESSIONS / 4)} .lock files on NFS in ${fmtMs(genMs, 1)} ms`);

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
  for (const name of nfsEntriesA) {
    if (!name.endsWith('.jsonl')) continue;
    const raw = fs.readFileSync(path.join(nfsSourceDir, name), 'utf8');
    dbA.exec('BEGIN IMMEDIATE;');
    insertA.run(name, 4, raw, Date.now());
    dbA.exec('COMMIT;');
  }
  dbA.close();
  const pathAMs = nsToMs(nowNs() - tPathA0);

  const tStepB1_0 = nowNs();
  const nfsEntriesB = fs.readdirSync(nfsSourceDir);
  for (const name of nfsEntriesB) {
    fs.copyFileSync(path.join(nfsSourceDir, name), path.join(emptyWorkDir, name));
  }
  const stepB1CopyToLocalMs = nsToMs(nowNs() - tStepB1_0);

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
  dbB.exec('PRAGMA wal_checkpoint(TRUNCATE);');
  dbB.exec('PRAGMA journal_mode=DELETE;');
  dbB.close();
  const stepB2LocalSqliteMs = nsToMs(nowNs() - tStepB2_0);

  const tStepB3_0 = nowNs();
  const finalNfsDbPath = path.join(nfsReturnDbDir, 'migrated_openclaw.sqlite');
  fs.copyFileSync(localDbPath, finalNfsDbPath);
  const stepB3CopyBackMs = nsToMs(nowNs() - tStepB3_0);

  const pathBTotalMs = stepB1CopyToLocalMs + stepB2LocalSqliteMs + stepB3CopyBackMs;
  const speedupFactor = pathAMs / pathBTotalMs;

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

  const sessionPayload20KB = Buffer.alloc(20 * 1024, 's');
  for (let i = 0; i < 1000; i++) {
    fs.writeFileSync(path.join(nfsSessionsDir, `session_${i}.jsonl`), sessionPayload20KB);
  }
  const popMs = nsToMs(nowNs() - tPop0);

  const dbSizeBytes = fs.statSync(dbPathNfs).size;
  const totalBytes = dbSizeBytes + 1000 * 20 * 1024;
  const totalMB = totalBytes / (1024 * 1024);

  console.log(`Populated ${totalMB.toFixed(2)} MB .openclaw user directory on Filestore (1 SQLite DB [${(dbSizeBytes / 1048576).toFixed(2)} MB] + 1,000 .jsonl files [19.53 MB]) in ${fmtMs(popMs, 1)} ms`);

  const tSync0 = nowNs();
  const pdStateDir = path.join(pdUserDir, 'state');
  const pdSessionsDir = path.join(pdUserDir, 'sessions');
  fs.mkdirSync(pdStateDir, { recursive: true });
  fs.mkdirSync(pdSessionsDir, { recursive: true });

  const tDbCopy0 = nowNs();
  const dbPathPd = path.join(pdStateDir, 'openclaw.sqlite');
  fs.copyFileSync(dbPathNfs, dbPathPd);
  const dbCopyMs = nsToMs(nowNs() - tDbCopy0);

  const tSessCopy0 = nowNs();
  const sessFiles = fs.readdirSync(nfsSessionsDir);
  for (const f of sessFiles) {
    fs.copyFileSync(path.join(nfsSessionsDir, f), path.join(pdSessionsDir, f));
  }
  const sessCopyMs = nsToMs(nowNs() - tSessCopy0);
  const totalSyncMs = nsToMs(nowNs() - tSync0);

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
  console.log(`| **Source Footprint (\`.openclaw\`)** | **${totalMB.toFixed(2)} MB** | 1 SQLite DB (${(dbSizeBytes / 1048576).toFixed(2)} MB) + 1,000 \`.jsonl\` files |`);
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
EOF_MJS
}

# 强制清理函数：无论是正常结束还是中途 Ctrl+C / 报错退出，均触发 trap 回收云资源
cleanup() {
  if [ "$MODE" = "local" ]; then
    rm -rf /tmp/test-pd /tmp/test-nfs /tmp/test-empty 2>/dev/null || true
    return 0
  fi

  echo ""
  echo "=================================================================="
  echo "[Cleanup Guardrail] 正在执行强制云资源回收与防漏删归零校验..."
  echo "=================================================================="

  echo "  -> [1/3] 清理测试 Pod ($BENCH_POD) 与块存储 PVC ($PD_PVC)..."
  kubectl delete pod "$BENCH_POD" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl delete pvc "$PD_PVC" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true

  echo "  -> [2/3] 销毁 1TiB Filestore 实例 ($FS_INSTANCE)..."
  if gcloud filestore instances describe "$FS_INSTANCE" --zone="$ZONE" --project="$PROJECT_ID" >/dev/null 2>&1; then
    gcloud filestore instances delete "$FS_INSTANCE" --zone="$ZONE" --project="$PROJECT_ID" --quiet || true
  fi

  echo "  -> [3/3] 将 gVisor 节点池 ($NODE_POOL) 缩容回 0 节点..."
  gcloud container clusters resize "$CLUSTER" \
    --node-pool="$NODE_POOL" \
    --num-nodes=0 \
    --zone="$ZONE" \
    --project="$PROJECT_ID" \
    --quiet || true

  echo "  -> [核验] 当前项目 Filestore 实例列表："
  gcloud filestore instances list --project="$PROJECT_ID" || true
  echo "  -> [核验] 当前集群 PVC 列表："
  kubectl get pvc -A || true
  echo "✅ [Cleanup Guardrail] 所有计费测试资源已触发强制回收！"
}

trap cleanup EXIT

run_local_smoke() {
  echo "=================================================================="
  echo "[Local Smoke Test] 在本地 /tmp 目录验证 bench_suite.mjs 4 大模块..."
  echo "=================================================================="
  local tmp_mjs="/tmp/bench_suite_local.mjs"
  if [ -f "$BENCH_MJS_SRC" ]; then
    cp "$BENCH_MJS_SRC" "$tmp_mjs"
  else
    write_embedded_bench_mjs "$tmp_mjs"
  fi
  node "$tmp_mjs" --pd-dir=/tmp/test-pd --nfs-dir=/tmp/test-nfs --empty-dir=/tmp/test-empty
  rm -f "$tmp_mjs"
}

run_gke_live_bench() {
  echo "=================================================================="
  echo "[Phase 1] 准备 GKE gVisor 节点、1TiB Filestore Zonal SSD 与 10GiB pd-balanced"
  echo "  Project     : $PROJECT_ID"
  echo "  Cluster     : $CLUSTER ($ZONE)"
  echo "  Filestore   : $FS_INSTANCE (1TiB Zonal SSD)"
  echo "  Block PVC   : $PD_PVC (10GiB $PD_SC)"
  echo "=================================================================="

  gcloud container clusters get-credentials "$CLUSTER" --zone="$ZONE" --project="$PROJECT_ID"

  # 清理历史残留快照（防止阻塞节点或 PVC 回收）
  kubectl delete podsnapshotmanualtriggers --all -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete podsnapshots --all -n "$NS" --ignore-not-found >/dev/null 2>&1 || true

  # 并行发起 Filestore 创建与 gVisor 节点池扩容
  if ! gcloud filestore instances describe "$FS_INSTANCE" --zone="$ZONE" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "  -> 异步创建 1TiB Filestore Zonal SSD ($FS_INSTANCE)..."
    gcloud filestore instances create "$FS_INSTANCE" \
      --zone="$ZONE" \
      --tier=ZONAL \
      --file-share="name=${FS_SHARE},capacity=1024" \
      --network="name=${VPC_NETWORK}" \
      --project="$PROJECT_ID" \
      --async
  fi

  echo "  -> 扩容 gVisor 节点池 ($NODE_POOL -> 1 node)..."
  gcloud container clusters resize "$CLUSTER" \
    --node-pool="$NODE_POOL" \
    --num-nodes=1 \
    --zone="$ZONE" \
    --project="$PROJECT_ID" \
    --quiet

  echo "  -> 等待 Filestore 实例 ($FS_INSTANCE) 就绪 (STATE=READY)..."
  until [ "$(gcloud filestore instances describe "$FS_INSTANCE" --zone="$ZONE" --project="$PROJECT_ID" --format='value(state)' 2>/dev/null || true)" = "READY" ]; do
    sleep 10
  done
  FS_IP=$(gcloud filestore instances describe "$FS_INSTANCE" --zone="$ZONE" --project="$PROJECT_ID" --format='value(networks[0].ipAddresses[0])')
  echo "  ✅ Filestore Zonal SSD 就绪，挂载 IP: $FS_IP:/${FS_SHARE}"

  echo "=================================================================="
  echo "[Phase 2] 部署挂载三层存储 (/mnt/pd, /mnt/filestore, /mnt/emptydir) 的 gVisor 压测沙箱"
  echo "=================================================================="
  kubectl apply -n "$NS" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${PD_PVC}
  labels:
    test-suite: openclaw-storage-bench
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: ${PD_SC}
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: ${BENCH_POD}
  labels:
    test-suite: openclaw-storage-bench
spec:
  runtimeClassName: gvisor
  nodeSelector:
    sandbox.gke.io/runtime: gvisor
  tolerations:
    - key: sandbox.gke.io/runtime
      value: gvisor
      effect: NoSchedule
  restartPolicy: Never
  containers:
    - name: bench
      image: node:22-slim
      command: ["sleep", "7200"]
      resources:
        requests: { cpu: "2", memory: "4Gi" }
        limits: { cpu: "4", memory: "6Gi" }
      volumeMounts:
        - name: pd-vol
          mountPath: /mnt/pd
        - name: nfs-vol
          mountPath: /mnt/filestore
        - name: empty-vol
          mountPath: /mnt/emptydir
  volumes:
    - name: pd-vol
      persistentVolumeClaim:
        claimName: ${PD_PVC}
    - name: nfs-vol
      nfs:
        server: ${FS_IP}
        path: /${FS_SHARE}
    - name: empty-vol
      emptyDir: {}
EOF

  echo "  -> 等待 gVisor 压测 Pod ($BENCH_POD) Ready..."
  kubectl wait --for=condition=Ready "pod/${BENCH_POD}" -n "$NS" --timeout=300s

  local tmp_mjs="/tmp/bench_suite_upload.mjs"
  if [ -f "$BENCH_MJS_SRC" ]; then
    cp "$BENCH_MJS_SRC" "$tmp_mjs"
  else
    write_embedded_bench_mjs "$tmp_mjs"
  fi

  kubectl cp "$tmp_mjs" "${NS}/${BENCH_POD}:/tmp/bench_suite.mjs"
  rm -f "$tmp_mjs"

  echo "=================================================================="
  echo "[Phase 3] 在 gVisor 沙箱内执行 OpenClaw 4 大存储与 SQLite 实测模块"
  echo "=================================================================="
  mkdir -p "$(dirname "$REPORT_OUT")"
  kubectl exec "$BENCH_POD" -n "$NS" -- node /tmp/bench_suite.mjs \
    --pd-dir=/mnt/pd \
    --nfs-dir=/mnt/filestore \
    --empty-dir=/mnt/emptydir | tee "$REPORT_OUT"

  echo ""
  echo "✅ 压测报告已落盘至: $REPORT_OUT"
}

case "$MODE" in
  local)
    run_local_smoke
    ;;
  cleanup)
    # trap cleanup EXIT 会自动执行清理
    echo "触发手动 cleanup 模式..."
    ;;
  all)
    run_gke_live_bench
    ;;
  *)
    echo "用法: $0 [all|local|cleanup]"
    exit 1
    ;;
esac
