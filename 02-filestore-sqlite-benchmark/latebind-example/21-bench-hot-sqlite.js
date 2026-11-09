#!/usr/bin/env node
/**
 * 21-bench-hot-sqlite.js
 *
 * 专用于验证「双 emptyDir 冷热分层 + Filestore 晚绑定」新架构下，
 * OpenClaw 热状态目录 (/home/node/.openclaw/state，位于 gVisor tmpfs emptyDir) 的真实 SQLite 读写性能：
 *   1. 自动校验目标目录底层挂载类型（确认命中 tmpfs 内存热盘）与 WAL 模式激活状态
 *   2. 连续 1,000 次独立同步写事务延迟 (BEGIN IMMEDIATE -> INSERT -> COMMIT: P50 / P95 / P99 / TPS)
 *   3. 连续 1,000 次主键点查读取耗时
 *   4. 17 个飞书通道并发模型：
 *      - 稳态并发（5 个活跃通道同时触发一轮对话 = 150 次写事务）
 *      - 峰值满载（17 个通道同一秒全部触发对话 = 510 次写事务，含单线程排队完成耗时）
 *
 * 说明：
 *   - 临时压测库文件名包含 `.tmp-`，可被运行中的 `hot-sqlite-wrapper` 自动忽略，不污染 Filestore 备份，压测结束自动清理。
 *   - 运行方式：node 21-bench-hot-sqlite.js [可选目录，默认 /home/node/.openclaw/state]
 */

'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');

const baseDir = process.argv[2] || '/home/node/.openclaw/state';
const N = Number(process.argv[3] || 1000);
fs.mkdirSync(baseDir, { recursive: true });
const testDir = path.join(baseDir, `.tmp-sqlite-bench-${process.pid}-${Math.floor(Math.random() * 1000)}`);
fs.mkdirSync(testDir, { recursive: true });

function detectMountType(targetPath) {
  try {
    const resolved = fs.realpathSync(targetPath);
    const mounts = fs.readFileSync('/proc/mounts', 'utf8').trim().split('\n');
    let bestMount = '/';
    let bestFsType = 'unknown';
    for (const line of mounts) {
      const parts = line.split(' ');
      if (parts.length < 3) continue;
      const mnt = parts[1];
      const fstype = parts[2];
      if (resolved === mnt || resolved.startsWith(mnt === '/' ? '/' : `${mnt}/`)) {
        if (mnt.length >= bestMount.length) {
          bestMount = mnt;
          bestFsType = fstype;
        }
      }
    }
    return { resolved, mountPoint: bestMount, fsType: bestFsType };
  } catch (_) {
    return { resolved: targetPath, mountPoint: 'unknown', fsType: 'unknown' };
  }
}

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx];
}

function cleanupDbFiles(dbPath) {
  for (const suffix of ['', '-journal', '-wal', '-shm']) {
    try {
      fs.rmSync(`${dbPath}${suffix}`, { force: true });
    } catch (_) {}
  }
}

function runSqliteBenchmark(label, fileTag, pragmaSql) {
  // 使用 .tmp- 文件名，确保运行中的 hot-sqlite-wrapper 自动跳过压测临时库
  const dbPath = path.join(testDir, `openclaw.tmp-${fileTag}.sqlite`);
  cleanupDbFiles(dbPath);

  const db = new DatabaseSync(dbPath);
  db.exec(pragmaSql);
  const activeJournal = db.prepare('PRAGMA journal_mode;').get()?.journal_mode || 'unknown';

  db.exec(`
    CREATE TABLE IF NOT EXISTS sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_key TEXT NOT NULL,
      channel TEXT NOT NULL,
      payload TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);

  const insertStmt = db.prepare(
    'INSERT INTO sessions (session_key, channel, payload, updated_at) VALUES (?, ?, ?, ?);'
  );
  const selectStmt = db.prepare('SELECT id, session_key, payload FROM sessions WHERE id = ?;');
  const payload = 'openclaw-feishu-turn-state-'.repeat(20); // ~540B 典型单轮会话状态 JSON

  // 1. 连续 N 次独立同步写事务 (BEGIN IMMEDIATE -> INSERT -> COMMIT)
  const latencies = [];
  const tWriteStart = process.hrtime.bigint();
  for (let i = 1; i <= N; i++) {
    const t0 = process.hrtime.bigint();
    db.exec('BEGIN IMMEDIATE;');
    insertStmt.run(`sess-${i}`, 'feishu', payload, Date.now());
    db.exec('COMMIT;');
    latencies.push(Number(process.hrtime.bigint() - t0) / 1e6);
  }
  const writeTotalMs = Number(process.hrtime.bigint() - tWriteStart) / 1e6;

  // 2. 连续 N 次主键点查读取
  const tReadStart = process.hrtime.bigint();
  for (let i = 1; i <= N; i++) {
    selectStmt.get(i);
  }
  const readTotalMs = Number(process.hrtime.bigint() - tReadStart) / 1e6;

  // 3A. 绑定 17 个飞书通道的「日常稳态并发」（5 个活跃通道同时触发一轮对话 = 150 次写事务）
  const realisticActiveChannels = 5;
  const txsPerChannelTurn = 30;
  const realisticTxLatencies = [];
  const tRealisticStart = process.hrtime.bigint();
  for (let ch = 1; ch <= realisticActiveChannels; ch++) {
    for (let t = 1; t <= txsPerChannelTurn; t++) {
      const tx0 = process.hrtime.bigint();
      db.exec('BEGIN IMMEDIATE;');
      insertStmt.run(`realistic-ch${ch}-tx${t}`, `feishu-ch-${ch}`, payload, Date.now());
      db.exec('COMMIT;');
      realisticTxLatencies.push(Number(process.hrtime.bigint() - tx0) / 1e6);
    }
  }
  const realisticTotalMs = Number(process.hrtime.bigint() - tRealisticStart) / 1e6;
  realisticTxLatencies.sort((a, b) => a - b);

  // 3B. 绑定 17 个飞书通道的「极端满载并发」（17 个通道 100% 同一秒触发对话 = 510 次写事务）
  const extremeChannels = 17;
  const extremeTxLatencies = [];
  const perChannelLog = [];
  const tExtremeStart = process.hrtime.bigint();
  for (let ch = 1; ch <= extremeChannels; ch++) {
    const chStart = process.hrtime.bigint();
    for (let t = 1; t <= txsPerChannelTurn; t++) {
      const tx0 = process.hrtime.bigint();
      db.exec('BEGIN IMMEDIATE;');
      insertStmt.run(`extreme-ch${ch}-tx${t}`, `feishu-ch-${ch}`, payload, Date.now());
      db.exec('COMMIT;');
      extremeTxLatencies.push(Number(process.hrtime.bigint() - tx0) / 1e6);
    }
    const chNow = process.hrtime.bigint();
    perChannelLog.push({
      ch,
      chExecMs: Number(chNow - chStart) / 1e6,
      chQueueWaitMs: Number(chNow - tExtremeStart) / 1e6,
    });
  }
  const extremeTotalMs = Number(process.hrtime.bigint() - tExtremeStart) / 1e6;
  extremeTxLatencies.sort((a, b) => a - b);

  db.close();
  cleanupDbFiles(dbPath);

  latencies.sort((a, b) => a - b);
  const p50 = percentile(latencies, 50);
  const p95 = percentile(latencies, 95);
  const p99 = percentile(latencies, 99);
  const tps = N / (writeTotalMs / 1000);

  console.log('==============================================================');
  console.log(`Mode: ${label}`);
  console.log(`Active journal_mode: ${activeJournal} | PRAGMA: ${pragmaSql}`);
  console.log('==============================================================');
  console.log(
    `sqliteWriteTx (${N} txs): ${writeTotalMs.toFixed(2)} ms | P50=${p50.toFixed(3)} ms, P95=${p95.toFixed(3)} ms, P99=${p99.toFixed(3)} ms | TPS=${tps.toFixed(1)}`
  );
  console.log(
    `sqliteReadRow (${N} rows): ${readTotalMs.toFixed(2)} ms | Avg=${(readTotalMs / N).toFixed(4)} ms/row`
  );
  console.log(
    `feishu5Concurrent  (150 txs): totalBlock=${realisticTotalMs.toFixed(2)} ms | tx P50=${percentile(realisticTxLatencies, 50).toFixed(3)} ms, P95=${percentile(realisticTxLatencies, 95).toFixed(3)} ms`
  );
  console.log(
    `feishu17Concurrent (510 txs): totalBlock=${extremeTotalMs.toFixed(2)} ms | tx P50=${percentile(extremeTxLatencies, 50).toFixed(3)} ms, P95=${percentile(extremeTxLatencies, 95).toFixed(3)} ms, P99=${percentile(extremeTxLatencies, 99).toFixed(3)} ms`
  );
  const sampleChannels = [1, 5, 10, 17].map((idx) => perChannelLog[idx - 1]).filter(Boolean);
  console.log(
    '  [Per-Channel Queue Latency]: ' +
      sampleChannels
        .map((c) => `ch#${c.ch}(exec=${c.chExecMs.toFixed(1)}ms, waitDone=${c.chQueueWaitMs.toFixed(1)}ms)`)
        .join(' -> ')
  );
  console.log('');
}

function detectLiveDbJournalMode(dir) {
  const liveDb = path.join(dir, 'openclaw.sqlite');
  if (!fs.existsSync(liveDb)) return null;
  let db;
  try {
    db = new DatabaseSync(liveDb, { readOnly: true });
    return db.prepare('PRAGMA journal_mode;').get()?.journal_mode || null;
  } catch (_) {
    return null;
  } finally {
    try { db?.close(); } catch (_) {}
  }
}

const mnt = detectMountType(baseDir);
const liveJournal = detectLiveDbJournalMode(baseDir);
console.log('==============================================================');
console.log('OpenClaw 2026.9.1 Hot-Tier (emptyDir tmpfs) SQLite Benchmark');
console.log(`Target Dir  : ${baseDir} (resolved: ${mnt.resolved})`);
console.log(`Mount Check : fsType=${mnt.fsType} on mountPoint=${mnt.mountPoint}`);
if (liveJournal) {
  console.log(`Live DB     : ${path.join(baseDir, 'openclaw.sqlite')} (journal_mode=${liveJournal})`);
}
console.log(`Node.js     : ${process.version}`);
console.log('==============================================================\n');

try {
  runSqliteBenchmark(
    'OpenClaw 2026.9.1 Native Mode on emptyDir (WAL + NORMAL)',
    'wal-normal',
    'PRAGMA journal_mode=WAL; PRAGMA locking_mode=NORMAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;'
  );
} finally {
  try {
    fs.rmSync(testDir, { recursive: true, force: true });
  } catch (_) {}
}
