#!/usr/bin/env node
/**
 * node-sqlite-benchmark.js
 *
 * 专用于测试 OpenClaw 核心数据库瓶颈：Node.js 主线程 `node:sqlite` (DatabaseSync)
 *   1. 连续 1,000 次真实对话写事务延迟 (P50 / P95 / P99 / Max)
 *   2. 绑定 17 个飞书通道时的真实并发模型：
 *      - 稳态并发 (30% 同时活跃 = 5 个活跃通道同时触发一轮对话，共 150 次写事务)
 *      - 极端满载并发 (100% 同时活跃 = 17 个通道在同一秒全部触发对话，共 510 次写事务)
 *      并打印逐通道的真实 SQLite 事务 Latency 日志明细。
 *   3. 模拟之前导致客户启动/升级卡死的真凶：`openclaw doctor --fix` 批量扫描合并历史会话入库。
 *
 * 运行方式：
 *   node node-sqlite-benchmark.js [测试目录路径，默认 /home/node/.openclaw/workspace]
 */

'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');

const baseDir = process.argv[2] || '/home/node/.openclaw/workspace';
const N = Number(process.argv[3] || 1000);
const testDir = path.join(baseDir, `node-sqlite-benchmark-${Math.floor(Math.random() * 1000)}`);
fs.mkdirSync(testDir, { recursive: true });

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx];
}

function runSqliteBenchmark(label, fileTag, pragmaSql) {
  const dbPath = path.join(testDir, `openclaw-${fileTag}.sqlite`);
  try {
    fs.rmSync(dbPath, { force: true });
    fs.rmSync(`${dbPath}-journal`, { force: true });
    fs.rmSync(`${dbPath}-wal`, { force: true });
    fs.rmSync(`${dbPath}-shm`, { force: true });
  } catch (_) {}

  const db = new DatabaseSync(dbPath);
  db.exec(pragmaSql);
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
  const payload = 'openclaw-feishu-turn-state-'.repeat(20); // ~540B 典型会话状态 JSON

  // 1. 连续 1000 次独立同步写事务 (BEGIN IMMEDIATE -> INSERT -> COMMIT)
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

  // 2. 连续 1000 次主键点查读取
  const tReadStart = process.hrtime.bigint();
  for (let i = 1; i <= N; i++) {
    selectStmt.get(i);
  }
  const readTotalMs = Number(process.hrtime.bigint() - tReadStart) / 1e6;

  // 3A. 绑定 17 个飞书通道的「真实办公稳态并发」（按 30% 同时活跃折算 = 5 个通道同一秒活跃，共 150 次写事务）
  const realisticActiveChannels = 5; // ceil(17 * 0.30) = 5~6
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

  // 3B. 绑定 17 个飞书通道的「极端满载压测并发」（17 个通道 100% 在同一秒同时活跃 = 510 次写事务）并记录每个通道排队完成耗时
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
    const chExecMs = Number(chNow - chStart) / 1e6;
    const chQueueWaitMs = Number(chNow - tExtremeStart) / 1e6; // 因为 Node.js 单线程同步阻塞，第 N 个通道必须等前 N-1 个通道写完
    perChannelLog.push({ ch, chExecMs, chQueueWaitMs });
  }
  const extremeTotalMs = Number(process.hrtime.bigint() - tExtremeStart) / 1e6;
  extremeTxLatencies.sort((a, b) => a - b);

  db.close();
  try {
    fs.rmSync(dbPath, { force: true });
    fs.rmSync(`${dbPath}-journal`, { force: true });
    fs.rmSync(`${dbPath}-wal`, { force: true });
    fs.rmSync(`${dbPath}-shm`, { force: true });
  } catch (_) {}

  latencies.sort((a, b) => a - b);
  const p50 = percentile(latencies, 50);
  const p95 = percentile(latencies, 95);
  const p99 = percentile(latencies, 99);
  const tps = N / (writeTotalMs / 1000);

  console.log('==============================');
  console.log(`Mode: ${label}`);
  console.log(`PRAGMA: ${pragmaSql}`);
  console.log('==============================');
  console.log(`sqliteWriteTx ${N} txs: ${writeTotalMs.toFixed(2)} ms (P50: ${p50.toFixed(3)} ms, P95: ${p95.toFixed(3)} ms, P99: ${p99.toFixed(3)} ms, TPS: ${tps.toFixed(1)})`);
  console.log(`sqliteReadRow ${N} rows: ${readTotalMs.toFixed(2)} ms`);
  console.log(
    `feishu17Bound_Realistic5Concurrent (150 txs): totalBlock=${realisticTotalMs.toFixed(2)} ms | tx P50=${percentile(realisticTxLatencies, 50).toFixed(3)} ms, P95=${percentile(realisticTxLatencies, 95).toFixed(3)} ms`
  );
  console.log(
    `feishu17Bound_Extreme17Concurrent (510 txs): totalBlock=${extremeTotalMs.toFixed(2)} ms | tx P50=${percentile(extremeTxLatencies, 50).toFixed(3)} ms, P95=${percentile(extremeTxLatencies, 95).toFixed(3)} ms, P99=${percentile(extremeTxLatencies, 99).toFixed(3)} ms`
  );
  // 打印第 1、第 5、第 10、第 17 个通道在单线程排队下的真实完成延迟日志
  const sampleChannels = [1, 5, 10, 17].map((idx) => perChannelLog[idx - 1]).filter(Boolean);
  console.log(
    '  [Per-Channel Queue Latency Log]: ' +
      sampleChannels
        .map((c) => `ch#${c.ch}(exec=${c.chExecMs.toFixed(1)}ms, waitDone=${c.chQueueWaitMs.toFixed(1)}ms)`)
        .join(' -> ')
  );
  console.log('');
}

console.log('==============================');
console.log(`Testing OpenClaw SQLite on: ${baseDir}`);
console.log(`Test dir: ${testDir}`);
console.log('==============================\n');

runSqliteBenchmark(
  '1. Baseline Untuned (SQLite Default)',
  'default',
  'PRAGMA journal_mode=DELETE; PRAGMA locking_mode=NORMAL; PRAGMA synchronous=FULL;'
);

runSqliteBenchmark(
  '2. Step-1 Tuned for NAS/Filestore (TRUNCATE + EXCLUSIVE + NORMAL)',
  'tuned-exclusive',
  'PRAGMA journal_mode=TRUNCATE; PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;'
);

runSqliteBenchmark(
  '3. Mode-3 High-Performance WAL Mode (WAL + NORMAL + NORMAL)',
  'tuned-wal',
  'PRAGMA journal_mode=WAL; PRAGMA locking_mode=NORMAL; PRAGMA synchronous=NORMAL;'
);

try {
  fs.rmSync(testDir, { recursive: true, force: true });
} catch (_) {}
