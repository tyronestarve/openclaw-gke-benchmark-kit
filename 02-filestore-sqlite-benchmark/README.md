# 02 — Filestore vs. Hyperdisk 存储性能与 SQLite 并发压测指南

本目录用于系统性测试 OpenClaw 在 **`gVisor` 安全沙箱** 内使用不同持久化存储介质（**Filestore NAS** vs. **Hyperdisk Balanced / SSD 块存储**）时的真实 I/O 性能与成本边界。

---

## 1. 为什么要做专项存储压测？

OpenClaw 运行时存在三类典型 I/O 负载，对底层文件系统的元数据延迟与文件锁极其敏感：
1. **主线程同步 SQLite 写入**：基于 Node.js 22 内置 `node:sqlite` (`DatabaseSync`)，每轮对话在主线程同步执行写事务；若存储层 fsync 或文件锁慢，会直接阻塞 Node.js EventLoop。
2. **高频小文件锁与目录扫描**：`session-locks/` 目录下的文件锁创建/删除与 `readdirSync`、`statSync` 调用。
3. **版本升级批量迁移（`openclaw doctor --fix`）**：启动时扫描上千个历史 `.jsonl` 会话文件并批量写入 SQLite 数据库。

---

## 2. 压测脚本清单

| 脚本文件 | 用途说明 |
| :--- | :--- |
| `01-filestore-multishare.yaml` | 创建 Filestore Multishare StorageClass（支持单实例切分至多 80 张持久卷）。 |
| `02-precarve-workspaces.sh` | 批量预切员工持久化 PVC 脚本（避免将切盘操作放在用户实时等待路径上）。 |
| `03-setup-agent-volumes.sh` | 初始化 Filestore Agent Volumes 与 CSI 权限配置脚本。 |
| `04-test-agent-volumes-suite.sh` | 验证 Agent Volumes 挂载、跨 Pod 漂移持久性与隔离性的自动化测试套件。 |
| `05-run-sqlite-tiered-bench.sh` | **一键评测入口**：在集群内自动创建挂载 Filestore 与 Hyperdisk 的 gVisor 压测 Pod 并输出完整 Markdown 对比表。 |
| `openclaw-hyperdisk-fsync-benchmark.mjs` | **基准内核 1（裸 I/O）**：测试 1,000 × 1KB 文件 `writeFileSync`、`readFileSync`、`statSync`、`readdirSync`、`rmSync` 及强制刷盘 `create1k_fsync`。 |
| `openclaw-storage-sqlite-benchmark.mjs` | **基准内核 2（SQLite & 多通道并发）**：测试默认锁 vs `EXCLUSIVE` 独占锁、`1 ~ 17` 个飞书通道并发写延迟、主线程 EventLoop 阻塞以及 `openclaw doctor --fix` 升级耗时。 |

---

## 3. 执行压测步骤

### 方式一：集群内一键运行全套对比压测
```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export ZONE="us-central1-a"

bash ./05-run-sqlite-tiered-bench.sh
```

### 方式二：在已挂载目标存储的沙箱 Pod 内单独运行 Node.js 基准脚本
如果您已经有一个挂载了 Filestore（例如 `/mnt/filestore`）和块存储（例如 `/mnt/hyperdisk`）的 gVisor Pod，可直接拷贝 `.mjs` 脚本进容器执行：

```bash
# 1. 运行 1000x1KB 文件元数据与真落盘 fsync 测试
node openclaw-hyperdisk-fsync-benchmark.mjs --dir /mnt/filestore/bench
node openclaw-hyperdisk-fsync-benchmark.mjs --dir /mnt/hyperdisk/bench

# 2. 运行 SQLite PRAGMA 调优与 1~17 飞书通道并发压测
node openclaw-storage-sqlite-benchmark.mjs --filestore-dir /mnt/filestore/sqlbench --block-dir /mnt/hyperdisk/sqlbench
```

---

## 4. 建议重点观测与记录的 4 组实验数据

建议在客户环境中填写并对比以下 4 张实测记录表：

### 实验 ①：1,000 × 1KB 文件操作与 `create1k_fsync` 真落盘延迟
| 指标项 | Filestore (gVisor) 实测值 | Hyperdisk Balanced (gVisor) 实测值 | 参考基线 (Baseline) |
| :--- | :--- | :--- | :--- |
| `create1k_fsync` 单次真落盘 P50 | *(待填入)* | *(待填入)* | Filestore `~6.0ms` / Hyperdisk `~1.54ms` |
| 1000 次 `readdirSync` + `statSync` | *(待填入)* | *(待填入)* | 观察 NFS 元数据往返开销 |

### 实验 ②：Filestore 下 SQLite PRAGMA 独占锁调优前后对比
在 NFS (Filestore) 上运行单实例 OpenClaw 时，验证开启以下 PRAGMA 的加速效果：
```sql
PRAGMA journal_mode = TRUNCATE;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA synchronous  = NORMAL;
```
- **观测点**：对比开启 `EXCLUSIVE` 独占锁前后，省去每次事务通过网络向 NFS Server 申请/释放 POSIX 字节范围锁（`fcntl`）带来的延迟下降倍数（参考基线：单事务从 `~20.3ms` 降至 `~6.1ms`，提速约 **`3.2x`**）。

### 实验 ③：单实例绑定不同数量「飞书账号通道」时的并发拐点测试
模拟单个 OpenClaw 实例绑定 `1 → 8 → 10 → 13 → 17` 个飞书通道（按 30% 突发同时活跃率折算并发写事务），观察何时触发慢事务红线：

| 绑定飞书通道数 | 等效并发活跃通道 | Filestore SQLite P95 (ms) | Hyperdisk SSD SQLite P95 (ms) | 状态判定标准 |
| :--- | :--- | :--- | :--- | :--- |
| **1 ~ 8 通道** | 1 ~ 3 并发 | *(待填入，参考 `<10ms`)* | `< 0.1 ms` | 🟢 **健康区**：Filestore 完全满足日常办公 |
| **9 ~ 12 通道** | 3 ~ 4 并发 | *(待填入，参考 `10~15ms`)* | `< 0.1 ms` | 🟡 **预警区**：NFS 锁排队显现 |
| **≥ 13 通道 (如 17 通道)** | ≥ 5 并发 | *(待填入，参考 `>15~25ms`)* | `< 0.1 ms` | 🔴 **触发热升配**：建议切至 Hyperdisk SSD |

### 实验 ④：版本升级 `openclaw doctor --fix` 本地 `emptyDir` 加速验证
对比直接在 Filestore 挂载目录上扫描 2,000 个 `.jsonl` 文件写入 SQLite，与 **先在 `initContainer` 的内存/本地 `emptyDir` 完成 SQLite 建库再顺序回写 Filestore** 的总耗时差异（参考基线：`44.8s` $ightarrow$ `4.67s`，提速约 **`9.6x`**）。
