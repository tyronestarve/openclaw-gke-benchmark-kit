# OpenClaw on GKE Agent Sandbox — 存储与弹性性能/成本实测套件

本仓库提供一套面向生产评估的 **可复现压测工具链、配置清单与成本测算工具**，用于验证大规模（千人至万人级）**OpenClaw AI Agent 沙箱（基于 GKE + gVisor 强隔离）** 在以下两个核心维度的表现：

1. **持久化存储选型与性能测试（Filestore NAS vs. Hyperdisk SSD）**：
   - 验证 OpenClaw `node:sqlite` (`DatabaseSync`) 与海量小文件会话（`session-locks/`、`.jsonl`）在不同存储介质上的 I/O 延迟、POSIX 文件锁竞争、SQLite PRAGMA 调优收益及多飞书通道并发上限。
2. **全局沙箱弹性供给性能与 TCO 成本测算（L1~L4 四级容量 + PodSnapshot 内存快照）**：
   - 验证热池秒级认领（`SandboxWarmPool`）、闲置内存快照休眠/唤醒（`PodSnapshot`）、挂起态节点缓冲（`CapacityBuffer`）的耗时与资源回收效率，并通过交互式控制台测算不同规模下的最佳计算+存储配比。

---

## 📂 仓库模块导航

| 目录 | 模块定位 | 核心交付物与用途 |
| :--- | :--- | :--- |
| **[`01-sandbox-deploy/`](./01-sandbox-deploy/)** | **沙箱底座与四级弹性部署** | 包含 GKE Standard / Autopilot 的 `SandboxTemplate`、`SandboxWarmPool`、`CapacityBuffer`（Active/Standby）YAML 清单及生命周期计时脚本 `pocfn.sh`。 |
| **[`02-filestore-sqlite-benchmark/`](./02-filestore-sqlite-benchmark/)** | **Filestore 与 SQLite 专项压测** | 包含 Filestore Multishare / Agent Volumes 部署脚本、Node.js `fsync` 1000×1KB 小文件基准脚本、1~17 飞书通道并发 SQLite 压测脚本及一键评测 Runner。 |
| **[`03-cost-and-sizing-guide/`](./03-cost-and-sizing-guide/)** | **性能评估标准与 TCO 成本模型** | 提供存储分层选型红线指标（4 大监控阈值）、Filestore 1TiB 保底分摊与 80/20 混合存储成本公式、以及计算层潮汐休眠 TCO 测算方法。 |
| **[`04-sandbox-portal/`](./04-sandbox-portal/)** | **可视化运营与成本推演控制台** | FastAPI + ECharts 沙箱管理门户源码（支持本地免集群离线启动体验），内置实时 SQLite 监控、一键热升配演练剧本及交互式成本模拟器。 |

---

## 🚀 30 分钟快速上手流程

### Step 1：本地免集群启动可视化控制台（最快体验性能实证与成本推演）
无需等待 GKE 集群创建，直接在本地启动 `04-sandbox-portal` 即可交互式体验 **80/20 混合存储成本推演滑块**、**4 大慢事务阈值告警** 与 **在线热升配演练**：
```bash
cd 04-sandbox-portal
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8890
# 浏览器打开 http://localhost:8890 （点击右上角「切换至管理员视角」即可操作全部 4 个 Tab）
```

### Step 2：在 GKE 集群执行 Filestore vs. Hyperdisk 实盘压测
参见 [`02-filestore-sqlite-benchmark/README.md`](./02-filestore-sqlite-benchmark/README.md)，在您的 GKE 集群（开启 `gVisor`）中一键拉起对比压测任务：
```bash
cd 02-filestore-sqlite-benchmark
export PROJECT_ID="your-gcp-project-id"
bash 05-run-sqlite-tiered-bench.sh
```

### Step 3：验证沙箱秒级认领与 PodSnapshot 内存休眠/唤醒
参见 [`01-sandbox-deploy/README.md`](./01-sandbox-deploy/README.md)，加载 `pocfn.sh` 工具函数测量各阶段真实端到端耗时：
```bash
source 01-sandbox-deploy/pocfn.sh
poc_claim emp001      # 测试从 WarmPool 秒级认领耗时
poc_hibernate emp001  # 测试触发 PodSnapshot 内存快照并释放 Pod 计算资源
poc_wake emp001       # 测试从 GCS 快照恢复内存态耗时
```
