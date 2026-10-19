# 03 — OpenClaw 沙箱存储与全局计算成本（TCO）评估指南

本文档提供评估大规模 OpenClaw 沙箱集群时的 **存储成本计算模型**、**监控自动升配阈值** 以及 **包含计算层与存储层的全局 TCO 测算公式**。您也可以直接启动 [`../04-sandbox-portal`](./../04-sandbox-portal/) 在 **【成本推演】Tab** 中使用交互式滑块实时测算。

---

## 一、 存储层成本与性能平衡模型（Filestore vs. Hyperdisk SSD）

### 1.1 两种存储介质的计费特性与边界

| 维度 | Tier 1：Filestore (NAS 共享存储池) | Tier 2：Hyperdisk Balanced / pd-balanced (SSD 块存储) |
| :--- | :--- | :--- |
| **最小起售与保底约束** | 单实例 **`1 TiB` 起步**（存在固定起步保底月费） | 单卷最小可按需分配（如 `10 GiB/人`，无大池保底） |
| **多沙箱空间复用能力** | **极高**：百人至千人可通过 `subPath` 或 Multishare 共享同一个容量池，按实际人均占用（如 `1~2 GiB/人`）高密度分摊 | **不可跨 Pod 共享写 (RWO)**：每个员工必须独立分配一块 PVC，即使只用 `500 MB` 也要按整块盘付费 |
| **并发挂载扩展性** | 原生 NFS 网络挂载，无单节点磁盘挂载数量上限 | 受限于单台 GCE VM 的 Persistent Disk 最大 attach 数量与并发挂载串行队列 |
| **I/O 性能与适用人群** | 经 `EXCLUSIVE` 锁调优后 P95 `< 10ms`，适合 **~80% 日常办公员工（绑定 1~8 个飞书通道）** | 极低延迟（写事务 `< 0.1ms`），适合 **~20% 重度高频用户/自动化群聊机器人（绑定 ≥13 个飞书通道）** |

### 1.2 为什么推荐「80/20 双层混合存储架构」？
- **当沙箱规模较小（如 `< 50` 人）时**：Filestore 的 `1 TiB` 保底容量无法被充分摊薄，此时直接使用 `pd-balanced` / `Hyperdisk` 起步总价更低。
- **当沙箱规模扩大（如 `500 ~ 9,000` 人）时**：
  - 若全员分配独立 `10 GiB` SSD 块存储，总容量将高达 `5 TiB ~ 90 TiB`，绝大部分轻度用户的磁盘空间与 IOPS 处于闲置浪费状态；
  - 采用 **「Tier 1 (80% 普通员工共享 Filestore 池) + Tier 2 (20% 重度用户独享 Hyperdisk SSD)」**：既用 80% 的基数吃满了 Filestore 的保底容量（将 Tier 1 人均存储成本压降至 **`~$0.30 ~ $0.35 /人/月`**），又为 20% 高并发用户提供了块存储的极致低延迟，综合存储成本相比全员独立 SSD **降低约 `60% ~ 68%`**。

---

## 二、 自动化存储分级：4 大实时监控与升配红线指标

为了不依赖人工经验判断“谁该用 Filestore、谁该升配 Hyperdisk SSD”，建议在控制面采集以下 **4 项实时遥测指标**，任一指标突破红线（🔴 Critical）即触发在线热升配（`rsync/sqlite3 .backup` + PVC 切换，停机窗口 `< 2s`）：

| 监控指标 | 🟢 健康 (保持 Tier 1 Filestore) | 🟡 预警 (观察哨) | 🔴 触发热升配 (切至 Tier 2 Hyperdisk SSD) |
| :--- | :--- | :--- | :--- |
| **① SQLite 写事务 P95 耗时** | `< 10 ms` | `10 ~ 15 ms` | **`> 15 ms`**（持续 3 分钟） |
| **② Node.js EventLoop P99 阻塞** | `< 250 ms` | `250 ~ 1000 ms` | **`> 1000 ms`**（用户感知明显卡顿） |
| **③ `session-locks/` 目录扫描耗时** | `< 15 ms` | `15 ~ 50 ms` | **`> 50 ms`**（NFS 元数据竞争加剧） |
| **④ 单实例绑定飞书账号通道数** | `1 ~ 8 个通道` | `9 ~ 12 个通道` | **`≥ 13 个通道`**（约等效 ≥5 路并发写） |

---

## 三、 超越存储：全局「计算 + 存储」综合性能与 TCO 成本模型

在 AI Agent 沙箱集群中，**计算资源（CPU / 内存）通常占总账单的 `80% ~ 85%`**，存储占 `15% ~ 20%`。评估整体方案时，需同时衡量 **四级弹性供给延迟** 与 **潮汐休眠带来的计算降本**。

### 3.1 全局弹性供给性能指标体系（L1 ~ L4）

| 供给层级 | 核心技术机制 | 目标响应耗时 | 解决的业务痛点与成本角色 |
| :--- | :--- | :--- | :--- |
| **L1 热池现货认领** | `SandboxWarmPool` + `SandboxClaim` | **`< 1 秒`** | **负责用户体验**：预热少量待命 Pod，新用户认领 0 等待，后台异步补池。 |
| **L2 休眠快照恢复** | `PodSnapshot` (内存页冻结至 GCS) | **`~3 ~ 8 秒`** | **负责波谷省钱**：员工闲置/下班后将内存态存盘并销毁 Pod，释放 100% CPU/内存；唤醒时免冷启动。 |
| **L3 挂起节点缓冲** | `CapacityBuffer` (Standby Suspended VM) | **`~30 ~ 50 秒`** | **负责早高峰突发**：预置处于 GCE 挂起态的节点（**挂起期间免收 vCPU/RAM 费用**），比原生买新节点快 2~3 倍。 |
| **L4 节点自动扩缩** | GKE Autopilot / NAP | **`~90 ~ 110 秒`** | **负责极限兜底**：超预期流量下自动向云端申请新计算节点，波谷自动回收空节点。 |

### 3.2 全局单用户月度 TCO 测算公式

对于总员工数 $N$、单沙箱规格 $(C_{	ext{cpu}}, M_{	ext{mem}})$（例如 `0.5 vCPU + 2 GiB RAM`）：

1. **计算层月成本 ($Cost_{	ext{compute}}$)**：
   - **全天候常驻模式（不休眠）**：
     $$Cost_{	ext{compute\_always\_on}} = N 	imes 	ext{Price}_{	ext{pod\_monthly}}(0.5	ext{ vCPU}, 2	ext{ GiB})$$
   - **潮汐休眠模式（开启 `PodSnapshot` 自动休眠 + 错峰复用）**：
     设工作日日均活跃时长占比为 $lpha$（如每天活跃 6~8 小时，折算周均在线收敛比 $lpha pprox 0.22 \sim 0.28$），加上少量 L1 `WarmPool` 水位 $W$：
     $$Cost_{	ext{compute\_elastic}} = (N 	imes lpha + W) 	imes 	ext{Price}_{	ext{pod\_monthly}} + N 	imes 	ext{Cost}_{	ext{gcs\_snapshot}}$$
     *(注：单份压缩后内存快照在 GCS Standard/Nearline 存储月费通常 `< $0.03/人/月`，计算层成本可直接降低 **`70%+`**)*。

2. **存储层月成本 ($Cost_{	ext{storage}}$)**：
   设 Tier 1 (Filestore) 用户比例为 $p_1$（默认 `80%`），Tier 2 (Hyperdisk SSD) 用户比例为 $p_2 = 1 - p_1$（默认 `20%`）：
   $$Cost_{	ext{storage}} = \max(	ext{Filestore 1TiB 保底费}, \; N 	imes p_1 	imes S_{	ext{avg\_gb}} 	imes 	ext{Price}_{	ext{filestore\_gb}}) + (N 	imes p_2 	imes S_{	ext{ssd\_gb}} 	imes 	ext{Price}_{	ext{hyperdisk\_gb}})$$

3. **可视化交互测算**：
   运行 `04-sandbox-portal` 并切换至 **【成本推演】Tab**，即可动态调整员工总数、Tier 1/Tier 2 比例与日均活跃度，实时生成对比图表。
