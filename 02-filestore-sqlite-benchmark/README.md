# 02 — Filestore Agent Volumes (`VolumePool`) 与 Hyperdisk 存储性能及 SQLite 并发压测指南

本目录用于系统性测试 OpenClaw 在 **`gVisor` 安全沙箱** 内使用不同持久化存储介质（**开白后的 Filestore Agent Volumes (`VolumePool`)** vs. **Hyperdisk Balanced / SSD 块存储**）时的真实 I/O 性能与成本边界。

---

## 1. 已有 GKE 集群的客户：从哪一步开始启用 `VolumePool`？

如果您**已经部署好了 GKE 集群、VPC 和子网**（并且项目已完成 Filestore Agent Volumes 开白，甚至已经创建了 PSC Service Connection Policy），**不需要从头建集群**！

### 方式 A：直接运行专为「已有 GKE 集群」精简的极简脚本 `03b-setup-agent-volumes-existing-gke.sh`（推荐）
我们已为您将冗余的创建 VPC、子网、新建 GKE 集群的命令全部剔除，整理为精简脚本 **`03b-setup-agent-volumes-existing-gke.sh`**（直接从原 `Step 3` 开始，若集群名填错只会给出提示并安全退出，绝不会误建新集群）：
1. **检查现有 GKE 集群并关闭冲突的内置 `GcpFilestoreCsiDriver` 插件**；
2. **复用已有的 PSC `service-connection-policies`**（若尚未创建则自动创建）；
3. **调用 REST API 创建「单实例 × 1 TiB」省配额版 `VolumePool`**（节省 95% 容量配额与成本）；
4. **部署支持 `volumepool` 的 CSI 驱动，并创建带 14 项 NFS 极限性能调优参数（`nocto, local_lock=all, actimeo=600, nconnect=8, async` 等）的 `StorageClass` (`volume-pool-sc` / `volume-pool-1tb-tuned-sc`)**。

只需传入您现有的集群与网络名称即可一键跑通：
```bash
export PROJECT_ID="your-gcp-project-id"          # 您的 GCP 项目 ID
export REGION="asia-southeast1"                  # VolumePool 所在 Region
export CLUSTER_LOCATION="asia-southeast1"        # 现有 GKE 集群所在 Region 或 Zone
export CLUSTER="your-existing-gke-cluster"       # 现有 GKE 集群名称
export VPC_NETWORK="your-existing-vpc"           # 现有 VPC 名称
export SUBNET_NAME="your-existing-subnet"        # 现有子网名称

bash ./03b-setup-agent-volumes-existing-gke.sh
```

### 方式 B：如果您想手动分步执行（仅执行已有 GKE 集群缺失的 3 步）
如果您已经手动跑过了 `gcloud network-connectivity service-connection-policies create`，只需依次执行脚本中的 **`Step 3`（关内置 CSI 插件） $\rightarrow$ `Step 7`（curl 创建 1TiB VolumePool） $\rightarrow$ `Step 8`（装 volumepool CSI 驱动 + 创建调优 StorageClass）**。

---

## 2. 压测脚本与配置文件清单

| 文件名 | 用途说明 |
| :--- | :--- |
| **`03b-setup-agent-volumes-existing-gke.sh`** | **【首选 · 已有 GKE 集群极简版】直接从原 Step 3 开始（剔除新建 VPC/子网/GKE 集群冗余代码），一键完成内置插件关闭、1TiB VolumePool 创建与 14 项调优 `StorageClass` 部署**。 |
| `03-setup-agent-volumes.sh` | （从零新建环境完整版）包含从新建 VPC/子网/GKE 集群到初始化 VolumePool 的全套流程。 |
| **`04-test-agent-volumes-suite.sh`** | 验证 `VolumePool` 秒级建卷风暴 (`t1`)、沙箱复挂 (`t2`)、WarmPool 认领 (`t3`)、动态晚绑定 (`t4`) 与裸 I/O (`t5`) 的自动化测试套件。 |
| **`latebind-example/17-latebind-agent-volumes.yaml`** | **生产安全加固版 `storage-node-daemon`**：采用 `open_tree + move_mount` 注入单 Pod `runsc-gofer` 挂载空间、`init_net` 宿主机网络锚定、`hard` 挂载 + `MNT_FORCE` 卸载及 `O_EXCL` 服务端原子租约。 |
| **`latebind-example/18-dual-emptydir-hot-sqlite-warmpool.yaml`** | **【双 `emptyDir` 冷热分层 + WarmPool 模板】**：卷 1 (`force-shared: "true"`) 动态挂载远端 Filestore `VolumePool`，卷 2 (默认 `emptyDir`) 提供 `gVisor` 原生 `tmpfs` (`0.06ms` 原生 `WAL`)，内置 `POD_UID` 幂等恢复、`sqlite.backup()` 在线原子热备与 `SIGTERM` 退出零丢失刷盘。 |
| **`latebind-example/19-test-dual-emptydir-e2e.sh`** | **【一键闭环验证脚本】**：全自动串联 WarmPool 亚秒级认领、`statfs` 文件系统魔数校验、`WAL` vs `DELETE` 性能对比、在线原子备份校验、`SIGTERM` 退出刷盘与跨 Pod 毫秒级 `RPO=0` 恢复。 |
| **`node-sqlite-benchmark.js`** | **【核心推荐】单文件零依赖 SQLite 1,000 次真实事务压测脚本**：直接对比同一个挂载目录上「默认配置」vs「开启 `TRUNCATE + EXCLUSIVE + NORMAL` 独占锁调优」的 P50/P95 延迟与 5 并发/17 并发主线程排队耗时。 |
| **`sqlite-pragma-preload.cjs`** | **零代码侵入 SQLite 调优预加载钩子**：通过 `NODE_OPTIONS="--require .../sqlite-pragma-preload.cjs"` 自动为 OpenClaw 注入最优 `PRAGMA`。 |
| `01-filestore-multishare.yaml` | （备选：未开白项目使用）传统 Filestore Enterprise Multishare StorageClass（单实例最多切 80 张卷）。 |
| `02-precarve-workspaces.sh` | （备选：未开白项目使用）传统 Multishare 批量预切 PVC 脚本。 |
| `05-run-sqlite-tiered-bench.sh` | 集群内一键创建 Filestore 与 Hyperdisk 对比压测 Pod 并输出完整 Markdown 报告。 |
| `openclaw-hyperdisk-fsync-benchmark.mjs` | 测试 1,000 × 1KB 文件 `writeFileSync`、`readFileSync`、`statSync`、`readdirSync`、`rmSync` 及强制刷盘 `create1k_fsync`。 |
| `openclaw-storage-sqlite-benchmark.mjs` | 完整四模块基准套件（含 `openclaw doctor --fix` 2,000 会话文件 `emptyDir` 预合并加速验证）。 |

---

## 3. 快速运行 OpenClaw 真实 SQLite 1,000 次事务压测

在任何已挂载存储卷的 OpenClaw 容器内，直接执行：

```bash
node node-sqlite-benchmark.js /home/node/.openclaw/workspace
```

即可在同一目录下一次性跑出：
1. **调优前默认配置 (`DELETE + NORMAL + FULL`)** 的 1,000 次写事务耗时与多通道排队延迟；
2. **开启 SQLite 独占锁调优 (`TRUNCATE + EXCLUSIVE + NORMAL`)** 后的提速对比；
3. **块存储 `WAL + NORMAL` 模式** 下的极限吞吐对比。

---

## 4. 一键验证「Filestore + 双 `emptyDir` 冷热分层 + `SandboxWarmPool`」全链路方案

在已完成 `03b-setup-agent-volumes-existing-gke.sh`（或已有任意 Filestore NFS 共享）的 GKE Standard 集群上，直接运行：

```bash
cd latebind-example
chmod +x 19-test-dual-emptydir-e2e.sh

# 方式 A：自动从 volume-pool-sc 创建 1Gi 测试卷并跑完 5 项端到端验收（跑完自动清理）
./19-test-dual-emptydir-e2e.sh

# 方式 B：如果尚未创建 VolumePool，也可直接指定现有 Filestore 实例 IP 与子目录测试
NFS_SERVER="<Filestore_IP>" NFS_PATH="/vol1/test-user" ./19-test-dual-emptydir-e2e.sh
```

脚本会在约 2 分钟内自动完成并输出 **5 项闭环验收结果**：
1. **`[Check 1]` WarmPool 亚秒级认领 + `bind_nfs` 动态挂载耗时**；
2. **`[Check 2]` `gVisor` 文件系统类型实证**（`/workspace/user_data` 为 `9p 0x01021997` vs `/sqlite-hot-dir` 为 `tmpfs 0x01021994`）及 **SQLite `WAL` (`~0.06ms`) vs `DELETE` (`~23ms`) 实测对比**；
3. **`[Check 3]` 在线原子热备校验**：前台写入 200 笔对话后，校验远端 Filestore 备份库 `PRAGMA integrity_check = ok`；
4. **`[Check 4]` `SIGTERM` 优雅回收刷盘（`RPO = 0`）**：追加 77 笔尚未到定时周期的增量后立即删除 `SandboxClaim`，验证退出钩子全量刷盘并安全 `unbind`；
5. **`[Check 5]` 跨 Pod 重连恢复**：从 `SandboxWarmPool` 认领第二台预热 Pod 挂载同一员工卷，验证 `277/277` 笔记录毫秒级完整恢复且运行在 `journal_mode = wal`。
