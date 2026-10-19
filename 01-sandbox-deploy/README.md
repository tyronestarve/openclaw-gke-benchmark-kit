# 01 — GKE gVisor 沙箱底座与四级弹性容量部署指南

本目录提供 OpenClaw 在 GKE（Standard 与 Autopilot 模式）下的沙箱模板、预热池（`SandboxWarmPool`）、节点容量缓冲池（`CapacityBuffer`）以及端到端耗时测量工具 `pocfn.sh`。

---

## 1. 清单文件说明

| 文件 | 适用集群 | 说明 |
| :--- | :--- | :--- |
| `01-template-pool-standard.yaml` | GKE Standard | 定义 `SandboxTemplate`（启用 `runtimeClassName: gvisor`）与 `SandboxWarmPool` 热池。 |
| `02-template-pool-autopilot.yaml` | GKE Autopilot | 包含自定义 `ComputeClass` (`sandbox-snap`) 与支持快照的 Autopilot 沙箱模板。 |
| `03-capacity-buffers-autopilot.yaml` | GKE Autopilot | 定义 L2 Active 热备缓冲与 L3 Standby（GCE Suspended VM 挂起态）容量缓冲。 |
| `04-capacity-buffers-standard.yaml` | GKE Standard | Standard 集群下的节点池容量缓冲配置。 |
| `05-router-gateway.yaml` | 通用 | 沙箱路由与 Gateway API 暴露模板。 |
| `pocfn.sh` | 通用 | 命令行测试与计时工具集（测量认领、冷启、休眠、唤醒真实墙钟耗时）。 |

---

## 2. 核心性能测试项与操作命令

在部署模板前，请先设置您的项目变量并替换 YAML 中的 `${PROJECT_ID}`：
```bash
export PROJECT_ID="your-gcp-project-id"
envsubst < 01-template-pool-standard.yaml | kubectl apply -f -
```

加载测试辅助脚本：
```bash
source ./pocfn.sh
```

### 测试项 A：L1 热池认领耗时（目标：`< 1s` 绑定）
```bash
# 确保 SandboxWarmPool 已有 Ready 副本后执行认领：
poc_claim test001
```
- **观测指标**：输出的 `bound` 时间（K8s 将预热 Pod 划拨给该工号的耗时）与 `serviceable` 时间（HTTP 探针 200 可打开界面的耗时）。

### 测试项 B：L2 内存快照休眠与恢复耗时（`PodSnapshot`）
```bash
# 1. 将运行中的沙箱触发内存快照并释放 Pod（休眠后 CPU/内存计费归零）：
poc_hibernate test001

# 2. 唤醒沙箱（验证从 GCS 内存快照直接恢复进程状态，跳过 Node.js 冷启动）：
poc_wake test001
```
- **观测指标**：对比从 `PodSnapshot` 恢复的耗时（通常 `3s ~ 8s`）与无快照冷启动耗时（通常 `25s ~ 35s`），并检查容器事件中是否包含 `Successfully restored from snapshot`。

### 测试项 C：L3 挂起节点缓冲池扩容测试（`CapacityBuffer`）
```bash
kubectl apply -f 03-capacity-buffers-autopilot.yaml
kubectl get capacitybuffers
```
- **观测指标**：当热池耗尽触发补池时，对比从 L3 挂起节点（Suspended VM，挂起期免收计算费）唤醒补充容量的耗时（约 `30s ~ 50s`）与 L4 原生新建节点（NAP，约 `90s ~ 110s`）的差异。
