# 普通 Filestore（无需 VolumePool 白名单）+ 双 `emptyDir` 热缓 + `WarmPool` 晚绑定完整操作指南

> **适用场景**：当 GCP 项目未开通 `Filestore Agent Volumes (VolumePool)` 白名单，或处于预览期（EAP）结束、等待正式版（GA）白名单期间，使用**无需任何白名单的普通 Filestore 实例**，零代码改动跑通 **GKE `gVisor` + `SandboxWarmPool` 秒级认领 + 双 `emptyDir`（`tmpfs` 热 SQLite）+ 0.2s 晚绑定** 全流程。

---

## 一、核心疑问解答（FAQ）

### 1. 普通 Filestore 最小容量 1TB 贵吗？和 VolumePool 切片有什么区别？容器内路径对应关系是什么？
* **PoC 测试成本极低**：普通 `Basic HDD`（`tier: standard`）最小容量为 `1Ti`，单价为 `$0.16 / GB / 月`（按小时计费，**每小时仅约 `$0.22` / 约 1.6 元人民币**）。测试完执行 `kubectl delete pvc manual-pvc-normal-1` 即刻自动销毁底层 Filestore 实例并停止计费。
* **与 `VolumePool` 的区别**：
  * **`VolumePool`（需白名单，预计下周 GA）**：在 **K8s PVC 层**自动切片，1TB 池子可按需切出多张独立的 `20Gi` 小 PVC。
  * **普通 Filestore（无需白名单，本指南方案）**：申请 `manual-pvc-normal-1` 时直接创建 1 台独立的 `1Ti` Filestore 实例（在 PV 中的共享名为 `/vol1`），**昨天的晚绑定脚本一行都不用改逻辑**，只需把 `PVC_NAME` 换成 `"manual-pvc-normal-1"` 即可直接挂载验证（若需在一张普通 PVC 上隔离多个员工，也可在 `NFS_SHARE` 后追加子目录）。
* **容器内 OpenClaw（龙虾）目录与挂载路径对应关系**：
  * **`/home/node/.openclaw`（容器内 OpenClaw 主目录 / `tmpfs` 内存热盘）**：对应 `sqlite-hot-dir`（`emptyDir`），存放 `openclaw.json`、热态 `SQLite/WAL` 数据库及运行态文件。
  * **`/workspace/user_data`（容器内 Filestore 晚绑定挂载点）**：`storage-node-daemon` 将 Filestore PVC 的共享目录（`/vol1`）直接挂载到容器内的 `/workspace/user_data`。`wrapper.mjs` 会自动在 `/home/node/.openclaw` 与 `/workspace/user_data` 之间同步热状态并建立冷数据软链（`workspace`、`workspace-<id>`、`agents/<id>/sessions`、`media`、`browser` 等）。

### 2. Filestore 创建后的 IP 在哪里？为什么晚绑定要读 IP，而普通挂载不用？
* **IP 在哪里**：当 K8s 的 `PVC` 变为 `Bound` 后，Filestore CSI 驱动会自动把分配好的内网 IP 写入对应 `PV`（PersistentVolume）对象的 `.spec.csi.volumeAttributes.ip` 字段中，脚本直接通过 `kubectl get pv` 自动读取，无需人工去控制台查找。
* **为什么晚绑定要通过 IP 挂载**：
  * **常规 K8s 静态挂载（不走 WarmPool 晚绑定）**：直接在 Pod `spec.volumes` 里写 `persistentVolumeClaim: { claimName: xxx }` 即可，Pod 启动时 Kubelet 会自动调用 CSI 驱动读取 PV 里的 IP 完成挂载。
  * **WarmPool 晚绑定（Late-Bind）**：因为 `SandboxWarmPool` 里的预热 Pod 已经提前启动运行，而 K8s 原生不允许在不重启 Pod 的前提下向运行中 Pod 的 `spec.volumes` 动态插入新 PVC。因此认领后由脚本自动从 `PVC/PV` 中提取 IP 和共享路径，交由节点 `storage-node-daemon` 在 `0.2s` 内直接注入到运行中沙箱的 `runsc-gofer` 挂载空间。

---

## 二、从 0 到 1 完整操作步骤

### Step 1：创建普通 Filestore StorageClass 与 1Ti PVC

查看集群当前所在的 VPC 网络名称（若为默认网络则为 `default`），修改 [`20-normal-filestore-pvc.yaml`](./20-normal-filestore-pvc.yaml) 中的 `network` 参数并执行：

```bash
# 如需替换为您的实际 VPC 名称（例如 my-vpc），可直接 sed 替换后 apply：
VPC_NAME="default"   # <--- 修改为您 GKE 集群所在的 VPC 名称
sed "s/network: default/network: ${VPC_NAME}/" 20-normal-filestore-pvc.yaml | kubectl apply -f -
```

等待 PVC 状态变为 `Bound`（因为后台正在新建一台 1TB 普通 Filestore 实例，**首次创建需等待约 5 至 8 分钟**；一旦变为 `Bound`，后续所有沙箱晚绑定均只需 `0.2 秒`）：

```bash
kubectl get pvc manual-pvc-normal-1 -w
```

当看到 `STATUS` 变为 `Bound` 后，可通过以下命令确认 K8s 已自动将 Filestore IP 记录在 PV 中：

```bash
PV_NAME=$(kubectl get pvc manual-pvc-normal-1 -o jsonpath='{.spec.volumeName}')
echo "PV Name:      $PV_NAME"
echo "Filestore IP: $(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeAttributes.ip}')"
echo "Share Path:   /$(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeAttributes.volume}')"
```

---

### Step 2：部署晚绑定 DaemonSet 与双 `emptyDir` 预热池

> **重要提示**：即使之前已部署过 `17` 和 `18`，也建议重新执行一次 `kubectl apply` 以加载最新的**普通 Filestore 子目录自动创建**与**新员工空卷默认 `openclaw.json` 初始化**增强逻辑。

```bash
kubectl apply -f 17-latebind-agent-volumes.yaml
kubectl apply -f 18-dual-emptydir-hot-sqlite-warmpool.yaml

# 若之前已运行过旧版 DaemonSet / WarmPool，重建一下预热池 Pod 以挂载最新 ConfigMap
kubectl rollout status ds/storage-node-daemon
kubectl delete pods -l app=dual-emptydir-hot-sqlite --ignore-not-found
kubectl get pods -l app=dual-emptydir-hot-sqlite
```

---

### Step 3：创建 `SandboxClaim` 秒级认领沙箱，并执行 `0.2s` 晚绑定

#### 3.1 认领一台预热沙箱（以员工 `18652181` 为例，自动兼容 `v1alpha1` / `v1beta1` CRD）

```bash
EMP_ID="18652181"
CLAIM_NAME="claim-${EMP_ID}"

# 自动检测集群当前安装的 Agent Sandbox CRD 使用 warmPoolRef 还是 sandboxTemplateRef
if kubectl explain sandboxclaim.spec 2>/dev/null | grep -q "warmPoolRef"; then
  SPEC_BLOCK="warmPoolRef: { name: dual-emptydir-hot-sqlite-pool }"
else
  SPEC_BLOCK="sandboxTemplateRef: { name: dual-emptydir-hot-sqlite-template }"
fi

kubectl apply -f - <<EOF
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxClaim
metadata:
  name: ${CLAIM_NAME}
  namespace: default
spec:
  ${SPEC_BLOCK}
EOF

# 获取认领出来的 Sandbox Pod 名称
SB_NAME=$(kubectl get sandboxclaim "$CLAIM_NAME" -o jsonpath='{.status.sandbox.Name}{.status.sandbox.name}')
if kubectl get pod "$SB_NAME" >/dev/null 2>&1; then
  POD_NAME="$SB_NAME"
else
  POD_NAME=$(kubectl get pod -l "agents.x-k8s.io/sandbox-name=${SB_NAME}" -o jsonpath='{.items[0].metadata.name}')
fi
echo "Claimed Sandbox Pod: ${POD_NAME}"
```

#### 3.2 执行晚绑定（与昨天脚本 100% 一致，仅将 `PVC_NAME` 换为 `manual-pvc-normal-1`）

直接复制运行以下脚本（自动从 `manual-pvc-normal-1` 对应的 `PV` 提取 Filestore IP 与共享路径，挂载到沙箱容器内的 `/workspace/user_data`，并由 `wrapper.mjs` 自动与 `/home/node/.openclaw` 内存热盘同步）：

```bash
# 1. 填入认领出来的 Pod 名，以及刚创建的普通 Filestore PVC 名
POD_NAME="dual-emptydir-hot-sqlite-pool-xxxxx"
PVC_NAME="manual-pvc-normal-1"
NS="default"

# 2. 自动获取 Pod UID、所在节点 Daemon Pod，以及 PVC 对应的 NFS IP 和路径
POD_UID=$(kubectl get pod "$POD_NAME" -n "$NS" -o jsonpath='{.metadata.uid}')
NODE_NAME=$(kubectl get pod "$POD_NAME" -n "$NS" -o jsonpath='{.spec.nodeName}')
DAEMON_POD=$(kubectl get pod -n "$NS" -l app=storage-node-daemon --field-selector "spec.nodeName=${NODE_NAME}" -o jsonpath='{.items[0].metadata.name}')

PV_NAME=$(kubectl get pvc "$PVC_NAME" -n "$NS" -o jsonpath='{.spec.volumeName}')
NFS_IP=$(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeAttributes.ip}')
NFS_SHARE="/$(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeAttributes.volume}')"

echo "Binding ${NFS_IP}:${NFS_SHARE} -> Pod ${POD_NAME} (${POD_UID}) at /workspace/user_data via ${DAEMON_POD}..."

# 3. 调用当前节点 storage-node-daemon 执行 0.2s 晚绑定（带 force_rebind: true 自动接管旧租约）
kubectl exec -i -n "$NS" "$DAEMON_POD" -c daemon -- python3 - <<EOF
import json, urllib.request
req = urllib.request.Request(
    "http://127.0.0.1:9090",
    data=json.dumps({
        "action": "bind_nfs",
        "pod_uid": "${POD_UID}",
        "volume_name": "workspace-volume",
        "sub_dir": "user_data",
        "nfs_server": "${NFS_IP}",
        "nfs_path": "${NFS_SHARE}",
        "force_rebind": True
    }).encode(),
    headers={"Content-Type": "application/json"}
)
print(urllib.request.urlopen(req, timeout=25).read().decode())
EOF

# 4. 给 Pod 打上 PVC 标签，方便通过 kubectl get pods -L latebind-pvc 查询
kubectl label pod "$POD_NAME" -n "$NS" latebind-pvc="${PVC_NAME}" --overwrite
```

#### 3.3 验证 OpenClaw（龙虾）启动与双层存储状态

```bash
kubectl logs "$POD_NAME" --tail=30
```
看到 `"Restored hot state from Filestore"` 与 `"Started main process"`，随后出现 `[gateway] ready`，即表示热缓恢复与 OpenClaw 启动成功。

> **排错提示（实测踩坑）**：若 `kubectl logs` 中反复出现 `Waiting for Filestore mount to become writable`，且 `kubectl get pod` 显示 `RESTARTS` 持续增加，请先 `kubectl logs "$POD_NAME" -p` 查看上一次容器退出原因。最常见的是 OpenClaw 以 `exit 78` 退出：`Refusing to bind gateway to lan without auth`（空白新卷上没有带 `gateway.auth.token` 的 `openclaw.json`）。容器一旦重启，`runsc-gofer` 会被重建，之前注入的 NFS 挂载随之丢失，需要重新执行 `3.2` 的 `bind_nfs`。最新版 `18-dual-emptydir-hot-sqlite-warmpool.yaml` 已在空卷首次启动时自动生成带随机 `gateway.auth.token` 的最小 `openclaw.json`（也可通过 `OPENCLAW_GATEWAY_TOKEN` 环境变量指定），请确保 Step 2 已 `kubectl apply` 最新版本并重建预热池 Pod。

---

## 三、日常休眠唤醒（Suspend / Resume）与销毁清理

1. **日常闲置回收（走 Snapshot / Suspend 与 Resume）**：
   * **休眠（Suspend）**：直接挂起停止旧 Pod 即可（容器收到 `SIGTERM` 会自动将 `tmpfs` 热数据刷回 `/workspace/user_data`，随后内核自动销毁 `runsc-gofer` 挂载空间并卸载 NFS；**切勿在容器退出前先调 `unbind`**）。
   * **唤醒（Resume）**：新 Pod 恢复或认领后，再次执行上方 `3.2` 的 `bind_nfs` 脚本（内置 `"force_rebind": True` 会自动接管该员工子目录租约并恢复数据）。
2. **测试结束彻底清理资源（停止 Filestore 计费）**：
   ```bash
   # 删除测试认领的沙箱
   kubectl delete sandboxclaim "claim-${EMP_ID}" --ignore-not-found

   # 若不再需要测试，删除 1Ti 普通 Filestore PVC 即可自动销毁底层 Filestore 实例并停止计费
   kubectl delete pvc manual-pvc-normal-1
   ```
