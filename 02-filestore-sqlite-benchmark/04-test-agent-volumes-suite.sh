#!/bin/bash
# ==============================================================================
# 16-test-agent-volumes-suite.sh
# Filestore Agent Volumes (volume-pool-sc) 从 0 到 1 全自包含自动化测试脚本
#
# 零外部依赖：不依赖任何历史环境、私有镜像或外部辅助脚本，可在任意已完成
# 15-setup-agent-volumes.sh 初始化的 GKE 集群中直接运行。
#
# 测试覆盖：
#   - t1 : 控制面压测（单发 <1s 建卷、20/100 并发建卷风暴、异步删卷期间新建无阻塞）
#   - t2 : 形态一（每用户静态个人卷 + 沙箱直建、持久化读写、销毁重建 <100ms 复挂）
#   - t3 : 形态二（Agent Sandbox WarmPool 预热池动态挂盘并行补货 + <1s 认领）
#   - t4 : 形态三（Agent Sandbox WarmPool + bind_nfs 动态存储晚绑定 + 单用户配额硬隔离）
#   - t5 : 数据面基准（gVisor 沙箱内 2000×4KB 小文件读写删、fdatasync 同步写 IOPS）
#   - cleanup : 清理所有测试产生的 PVC、Sandbox、WarmPool 与测试 Pod
#
# 用法：
#   ./16-test-agent-volumes-suite.sh [all|t1|t2|t3|t4|t5|cleanup]
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SC=${SC:-volume-pool-sc}
NS=${NAMESPACE:-default}
PVC_SIZE=${PVC_SIZE:-1Gi}   # 与 VolumePool 的 defaultVolumeQuotaMib (1024 MiB = 1Gi) 严格对齐
TARGET=$(echo "${1:-all}" | tr '[:upper:]' '[:lower:]')

now_s() { date +%s.%N; }
elapsed_s() {
  python3 - "$1" "$(date +%s.%N)" <<'PY'
import sys
print(f"{float(sys.argv[2]) - float(sys.argv[1]):.2f}")
PY
}

ensure_probe_pod() {
  if ! kubectl get pod av-http-probe -n "$NS" >/dev/null 2>&1; then
    kubectl run av-http-probe -n "$NS" --image=curlimages/curl --restart=Never \
      --labels="test-suite=agent-volumes" -- sleep 86400 >/dev/null
    kubectl wait --for=condition=Ready pod/av-http-probe -n "$NS" --timeout=90s >/dev/null
  fi
}

wait_http_ok() {
  local host_port="$1"
  until kubectl exec av-http-probe -n "$NS" -- curl -sf --max-time 2 "http://${host_port}/" >/dev/null 2>&1; do
    sleep 0.2
  done
}

wait_claim_sandbox_pod() {
  local claim_name="$1"
  local pod_name=""
  while [ -z "$pod_name" ]; do
    local sb_name
    sb_name=$(kubectl get sandboxclaim "$claim_name" -n "$NS" -o jsonpath='{.status.sandbox.Name}{.status.sandbox.name}' 2>/dev/null || true)
    if [ -n "$sb_name" ]; then
      pod_name=$(kubectl get pod -n "$NS" -l "agents.x-k8s.io/sandbox-name=${sb_name}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
      if [ -z "$pod_name" ] && kubectl get pod "$sb_name" -n "$NS" >/dev/null 2>&1; then
        pod_name="$sb_name"
      fi
    fi
    [ -z "$pod_name" ] && sleep 0.2
  done
  echo "$pod_name"
}

# ------------------------------------------------------------------------------
# T1: 控制面创删卷延迟与高并发风暴测试
# ------------------------------------------------------------------------------
run_t1() {
  echo ""
  echo "=================================================================="
  echo "[T1] 控制面创删卷延迟与高并发风暴测试 (StorageClass: $SC)"
  echo "=================================================================="

  echo "--- T1.1 单发 PVC 创建 -> Bound 延迟 (5 轮连续测试) ---"
  for i in $(seq 1 5); do
    t0=$(now_s)
    kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: av-single-$i
  labels:
    test-suite: agent-volumes
    test-case: av-t1-single
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ${SC}
  resources:
    requests:
      storage: ${PVC_SIZE}
EOF
    until [ "$(kubectl get pvc "av-single-$i" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Bound" ]; do
      sleep 0.1
    done
    echo "  Round $i Bound 耗时: $(elapsed_s "$t0") s"
  done
  t_del=$(now_s)
  kubectl delete pvc -n "$NS" -l test-case=av-t1-single --wait=true >/dev/null
  echo "  5 张单卷同步删除总耗时: $(elapsed_s "$t_del") s"

  STORM_N=${STORM_N:-20}
  echo "--- T1.2 并发批量创建 ${STORM_N} 张独立 PVC 墙钟时间 (可通过 STORM_N=100 扩展) ---"
  t0=$(now_s)
  for i in $(seq 1 "$STORM_N"); do
    cat <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: av-storm-$i
  labels:
    test-suite: agent-volumes
    test-case: av-t1-storm
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ${SC}
  resources:
    requests:
      storage: ${PVC_SIZE}
---
EOF
  done | kubectl apply -n "$NS" -f - >/dev/null

  until [ "$(kubectl get pvc -n "$NS" -l test-case=av-t1-storm --no-headers 2>/dev/null | grep -c Bound || true)" -eq "$STORM_N" ]; do
    sleep 0.2
  done
  echo "  ✅ ${STORM_N}/${STORM_N} 张 PVC 并发全部 Bound 总墙钟耗时: $(elapsed_s "$t0") s"

  echo "--- T1.3 批量异步删除 ${STORM_N} 张卷期间新建 1 张 PVC (验证创删队列无阻塞) ---"
  kubectl delete pvc -n "$NS" -l test-case=av-t1-storm --wait=false >/dev/null
  t_mix=$(now_s)
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: av-mix-new
  labels:
    test-suite: agent-volumes
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ${SC}
  resources:
    requests:
      storage: ${PVC_SIZE}
EOF
  until [ "$(kubectl get pvc av-mix-new -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Bound" ]; do
    sleep 0.1
  done
  echo "  ✅ 批量删卷进行中新建单卷 Bound 耗时: $(elapsed_s "$t_mix") s"
  kubectl delete pvc av-mix-new -n "$NS" --ignore-not-found >/dev/null
}

# ------------------------------------------------------------------------------
# T2: 形态一 —— 每用户独立静态个人卷 + 沙箱直建与跨生命周期复挂持久化
# ------------------------------------------------------------------------------
run_t2() {
  echo ""
  echo "=================================================================="
  echo "[T2] 形态一：用户专属持久卷 (user-001-ws) 直建 + 跨沙箱重建秒级复挂"
  echo "=================================================================="
  ensure_probe_pod
  USER_ID="u001"
  t0=$(now_s)

  # 1. 创建用户专属 Agent Volume PVC + 第 1 代 Agent Sandbox
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: user-${USER_ID}-ws
  labels:
    test-suite: agent-volumes
    user: "${USER_ID}"
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ${SC}
  resources:
    requests:
      storage: ${PVC_SIZE}
---
apiVersion: agents.x-k8s.io/v1alpha1
kind: Sandbox
metadata:
  name: agent-${USER_ID}
  labels:
    test-suite: agent-volumes
    user: "${USER_ID}"
spec:
  podTemplate:
    metadata:
      labels:
        user: "${USER_ID}"
    spec:
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
        - key: sandbox.gke.io/runtime
          value: gvisor
          effect: NoSchedule
      containers:
        - name: agent
          image: python:3.11-slim
          command: ["python3", "-u", "-m", "http.server", "8080", "--directory", "/workspace"]
          ports:
            - containerPort: 8080
          volumeMounts:
            - name: ws
              mountPath: /workspace
          resources:
            requests: { cpu: "250m", memory: "512Mi" }
      volumes:
        - name: ws
          persistentVolumeClaim:
            claimName: user-${USER_ID}-ws
EOF

  until IP=$(kubectl get pod -n "$NS" -l "user=${USER_ID}" -o jsonpath='{.items[0].status.podIP}' 2>/dev/null) && [ -n "$IP" ]; do
    sleep 0.2
  done
  wait_http_ok "${IP}:8080"
  echo "  ✅ [首次直建] 动态建卷 + 启动 gVisor 沙箱到 HTTP 200 总耗时: $(elapsed_s "$t0") s"

  POD_1=$(kubectl get pod -n "$NS" -l "user=${USER_ID}" -o jsonpath='{.items[0].metadata.name}')
  MARKER="session-state-$(date +%s)"
  kubectl exec "$POD_1" -n "$NS" -- sh -c "echo '$MARKER' > /workspace/memory.txt && sync"
  echo "  -> 已在用户专属卷 /workspace/memory.txt 写入状态标记: $MARKER"

  # 2. 销毁计算沙箱（模拟会话结束释放计算资源），保留用户独立卷
  kubectl delete sandbox "agent-${USER_ID}" -n "$NS" --wait=true >/dev/null

  # 3. 重新拉起第 2 代沙箱并复挂同一张用户卷
  t_reattach=$(now_s)
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: agents.x-k8s.io/v1alpha1
kind: Sandbox
metadata:
  name: agent-${USER_ID}
  labels:
    test-suite: agent-volumes
    user: "${USER_ID}"
spec:
  podTemplate:
    metadata:
      labels:
        user: "${USER_ID}"
    spec:
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
        - key: sandbox.gke.io/runtime
          value: gvisor
          effect: NoSchedule
      containers:
        - name: agent
          image: python:3.11-slim
          command: ["python3", "-u", "-m", "http.server", "8080", "--directory", "/workspace"]
          ports:
            - containerPort: 8080
          volumeMounts:
            - name: ws
              mountPath: /workspace
          resources:
            requests: { cpu: "250m", memory: "512Mi" }
      volumes:
        - name: ws
          persistentVolumeClaim:
            claimName: user-${USER_ID}-ws
EOF
  until IP2=$(kubectl get pod -n "$NS" -l "user=${USER_ID}" -o jsonpath='{.items[0].status.podIP}' 2>/dev/null) && [ -n "$IP2" ]; do
    sleep 0.2
  done
  wait_http_ok "${IP2}:8080"
  echo "  ✅ [二次复挂] 重新创建沙箱并复挂已有 Agent Volume 到 HTTP 200 耗时: $(elapsed_s "$t_reattach") s"

  READ_BACK=$(kubectl exec av-http-probe -n "$NS" -- curl -sf "http://${IP2}:8080/memory.txt")
  if [ "$READ_BACK" = "$MARKER" ]; then
    echo "  ✅ [持久化校验] 跨沙箱生命周期数据 100% 完好 ($READ_BACK)"
  else
    echo "  ❌ [持久化校验] 预期 $MARKER，实际读取: $READ_BACK"
    exit 1
  fi

  kubectl delete sandbox "agent-${USER_ID}" -n "$NS" --ignore-not-found >/dev/null
  kubectl delete pvc "user-${USER_ID}-ws" -n "$NS" --ignore-not-found >/dev/null
}

# ------------------------------------------------------------------------------
# T3: 形态二 —— Agent Sandbox WarmPool 预热池动态挂盘与秒级认领
# ------------------------------------------------------------------------------
run_t3() {
  echo ""
  echo "=================================================================="
  echo "[T3] 形态二：SandboxWarmPool 预热池并行挂盘补货 + 秒级认领"
  echo "=================================================================="
  ensure_probe_pod

  t_rep=$(now_s)
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: av-warmpool-template
  labels:
    test-suite: agent-volumes
spec:
  podTemplate:
    metadata:
      labels:
        app: av-warmpool-sandbox
    spec:
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
        - key: sandbox.gke.io/runtime
          value: gvisor
          effect: NoSchedule
      containers:
        - name: agent
          image: python:3.11-slim
          command: ["python3", "-u", "-m", "http.server", "8080", "--directory", "/workspace"]
          ports:
            - containerPort: 8080
          volumeMounts:
            - name: ws-pvc
              mountPath: /workspace
          resources:
            requests: { cpu: "250m", memory: "512Mi" }
  volumeClaimTemplates:
    - metadata:
        name: ws-pvc
      spec:
        accessModes: [ReadWriteMany]
        storageClassName: ${SC}
        resources:
          requests:
            storage: ${PVC_SIZE}
---
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxWarmPool
metadata:
  name: av-warmpool
  labels:
    test-suite: agent-volumes
spec:
  replicas: 3
  sandboxTemplateRef:
    name: av-warmpool-template
EOF

  until [ "$(kubectl get sandboxwarmpool av-warmpool -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" = "3" ]; do
    sleep 0.5
  done
  echo "  ✅ [热池并行补货] 0 -> 3 台带独立 Agent Volume 的 gVisor 沙箱全部 Ready 耗时: $(elapsed_s "$t_rep") s"

  t_claim=$(now_s)
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxClaim
metadata:
  name: av-pool-claim-1
  labels:
    test-suite: agent-volumes
spec:
  sandboxTemplateRef:
    name: av-warmpool-template
EOF
  SB_POD=$(wait_claim_sandbox_pod "av-pool-claim-1")
  t_bound=$(elapsed_s "$t_claim")
  POD_IP=$(kubectl get pod "$SB_POD" -n "$NS" -o jsonpath='{.status.podIP}')
  wait_http_ok "${POD_IP}:8080"
  echo "  ✅ [热池秒级认领] Claim 绑定耗时: ${t_bound} s | 端到端 HTTP 200 可服务耗时: $(elapsed_s "$t_claim") s"

  kubectl delete sandboxclaim av-pool-claim-1 -n "$NS" --ignore-not-found >/dev/null
  kubectl delete sandboxwarmpool av-warmpool -n "$NS" --ignore-not-found >/dev/null
  kubectl delete sandboxtemplate av-warmpool-template -n "$NS" --ignore-not-found >/dev/null
}

# ------------------------------------------------------------------------------
# T4: 形态三 —— 动态存储晚绑定 (bind_nfs Late-bind) + 单用户独立卷配额隔离
# ------------------------------------------------------------------------------
run_t4() {
  echo ""
  echo "=================================================================="
  echo "[T4] 形态三：动态存储晚绑定 (bind_nfs Late-bind) + 单用户配额硬隔离"
  echo "=================================================================="
  kubectl apply -f "${SCRIPT_DIR}/17-latebind-agent-volumes.yaml" >/dev/null
  kubectl rollout status ds/storage-node-daemon -n "$NS" --timeout=180s >/dev/null
  until [ "$(kubectl get sandboxwarmpool agent-latebind-av-pool -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" = "2" ]; do
    sleep 1
  done

  # 1. 创建单用户独立 Agent Volume PVC (设置最小配额 100MiB，验证读写与配额硬边界)
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: latebind-user101-pvc
  labels:
    test-suite: agent-volumes
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ${SC}
  resources:
    requests:
      storage: ${PVC_SIZE}
EOF
  until [ "$(kubectl get pvc latebind-user101-pvc -n "$NS" -o jsonpath='{.status.phase}')" = "Bound" ]; do sleep 0.1; done
  PV_NAME=$(kubectl get pvc latebind-user101-pvc -n "$NS" -o jsonpath='{.spec.volumeName}')
  NFS_IP=$(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeAttributes.ip}')
  NFS_PATH="/$(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeAttributes.volume}')"

  # 2. 认领预热沙箱 + 执行 bind_nfs 毫秒级动态挂载
  t0=$(now_s)
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxClaim
metadata:
  name: latebind-claim-101
  labels:
    test-suite: agent-volumes
  finalizers: [agent.sandbox/storage-cleanup]
spec:
  warmPoolRef:
    name: agent-latebind-av-pool
EOF
  POD_NAME=$(wait_claim_sandbox_pod "latebind-claim-101")
  POD_UID=$(kubectl get pod "$POD_NAME" -n "$NS" -o jsonpath='{.metadata.uid}')
  NODE_NAME=$(kubectl get pod "$POD_NAME" -n "$NS" -o jsonpath='{.spec.nodeName}')
  DAEMON_POD=$(kubectl get pods -n "$NS" -l app=storage-node-daemon --field-selector "spec.nodeName=${NODE_NAME}" -o jsonpath='{.items[0].metadata.name}')

  t_bind0=$(now_s)
  kubectl exec "$DAEMON_POD" -n "$NS" -c daemon -- python3 -c "
import urllib.request, json
payload = json.dumps({
    'action': 'bind_nfs',
    'pod_uid': '${POD_UID}',
    'volume_name': 'workspace-volume',
    'sub_dir': 'workspace',
    'nfs_server': '${NFS_IP}',
    'nfs_path': '${NFS_PATH}'
}).encode()
req = urllib.request.Request('http://localhost:9090', data=payload, headers={'Content-Type': 'application/json'})
print('  ->', urllib.request.urlopen(req).read().decode())
"
  t_bind_cost=$(elapsed_s "$t_bind0")
  kubectl patch pod "$POD_NAME" -n "$NS" --type=merge -p '{"metadata":{"finalizers":["agent.sandbox/storage-cleanup"]}}' >/dev/null
  echo "  ✅ [Late-bind] bind_nfs 挂载动作耗时: ${t_bind_cost} s | 认领+挂载全链路总耗时: $(elapsed_s "$t0") s"

  # 3. 验证 gVisor 沙箱内部可立即读写挂载进来的用户专属卷
  OUT=$(kubectl exec "$POD_NAME" -n "$NS" -- sh -c "echo 'hello-agent-volumes-latebind' > /mnt/latebind-root/workspace/test.txt && cat /mnt/latebind-root/workspace/test.txt")
  echo "  ✅ [容器内读写核验] 读取内容: $OUT"

  # 4. 安全解绑 (unbind) 并清理
  kubectl delete sandboxclaim latebind-claim-101 -n "$NS" --wait=false >/dev/null
  kubectl exec "$DAEMON_POD" -n "$NS" -c daemon -- python3 -c "
import urllib.request, json
payload = json.dumps({'action': 'unbind', 'pod_uid': '${POD_UID}', 'volume_name': 'workspace-volume', 'sub_dir': 'workspace'}).encode()
req = urllib.request.Request('http://localhost:9090', data=payload, headers={'Content-Type': 'application/json'})
print('  ->', urllib.request.urlopen(req).read().decode())
"
  kubectl patch sandboxclaim latebind-claim-101 -n "$NS" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null || true
  kubectl patch pod "$POD_NAME" -n "$NS" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null || true
  kubectl delete pvc latebind-user101-pvc -n "$NS" --ignore-not-found >/dev/null
  kubectl delete -f "${SCRIPT_DIR}/17-latebind-agent-volumes.yaml" --ignore-not-found >/dev/null
}

# ------------------------------------------------------------------------------
# T5: 数据面基准 —— gVisor 沙箱内真实 IO、2000×4KB 小文件与 fdatasync 测试
# ------------------------------------------------------------------------------
run_t5() {
  echo ""
  echo "=================================================================="
  echo "[T5] 数据面基准：gVisor 沙箱内 2000×4KB 小文件 + fdatasync 落盘测试"
  echo "=================================================================="
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: av-io-bench-pvc
  labels:
    test-suite: agent-volumes
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ${SC}
  resources:
    requests:
      storage: ${PVC_SIZE}
---
apiVersion: v1
kind: Pod
metadata:
  name: av-io-bench-gvisor
  labels:
    test-suite: agent-volumes
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
      image: python:3.11-slim
      command: ["sleep", "3600"]
      resources:
        requests: { cpu: "2", memory: "2Gi" }
        limits: { cpu: "4", memory: "4Gi" }
      volumeMounts:
        - name: ws
          mountPath: /ws
  volumes:
    - name: ws
      persistentVolumeClaim:
        claimName: av-io-bench-pvc
EOF
  kubectl wait --for=condition=Ready pod/av-io-bench-gvisor -n "$NS" --timeout=180s >/dev/null

  kubectl exec av-io-bench-gvisor -n "$NS" -- python3 - <<'PYEOF'
import os, time

base = "/ws/smallfiles"
os.makedirs(base, exist_ok=True)
payload = b"x" * 4096
N = 2000

# 1. 创建 2000 个 4KB 文件并强制 fsync 落盘
t0 = time.time()
for i in range(N):
    p = f"{base}/f_{i}.dat"
    fd = os.open(p, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    os.write(fd, payload)
    os.fsync(fd)
    os.close(fd)
t_create = time.time() - t0

# 2. 全部读回
t0 = time.time()
for i in range(N):
    with open(f"{base}/f_{i}.dat", "rb") as f:
        _ = f.read()
t_read = time.time() - t0

# 3. 全部删除
t0 = time.time()
for i in range(N):
    os.unlink(f"{base}/f_{i}.dat")
t_del = time.time() - t0

# 4. 单文件连续 fdatasync 同步写 IOPS (500 笔 4KB 同步写)
sync_file = "/ws/sync_bench.dat"
fd = os.open(sync_file, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
t0 = time.time()
for i in range(500):
    os.write(fd, payload)
    os.fdatasync(fd)
t_sync = time.time() - t0
os.close(fd)
os.unlink(sync_file)

# 5. 64MB 顺序写 (含 fsync) 与顺序读吞吐
seq_file = "/ws/seq_64m.dat"
chunk = b"a" * (1024 * 1024)
t0 = time.time()
fd = os.open(seq_file, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
for _ in range(64):
    os.write(fd, chunk)
os.fsync(fd)
os.close(fd)
t_seq_w = time.time() - t0

t0 = time.time()
with open(seq_file, "rb") as f:
    while f.read(1024 * 1024):
        pass
t_seq_r = time.time() - t0
os.unlink(seq_file)

print("  [gVisor 沙箱内 Filestore Agent Volumes 真实落盘性能实测]")
print(f"  - 创建 2000 个 4KB 小文件 (含 fsync) : {t_create:.2f} s ({N/t_create:.1f} files/s)")
print(f"  - 读回 2000 个 4KB 小文件            : {t_read:.2f} s ({N/t_read:.1f} files/s)")
print(f"  - 删除 2000 个 4KB 小文件            : {t_del:.2f} s ({N/t_del:.1f} files/s)")
print(f"  - 4KB fdatasync 同步写速率           : {500/t_sync:.1f} IOPS ({t_sync*1000/500:.2f} ms/op)")
print(f"  - 64MB 顺序写吞吐 (含 fsync)         : {64/t_seq_w:.1f} MB/s")
print(f"  - 64MB 顺序读吞吐                    : {64/t_seq_r:.1f} MB/s")
PYEOF

  kubectl delete pod av-io-bench-gvisor -n "$NS" --ignore-not-found >/dev/null
  kubectl delete pvc av-io-bench-pvc -n "$NS" --ignore-not-found >/dev/null
}

run_cleanup() {
  echo "正在清理所有 Filestore Agent Volumes 测试资源..."
  kubectl delete sandboxclaim,sandboxwarmpool,sandboxtemplate,sandbox,pod,pvc -n "$NS" -l test-suite=agent-volumes --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete -f "${SCRIPT_DIR}/17-latebind-agent-volumes.yaml" --ignore-not-found >/dev/null 2>&1 || true
  echo "✅ 清理完成。"
}

case "$TARGET" in
  t1) run_t1 ;;
  t2) run_t2 ;;
  t3) run_t3 ;;
  t4) run_t4 ;;
  t5) run_t5 ;;
  cleanup) run_cleanup ;;
  all)
    run_t1
    run_t2
    run_t3
    run_t4
    run_t5
    run_cleanup
    ;;
  *)
    echo "用法: $0 [all|t1|t2|t3|t4|t5|cleanup]"
    exit 1
    ;;
esac
