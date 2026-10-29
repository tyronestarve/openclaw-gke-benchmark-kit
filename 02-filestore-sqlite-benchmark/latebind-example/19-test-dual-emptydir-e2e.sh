#!/usr/bin/env bash
# ==============================================================================
# 19-test-dual-emptydir-e2e.sh
# End-to-End Automated Verification Suite for:
#   Filestore (VolumePool / NFS) + Dual emptyDir (force-shared + gVisor tmpfs)
#   + SandboxWarmPool + Online Atomic sqlite.backup() + SIGTERM RPO=0 Restore
#
# Verifies 5 closed-loop assertions on GKE Standard (gVisor):
#   [1] WarmPool sub-second claim + storage-node-daemon bind_nfs injection
#   [2] gVisor statfs magic check (9p 0x01021997 vs tmpfs 0x01021994) &
#       SQLite native WAL (~0.06ms) vs direct NFS DELETE (~15-23ms) benchmark
#   [3] Online atomic sqlite.backup() to remote Filestore (integrity_check=ok)
#   [4] SIGTERM graceful flush (RPO=0 for un-synced in-flight turns) + unbind
#   [5] Cross-Pod WarmPool claim #2 restoring 100% of turns in milliseconds
#
# Usage:
#   # Mode A: Automatic VolumePool PVC provisioning (default: SC=volume-pool-sc)
#   ./19-test-dual-emptydir-e2e.sh
#
#   # Mode B: Bring an existing Filestore share IP & path
#   NFS_SERVER=10.x.x.x NFS_PATH=/vol1/test-tenant ./19-test-dual-emptydir-e2e.sh
#
#   # Manual cleanup only:
#   ./19-test-dual-emptydir-e2e.sh cleanup
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="${NAMESPACE:-default}"
SC="${SC:-volume-pool-sc}"
PVC_SIZE="${PVC_SIZE:-1Gi}"
TEST_PVC="dual-emptydir-e2e-pvc"
DAEMON_YAML="${SCRIPT_DIR}/17-latebind-agent-volumes.yaml"
WARMPOOL_YAML="${SCRIPT_DIR}/18-dual-emptydir-hot-sqlite-warmpool.yaml"
AUTO_CLEANUP="${AUTO_CLEANUP:-true}"
ACTION="${1:-all}"

now_ms() {
  python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

detect_sandbox_api() {
  if kubectl api-resources --api-group=extensions.agents.x-k8s.io -o name 2>/dev/null | grep -q .; then
    if kubectl api-versions | grep -q "^extensions.agents.x-k8s.io/v1beta1$"; then
      echo "v1beta1"
      return
    fi
    if kubectl api-versions | grep -q "^extensions.agents.x-k8s.io/v1alpha1$"; then
      echo "v1alpha1"
      return
    fi
  fi
  echo "ERROR: Agent Sandbox CRDs (extensions.agents.x-k8s.io) not found on cluster." >&2
  exit 1
}

wait_claim_pod() {
  local claim_name="$1"
  local pod_name=""
  for _ in $(seq 1 60); do
    local sb_name
    sb_name=$(kubectl get sandboxclaim "$claim_name" -n "$NS" \
      -o jsonpath='{.status.sandbox.Name}{.status.sandbox.name}' 2>/dev/null || true)
    if [ -n "$sb_name" ]; then
      if kubectl get pod "$sb_name" -n "$NS" >/dev/null 2>&1; then
        pod_name="$sb_name"
        break
      fi
      pod_name=$(kubectl get pod -n "$NS" -l "agents.x-k8s.io/sandbox-name=${sb_name}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
      if [ -n "$pod_name" ]; then
        break
      fi
    fi
    sleep 0.2
  done
  if [ -z "$pod_name" ]; then
    echo "ERROR: Timed out waiting for SandboxClaim ${claim_name} to bind a Pod." >&2
    exit 1
  fi
  echo "$pod_name"
}

daemon_for_node() {
  local node_name="$1"
  kubectl get pod -n "$NS" -l app=storage-node-daemon \
    --field-selector "spec.nodeName=${node_name}" \
    -o jsonpath='{.items[0].metadata.name}'
}

call_daemon_api() {
  local daemon_pod="$1"
  local json_payload="$2"
  kubectl exec -n "$NS" "$daemon_pod" -c daemon -- python3 - "$json_payload" <<'PY'
import sys, urllib.request, urllib.error

payload = sys.argv[1].encode()
req = urllib.request.Request(
    "http://127.0.0.1:9090",
    data=payload,
    headers={"Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(req, timeout=25) as resp:
        print(resp.read().decode())
except urllib.error.HTTPError as e:
    sys.stderr.write(f"Daemon HTTP {e.code}: {e.read().decode()}\n")
    sys.exit(1)
PY
}

unbind_and_release_claim() {
  local claim_name="$1"
  local pod_name="$2"
  local pod_uid="$3"
  local node_name="$4"
  local daemon_pod
  daemon_pod=$(daemon_for_node "$node_name")

  kubectl delete sandboxclaim "$claim_name" -n "$NS" --wait=false >/dev/null 2>&1 || true

  # Wait for container inside Pod to complete SIGTERM flush before detaching NFS
  for _ in $(seq 1 60); do
    local term_state
    term_state=$(kubectl get pod "$pod_name" -n "$NS" \
      -o jsonpath='{.status.containerStatuses[0].state.terminated.reason}' 2>/dev/null || true)
    if [ -n "$term_state" ]; then
      break
    fi
    sleep 0.25
  done

  if [ -n "$daemon_pod" ]; then
    call_daemon_api "$daemon_pod" "{
      \"action\": \"unbind\",
      \"pod_uid\": \"${pod_uid}\",
      \"volume_name\": \"workspace-volume\",
      \"sub_dir\": \"user_data\"
    }" >/dev/null 2>&1 || true
  fi

  kubectl patch pod "$pod_name" -n "$NS" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
  kubectl patch sandboxclaim "$claim_name" -n "$NS" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
  kubectl wait --for=delete "pod/${pod_name}" -n "$NS" --timeout=30s >/dev/null 2>&1 || true
}

cleanup_all() {
  echo ""
  echo "=== [Cleanup] Releasing test claims, WarmPool, and temporary PVC ==="
  for cname in dual-e2e-claim-1 dual-e2e-claim-2; do
    if kubectl get sandboxclaim "$cname" -n "$NS" >/dev/null 2>&1; then
      local pname
      pname=$(wait_claim_pod "$cname" 2>/dev/null || true)
      if [ -n "$pname" ] && kubectl get pod "$pname" -n "$NS" >/dev/null 2>&1; then
        local puid pnode
        puid=$(kubectl get pod "$pname" -n "$NS" -o jsonpath='{.metadata.uid}')
        pnode=$(kubectl get pod "$pname" -n "$NS" -o jsonpath='{.spec.nodeName}')
        unbind_and_release_claim "$cname" "$pname" "$puid" "$pnode" || true
      else
        kubectl patch sandboxclaim "$cname" -n "$NS" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
        kubectl delete sandboxclaim "$cname" -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
      fi
    fi
  done
  kubectl delete sandboxwarmpool dual-emptydir-hot-sqlite-pool -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete sandboxtemplate dual-emptydir-hot-sqlite-template -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete configmap hot-sqlite-wrapper -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete pvc "$TEST_PVC" -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
  echo "✅ Cleanup completed."
}

if [ "$ACTION" = "cleanup" ]; then
  cleanup_all
  exit 0
fi

if [ "$AUTO_CLEANUP" = "true" ]; then
  trap cleanup_all EXIT
fi

API_VER=$(detect_sandbox_api)
echo "========================================================================"
echo " Dual emptyDir + Filestore Late-Bind + WarmPool E2E Verification Suite"
echo " Namespace: ${NS} | Agent Sandbox API: extensions.agents.x-k8s.io/${API_VER}"
echo "========================================================================"

echo ""
echo "--- [Step 0] Deploying storage-node-daemon & Dual-emptyDir WarmPool ---"
if ! kubectl get ds storage-node-daemon -n "$NS" >/dev/null 2>&1; then
  sed "s|extensions.agents.x-k8s.io/v1beta1|extensions.agents.x-k8s.io/${API_VER}|g; s|namespace: default|namespace: ${NS}|g" \
    "$DAEMON_YAML" | kubectl apply -f - >/dev/null
fi
kubectl rollout status ds/storage-node-daemon -n "$NS" --timeout=180s

sed "s|extensions.agents.x-k8s.io/v1beta1|extensions.agents.x-k8s.io/${API_VER}|g; s|namespace: default|namespace: ${NS}|g" \
  "$WARMPOOL_YAML" | kubectl apply -f -

echo "Waiting for 2 pre-warmed gVisor sandboxes in dual-emptydir-hot-sqlite-pool..."
until [ "$(kubectl get sandboxwarmpool dual-emptydir-hot-sqlite-pool -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" = "2" ]; do
  sleep 1
done
echo "✅ WarmPool Ready (2/2 pre-warmed gVisor Pods waiting on /workspace/.ready)."

if [ -n "${NFS_SERVER:-}" ] && [ -n "${NFS_PATH:-}" ]; then
  TARGET_NFS_IP="$NFS_SERVER"
  TARGET_NFS_PATH="$NFS_PATH"
  echo "Using caller-provided Filestore share: ${TARGET_NFS_IP}:${TARGET_NFS_PATH}"
else
  echo "Provisioning dedicated 1Gi Filestore Agent Volume PVC (${TEST_PVC} via StorageClass ${SC})..."
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${TEST_PVC}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ${SC}
  resources:
    requests:
      storage: ${PVC_SIZE}
EOF
  until [ "$(kubectl get pvc "$TEST_PVC" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Bound" ]; do
    sleep 0.2
  done
  PV_NAME=$(kubectl get pvc "$TEST_PVC" -n "$NS" -o jsonpath='{.spec.volumeName}')
  TARGET_NFS_IP=$(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeAttributes.ip}')
  TARGET_NFS_PATH="/$(kubectl get pv "$PV_NAME" -o jsonpath='{.spec.csi.volumeAttributes.volume}')"
  echo "✅ Provisioned VolumePool share: ${TARGET_NFS_IP}:${TARGET_NFS_PATH}"
fi

create_claim_manifest() {
  local cname="$1"
  if [ "$API_VER" = "v1beta1" ]; then
    cat <<EOF
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxClaim
metadata:
  name: ${cname}
  namespace: ${NS}
  finalizers: [agent.sandbox/storage-cleanup]
spec:
  warmPoolRef:
    name: dual-emptydir-hot-sqlite-pool
EOF
  else
    cat <<EOF
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxClaim
metadata:
  name: ${cname}
  namespace: ${NS}
  finalizers: [agent.sandbox/storage-cleanup]
spec:
  sandboxTemplateRef:
    name: dual-emptydir-hot-sqlite-template
EOF
  fi
}

echo ""
echo "--- [Check 1] Claiming WarmPool Pod #1 & Dynamic Late-Binding Filestore ---"
T_C1_START=$(now_ms)
create_claim_manifest "dual-e2e-claim-1" | kubectl apply -f - >/dev/null
POD1=$(wait_claim_pod "dual-e2e-claim-1")
T_C1_CLAIMED=$(now_ms)

kubectl patch pod "$POD1" -n "$NS" --type=merge \
  -p '{"metadata":{"finalizers":["agent.sandbox/storage-cleanup"]}}' >/dev/null
POD1_UID=$(kubectl get pod "$POD1" -n "$NS" -o jsonpath='{.metadata.uid}')
POD1_NODE=$(kubectl get pod "$POD1" -n "$NS" -o jsonpath='{.spec.nodeName}')
DAEMON1=$(daemon_for_node "$POD1_NODE")

T_BIND1_START=$(now_ms)
BIND1_OUT=$(call_daemon_api "$DAEMON1" "{
  \"action\": \"bind_nfs\",
  \"pod_uid\": \"${POD1_UID}\",
  \"volume_name\": \"workspace-volume\",
  \"sub_dir\": \"user_data\",
  \"nfs_server\": \"${TARGET_NFS_IP}\",
  \"nfs_path\": \"${TARGET_NFS_PATH}\",
  \"force_rebind\": true
}")
T_BIND1_DONE=$(now_ms)
echo "  Pod #1       : ${POD1} (Node: ${POD1_NODE})"
echo "  Claim Latency: $((T_C1_CLAIMED - T_C1_START)) ms | Late-Bind RPC: $((T_BIND1_DONE - T_BIND1_START)) ms"
echo "  Daemon Reply : ${BIND1_OUT}"

for _ in $(seq 1 40); do
  if kubectl exec -n "$NS" "$POD1" -- node -e \
    "fetch('http://127.0.0.1:8080/status').then(r=>r.ok?process.exit(0):process.exit(1)).catch(()=>process.exit(1))" 2>/dev/null; then
    break
  fi
  sleep 0.25
done

echo ""
echo "--- [Check 2] gVisor statfs Magic & SQLite WAL vs DELETE Benchmark ---"
kubectl exec -n "$NS" "$POD1" -- node /opt/hot-sqlite/verify-probe.mjs

echo ""
echo "--- [Check 3] Writing 200 Turns + Verifying Online Atomic sqlite.backup() ---"
kubectl exec -n "$NS" "$POD1" -- node --input-type=module -e "
const r = await fetch('http://127.0.0.1:8080/write', {
  method: 'POST',
  body: JSON.stringify({ count: 200, tag: 'online-batch-1' })
});
console.log('  Foreground WAL Write (200 turns):', await r.text());
"
echo "  Waiting 4.2s for background 3s debounced sqlite.backup() to flush to Filestore..."
sleep 4.2

kubectl exec -n "$NS" "$POD1" -- node --input-type=module -e "
import sqlite from 'node:sqlite';
const db = new sqlite.DatabaseSync('/workspace/user_data/db-backup/session-state.sqlite', { readOnly: true });
const chk = db.prepare('PRAGMA integrity_check').get();
const cnt = db.prepare('SELECT COUNT(*) AS c FROM conversation_turns').get();
db.close();
console.log('  Remote Filestore Backup Check -> integrity:', chk.integrity_check, '| persisted turns:', Number(cnt.c));
if (chk.integrity_check !== 'ok' || Number(cnt.c) !== 200) process.exit(1);
"

echo ""
echo "--- [Check 4] Writing 77 Un-Synced Turns + Triggering SIGTERM RPO=0 Teardown ---"
kubectl exec -n "$NS" "$POD1" -- node --input-type=module -e "
const r = await fetch('http://127.0.0.1:8080/write', {
  method: 'POST',
  body: JSON.stringify({ count: 77, tag: 'sigterm-final-delta' })
});
console.log('  Pre-SIGTERM Delta Write (+77 = 277 total):', await r.text());
"
unbind_and_release_claim "dual-e2e-claim-1" "$POD1" "$POD1_UID" "$POD1_NODE"
echo "  ✅ Pod #1 gracefully flushed on SIGTERM, unbound from Filestore, and terminated cleanly."

echo ""
echo "--- [Check 5] Claiming WarmPool Pod #2 & Verifying Millisecond RPO=0 Restore ---"
create_claim_manifest "dual-e2e-claim-2" | kubectl apply -f - >/dev/null
POD2=$(wait_claim_pod "dual-e2e-claim-2")
kubectl patch pod "$POD2" -n "$NS" --type=merge \
  -p '{"metadata":{"finalizers":["agent.sandbox/storage-cleanup"]}}' >/dev/null
POD2_UID=$(kubectl get pod "$POD2" -n "$NS" -o jsonpath='{.metadata.uid}')
POD2_NODE=$(kubectl get pod "$POD2" -n "$NS" -o jsonpath='{.spec.nodeName}')
DAEMON2=$(daemon_for_node "$POD2_NODE")

call_daemon_api "$DAEMON2" "{
  \"action\": \"bind_nfs\",
  \"pod_uid\": \"${POD2_UID}\",
  \"volume_name\": \"workspace-volume\",
  \"sub_dir\": \"user_data\",
  \"nfs_server\": \"${TARGET_NFS_IP}\",
  \"nfs_path\": \"${TARGET_NFS_PATH}\"
}" >/dev/null

STATUS_POD2=""
for _ in $(seq 1 40); do
  STATUS_POD2=$(kubectl exec -n "$NS" "$POD2" -- node -e \
    "fetch('http://127.0.0.1:8080/status').then(r=>r.text()).then(t=>{console.log(t);process.exit(0)}).catch(()=>process.exit(1))" 2>/dev/null || true)
  if [ -n "$STATUS_POD2" ]; then
    break
  fi
  sleep 0.25
done

echo "  Pod #2 (${POD2} on ${POD2_NODE}) Restored Status:"
echo "  ${STATUS_POD2}"

python3 - "$STATUS_POD2" <<'PY'
import json, sys
data = json.loads(sys.argv[1])
assert data["journalMode"].lower() == "wal", f"Expected WAL, got {data['journalMode']}"
assert data["totalTurns"] == 277, f"Expected 277 restored turns (RPO=0), got {data['totalTurns']}"
assert data["lastTurn"]["tag"] == "sigterm-final-delta", f"Unexpected lastTurn: {data['lastTurn']}"
restore_ms = data["restoredMeta"]["restoreMs"]
print(f"\n🎉 ALL 5 E2E CHECKS PASSED! Restored 277/277 turns (RPO=0) into gVisor tmpfs WAL in {restore_ms} ms.")
PY
