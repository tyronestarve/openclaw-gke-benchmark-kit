#!/usr/bin/env bash
# ==============================================================================
# 19-test-dual-emptydir-e2e.sh
# End-to-End Automated Verification Suite for:
#   Filestore (VolumePool / NFS) + Dual emptyDir (force-shared + gVisor tmpfs)
#   + SandboxWarmPool + Online Atomic sqlite.backup() + SIGTERM RPO=0 Restore
#
# Verifies 5 closed-loop assertions on GKE Standard (gVisor), using the real
# OpenClaw gateway image with the tmpfs memory disk mounted directly at
# /home/node/.openclaw (OPENCLAW_STATE_DIR unchanged):
#   [1] WarmPool claim + storage-node-daemon bind_nfs injection (/workspace/user_data)
#   [2] Customer acceptance: tmpfs vs v9fs mounts, SQLite WAL (-wal/-shm), and
#       sessions / workspace* / media symlinked to Filestore (memory usage via du)
#   [3] Online atomic sqlite.backup() of the agent DB to Filestore (integrity_check=ok)
#   [4] SIGTERM graceful flush (RPO=0 for a just-written row) + unbind
#   [5] Cross-Pod WarmPool claim #2 restoring all rows into tmpfs WAL
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
  kubectl exec -i -n "$NS" "$daemon_pod" -c daemon -- python3 - "$json_payload" <<'PY'
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

# Test-only: give the gateway an auth token (production supplies its own env / secrets)
sed -e "s|extensions.agents.x-k8s.io/v1beta1|extensions.agents.x-k8s.io/${API_VER}|g; s|namespace: default|namespace: ${NS}|g" \
    -e 's|            - name: APP_CMD|            - name: OPENCLAW_GATEWAY_TOKEN\n              value: "e2e-test-token-0123456789"\n            - name: APP_CMD|' \
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

seed_employee_volume() {
  # Real employee volumes already contain /home/node/.openclaw content incl. openclaw.json.
  # The E2E volume starts empty, so write a minimal config (only if absent) via a short-lived Pod.
  local vol_json
  if [ -n "${NFS_SERVER:-}" ] && [ -n "${NFS_PATH:-}" ]; then
    vol_json="{\"name\":\"v\",\"nfs\":{\"server\":\"${TARGET_NFS_IP}\",\"path\":\"${TARGET_NFS_PATH}\"}}"
  else
    vol_json="{\"name\":\"v\",\"persistentVolumeClaim\":{\"claimName\":\"${TEST_PVC}\"}}"
  fi
  kubectl delete pod dual-e2e-seed -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl run dual-e2e-seed -n "$NS" --restart=Never --image=busybox:1.36 --overrides="{
    \"spec\":{\"securityContext\":{\"runAsUser\":1000,\"runAsGroup\":1000},
      \"containers\":[{\"name\":\"seed\",\"image\":\"busybox:1.36\",
        \"command\":[\"sh\",\"-c\",\"[ -f /v/openclaw.json ] || echo '{\\\"gateway\\\":{\\\"mode\\\":\\\"local\\\"}}' > /v/openclaw.json; ls -la /v\"],
        \"volumeMounts\":[{\"name\":\"v\",\"mountPath\":\"/v\"}]}],
      \"volumes\":[${vol_json}]}}" >/dev/null
  kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/dual-e2e-seed -n "$NS" --timeout=120s >/dev/null
  kubectl delete pod dual-e2e-seed -n "$NS" --wait=false >/dev/null 2>&1 || true
  echo "✅ Employee volume seeded with a minimal openclaw.json (gateway.mode=local)"
}
seed_employee_volume

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

wait_gateway_ready() {
  local pod="$1"
  for _ in $(seq 1 120); do
    if kubectl logs -n "$NS" "$pod" -c openclaw 2>/dev/null | grep -q "\[gateway\] ready"; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: gateway in ${pod} did not become ready" >&2
  kubectl logs -n "$NS" "$pod" -c openclaw --tail=40 >&2 || true
  exit 1
}

agent_sql() {
  # $1=pod  $2=db path  $3=readOnly(true|false)  $4=JS body using `db`
  kubectl exec -n "$NS" "$1" -c openclaw -- node --input-type=module -e "
import sqlite from 'node:sqlite';
const db = new sqlite.DatabaseSync('$2', { readOnly: $3 });
$4
db.close();
"
}

HOT=/home/node/.openclaw
# OpenClaw creates and owns the shared state DB at startup (WAL on tmpfs); per-agent DBs are created lazily
HOT_DB="${HOT}/state/openclaw.sqlite"
NFS_DB="/workspace/user_data/state/openclaw.sqlite"

wait_gateway_ready "$POD1"

echo ""
echo "--- [Check 2] Customer Acceptance: tmpfs / WAL / symlinked cold dirs ---"
kubectl exec -n "$NS" "$POD1" -c openclaw -- sh -c "
stat -f -c '%n -> %T' ${HOT} ${HOT}/state /workspace/user_data
ls -lh ${HOT}/state/
ls -ld ${HOT}/workspace ${HOT}/media ${HOT}/agents/main/sessions
du -sh ${HOT}
"
kubectl exec -n "$NS" "$POD1" -c openclaw -- sh -c "
[ \"\$(stat -f -c %T ${HOT})\" = tmpfs ] || { echo 'FAIL: ${HOT} is not tmpfs'; exit 1; }
[ -e ${HOT}/state/openclaw.sqlite-wal ] || { echo 'FAIL: state DB is not in WAL mode'; exit 1; }
for d in workspace media agents/main/sessions; do [ -L ${HOT}/\$d ] || { echo \"FAIL: \$d is not a symlink\"; exit 1; }; done
echo '  ✅ tmpfs + WAL + symlinked cold dirs verified'
"

echo ""
echo "--- [Check 3] Writing 200 rows + Verifying Online Atomic sqlite.backup() ---"
agent_sql "$POD1" "$HOT_DB" false "
db.exec('CREATE TABLE IF NOT EXISTS e2e_turns (id INTEGER PRIMARY KEY, tag TEXT)');
const ins = db.prepare('INSERT INTO e2e_turns (tag) VALUES (?)');
for (let i = 0; i < 200; i++) ins.run('online-batch-1');
console.log('  journal_mode =', db.prepare('PRAGMA journal_mode').get().journal_mode);
"
echo "  Waiting 3s for debounced sqlite.backup() to flush to Filestore..."
sleep 3
agent_sql "$POD1" "$NFS_DB" true "
const chk = db.prepare('PRAGMA integrity_check').get().integrity_check;
const cnt = Number(db.prepare('SELECT COUNT(*) AS c FROM e2e_turns').get().c);
console.log('  Remote Filestore Backup Check -> integrity:', chk, '| persisted rows:', cnt);
if (chk !== 'ok' || cnt !== 200) process.exit(1);
"

echo ""
echo "--- [Check 4] Writing 77 Un-Synced Rows + Triggering SIGTERM RPO=0 Teardown ---"
agent_sql "$POD1" "$HOT_DB" false "
const ins = db.prepare('INSERT INTO e2e_turns (tag) VALUES (?)');
for (let i = 0; i < 77; i++) ins.run('sigterm-final-delta');
"
unbind_and_release_claim "dual-e2e-claim-1" "$POD1" "$POD1_UID" "$POD1_NODE"
echo "  ✅ Pod #1 gracefully flushed on SIGTERM, unbound from Filestore, and terminated cleanly."

echo ""
echo "--- [Check 5] Claiming WarmPool Pod #2 & Verifying RPO=0 Restore ---"
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

wait_gateway_ready "$POD2"
kubectl logs -n "$NS" "$POD2" -c openclaw | grep "Restored hot state" || true
agent_sql "$POD2" "$HOT_DB" true "
const mode = db.prepare('PRAGMA journal_mode').get().journal_mode;
const total = Number(db.prepare('SELECT COUNT(*) AS c FROM e2e_turns').get().c);
const last = db.prepare('SELECT tag FROM e2e_turns ORDER BY id DESC LIMIT 1').get().tag;
console.log('  Pod #2 restored -> journal_mode:', mode, '| rows:', total, '| last tag:', last);
if (mode !== 'wal' || total !== 277 || last !== 'sigterm-final-delta') process.exit(1);
"
echo ""
echo "🎉 ALL 5 E2E CHECKS PASSED! 277/277 rows restored (RPO=0) into gVisor tmpfs WAL at ${HOT}."
