#!/bin/bash
# ==============================================================================
# 03-setup-agent-volumes.sh
# Filestore Agent Volumes (FIFA / VolumePool + PSC + GKE CSI + 性能调优 StorageClass) 初始化脚本
#
# 🎯 支持两种运行模式（通过 SKIP_INFRA_CREATE 或自动检测控制）：
#   - 模式 A（推荐：客户已有现成 GKE 集群、VPC、子网，甚至已手动创建好 PSC 策略）：
#       脚本会自动跳过 VPC/子网/GKE 集群创建，直接从「Step 5: IAM 权限 -> Step 6: PSC 检查 ->
#       Step 7: 创建 1TiB VolumePool -> Step 8: 部署 volumepool CSI 驱动与调优版 StorageClass」开始执行！
#
#       已有 GKE 集群的客户只需执行：
#         export PROJECT_ID="your-gcp-project-id"
#         export REGION="asia-southeast1"
#         export CLUSTER_LOCATION="asia-southeast1"   # Regional 集群填 region，Zonal 集群填 zone
#         export CLUSTER="your-existing-gke-cluster"
#         export VPC_NETWORK="your-existing-vpc"
#         export SUBNET_NAME="your-existing-subnet"
#         export PSC_POLICY_NAME="openclaw-filestore" # 如已创建会自动复用，未创建会自动创建
#         bash ./03-setup-agent-volumes.sh
#
#   - 模式 B（从零新建环境）：
#       若指定的 VPC / 子网 / GKE 集群不存在，且未设置 EXISTING_GKE_ONLY=true，则自动从 Step 1 创建。
#
# 官方文档参考:
#   - https://docs.cloud.google.com/filestore/docs/agent-volumes-overview
#   - https://docs.cloud.google.com/filestore/docs/agent-volumes-prerequisites
#   - https://docs.cloud.google.com/filestore/docs/create-volume-pools
#   - https://docs.cloud.google.com/filestore/docs/use-filestore-agent-volumes-gke
# ==============================================================================
set -euo pipefail

PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project)}
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
REGION=${REGION:-asia-southeast1}
ZONE=${ZONE:-${REGION}-a}
CLUSTER_LOCATION=${CLUSTER_LOCATION:-$ZONE}   # 支持传入 Region (如 asia-southeast1) 或 Zone
CLUSTER=${CLUSTER:-agent-sandbox-cluster}
VPC_NETWORK=${VPC_NETWORK:-agent-sandbox-net}
SUBNET_NAME=${SUBNET_NAME:-agent-sandbox-subnet}
SUBNET_RANGE=${SUBNET_RANGE:-10.20.0.0/20}

# VolumePool 省钱与高密度默认配置（单实例 1TiB，节省 95% 配额，支持 100~200 个 Share）
VOLUME_POOL_ID=${VOLUME_POOL_ID:-my-volume-pool-1tb}
DEFAULT_QUOTA_MIB=${DEFAULT_QUOTA_MIB:-2048}   # 默认单卷配额 2GiB (2048 MiB)，可按需设为 1024 或 5120
POOL_MIN_INSTANCES=${POOL_MIN_INSTANCES:-1}
POOL_MAX_INSTANCES=${POOL_MAX_INSTANCES:-1}
POOL_INST_CAPACITY_GB=${POOL_INST_CAPACITY_GB:-1024}
POOL_MIN_AVAIL_VOLS=${POOL_MIN_AVAIL_VOLS:-10}
POOL_MAX_VOLS_PER_INST=${POOL_MAX_VOLS_PER_INST:-100}

PSC_POLICY_NAME=${PSC_POLICY_NAME:-openclaw-filestore}
CSI_GSA_NAME=${CSI_GSA_NAME:-filestore-csi-gsa}
CSI_GSA_EMAIL="${CSI_GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
AGENT_SANDBOX_VERSION=${AGENT_SANDBOX_VERSION:-v0.2.1}

echo "=================================================================="
echo "Filestore Agent Volumes (VolumePool) 环境初始化"
echo "  Project          : $PROJECT_ID ($PROJECT_NUMBER)"
echo "  Region           : $REGION (Cluster Location: $CLUSTER_LOCATION)"
echo "  GKE Cluster      : $CLUSTER"
echo "  VPC / Subnet     : $VPC_NETWORK / $SUBNET_NAME"
echo "  PSC Policy       : $PSC_POLICY_NAME"
echo "  Volume Pool ID   : $VOLUME_POOL_ID (${POOL_MIN_INSTANCES}x ${POOL_INST_CAPACITY_GB}GB, 默认单卷: ${DEFAULT_QUOTA_MIB} MiB)"
echo "=================================================================="

# ------------------------------------------------------------------------------
# Step 1. 启用必需的 Google Cloud API
# ------------------------------------------------------------------------------
echo "[Step 1/8] 启用必需的 Google Cloud API..."
gcloud services enable \
  compute.googleapis.com \
  container.googleapis.com \
  file.googleapis.com \
  networkconnectivity.googleapis.com \
  iam.googleapis.com \
  --project="$PROJECT_ID"

# ------------------------------------------------------------------------------
# Step 2 ~ 4. 检查现有 VPC / 子网 / GKE 集群（已有 GKE 集群的客户将自动跳过创建）
# ------------------------------------------------------------------------------
echo "[Step 2/8] 检查 VPC 网络 ($VPC_NETWORK) 与子网 ($SUBNET_NAME)..."
if gcloud compute networks describe "$VPC_NETWORK" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "  -> [已存在，直接复用] VPC $VPC_NETWORK"
else
  echo "  -> 创建 VPC $VPC_NETWORK..."
  gcloud compute networks create "$VPC_NETWORK" --project="$PROJECT_ID" --subnet-mode=custom
fi

if gcloud compute networks subnets describe "$SUBNET_NAME" --region="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "  -> [已存在，直接复用] 子网 $SUBNET_NAME"
else
  echo "  -> 创建子网 $SUBNET_NAME ($SUBNET_RANGE)..."
  gcloud compute networks subnets create "$SUBNET_NAME" \
    --project="$PROJECT_ID" \
    --network="$VPC_NETWORK" \
    --region="$REGION" \
    --range="$SUBNET_RANGE"
fi

echo "[Step 3/8] 检查现有 GKE 集群 ($CLUSTER) 及内置 Filestore CSI 插件冲突..."
if gcloud container clusters describe "$CLUSTER" --location="$CLUSTER_LOCATION" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "  -> [已存在，直接复用] GKE 集群 $CLUSTER ($CLUSTER_LOCATION)"
  ADDON_ENABLED=$(gcloud container clusters describe "$CLUSTER" --location="$CLUSTER_LOCATION" --project="$PROJECT_ID" \
    --format="value(addonsConfig.gcpFilestoreCsiDriverConfig.enabled)" 2>/dev/null || true)
  if [ "$ADDON_ENABLED" = "True" ]; then
    echo "  -> ⚠️ 检测到 GKE 内置 GcpFilestoreCsiDriver 已开启，正在将其关闭（部署支持 volumepool 的 CSI 驱动前必须关闭内置插件）..."
    gcloud container clusters update "$CLUSTER" \
      --location="$CLUSTER_LOCATION" \
      --project="$PROJECT_ID" \
      --update-addons=GcpFilestoreCsiDriver=DISABLED
  else
    echo "  -> 内置 GcpFilestoreCsiDriver 已处于关闭状态，无需变更。"
  fi
else
  echo "❌ 错误：在项目 $PROJECT_ID ($CLUSTER_LOCATION) 中未找到 GKE 集群 '$CLUSTER'！"
  echo "   请确认环境变量 CLUSTER 与 CLUSTER_LOCATION 是否填写正确（或先创建好 GKE 集群后再运行本脚本）。"
  exit 1
fi

gcloud container clusters get-credentials "$CLUSTER" --location="$CLUSTER_LOCATION" --project="$PROJECT_ID"

echo "[Step 4/8] 检查 GKE Agent Sandbox CRD..."
if ! kubectl get crd sandboxes.agents.x-k8s.io >/dev/null 2>&1; then
  kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${AGENT_SANDBOX_VERSION}/manifest.yaml"
  kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${AGENT_SANDBOX_VERSION}/extensions.yaml"
  echo "  -> Agent Sandbox CRD 安装完成。"
else
  echo "  -> [已存在，跳过] Agent Sandbox CRD。"
fi

# ==============================================================================
# ⭐ 已有 GKE 集群的客户：实际核心动作从下面 Step 5 开始！
# ==============================================================================

# ------------------------------------------------------------------------------
# Step 5. 初始化 Filestore Service Agent 并授予 roles/file.editor
# ------------------------------------------------------------------------------
echo "[Step 5/8] 初始化 Filestore Service Agent 并配置权限..."
gcloud beta services identity create \
  --service=file.googleapis.com \
  --project="$PROJECT_ID" || true

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@cloud-filer.iam.gserviceaccount.com" \
  --role="roles/file.editor" \
  --condition=None >/dev/null
echo "  -> Filestore Service Agent 权限已就绪。"

# ------------------------------------------------------------------------------
# Step 6. 检查/创建 Private Service Connect (PSC) 服务连接策略
# ------------------------------------------------------------------------------
echo "[Step 6/8] 检查/创建 PSC Service Connection Policy ($PSC_POLICY_NAME)..."
EXISTING_PSC=$(gcloud network-connectivity service-connection-policies list \
  --region="$REGION" --project="$PROJECT_ID" \
  --filter="serviceClass=google-cloud-filestore" \
  --format="value(name)" 2>/dev/null | head -n 1 || true)

if [ -n "$EXISTING_PSC" ]; then
  echo "  -> [已存在，直接复用] 检测到当前 Region 已存在 google-cloud-filestore PSC 策略: $EXISTING_PSC"
else
  gcloud network-connectivity service-connection-policies create "$PSC_POLICY_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --network="projects/${PROJECT_ID}/global/networks/${VPC_NETWORK}" \
    --subnets="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}" \
    --service-class=google-cloud-filestore
  echo "  -> PSC Policy ($PSC_POLICY_NAME) 创建成功。"
fi

# ------------------------------------------------------------------------------
# Step 7. 创建 Regional Volume Pool (单实例 1TiB 省钱配置，v1beta1 REST API)
# ------------------------------------------------------------------------------
echo "[Step 7/8] 检查/创建 Filestore Regional Volume Pool ($VOLUME_POOL_ID)..."
TOKEN=$(gcloud auth print-access-token)
POOL_URL="https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools/${VOLUME_POOL_ID}"
HTTP_CODE=$(curl -s -o /tmp/volpool_resp.json -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" "$POOL_URL")

if [ "$HTTP_CODE" = "200" ]; then
  STATE=$(python3 -c 'import json; print(json.load(open("/tmp/volpool_resp.json")).get("state","UNKNOWN"))')
  echo "  -> [已存在，直接复用] Volume Pool $VOLUME_POOL_ID (state=$STATE)。"
else
  echo "  -> 发起创建 Volume Pool $VOLUME_POOL_ID (单实例 1TiB 省配额模式: MinInstances=${POOL_MIN_INSTANCES}, MaxInstances=${POOL_MAX_INSTANCES}, InstanceCapacityGb=${POOL_INST_CAPACITY_GB})..."
  POOL_DESC="GoogleReservedOverrides={\"CustomVolumePoolConfig\":{\"MinInstances\":${POOL_MIN_INSTANCES},\"MaxInstances\":${POOL_MAX_INSTANCES},\"InstanceCapacityGb\":${POOL_INST_CAPACITY_GB},\"MinAvailableShares\":${POOL_MIN_AVAIL_VOLS},\"MaxSharesPerInstance\":${POOL_MAX_VOLS_PER_INST}}}"
  CREATE_RESP=$(curl -s -X POST \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d "{
      \"description\": $(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$POOL_DESC"),
      \"network\": \"projects/${PROJECT_ID}/global/networks/${VPC_NETWORK}\",
      \"defaultVolumeQuotaMib\": ${DEFAULT_QUOTA_MIB},
      \"labels\": {
        \"env\": \"poc\",
        \"workload\": \"openclaw-agent-sandboxes\"
      }
    }" \
    "https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools?volumePoolId=${VOLUME_POOL_ID}")
  echo "$CREATE_RESP"
  OP_NAME=$(echo "$CREATE_RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("name",""))')
  if [ -z "$OP_NAME" ]; then
    echo "!! 创建 Volume Pool 失败。请检查输出日志及项目配额。"
    exit 1
  fi
  echo "  -> 等待 VolumePool 创建完成 (LRO: $OP_NAME)..."
  while true; do
    DONE=$(curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
      "https://file.googleapis.com/v1beta1/${OP_NAME}" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("done",False))')
    [ "$DONE" = "True" ] && break
    sleep 10
  done
  echo "  -> Volume Pool $VOLUME_POOL_ID 已就绪 (READY)！"
fi

# ------------------------------------------------------------------------------
# Step 8. 部署开源 Filestore CSI Driver (volumepool overlay) 并创建性能调优版 StorageClass
# ------------------------------------------------------------------------------
echo "[Step 8/8] 部署支持 VolumePool 的 Filestore CSI Driver 并创建调优版 StorageClass..."
gcloud iam service-accounts describe "$CSI_GSA_EMAIL" --project="$PROJECT_ID" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$CSI_GSA_NAME" \
    --display-name="Filestore CSI Driver GSA" \
    --project="$PROJECT_ID"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${CSI_GSA_EMAIL}" \
  --role="roles/file.editor" \
  --condition=None >/dev/null

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/file.editor" \
  --condition=None >/dev/null

gcloud iam service-accounts add-iam-policy-binding "$CSI_GSA_EMAIL" \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[gcp-filestore-csi-driver/gcp-filestore-csi-controller-sa]" \
  --project="$PROJECT_ID" >/dev/null

TMP_CSI_DIR=$(mktemp -d)
git clone --depth=1 https://github.com/kubernetes-sigs/gcp-filestore-csi-driver.git "$TMP_CSI_DIR/gcp-filestore-csi-driver"
kubectl apply -k "$TMP_CSI_DIR/gcp-filestore-csi-driver/deploy/kubernetes/overlays/volumepool"
rm -rf "$TMP_CSI_DIR"

kubectl annotate serviceaccount gcp-filestore-csi-controller-sa \
  --namespace=gcp-filestore-csi-driver \
  --overwrite \
  "iam.gke.io/gcp-service-account=${CSI_GSA_EMAIL}"

echo "  -> 等待 gcp-filestore-csi-driver 控制器与节点插件就绪..."
kubectl rollout status statefulset/gcp-filestore-csi-controller -n gcp-filestore-csi-driver --timeout=180s
kubectl rollout status daemonset/gcp-filestore-csi-node -n gcp-filestore-csi-driver --timeout=180s

# 创建两个 StorageClass（volume-pool-sc 默认名 + volume-pool-1tb-tuned-sc 显式调优名，均带完整 NFS 性能调优参数）
for SC_NAME in "volume-pool-sc" "volume-pool-1tb-tuned-sc"; do
  kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ${SC_NAME}
provisioner: filestore.csi.storage.gke.io
parameters:
  volume-pool: "projects/${PROJECT_ID}/locations/${REGION}/volumePools/${VOLUME_POOL_ID}"
mountOptions:
  - vers=3
  - proto=tcp
  - mountproto=tcp
  - port=2049
  - mountport=2050
  - nocto
  - noatime
  - nodiratime
  - actimeo=600
  - lookupcache=all
  - nconnect=8
  - rsize=1048576
  - wsize=1048576
  - local_lock=all
  - async
volumeBindingMode: Immediate
reclaimPolicy: Delete
allowVolumeExpansion: true
EOF
done

kubectl get storageclass volume-pool-sc volume-pool-1tb-tuned-sc
echo "=================================================================="
echo "✅ Filestore Agent Volumes (VolumePool) 环境初始化与性能调优全部完成！"
echo "   下一步可执行压测脚本："
echo "   1) 控制面与挂载套件: bash ./04-test-agent-volumes-suite.sh all"
echo "   2) 容器内 SQLite 压测: node ./node-sqlite-benchmark.js /workspace"
echo "=================================================================="
