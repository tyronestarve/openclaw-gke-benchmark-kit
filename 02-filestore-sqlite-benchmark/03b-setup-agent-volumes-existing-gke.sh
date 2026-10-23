#!/bin/bash
# ==============================================================================
# 03b-setup-agent-volumes-existing-gke.sh
# 专供【已有 GKE 集群 + 已有 VPC/子网】客户的极简版 Filestore Agent Volumes (VolumePool) 初始化脚本
#
# 已精简剔除所有新建 VPC、新建子网、新建 GKE 集群的冗余代码，直接从原 Step 3 开始，仅包含 5 个纯粹步骤：
#   1. 检查现有 GKE 集群（若不存在直接报错退出，绝不新建集群）并关闭冲突的内置 Filestore CSI 插件
#   2. 检查/安装 Agent Sandbox CRD
#   3. 配置 Filestore Service Agent IAM 权限并检查/复用 PSC 连接策略
#   4. 调用 REST API 创建「单实例 × 1 TiB」省配额版 VolumePool
#   5. 部署支持 VolumePool 的 CSI 驱动，并创建带 14 项 NFS 极限性能调优参数的 StorageClass
#
# 运行方式：
#   export PROJECT_ID="your-gcp-project-id"
#   export REGION="asia-southeast1"
#   export CLUSTER_LOCATION="asia-southeast1"   # Regional 集群填 region，Zonal 集群填 zone
#   export CLUSTER="your-existing-gke-cluster"
#   export VPC_NETWORK="your-existing-vpc"
#   export SUBNET_NAME="your-existing-subnet"
#   bash ./03b-setup-agent-volumes-existing-gke.sh
# ==============================================================================
set -euo pipefail

PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project)}
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
REGION=${REGION:-asia-southeast1}
CLUSTER_LOCATION=${CLUSTER_LOCATION:-$REGION}
CLUSTER=${CLUSTER:-your-existing-gke-cluster}
VPC_NETWORK=${VPC_NETWORK:-your-existing-vpc}
SUBNET_NAME=${SUBNET_NAME:-your-existing-subnet}

# VolumePool 省配额默认配置（单实例 1TiB，支持 100 个 Share，默认单卷配额 2GiB）
VOLUME_POOL_ID=${VOLUME_POOL_ID:-my-volume-pool-1tb}
DEFAULT_QUOTA_MIB=${DEFAULT_QUOTA_MIB:-2048}
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
echo "Filestore Agent Volumes (VolumePool) 极简初始化 (针对已有 GKE 集群)"
echo "  Project          : $PROJECT_ID ($PROJECT_NUMBER)"
echo "  Region           : $REGION (Cluster Location: $CLUSTER_LOCATION)"
echo "  Existing Cluster : $CLUSTER"
echo "  Existing VPC     : $VPC_NETWORK / $SUBNET_NAME"
echo "  Volume Pool ID   : $VOLUME_POOL_ID (${POOL_MIN_INSTANCES}x ${POOL_INST_CAPACITY_GB}GB)"
echo "=================================================================="

# ------------------------------------------------------------------------------
# 1. (对应原 Step 3) 校验现有 GKE 集群并关闭冲突的内置 GcpFilestoreCsiDriver 插件
# ------------------------------------------------------------------------------
echo "[1/5] 校验现有 GKE 集群 ($CLUSTER) 并检查内置 Filestore CSI 插件状态..."
if ! gcloud container clusters describe "$CLUSTER" --location="$CLUSTER_LOCATION" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "❌ 错误：在项目 $PROJECT_ID ($CLUSTER_LOCATION) 中未找到 GKE 集群 '$CLUSTER'！"
  echo "   请检查环境变量 CLUSTER 与 CLUSTER_LOCATION 是否填写正确（本脚本不会新建集群）。"
  exit 1
fi

ADDON_ENABLED=$(gcloud container clusters describe "$CLUSTER" --location="$CLUSTER_LOCATION" --project="$PROJECT_ID" \
  --format="value(addonsConfig.gcpFilestoreCsiDriverConfig.enabled)" 2>/dev/null || true)
if [ "$ADDON_ENABLED" = "True" ]; then
  echo "  -> 检测到 GKE 内置 GcpFilestoreCsiDriver 已开启，正在将其关闭（使用 volumepool CSI 驱动的前置要求）..."
  gcloud container clusters update "$CLUSTER" \
    --location="$CLUSTER_LOCATION" \
    --project="$PROJECT_ID" \
    --update-addons=GcpFilestoreCsiDriver=DISABLED
else
  echo "  -> 内置 GcpFilestoreCsiDriver 已处于关闭状态，无需变更。"
fi

gcloud container clusters get-credentials "$CLUSTER" --location="$CLUSTER_LOCATION" --project="$PROJECT_ID"

# ------------------------------------------------------------------------------
# 2. (对应原 Step 4) 检查/安装 Agent Sandbox CRD
# ------------------------------------------------------------------------------
echo "[2/5] 检查 GKE Agent Sandbox CRD..."
if ! kubectl get crd sandboxes.agents.x-k8s.io >/dev/null 2>&1; then
  kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${AGENT_SANDBOX_VERSION}/manifest.yaml"
  kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${AGENT_SANDBOX_VERSION}/extensions.yaml"
  echo "  -> Agent Sandbox CRD 安装完成。"
else
  echo "  -> [已存在，跳过] Agent Sandbox CRD。"
fi

# ------------------------------------------------------------------------------
# 3. (对应原 Step 5 & 6) 配置 Filestore Service Agent 权限并检查/复用 PSC 连接策略
# ------------------------------------------------------------------------------
echo "[3/5] 配置 Filestore Service Agent 权限并检查 PSC Service Connection Policy..."
gcloud services enable file.googleapis.com networkconnectivity.googleapis.com iam.googleapis.com --project="$PROJECT_ID"

gcloud beta services identity create --service=file.googleapis.com --project="$PROJECT_ID" || true
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@cloud-filer.iam.gserviceaccount.com" \
  --role="roles/file.editor" \
  --condition=None >/dev/null

EXISTING_PSC=$(gcloud network-connectivity service-connection-policies list \
  --region="$REGION" --project="$PROJECT_ID" \
  --filter="serviceClass=google-cloud-filestore" \
  --format="value(name)" 2>/dev/null | head -n 1 || true)

if [ -n "$EXISTING_PSC" ]; then
  echo "  -> [已存在，直接复用] 检测到当前 Region 已有 google-cloud-filestore PSC 策略: $EXISTING_PSC"
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
# 4. (对应原 Step 7) 创建「单实例 × 1 TiB」VolumePool (v1beta1 REST API)
# ------------------------------------------------------------------------------
echo "[4/5] 检查/创建 Filestore Regional Volume Pool ($VOLUME_POOL_ID)..."
TOKEN=$(gcloud auth print-access-token)
POOL_URL="https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools/${VOLUME_POOL_ID}"
HTTP_CODE=$(curl -s -o /tmp/volpool_resp.json -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" "$POOL_URL")

if [ "$HTTP_CODE" = "200" ]; then
  STATE=$(python3 -c 'import json; print(json.load(open("/tmp/volpool_resp.json")).get("state","UNKNOWN"))')
  echo "  -> [已存在，直接复用] Volume Pool $VOLUME_POOL_ID (state=$STATE)。"
else
  echo "  -> 发起创建 Volume Pool $VOLUME_POOL_ID (单实例 1TiB 省配额模式)..."
  POOL_DESC="GoogleReservedOverrides={\"CustomVolumePoolConfig\":{\"MinInstances\":${POOL_MIN_INSTANCES},\"MaxInstances\":${POOL_MAX_INSTANCES},\"InstanceCapacityGb\":${POOL_INST_CAPACITY_GB},\"MinAvailableShares\":${POOL_MIN_AVAIL_VOLS},\"MaxSharesPerInstance\":${POOL_MAX_VOLS_PER_INST}}}"
  CREATE_RESP=$(curl -s -X POST \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d "{
      \"description\": $(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$POOL_DESC"),
      \"network\": \"projects/${PROJECT_ID}/global/networks/${VPC_NETWORK}\",
      \"defaultVolumeQuotaMib\": ${DEFAULT_QUOTA_MIB}
    }" \
    "https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools?volumePoolId=${VOLUME_POOL_ID}")
  echo "$CREATE_RESP"
  OP_NAME=$(echo "$CREATE_RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("name",""))')
  if [ -z "$OP_NAME" ]; then
    echo "❌ 创建 Volume Pool 失败，请检查上述返回信息。"
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
# 5. (对应原 Step 8) 部署支持 VolumePool 的 CSI 驱动并创建 14 项参数调优版 StorageClass
# ------------------------------------------------------------------------------
echo "[5/5] 部署支持 VolumePool 的 Filestore CSI Driver 并创建调优版 StorageClass..."
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
echo "✅ 已有 GKE 集群的 VolumePool 与调优版 StorageClass 部署完成！"
echo "=================================================================="
