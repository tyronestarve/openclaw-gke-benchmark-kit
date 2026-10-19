#!/bin/bash
# ==============================================================================
# 15-setup-agent-volumes.sh
# 从 0 到 1 一键初始化 Filestore Agent Volumes (VolumePool + PSC + GKE CSI 驱动)
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
REGION=${REGION:-us-central1}
ZONE=${ZONE:-us-central1-a}
CLUSTER=${CLUSTER:-agent-sandbox-cluster}
VPC_NETWORK=${VPC_NETWORK:-agent-sandbox-net}
SUBNET_NAME=${SUBNET_NAME:-agent-sandbox-subnet}
SUBNET_RANGE=${SUBNET_RANGE:-10.20.0.0/20}
VOLUME_POOL_ID=${VOLUME_POOL_ID:-agent-vol-pool}
DEFAULT_QUOTA_MIB=${DEFAULT_QUOTA_MIB:-1024}   # 默认每卷配额 1GiB (1024 MiB)，与 PVC 请求容量保持一致以避免触发 Resize
POOL_MIN_INSTANCES=${POOL_MIN_INSTANCES:-1}
POOL_MAX_INSTANCES=${POOL_MAX_INSTANCES:-2}
POOL_INST_CAPACITY_GB=${POOL_INST_CAPACITY_GB:-1024}
POOL_MIN_AVAIL_VOLS=${POOL_MIN_AVAIL_VOLS:-120}
POOL_MAX_VOLS_PER_INST=${POOL_MAX_VOLS_PER_INST:-200}
PSC_POLICY_NAME=${PSC_POLICY_NAME:-filestore-agent-psc-policy}
CSI_GSA_NAME=${CSI_GSA_NAME:-filestore-csi-gsa}
CSI_GSA_EMAIL="${CSI_GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
AGENT_SANDBOX_VERSION=${AGENT_SANDBOX_VERSION:-v0.2.1}

echo "=================================================================="
echo "Phase 0: 从 0 到 1 初始化 Filestore Agent Volumes 测试环境"
echo "  Project        : $PROJECT_ID ($PROJECT_NUMBER)"
echo "  Region / Zone  : $REGION / $ZONE"
echo "  GKE Cluster    : $CLUSTER"
echo "  VPC / Subnet   : $VPC_NETWORK / $SUBNET_NAME"
echo "  Volume Pool ID : $VOLUME_POOL_ID (默认单卷配额: ${DEFAULT_QUOTA_MIB} MiB)"
echo "=================================================================="

# 1. 启用必需的 Google Cloud API
echo "[1/8] 启用必需的 Google Cloud API..."
gcloud services enable \
  compute.googleapis.com \
  container.googleapis.com \
  file.googleapis.com \
  networkconnectivity.googleapis.com \
  iam.googleapis.com \
  --project="$PROJECT_ID"

# 2. 检查/创建 VPC 网络与子网
echo "[2/8] 检查/创建 VPC 网络 ($VPC_NETWORK) 与子网 ($SUBNET_NAME)..."
if ! gcloud compute networks describe "$VPC_NETWORK" --project="$PROJECT_ID" >/dev/null 2>&1; then
  gcloud compute networks create "$VPC_NETWORK" \
    --project="$PROJECT_ID" \
    --subnet-mode=custom
  echo "  -> VPC $VPC_NETWORK 创建成功。"
else
  echo "  -> VPC $VPC_NETWORK 已存在。"
fi

if ! gcloud compute networks subnets describe "$SUBNET_NAME" --region="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
  gcloud compute networks subnets create "$SUBNET_NAME" \
    --project="$PROJECT_ID" \
    --network="$VPC_NETWORK" \
    --region="$REGION" \
    --range="$SUBNET_RANGE"
  echo "  -> 子网 $SUBNET_NAME ($SUBNET_RANGE) 创建成功。"
else
  echo "  -> 子网 $SUBNET_NAME 已存在。"
fi

# 3. 检查/创建 GKE Standard 集群（含 Workload Identity 与 gVisor 节点池）
echo "[3/8] 检查/创建 GKE 集群 ($CLUSTER)..."
if ! gcloud container clusters describe "$CLUSTER" --location="$ZONE" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "  -> 未检测到集群 $CLUSTER，开始创建 GKE Standard 集群 (开启 Workload Identity，禁用内置 Filestore CSI 插件)..."
  gcloud container clusters create "$CLUSTER" \
    --project="$PROJECT_ID" \
    --zone="$ZONE" \
    --release-channel=rapid \
    --network="$VPC_NETWORK" \
    --subnetwork="$SUBNET_NAME" \
    --ip-alias \
    --num-nodes=1 \
    --machine-type=e2-standard-4 \
    --workload-pool="${PROJECT_ID}.svc.id.goog"
else
  echo "  -> GKE 集群 $CLUSTER 已存在，检查内置 GcpFilestoreCsiDriver 插件状态..."
  ADDON_ENABLED=$(gcloud container clusters describe "$CLUSTER" --location="$ZONE" --project="$PROJECT_ID" \
    --format="value(addonsConfig.gcpFilestoreCsiDriverConfig.enabled)" 2>/dev/null || true)
  if [ "$ADDON_ENABLED" = "True" ]; then
    echo "  -> 关闭 GKE 内置 GcpFilestoreCsiDriver 插件（使用开源 volumepool 驱动的前置要求）..."
    gcloud container clusters update "$CLUSTER" \
      --location="$ZONE" \
      --project="$PROJECT_ID" \
      --update-addons=GcpFilestoreCsiDriver=DISABLED
  fi
fi

# 检查/创建 gVisor 沙箱节点池
if ! gcloud container node-pools describe gvisor-pool --cluster="$CLUSTER" --zone="$ZONE" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "  -> 创建 gVisor 沙箱节点池 (gvisor-pool)..."
  gcloud container node-pools create gvisor-pool \
    --project="$PROJECT_ID" \
    --cluster="$CLUSTER" \
    --zone="$ZONE" \
    --machine-type=e2-standard-8 \
    --num-nodes=2 \
    --sandbox="type=gvisor"
else
  echo "  -> gVisor 节点池已存在。"
fi

gcloud container clusters get-credentials "$CLUSTER" --zone="$ZONE" --project="$PROJECT_ID"

# 4. 安装 GKE Agent Sandbox 控制器与 CRD (如尚未安装)
echo "[4/8] 检查/安装 Agent Sandbox 控制器 (${AGENT_SANDBOX_VERSION})..."
if ! kubectl get crd sandboxes.agents.x-k8s.io >/dev/null 2>&1; then
  kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${AGENT_SANDBOX_VERSION}/manifest.yaml"
  kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${AGENT_SANDBOX_VERSION}/extensions.yaml"
  echo "  -> Agent Sandbox 控制器与 Extensions CRD 安装完成。"
else
  echo "  -> Agent Sandbox CRD 已安装。"
fi

# 5. 初始化 Filestore Service Agent 并授予 roles/file.editor
echo "[5/8] 初始化 Filestore Service Agent 并配置权限..."
gcloud beta services identity create \
  --service=file.googleapis.com \
  --project="$PROJECT_ID" || true

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@cloud-filer.iam.gserviceaccount.com" \
  --role="roles/file.editor" \
  --condition=None >/dev/null

# 6. 创建 Private Service Connect (PSC) 服务连接策略
echo "[6/8] 检查/创建 PSC Service Connection Policy ($PSC_POLICY_NAME)..."
if gcloud network-connectivity service-connection-policies describe "$PSC_POLICY_NAME" \
     --region="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "  -> PSC Policy $PSC_POLICY_NAME 已存在。"
else
  gcloud network-connectivity service-connection-policies create "$PSC_POLICY_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --network="projects/${PROJECT_ID}/global/networks/${VPC_NETWORK}" \
    --subnets="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}" \
    --service-class=google-cloud-filestore
  echo "  -> PSC Policy 创建成功。"
fi

# 7. 创建 Regional Volume Pool (v1beta1 REST API)
echo "[7/8] 检查/创建 Filestore Regional Volume Pool ($VOLUME_POOL_ID)..."
TOKEN=$(gcloud auth print-access-token)
POOL_URL="https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools/${VOLUME_POOL_ID}"
HTTP_CODE=$(curl -s -o /tmp/volpool_resp.json -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" "$POOL_URL")

if [ "$HTTP_CODE" = "200" ]; then
  STATE=$(python3 -c 'import json; print(json.load(open("/tmp/volpool_resp.json")).get("state","UNKNOWN"))')
  echo "  -> Volume Pool $VOLUME_POOL_ID 已存在 (state=$STATE)。"
else
  echo "  -> 发起创建 Volume Pool $VOLUME_POOL_ID (轻量池配置: MinInstances=${POOL_MIN_INSTANCES}, InstanceCapacityGb=${POOL_INST_CAPACITY_GB})..."
  POOL_DESC="GoogleReservedOverrides={\"CustomVolumePoolConfig\":{\"MinInstances\":${POOL_MIN_INSTANCES},\"MaxInstances\":${POOL_MAX_INSTANCES},\"InstanceCapacityGb\":${POOL_INST_CAPACITY_GB},\"MinAvailableVolumes\":${POOL_MIN_AVAIL_VOLS},\"MaxVolumesPerInstance\":${POOL_MAX_VOLS_PER_INST},\"MinAvailableShares\":${POOL_MIN_AVAIL_VOLS},\"MaxSharesPerInstance\":${POOL_MAX_VOLS_PER_INST}}}"
  CREATE_RESP=$(curl -s -X POST \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d "{
      \"description\": $(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$POOL_DESC"),
      \"network\": \"projects/${PROJECT_ID}/global/networks/${VPC_NETWORK}\",
      \"defaultVolumeQuotaMib\": ${DEFAULT_QUOTA_MIB},
      \"labels\": {
        \"env\": \"eval\",
        \"workload\": \"agent-sandboxes\"
      }
    }" \
    "https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools?volumePoolId=${VOLUME_POOL_ID}")
  echo "$CREATE_RESP"
  OP_NAME=$(echo "$CREATE_RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("name",""))')
  if [ -z "$OP_NAME" ]; then
    echo "!! 创建 Volume Pool 失败。请确认项目是否已开通 Filestore Agent Volumes 权限: https://forms.gle/vYPkcFiZVoTjf7Ah7"
    exit 1
  fi
  echo "  -> 等待 LRO 完成: $OP_NAME"
  while true; do
    DONE=$(curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
      "https://file.googleapis.com/v1beta1/${OP_NAME}" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("done",False))')
    [ "$DONE" = "True" ] && break
    sleep 5
  done
  echo "  -> Volume Pool $VOLUME_POOL_ID 已就绪 (READY)！"
fi

# 8. 配置 Workload Identity、部署开源 Filestore CSI Driver (volumepool overlay) 并创建 StorageClass
echo "[8/8] 部署开源 gcp-filestore-csi-driver (volumepool overlay) 与 StorageClass..."
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

kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: volume-pool-sc
provisioner: filestore.csi.storage.gke.io
parameters:
  volume-pool: "projects/${PROJECT_ID}/locations/${REGION}/volumePools/${VOLUME_POOL_ID}"
mountOptions:
  - vers=3
  - proto=tcp
  - mountproto=tcp
  - port=2049
  - mountport=2050
volumeBindingMode: Immediate
reclaimPolicy: Delete
allowVolumeExpansion: true
EOF

kubectl get storageclass volume-pool-sc
echo "=================================================================="
echo "✅ Filestore Agent Volumes 从 0 到 1 环境初始化全部完成！"
echo "   下一步可执行全维度测试套件："
echo "   ./16-test-agent-volumes-suite.sh all"
echo "=================================================================="
