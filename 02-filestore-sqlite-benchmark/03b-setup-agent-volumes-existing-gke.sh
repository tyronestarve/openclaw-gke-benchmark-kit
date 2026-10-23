#!/bin/bash
# ==============================================================================
# 03b-setup-agent-volumes-existing-gke.sh
# 专供【已有 GKE 集群 + 已有 VPC/子网】客户的纯命令清单版（无 if/else 分支）
#
# 说明：
#   - 每一条命令直观透明，可直接整段运行，也可逐段复制到终端手动执行。
#   - 执行前请先修改下方环境变量为您的真实环境名称。
# ==============================================================================
set -euo pipefail

# ------------------------------------------------------------------------------
# 0. 环境变量配置（请根据实际环境修改）
# ------------------------------------------------------------------------------
export PROJECT_ID="your-gcp-project-id"
export PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
export REGION="asia-southeast1"
export CLUSTER_LOCATION="asia-southeast1"          # Regional 集群填 region，Zonal 集群填 zone
export CLUSTER="your-existing-gke-cluster"
export VPC_NETWORK="your-existing-vpc"
export SUBNET_NAME="your-existing-subnet"

export VOLUME_POOL_ID="my-volume-pool-1tb"
export DEFAULT_QUOTA_MIB=2048                      # 默认单卷配额 2GiB (2048 MiB)
export PSC_POLICY_NAME="openclaw-filestore"
export CSI_GSA_NAME="filestore-csi-gsa"
export CSI_GSA_EMAIL="${CSI_GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
export AGENT_SANDBOX_VERSION="v0.2.1"

# ------------------------------------------------------------------------------
# Step 1. 关闭现有 GKE 集群内置的旧版 Filestore CSI 插件并获取集群凭证
# ------------------------------------------------------------------------------
gcloud container clusters update "$CLUSTER" \
  --location="$CLUSTER_LOCATION" \
  --project="$PROJECT_ID" \
  --update-addons=GcpFilestoreCsiDriver=DISABLED

gcloud container clusters get-credentials "$CLUSTER" \
  --location="$CLUSTER_LOCATION" \
  --project="$PROJECT_ID"

# ------------------------------------------------------------------------------
# Step 2. 安装 GKE Agent Sandbox CRD
# ------------------------------------------------------------------------------
kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${AGENT_SANDBOX_VERSION}/manifest.yaml"
kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${AGENT_SANDBOX_VERSION}/extensions.yaml"

# ------------------------------------------------------------------------------
# Step 3. 启用 API、配置 Filestore Service Agent 权限并创建 PSC 连接策略
# （注：如果您之前已执行过 PSC service-connection-policies create，可注释掉最后一条命令）
# ------------------------------------------------------------------------------
gcloud services enable file.googleapis.com networkconnectivity.googleapis.com iam.googleapis.com \
  --project="$PROJECT_ID"

gcloud beta services identity create \
  --service=file.googleapis.com \
  --project="$PROJECT_ID"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@cloud-filer.iam.gserviceaccount.com" \
  --role="roles/file.editor" \
  --condition=None

gcloud network-connectivity service-connection-policies create "$PSC_POLICY_NAME" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --network="projects/${PROJECT_ID}/global/networks/${VPC_NETWORK}" \
  --subnets="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}" \
  --service-class=google-cloud-filestore

# ------------------------------------------------------------------------------
# Step 4. 调用 REST API 创建「单实例 × 1 TiB」VolumePool
# （注：创建为后台异步操作，可通过下方 GET 命令查看 state 是否变为 READY）
# ------------------------------------------------------------------------------
curl -s -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d "{
    \"description\": \"GoogleReservedOverrides={\\\"CustomVolumePoolConfig\\\":{\\\"MinInstances\\\":1,\\\"MaxInstances\\\":1,\\\"InstanceCapacityGb\\\":1024,\\\"MinAvailableShares\\\":10,\\\"MaxSharesPerInstance\\\":100}}\",
    \"network\": \"projects/${PROJECT_ID}/global/networks/${VPC_NETWORK}\",
    \"defaultVolumeQuotaMib\": ${DEFAULT_QUOTA_MIB}
  }" \
  "https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools?volumePoolId=${VOLUME_POOL_ID}"

# 查询 VolumePool 状态（确认 state 为 READY 后再创建 PVC）：
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools/${VOLUME_POOL_ID}"

# ------------------------------------------------------------------------------
# Step 5. 部署支持 VolumePool 的开源 Filestore CSI 驱动并绑定 Workload Identity
# ------------------------------------------------------------------------------
gcloud iam service-accounts create "$CSI_GSA_NAME" \
  --display-name="Filestore CSI Driver GSA" \
  --project="$PROJECT_ID"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${CSI_GSA_EMAIL}" \
  --role="roles/file.editor" \
  --condition=None

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/file.editor" \
  --condition=None

gcloud iam service-accounts add-iam-policy-binding "$CSI_GSA_EMAIL" \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[gcp-filestore-csi-driver/gcp-filestore-csi-controller-sa]" \
  --project="$PROJECT_ID"

git clone --depth=1 https://github.com/kubernetes-sigs/gcp-filestore-csi-driver.git /tmp/gcp-filestore-csi-driver
kubectl apply -k /tmp/gcp-filestore-csi-driver/deploy/kubernetes/overlays/volumepool
rm -rf /tmp/gcp-filestore-csi-driver

kubectl annotate serviceaccount gcp-filestore-csi-controller-sa \
  --namespace=gcp-filestore-csi-driver \
  --overwrite \
  "iam.gke.io/gcp-service-account=${CSI_GSA_EMAIL}"

kubectl rollout status statefulset/gcp-filestore-csi-controller -n gcp-filestore-csi-driver --timeout=180s
kubectl rollout status daemonset/gcp-filestore-csi-node -n gcp-filestore-csi-driver --timeout=180s

# ------------------------------------------------------------------------------
# Step 6. 创建带 14 项 NFS 性能调优参数的 StorageClass
# ------------------------------------------------------------------------------
kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: volume-pool-1tb-tuned-sc
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

kubectl get storageclass volume-pool-1tb-tuned-sc
