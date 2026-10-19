#!/bin/bash
# 批量预切工号盘("盘池"):为每个员工一次性创建静态工作盘 PVC。
# share 切分是实例级串行的慢操作(单刀数十秒;官方口径 6 分钟 80 个)——
# 本脚本把它做成"入职时的一次性流水作业",此后认领/休眠/唤醒/重建只剩亚秒挂载。
#
# 用法:
#   ./14-precarve-workspaces.sh 10001 10002 10003        # 指定工号列表
#   ./14-precarve-workspaces.sh $(seq -f "1%04g" 1 20)   # 批量 10001~10020
#
# 注意:
# - StorageClass 需带 max-volume-size:"128Gi"(见 12-filestore-multishare.yaml),
#   单实例可切 80 个 share;旧实例(该参数生效前创建的)上限仅 10 个,超了会
#   自动裂出新的 1TiB 实例(计费翻倍)——大批量预切前先确认实例代际;
# - PVC 为 WaitForFirstConsumer 绑定模式:share 在首个 pod 调度时才真正切分。
#   如需"纯预切"(创建即切,不等 pod),给每张 PVC 配一个短命触发 pod,见 --warm。
set -euo pipefail
NS=${NAMESPACE:-default}
SC=${WORKSPACE_SC:-openclaw-fsms}
SIZE=${WORKSPACE_SIZE:-10Gi}
WARM=false
[ "${1:-}" = "--warm" ] && { WARM=true; shift; }
[ $# -ge 1 ] || { echo "用法: $0 [--warm] <工号...>"; exit 1; }

T0=$(date +%s)
for EMP in "$@"; do
  kubectl apply -n "$NS" -f - <<EOF >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: emp-${EMP}-ws
  labels: { app: sandbox-portal, employee: "${EMP}" }
spec:
  accessModes: ["ReadWriteMany"]
  storageClassName: ${SC}
  resources: { requests: { storage: ${SIZE} } }
EOF
  echo "已登记: emp-${EMP}-ws"
done

if $WARM; then
  # 触发切分:每张盘挂一个短命 pod,share 切完即退出(切分仍串行,总时长≈N×单刀)
  for EMP in "$@"; do
    kubectl run "precarve-${EMP}" -n "$NS" --restart=Never --image=busybox \
      --overrides="{\"spec\":{\"containers\":[{\"name\":\"c\",\"image\":\"busybox\",
        \"command\":[\"true\"],\"resources\":{\"requests\":{\"cpu\":\"10m\",\"memory\":\"16Mi\"}},
        \"volumeMounts\":[{\"name\":\"ws\",\"mountPath\":\"/ws\"}]}],
        \"volumes\":[{\"name\":\"ws\",\"persistentVolumeClaim\":{\"claimName\":\"emp-${EMP}-ws\"}}]}}" \
      >/dev/null 2>&1 || true
  done
  echo "等待全部 share 切分(Bound)…"
  for EMP in "$@"; do
    while [ "$(kubectl get pvc "emp-${EMP}-ws" -n "$NS" -o jsonpath='{.status.phase}')" != "Bound" ]; do
      sleep 5
    done
    echo "  emp-${EMP}-ws Bound (+$(( $(date +%s) - T0 ))s)"
  done
  kubectl delete pod -n "$NS" $(printf "precarve-%s " "$@") --ignore-not-found >/dev/null 2>&1
fi
echo "完成: $# 张工号盘,总耗时 $(( $(date +%s) - T0 ))s"
