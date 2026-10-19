# PoC 验证辅助函数（source 使用）。前提：poc-probe pod 已运行。
probe() {  # $1 = host（pod IP 或 service DNS）
  kubectl exec poc-probe -- curl -s -o /dev/null -w '%{http_code}' \
    --max-time 5 "http://$1:18789/" 2>/dev/null
}

wait_serviceable() {  # $1 = host；输出耗时（秒）
  local t0=$(date +%s.%N)
  until [ "$(probe "$1")" = "200" ]; do sleep 0.3; done
  echo "$(echo "$(date +%s.%N) - $t0" | bc)"
}

wait_claim_bound() {  # $1 = claim 名；输出被认领的 sandbox 名
  local host=""
  until host=$(kubectl get sandboxclaim "$1" -o jsonpath='{.status.sandbox.name}' 2>/dev/null) \
        && [ -n "$host" ]; do sleep 0.2; done
  echo "$host"
}

make_claim() {  # $1 = claim 名；从热池认领（v0.5.2 起 claim 只能指向热池）
  kubectl apply -f - <<EOF
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxClaim
metadata:
  name: $1
  namespace: default
spec:
  warmPoolRef:
    name: openclaw-pool
EOF
}
