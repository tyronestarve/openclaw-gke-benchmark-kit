# OpenClaw Sandbox Portal

沙箱生命周期管理台：领取/删除、休眠（快照 + 原生 Suspended）/唤醒（含"真恢复"校验）、
按工号固定地址、SSO（IAP-ready，未接 IAP 时用演示登录）。

## 架构

```
浏览器 ──(IAP SSO / 演示登录)──▶ portal (FastAPI, system 节点)
                                   │ K8s API（最小 RBAC）
                 ┌─────────────────┼──────────────────┐
           SandboxClaim      Sandbox.operatingMode   PodSnapshotManualTrigger
          （领取/删除）      （Suspended/Running）     （休眠前拍快照）
```

- 领取 = 创建 SandboxClaim（warmPoolRef）+ `additionalPodMetadata` 注工号标签
  + 建 `emp-<工号>` Service（固定地址，跨重建不变）；
- 休眠 = 拍内存快照 → 等 `condition=Ready` → `operatingMode: Suspended`
  （控制器删 pod，**资源真实释放**，Sandbox 对象保留）；
- 唤醒 = `operatingMode: Running` → 控制器重建 pod → 自动从快照恢复 →
  应用层 HTTP 探活 + 核对 `Successfully restored` 事件（防静默降级）。

## 部署

```bash
gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/openclaw-poc/sandbox-portal:v1
export PORTAL_IMAGE=$REGION-docker.pkg.dev/$PROJECT_ID/openclaw-poc/sandbox-portal:v1
envsubst < deploy/k8s.yaml | kubectl apply -f -
```

前提：agent-sandbox ≥ v0.5.2、热池 `openclaw-pool`、Pod Snapshot 已启用
（见《openclaw-客户PoC应答方案》§0）。

## 访问

- 调试：`kubectl port-forward svc/sandbox-portal 8080:80` → http://localhost:8080
- 生产：挂到 Gateway（HTTPRoute → sandbox-portal:80）+ **开启 IAP**，然后把
  Deployment 环境变量 `DEV_LOGIN` 改为 `false`。portal 自动读取 IAP 注入的
  `X-Goog-Authenticated-User-Email` 作为身份；`ADMIN_EMPLOYEES` 配管理员。

## 已知事项 / 待办

- 唤醒实测 ~2.5 分钟（手工路径 22 秒）：疑似控制器对 operatingMode 的
  reconcile 间隔，投产前调查；
- 任务表在内存中（单副本）；多副本需外置（ConfigMap/DB）；
- IAP 接入需要：OAuth 同意屏 + 公网/内网 LB + （客户 IdP 时）Workforce
  Identity Federation 配置——属环境配置，非代码改动。
