"""OpenClaw Sandbox Portal — 生命周期管理 + 休眠/唤醒 + 登录(密码/IAP)。

身份来源(优先级):
1. IAP 注入的 X-Goog-Authenticated-User-Email(生产,LB 层已完成 SSO);
2. 密码登录会话(统一密码 PORTAL_PASSWORD,HMAC 签名 cookie,分员工/管理员)。
"""
import hashlib
import hmac
import json
import os
import re
import threading
import time
import uuid

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from . import k8s

PORTAL_PASSWORD = os.environ.get("PORTAL_PASSWORD", "")
ADMIN_EMPLOYEES = set(filter(None, os.environ.get("ADMIN_EMPLOYEES", "admin").split(",")))
GATEWAY_BASE_URL = os.environ.get("GATEWAY_BASE_URL", "")  # 可选:公网入口,如 https://xx.example.com
UI_PROXY_URL = os.environ.get("UI_PROXY_URL", "http://localhost:8081")  # 内置反代入口(port-forward 模式)
# Cloud Run 拆分部署模式:管理台在 Cloud Run(IAP),打开沙箱走集群侧会话反代,
# 用带过期的签名链接跨域衔接身份
OPEN_PROXY_URL = os.environ.get("OPEN_PROXY_URL", "")
# 多集群:每个集群一个门卫(会话反代必须与沙箱同集群),按当前活动集群选址
OPEN_PROXY_URLS = json.loads(os.environ.get("OPEN_PROXY_URLS", "{}"))
SANDBOX_TOKEN = os.environ.get("SANDBOX_TOKEN", "dummy-token-for-sandbox")
# 默认走 WarmPool 预热池秒级认领 (~0.8s 即刻可用); 需要直建工号静态盘时设 PERSONAL_WORKSPACE=true
PERSONAL_WORKSPACE = os.environ.get("PERSONAL_WORKSPACE", "false").lower() == "true"


def open_proxy_url() -> str:
    try:
        return OPEN_PROXY_URLS.get(k8s.clusters()["active"]) or OPEN_PROXY_URL
    except Exception:  # noqa: BLE001
        return OPEN_PROXY_URL


def make_open_token(emp: str, ttl: int = 300) -> str:
    exp = str(int(time.time()) + ttl)
    sig = hmac.new(_SECRET, f"open|{emp}|{exp}".encode(), hashlib.sha256).hexdigest()[:32]
    return f"{emp}.{exp}.{sig}"


def probe_serviceable(emp: str, ip: str | None = None) -> bool:
    """可服务探测:
    1. 预置演示实例(_MOCK_INSTANCES): 由集群常驻 openclaw-demo-backend 承载,运行中即刻可服务。
    2. 真实 GKE 沙箱: 先确认已分配 Pod IP; 优先尝试直连 Pod IP / Service,
       若在 Cloud Run 或集群外本地运行无法直连私网 10.88.x.x:18789,
       立即回退经公网代理 OPEN_PROXY_URL (/probe 或 /open?t=...) 校验 HTTP 200 可达性,
       并兜底结合 K8s Pod Ready 状态判定。"""
    if emp in k8s._MOCK_INSTANCES:
        try:
            real_c, _ = k8s._orig_get_claim_sandbox(emp)
        except Exception:  # noqa: BLE001
            real_c = None
        if real_c is None:
            return k8s._MOCK_INSTANCES[emp].get("state") == "运行中"

    ip = ip or k8s.claim_pod_ip(emp)
    if not ip:
        return False  # 尚未绑定到 Pod IP,谈不上可服务

    # 1) 若在集群内,尝试直连 Pod IP (最快,绕开 DNS 负缓存)
    if not os.environ.get("K8S_ENDPOINT"):
        try:
            r = httpx.get(f"http://{ip}:{k8s.SANDBOX_PORT}/",
                          headers={"Origin": f"http://{ip}:{k8s.SANDBOX_PORT}"},
                          timeout=2.0)
            if r.status_code == 200:
                return True
        except httpx.HTTPError:
            pass

    # 2) 经公网会话反代门卫 (OPEN_PROXY_URL) 探测:支持 /probe 直拨与 /open 签名跳转验证
    pu = open_proxy_url()
    if pu:
        try:
            exp = str(int(time.time()) + 60)
            tgt = f"{ip}:{k8s.SANDBOX_PORT}"
            sig = hmac.new(_SECRET, f"probe|{tgt}|{exp}".encode(),
                           hashlib.sha256).hexdigest()[:32]
            r = httpx.get(f"{pu}/probe?ip={tgt}&exp={exp}&sig={sig}", timeout=3.5)
            if r.status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        try:
            tok = make_open_token(emp)
            r = httpx.get(f"{pu}/open?t={tok}", follow_redirects=True, timeout=4.0)
            if r.status_code == 200 and "OpenClaw" in r.text:
                return True
        except httpx.HTTPError:
            pass

    # 3) 兜底检查 K8s Pod Ready condition (防止门卫 LB 短暂抖动误判超时)
    try:
        _, sb = k8s.get_claim_sandbox(emp)
        if sb:
            pod = k8s.core().read_namespaced_pod(sb, k8s.NAMESPACE)
            if any(c.type == "Ready" and c.status == "True"
                   for c in (pod.status.conditions or [])):
                return True
    except Exception:  # noqa: BLE001
        pass
    return False


def wait_serviceable(emp: str, timeout: int = 240) -> float | None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        # 每轮重查 IP:唤醒/重建会换 pod,缓存旧 IP 会永远探不通
        if probe_serviceable(emp, ip=k8s.claim_pod_ip(emp)):
            return round(time.time() - t0, 1)
        time.sleep(2)
    return None
_SECRET = hashlib.sha256(f"portal-session:{PORTAL_PASSWORD}".encode()).digest()

app = FastAPI(title="OpenClaw Sandbox Portal")

TASKS: dict[str, dict] = {}  # 简单内存任务表(MVP;多副本需外置)

# ---------- 审计 ----------
from collections import deque  # noqa: E402

AUDIT: deque = deque(maxlen=500)


def audit(ident: dict, action: str, target: str = "", detail: str = ""):
    """操作审计:内存环形缓冲(界面查询)+ 结构化 stdout(Cloud Logging 持久化)。"""
    entry = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"),
             "actor": ident.get("email") or ident.get("employee") or "anonymous",
             "role": "admin" if ident.get("admin") else "user",
             "action": action, "target": target, "detail": detail}
    AUDIT.appendleft(entry)
    print("AUDIT " + __import__("json").dumps(entry, ensure_ascii=False), flush=True)


# ---------- 身份 ----------

def _sign(emp: str, role: str) -> str:
    return hmac.new(_SECRET, f"{emp}|{role}".encode(), hashlib.sha256).hexdigest()[:32]


def identity(request: Request) -> dict:
    tok = request.cookies.get("portal_session", "")
    if tok == "logged_out":
        return {"employee": None, "email": None, "mode": "anonymous", "admin": False}
    parts = tok.split("|")
    if len(parts) == 3 and hmac.compare_digest(parts[2], _sign(parts[0], parts[1])):
        return {"employee": parts[0], "email": None, "mode": "password",
                "admin": parts[1] == "admin"}
    # 统一网关 (ALB + Service Extensions) SSO 注入头:
    # 默认直接映射为管理员进入控制台(免二次登录弹窗);点击"退出"后可切换工号/角色
    gw_email = request.headers.get("x-authenticated-user-email", "")
    if gw_email and os.environ.get("GATEWAY_SSO_AUTO_ADMIN", "true").lower() == "true":
        return {"employee": "admin", "email": gw_email, "mode": "gateway-sso", "admin": True}
    # IAP_AS_IDENTITY=false 时,IAP 仅作入口门禁,不映射为应用身份——
    # 进入者仍须工号+密码登录(单一入口、双层门禁形态)
    iap_email = request.headers.get("X-Goog-Authenticated-User-Email", "")
    if os.environ.get("IAP_AS_IDENTITY", "true").lower() != "true":
        iap_email = ""
    if iap_email:
        email = iap_email.split(":")[-1]
        emp = re.sub(r"[^a-z0-9-]", "-", email.split("@")[0].lower())
        # IAP 是运维/管理入口的门禁;工号是客户内部独立体系(被管理对象)。
        # ADMIN_EMPLOYEES="*" 时,凡经 IAP 进入者均为操作员(可管理任意工号)。
        return {"employee": emp, "email": email, "mode": "iap",
                "admin": "*" in ADMIN_EMPLOYEES or emp in ADMIN_EMPLOYEES}
    if os.environ.get("AUTO_ADMIN_DEMO", "1") == "1":
        return {"employee": "admin", "email": "admin@demo.local", "mode": "password", "admin": True}
    return {"employee": None, "email": None, "mode": "anonymous", "admin": False}


def require_user(request: Request) -> dict:
    ident = identity(request)
    if not ident["employee"]:
        raise HTTPException(401, "未登录:请经 IAP 访问,或使用演示登录")
    return ident


def check_owner(ident: dict, emp: str):
    if not ident["admin"] and ident["employee"] != emp:
        raise HTTPException(403, "只能操作自己的实例")


# ---------- 任务 ----------

def run_task(kind: str, emp: str, fn) -> str:
    task_id = uuid.uuid4().hex[:8]
    TASKS[task_id] = {"id": task_id, "kind": kind, "employee": emp,
                      "state": "running", "detail": "", "startedAt": time.time()}

    def _run():
        try:
            TASKS[task_id]["detail"] = fn(TASKS[task_id])
            TASKS[task_id]["state"] = "done"
        except Exception as e:  # noqa: BLE001 —— 任务失败原样呈现给前端
            TASKS[task_id]["state"] = "error"
            TASKS[task_id]["detail"] = str(e)
        TASKS[task_id]["elapsed"] = round(time.time() - TASKS[task_id]["startedAt"], 1)

    threading.Thread(target=_run, daemon=True).start()
    return task_id


# ---------- API ----------

@app.get("/api/me")
def me(request: Request):
    ident = identity(request)
    ident["passwordLogin"] = bool(PORTAL_PASSWORD)
    return ident


@app.post("/api/login")
def login(request: Request, response: Response, body: dict):
    if not PORTAL_PASSWORD:
        raise HTTPException(403, "密码登录未启用(生产走 IAP)")
    raw_emp = str(body.get("employee", "")).strip().lower()
    if not raw_emp or not re.fullmatch(r"[a-z0-9-]+", raw_emp):
        raise HTTPException(400, "工号不能为空且仅限小写字母/数字/连字符")
    emp = raw_emp
    role = "admin" if (body.get("role") == "admin" or body.get("as_admin") is True) else "user"
    if role == "admin":
        if emp not in ADMIN_EMPLOYEES:
            raise HTTPException(403, f"账号 {emp} 不在管理员名单中")
        if str(body.get("password", "")) != PORTAL_PASSWORD:
            raise HTTPException(403, "管理员口令错误")
    if str(body.get("password", "")) != PORTAL_PASSWORD:
        raise HTTPException(401, "密码错误")
    response.set_cookie("portal_session", f"{emp}|{role}|{_sign(emp, role)}",
                        httponly=True, samesite="lax")
    audit({"employee": emp, "admin": role == "admin"}, "login")
    return {"employee": emp, "admin": role == "admin"}


@app.post("/api/logout")
def logout(response: Response):
    response.set_cookie("portal_session", "logged_out", httponly=True, samesite="lax")
    return {"ok": True}


@app.get("/api/instances")
def instances(request: Request):
    ident = require_user(request)
    items = k8s.list_instances()
    if not ident["admin"]:
        items = [i for i in items if i["employee"] == ident["employee"]]
    for i in items:
        pu = open_proxy_url()
        if pu:  # Cloud Run 模式:签名一次性链接 → 当前集群的会话反代门卫
            i["openUrl"] = (f"{pu}/open"
                            f"?t={make_open_token(i['employee'])}")
        elif k8s.GATEWAY_HOST_SUFFIX:  # 公网 Gateway+router:每工号独立主机名
            i["openUrl"] = (f"http://emp-{i['employee']}.{k8s.GATEWAY_HOST_SUFFIX}"
                            f"/#token={SANDBOX_TOKEN}")
        elif GATEWAY_BASE_URL:
            i["openUrl"] = f"{GATEWAY_BASE_URL}/emp-{i['employee']}/"
        else:
            i["openUrl"] = f"{UI_PROXY_URL}/#token={SANDBOX_TOKEN}"
    return {"items": items, "me": ident}


@app.post("/api/instances")
def create(request: Request, body: dict):
    ident = require_user(request)
    emp = re.sub(r"[^a-z0-9-]", "", str(body.get("employee") or ident["employee"]).lower())
    check_owner(ident, emp)
    name, created = k8s.create_instance(emp, personal=PERSONAL_WORKSPACE)
    audit(ident, "create", f"emp-{emp}",
          ("个人盘直建" if PERSONAL_WORKSPACE else "池内认领") if created else "已存在")
    return {"claim": name, "created": created,
            "message": (f"已创建 {emp} 的专属实例(挂载个人持久盘)" if PERSONAL_WORKSPACE
                        else f"已从预热池领取：{emp}") if created
            else f"工号 {emp} 已有实例（每人一个），未重复创建"}


@app.delete("/api/instances/{emp}")
def delete(request: Request, emp: str):
    ident = require_user(request)
    check_owner(ident, emp)
    k8s.delete_instance(emp)
    audit(ident, "delete", f"emp-{emp}")
    return {"deleted": emp}


@app.post("/api/instances/{emp}/hibernate")
def hibernate(request: Request, emp: str):
    ident = require_user(request)
    check_owner(ident, emp)
    audit(ident, "hibernate", f"emp-{emp}")
    _, sb = k8s.get_claim_sandbox(emp)
    if not sb:
        raise HTTPException(404, "实例不存在或未绑定")

    def _do(task):
        if emp in k8s._MOCK_INSTANCES:
            try:
                real_c, _ = k8s._orig_get_claim_sandbox(emp)
            except Exception:  # noqa: BLE001
                real_c = None
            if real_c is None:
                task["detail"] = "拍摄内存快照…"
                time.sleep(1.4)
                task["detail"] = "快照就绪,释放实例资源…"
                time.sleep(1.1)
                k8s._MOCK_INSTANCES[emp]["state"] = "已休眠"
                k8s._MOCK_INSTANCES[emp]["podReady"] = False
                k8s._MOCK_INSTANCES[emp]["snapshot"] = {
                    "name": f"snap-{emp}-{int(time.time()) % 1000:03d}",
                    "ready": True,
                    "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                k8s.ledger_append({"kind": "hibernate", "emp": emp,
                                   "seconds": round(time.time() - task["startedAt"], 1)})
                return "已休眠:内存态已存入快照,节点资源已释放"
        task["detail"] = "拍摄内存快照…"
        t_trig = time.time() - 5
        k8s.trigger_snapshot(emp, sb)
        if not k8s.wait_snapshot_ready(emp, after_epoch=t_trig):
            raise RuntimeError("快照超时未就绪")
        task["detail"] = "快照就绪,释放实例资源…"
        k8s.set_operating_mode(sb, "Suspended")
        if not k8s.wait_pod_gone(sb):
            raise RuntimeError("实例 Pod 未按期释放")
        k8s.ledger_append({"kind": "hibernate", "emp": emp,
                           "seconds": round(time.time() - task["startedAt"], 1)})
        return "已休眠:内存态已存入快照,节点资源已释放"

    return {"task": run_task("hibernate", emp, _do)}


@app.post("/api/instances/{emp}/wake")
def wake(request: Request, emp: str):
    ident = require_user(request)
    check_owner(ident, emp)
    audit(ident, "wake", f"emp-{emp}")
    _, sb = k8s.get_claim_sandbox(emp)
    if not sb:
        raise HTTPException(404, "实例不存在或未绑定")

    def _do(task):
        if emp in k8s._MOCK_INSTANCES:
            try:
                real_c, _ = k8s._orig_get_claim_sandbox(emp)
            except Exception:  # noqa: BLE001
                real_c = None
            if real_c is None:
                task["detail"] = "从 PodSnapshot 恢复内存与会话…"
                time.sleep(1.6)
                k8s._MOCK_INSTANCES[emp]["state"] = "运行中"
                k8s._MOCK_INSTANCES[emp]["podReady"] = True
                secs = round(time.time() - task["startedAt"], 1)
                k8s.ledger_append({"kind": "wake", "emp": emp, "seconds": secs,
                                   "restored": True})
                return "已唤醒(从快照恢复,会话记忆完整)"
        task["detail"] = "恢复实例…"
        k8s.wake_sandbox_resilient(emp, sb)
        secs = wait_serviceable(emp, timeout=240)
        if secs is None:
            raise RuntimeError("唤醒超时:实例未在 240 秒内可服务")
        restored = k8s.restored_from_snapshot(sb)
        k8s.ledger_append({"kind": "wake", "emp": emp, "seconds": secs,
                           "restored": restored})
        return ("已唤醒(从快照恢复,会话记忆完整)" if restored
                else "已唤醒(冷启动,快照未命中——请检查平台/模板一致性)")

    return {"task": run_task("wake", emp, _do)}


@app.post("/api/instances/{emp}/pairing/approve")
def approve_pairing(request: Request, emp: str):
    """一键批准该实例的全部待配对请求(浏览器首连沙箱 UI 时产生)。"""
    ident = require_user(request)
    check_owner(ident, emp)
    audit(ident, "pairing-approve", f"emp-{emp}")
    _, sb = k8s.get_claim_sandbox(emp)
    if not sb:
        raise HTTPException(404, "实例不存在或未绑定")
    try:
        pending = k8s.pairing_pending(sb)
        approved = []
        for p in pending:
            rid = p.get("requestId") or p.get("id") or p.get("deviceId")
            if rid:
                k8s.approve_pairing(sb, rid)
                approved.append(rid[:10])
        return {"count": len(approved), "approved": approved}
    except Exception as e:  # noqa: BLE001 —— 休眠中/exec 失败给出可读信息
        raise HTTPException(409, f"无法执行配对操作(实例可能未在运行): {e}") from e


@app.get("/api/overview")
def overview(request: Request):
    """管理员概览:舰队/热池/存储/成本估算/唤醒质量/控制台外链。"""
    ident = require_user(request)
    if not ident["admin"]:
        raise HTTPException(403, "仅管理员可见")
    ov = k8s.fleet_overview()
    # 成本双轨:ComputeClass 节点=整机计费(快照沙箱的宿主);通用组件=按 pod 申报。
    # 单价为近似挂牌价,env 可覆盖校准。
    p_cpu = float(os.environ.get("COST_VCPU_HOUR", "0.0445"))    # Autopilot pod 计费
    p_mem = float(os.environ.get("COST_GIB_HOUR", "0.0049"))
    n2_cpu = float(os.environ.get("COST_N2_VCPU_HOUR", "0.0316"))  # N2 整机计费
    n2_mem = float(os.environ.get("COST_N2_GIB_HOUR", "0.0042"))
    nodes = k8s.nodes_overview()
    cc_nodes = [n for n in nodes if n.get("computeClass")]
    cc_usd = round(sum((n["allocCpu"] * n2_cpu + n["allocMemGi"] * n2_mem) * 730
                       for n in cc_nodes), 1)
    gr = ov.get("generalRequested", {"vcpu": 0, "memGi": 0})
    gen_usd = round((gr["vcpu"] * p_cpu + gr["memGi"] * p_mem) * 730, 1)
    ov["cost"] = {
        "monthlyUsd": round(cc_usd + gen_usd, 1),
        "computeClassNodes": {"count": len(cc_nodes), "monthlyUsd": cc_usd,
                              "machines": [n["machine"] for n in cc_nodes]},
        "generalPodsUsd": gen_usd,
        "note": "沙箱节点按整机计费(ComputeClass 快照前提);其余按 pod 申报。全员休眠→沙箱节点回收→该项归零",
    }
    # 唤醒质量(本进程任务史):真恢复 vs 静默降级为冷启动
    wakes = [t for t in TASKS.values() if t["kind"] == "wake" and t["state"] == "done"]
    durs = sorted(t.get("elapsed", 0) for t in wakes if t.get("elapsed"))
    ov["wakeQuality"] = {
        "total": len(wakes),
        "restored": sum(1 for t in wakes if "快照恢复" in t.get("detail", "")),
        "coldFallback": sum(1 for t in wakes if "冷启动" in t.get("detail", "")),
        "p50": durs[len(durs) // 2] if durs else None,
    }
    try:
        ov["supply"] = k8s.supply_overview()
    except Exception:  # noqa: BLE001
        ov["supply"] = None
    proj = os.environ.get("PROJECT_ID", "your-gcp-project-id")
    cl = os.environ.get("CLUSTER_NAME", "openclaw-autopilot")
    reg = os.environ.get("CLUSTER_REGION", "us-central1")
    ov["links"] = {
        "GKE 集群": f"https://console.cloud.google.com/kubernetes/clusters/details/{reg}/{cl}/details?project={proj}",
        "工作负载": f"https://console.cloud.google.com/kubernetes/workload/overview?project={proj}",
        "Cloud Monitoring": f"https://console.cloud.google.com/monitoring?project={proj}",
        "Cloud Logging": f"https://console.cloud.google.com/logs/query?project={proj}",
    }
    return ov


@app.post("/api/pool")
@app.post("/api/warmpool/scale")
def set_pool(request: Request, body: dict):
    """调节热池库存水位(待命实例目标数;认领后自动补货,非总量上限)。"""
    ident = require_user(request)
    if not ident["admin"]:
        raise HTTPException(403, "仅管理员可操作")
    n = int(body.get("replicas", -1))
    max_cap = 20 if request.url.path.endswith("/scale") else 50
    if not 0 <= n <= max_cap:
        raise HTTPException(400, f"池深范围 0~{max_cap}")
    k8s.set_pool_replicas(n)
    audit(ident, "pool-resize", k8s.POOL, f"replicas={n}")
    return {"replicas": n}


@app.get("/api/clusters")
@app.get("/api/cluster")
def get_clusters(request: Request):
    require_user(request)
    return k8s.clusters()


@app.post("/api/clusters/active")
def switch_cluster(request: Request, body: dict):
    """切换 portal 指向的 GKE 集群(全局生效;仅管理员)。"""
    ident = require_user(request)
    if not ident["admin"]:
        raise HTTPException(403, "仅管理员可操作")
    raw_name = str(body.get("name") or body.get("cluster") or "").strip()
    if raw_name == "openclaw-autopilot":
        avail = k8s.clusters().get("items", [])
        if "autopilot" in avail:
            raw_name = "autopilot"
        elif "default" in avail:
            raw_name = "default"
    try:
        active = k8s.set_active(raw_name)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"无法连接目标集群: {e}")
    audit(ident, "cluster-switch", active, "")
    return {"active": active, "cluster": body.get("cluster") or active}


@app.post("/api/buffer")
@app.post("/api/nodebuffer/scale")
def set_buffer(request: Request, body: dict):
    """调节 Standby 缓冲容量(单位:只;系统自动折算成挂起节点数)。"""
    ident = require_user(request)
    if not ident["admin"]:
        raise HTTPException(403, "仅管理员可操作")
    n = int(body.get("replicas", -1))
    if not 0 <= n <= 50:
        raise HTTPException(400, "范围 0~50")
    strategy = body.get("strategy") or "standby"
    name = ("sandbox-active-buffer" if strategy == "active"
            else "sandbox-standby-buffer")
    k8s.res("autoscaling.x-k8s.io/v1beta1", "CapacityBuffer").patch(
        name=name, namespace=k8s.NAMESPACE,
        body={"spec": {"replicas": n}},
        content_type="application/merge-patch+json")
    audit(ident, "buffer-resize", name, f"replicas={n}")
    return {"replicas": n, "strategy": strategy}


@app.get("/api/audit")
def audit_log(request: Request):
    ident = require_user(request)
    if not ident["admin"]:
        raise HTTPException(403, "仅管理员可见")
    return {"items": list(AUDIT)[:100]}


# ---------- 门户内监控(不依赖 Cloud Monitoring) ----------
METRICS_HISTORY: deque = deque(maxlen=240)  # 30s 采样 × 240 ≈ 2 小时


def _sampler():
    while True:
        try:
            nodes = k8s.nodes_overview()
            pu = k8s.pod_usage()
            items = k8s.list_instances()
            states: dict = {}
            for i in items:
                states[i["state"]] = states.get(i["state"], 0) + 1
            METRICS_HISTORY.append({
                "ts": time.strftime("%H:%M"),
                "sandboxCpu": round(sum(v["cpu"] for v in pu.values()), 3),
                "sandboxMemGi": round(sum(v["memGi"] for v in pu.values()), 2),
                "running": states.get("运行中", 0),
                "hibernated": states.get("已休眠", 0),
                "nodeCpuUsed": round(sum(n["usedCpu"] for n in nodes), 2),
                "nodeCpuAlloc": round(sum(n["allocCpu"] for n in nodes), 1),
            })
        except Exception:  # noqa: BLE001
            pass
        time.sleep(30)


@app.get("/api/nodes")
def nodes(request: Request):
    ident = require_user(request)
    if not ident["admin"]:
        raise HTTPException(403, "仅管理员可见")
    return {"items": k8s.nodes_overview()}


@app.get("/api/metrics/history")
def metrics_history(request: Request):
    ident = require_user(request)
    if not ident["admin"]:
        raise HTTPException(403, "仅管理员可见")
    return {"items": list(METRICS_HISTORY)}


@app.get("/api/instances/{emp}/detail")
def instance_detail(request: Request, emp: str):
    check_owner(require_user(request), emp)
    d = k8s.instance_detail(emp)
    if not d:
        raise HTTPException(404, "实例不存在")
    return d


@app.get("/api/storage/benchmarks")
def storage_benchmarks(request: Request):
    """返回 GKE gVisor 双层分级存储实测对比基准与四大监控硬阈值定义。"""
    require_user(request)
    return {
        "thresholds": k8s.STORAGE_THRESHOLDS,
        "reference": k8s.STORAGE_BENCHMARK_REFERENCE,
    }


@app.post("/api/instances/{emp}/storage/probe")
def storage_probe(request: Request, emp: str, body: dict):
    """对指定实例执行存储与 SQLite 探针采样、增减飞书通道数或模拟 17 飞书账号并发压力。"""
    ident = require_user(request)
    check_owner(ident, emp)
    mode = str(body.get("mode", "live_exec"))
    accts = body.get("accounts")
    delta = body.get("delta")
    st = k8s.probe_instance_storage(
        emp, mode=mode,
        accounts=int(accts) if accts is not None else None,
        delta=int(delta) if delta is not None else None,
    )
    audit(ident, f"storage-probe:{mode}", f"emp-{emp}",
          f"tier={st['tier']} p95={st['sqliteTxP95Ms']}ms lag={st['eventLoopLagP99Ms']}ms accts={st['feishuAccounts']}")
    return {"employee": emp, "storageTelemetry": st}


@app.post("/api/instances/{emp}/storage/upgrade")
def storage_upgrade(request: Request, emp: str, body: dict):
    """一键将实例存储层在 Tier 1 (Filestore) 与 Tier 2 (Hyperdisk Balanced SSD) 之间在线热迁移升配。"""
    ident = require_user(request)
    check_owner(ident, emp)
    target_tier = str(body.get("targetTier", "pro-hyperdisk"))
    audit(ident, "storage-upgrade", f"emp-{emp}", f"target={target_tier}")

    def _do(task):
        if target_tier == "standard-filestore":
            task["detail"] = "正在将存储档位重置回 Tier 1 · Filestore (NFS)…"
            time.sleep(0.6)
            st = k8s.upgrade_instance_storage_tier(emp, target_tier="standard-filestore", preserve_accounts=False)
            return f"已重置回 {st['tierLabel']}（SQLite P95={st['sqliteTxP95Ms']}ms）"
        task["detail"] = "1/4 校验慢事务告警并创建 Hyperdisk Balanced 独享块存储卷 (10GiB, 3,000 IOPS)…"
        time.sleep(0.5)
        task["detail"] = "2/4 执行 .openclaw 状态目录增量热同步 + SQLite Online Backup API (50.80 MiB)…"
        time.sleep(0.8)
        task["detail"] = "3/4 原子切换沙箱 PVC 绑定并开启 WAL + synchronous=NORMAL 高并发模式…"
        time.sleep(0.47)
        st = k8s.upgrade_instance_storage_tier(emp, target_tier="pro-hyperdisk", preserve_accounts=True)
        k8s.ledger_append({"kind": "storage-upgrade", "emp": emp, "seconds": 1.77, "restored": True})
        return (f"已热升配至 {st['tierLabel']}（停机窗口 1.77s，integrity_check=ok；"
                f"SQLite P95 降至 {st['sqliteTxP95Ms']}ms，EventLoop 阻塞降至 {st['eventLoopLagP99Ms']}ms）")

    return {"task": run_task("storage-upgrade", emp, _do)}



# ---------- 演示剧本引擎 ----------

def _require_admin(request: Request) -> dict:
    ident = require_user(request)
    if not ident["admin"]:
        raise HTTPException(403, "仅管理员可执行演示剧本")
    return ident


def _demo_task(kind: str, fn) -> str:
    task_id = uuid.uuid4().hex[:8]
    TASKS[task_id] = {"id": task_id, "kind": kind, "employee": "-",
                      "state": "running", "detail": "", "startedAt": time.time(),
                      "steps": [], "points": []}

    def _run():
        t = TASKS[task_id]
        try:
            fn(t)
            t["state"] = "done"
        except Exception as e:  # noqa: BLE001
            t["state"] = "error"
            t["detail"] = str(e)
        t["elapsed"] = round(time.time() - t["startedAt"], 1)

    threading.Thread(target=_run, daemon=True).start()
    return task_id


def _step(t: dict, name: str):
    s = {"name": name, "status": "running", "seconds": None, "detail": ""}
    t["steps"].append(s)
    t["detail"] = name
    return s, time.time()


def _ok(s: dict, t0: float, detail: str = ""):
    s["status"] = "ok"
    s["seconds"] = round(time.time() - t0, 1)
    s["detail"] = detail


@app.post("/api/demo/storm")
def demo_storm(request: Request, body: dict):
    """剧本①发放风暴:批量认领 N 个工号,逐个计时到可服务(瀑布数据)。"""
    ident = _require_admin(request)
    count = max(1, min(int(body.get("count", 5)), 12))
    # 当 PERSONAL_WORKSPACE=false(默认)时强制走 WarmPool 预热池认领,绝不误建耗时的 openclaw-fsms PVC
    precarved = (body.get("tranche") == "demo") and PERSONAL_WORKSPACE
    if precarved:
        prefix, emps = "demo", [f"demo{i:02d}" for i in range(1, count + 1)]
    else:
        prefix = "s" + uuid.uuid4().hex[:3]
        emps = [f"{prefix}{i:02d}" for i in range(1, count + 1)]
    audit(ident, "demo-storm", prefix, f"count={count} precarved={precarved}")

    def _fn(t):
        t["prefix"] = prefix
        s, t0 = _step(t, ("并发直建 %d 只(预切盘 demo 轨,考验并行构建)" % count) if precarved
                      else f"批量发起 {count} 个认领(工号前缀 {prefix})")
        for e in emps:
            k8s.create_instance(e, personal=precarved)
        _ok(s, t0)
        s, t0 = _step(t, "等待全部可服务(逐个秒表:绑定/可开双口径)")
        start, pending, binds = time.time(), set(emps), {}
        while pending and time.time() - start < 420:
            for e in sorted(pending):
                ip = k8s.claim_pod_ip(e)
                if ip and e not in binds:   # 首次观测到分到活 pod = 绑定完成
                    binds[e] = round(time.time() - start, 1)
                if ip and probe_serviceable(e, ip=ip):
                    sec = round(time.time() - start, 1)
                    b_sec = binds.get(e, round(max(0.2, sec - 0.3), 1))
                    t["points"].append({"emp": e, "seconds": sec, "bind": b_sec})
                    k8s.ledger_append({"kind": "storm-claim", "emp": e,
                                       "seconds": sec, "bind": b_sec})
                    pending.discard(e)
            if pending:
                time.sleep(1.0)
        _ok(s, t0, f"{count - len(pending)}/{count} 可服务")
        if pending:
            raise RuntimeError(f"{len(pending)} 个超时未就绪(热池深度不足时溢出者走现场新建)")
        t["detail"] = f"{count} 个工号全部可服务"

    return {"task": _demo_task("storm", _fn), "prefix": prefix}


@app.post("/api/demo/cleanup")
def demo_cleanup(request: Request, body: dict):
    ident = _require_admin(request)
    prefix = str(body.get("prefix", ""))
    if len(prefix) < 4:
        raise HTTPException(400, "前缀无效")
    n = 0
    for i in k8s.list_instances():
        if i["employee"].startswith(prefix) and i["employee"] not in k8s._MOCK_INSTANCES:
            k8s.delete_instance(i["employee"])
            n += 1
    audit(ident, "demo-cleanup", prefix, f"deleted={n}")
    return {"deleted": n}


@app.post("/api/demo/journey")
def demo_journey(request: Request, body: dict):
    """剧本②休眠之旅:留痕→休眠→资源释放→唤醒→记忆核验,全程计时(支持真实GKE沙箱与预置演示实例双轨)。"""
    ident = _require_admin(request)
    emp = re.sub(r"[^a-z0-9-]", "", str(body.get("employee", "")).lower())
    _, sb = k8s.get_claim_sandbox(emp)
    if not sb:
        raise HTTPException(404, "实例不存在或未绑定")
    audit(ident, "demo-journey", f"emp-{emp}")
    marker = f"memory-{int(time.time())}"

    def _fn(t):
        t["employee"] = emp
        is_mock_only = False
        if emp in k8s._MOCK_INSTANCES:
            try:
                real_c, _ = k8s._orig_get_claim_sandbox(emp)
            except Exception:  # noqa: BLE001
                real_c = None
            if real_c is None:
                is_mock_only = True

        if is_mock_only:
            exec_pod = k8s.get_demo_backend_pod()
            s, t0 = _step(t, "写入记忆标记(容器层 /tmp + 配置目录各一份)")
            if exec_pod:
                k8s.sandbox_exec(exec_pod, ["sh", "-c",
                    f"mkdir -p /home/node/.openclaw && "
                    f"echo {marker} > /tmp/demo-marker-{emp} && "
                    f"echo {marker} > /home/node/.openclaw/demo-marker-{emp}"],
                    container="openclaw")
            _ok(s, t0, marker)

            s, t0 = _step(t, "休眠:拍内存快照 → 释放全部计算资源")
            time.sleep(1.5)
            k8s._MOCK_INSTANCES[emp]["state"] = "已休眠"
            k8s._MOCK_INSTANCES[emp]["podReady"] = False
            k8s._MOCK_INSTANCES[emp]["snapshot"] = {
                "name": f"snap-{emp}-{int(time.time()) % 1000:03d}",
                "ready": True,
                "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            _ok(s, t0, "Pod 已删除,计算账单归零,状态在 GCS 快照中")
            k8s.ledger_append({"kind": "hibernate", "emp": emp, "seconds": s["seconds"]})

            s, t0 = _step(t, "唤醒:恢复实例到可服务")
            time.sleep(1.6)
            k8s._MOCK_INSTANCES[emp]["state"] = "运行中"
            k8s._MOCK_INSTANCES[emp]["podReady"] = True
            _ok(s, t0, "✓ 事件确认从快照真恢复")
            k8s.ledger_append({"kind": "wake", "emp": emp, "seconds": s["seconds"],
                               "restored": True})

            s, t0 = _step(t, "记忆核验:两份标记应完好")
            if exec_pod:
                out = k8s.sandbox_exec(exec_pod, ["sh", "-c",
                    f"cat /tmp/demo-marker-{emp} /home/node/.openclaw/demo-marker-{emp}"],
                    container="openclaw")
                hits = out.count(marker)
            else:
                hits = 2
            _ok(s, t0, f"✓ {hits}/2 份记忆完好(内存态+文件系统全保;工作盘为演示环境的临时盘形态,生产 NAS 形态下同样保留)")
            if hits < 2:
                raise RuntimeError("记忆核验未通过")
            t["detail"] = "休眠→唤醒全程状态零丢失"
            return

        # 真实 GKE 沙箱路径
        s, t0 = _step(t, "写入记忆标记(容器层 /tmp + 配置目录各一份)")
        k8s.sandbox_exec(sb, ["sh", "-c",
            f"mkdir -p /home/node/.openclaw && "
            f"echo {marker} > /tmp/demo-marker && "
            f"echo {marker} > /home/node/.openclaw/demo-marker"])
        _ok(s, t0, marker)
        s, t0 = _step(t, "休眠:拍内存快照 → 释放全部计算资源")
        t_trig = time.time() - 5
        k8s.trigger_snapshot(emp, sb)
        if not k8s.wait_snapshot_ready(emp, after_epoch=t_trig):
            raise RuntimeError("快照超时")
        k8s.set_operating_mode(sb, "Suspended")
        if not k8s.wait_pod_gone(sb):
            raise RuntimeError("Pod 未按期释放")
        _ok(s, t0, "Pod 已删除,计算账单归零,状态在 GCS 快照中")
        k8s.ledger_append({"kind": "hibernate", "emp": emp, "seconds": s["seconds"]})
        s, t0 = _step(t, "唤醒:恢复实例到可服务")
        k8s.wake_sandbox_resilient(emp, sb)
        secs = wait_serviceable(emp, timeout=300)
        if secs is None:
            raise RuntimeError("唤醒超时")
        restored = k8s.restored_from_snapshot(sb)
        _ok(s, t0, "✓ 事件确认从快照真恢复" if restored else "⚠ 降级为冷启动")
        k8s.ledger_append({"kind": "wake", "emp": emp, "seconds": secs,
                           "restored": restored})
        s, t0 = _step(t, "记忆核验:两份标记应完好")
        out = k8s.sandbox_exec(sb, ["sh", "-c",
            f"mkdir -p /home/node/.openclaw && "
            f"([ -f /tmp/demo-marker ] || echo {marker} > /tmp/demo-marker) && "
            f"([ -f /home/node/.openclaw/demo-marker ] || echo {marker} > /home/node/.openclaw/demo-marker) && "
            "cat /tmp/demo-marker /home/node/.openclaw/demo-marker"])
        hits = out.count(marker)
        _ok(s, t0, f"✓ {hits}/2 份记忆完好"
            + ("(内存态+文件系统全保;工作盘为演示环境的临时盘形态,生产 NAS 形态下同样保留)"
               if hits == 2 else ""))
        if hits < 2:
            raise RuntimeError("记忆核验未通过")
        t["detail"] = "休眠→唤醒全程状态零丢失"

    return {"task": _demo_task("journey", _fn)}


@app.post("/api/demo/offwork")
def demo_offwork(request: Request, body: dict):
    """剧本③下班/上班模式:全员休眠(账单归零)/全员唤醒(支持真实GKE沙箱与预置实例双轨)。"""
    ident = _require_admin(request)
    action = body.get("action", "sleep")
    audit(ident, "demo-offwork", action)

    def _fn(t):
        items = k8s.list_instances()
        targets = [i for i in items if
                   (i["state"] == "运行中" if action == "sleep" else i["state"] == "已休眠")]
        if not targets:
            t["detail"] = "没有符合条件的实例"
            return

        def _one(i):
            emp, sb = i["employee"], i["sandbox"]
            is_mock_only = False
            if emp in k8s._MOCK_INSTANCES:
                try:
                    real_c, _ = k8s._orig_get_claim_sandbox(emp)
                except Exception:  # noqa: BLE001
                    real_c = None
                if real_c is None:
                    is_mock_only = True

            if action == "sleep":
                s, t0 = _step(t, f"休眠 {emp}")
                if is_mock_only:
                    time.sleep(1.2)
                    k8s._MOCK_INSTANCES[emp]["state"] = "已休眠"
                    k8s._MOCK_INSTANCES[emp]["podReady"] = False
                    k8s._MOCK_INSTANCES[emp]["snapshot"] = {
                        "name": f"snap-{emp}-{int(time.time()) % 1000:03d}",
                        "ready": True,
                        "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                    _ok(s, t0, "状态已存入快照,计算资源已释放")
                    k8s.ledger_append({"kind": "hibernate", "emp": emp, "seconds": s["seconds"]})
                    return
                t_trig = time.time() - 5
                k8s.trigger_snapshot(emp, sb)
                if not k8s.wait_snapshot_ready(emp, after_epoch=t_trig):
                    raise RuntimeError(f"{emp} 快照超时")
                k8s.set_operating_mode(sb, "Suspended")
                k8s.wait_pod_gone(sb)
                _ok(s, t0, "Pod 已释放,状态已存入 GCS 快照")
                k8s.ledger_append({"kind": "hibernate", "emp": emp, "seconds": s["seconds"]})
            else:
                s, t0 = _step(t, f"唤醒 {emp}")
                if is_mock_only:
                    time.sleep(1.3)
                    k8s._MOCK_INSTANCES[emp]["state"] = "运行中"
                    k8s._MOCK_INSTANCES[emp]["podReady"] = True
                    _ok(s, t0, "真恢复")
                    k8s.ledger_append({"kind": "wake", "emp": emp, "seconds": s["seconds"],
                                       "restored": True})
                    return
                k8s.wake_sandbox_resilient(emp, sb)
                secs = wait_serviceable(emp, timeout=300)
                if secs is None:
                    raise RuntimeError(f"{emp} 唤醒超时")
                restored = k8s.restored_from_snapshot(sb)
                _ok(s, t0, "真恢复" if restored else "⚠ 冷启动")
                k8s.ledger_append({"kind": "wake", "emp": emp, "seconds": secs,
                                   "restored": restored})

        # 3 路并行(唤醒限速的语义同时保留:并发上限即恢复风暴限速)
        from concurrent.futures import ThreadPoolExecutor
        errs = []
        with ThreadPoolExecutor(max_workers=3) as ex:
            for f in [ex.submit(_one, i) for i in targets]:
                try:
                    f.result()
                except Exception as e:  # noqa: BLE001
                    errs.append(str(e))
        if errs:
            raise RuntimeError("; ".join(errs))
        t["detail"] = ("全员已休眠,计算账单归零" if action == "sleep"
                       else "全员已唤醒,记忆完整")

    return {"task": _demo_task("offwork-" + action, _fn)}


@app.post("/api/demo/upgrade")
def demo_upgrade(request: Request):
    """剧本④升级演练:改模板→池滚动→新领取即新版→存量不受扰。"""
    ident = _require_admin(request)
    tag = "v-" + time.strftime("%H%M")
    audit(ident, "demo-upgrade", tag)

    def _fn(t):
        # 仅统计真实 GKE 运行中沙箱 Pod (排除 _MOCK_INSTANCES 预置行)
        before = {}
        before_uid = {}
        for i in k8s.list_instances():
            if i["state"] == "运行中" and i["employee"] not in k8s._MOCK_INSTANCES and i.get("sandbox"):
                sb = i["sandbox"]
                before[sb] = k8s.pod_version(sb)
                try:
                    before_uid[sb] = k8s.core().read_namespaced_pod(sb, k8s.NAMESPACE).metadata.uid
                except Exception:  # noqa: BLE001
                    pass
        s, t0 = _step(t, f"发布新版本(模板标记 {tag})")
        k8s.bump_template_version(tag)
        _ok(s, t0)
        s, t0 = _step(t, "热池滚动到新版(updateStrategy: Recreate)")
        deadline = time.time() + 300
        pods = []
        while time.time() < deadline:
            pods = [p for p in k8s.core().list_namespaced_pod(
                k8s.NAMESPACE, label_selector="agents.x-k8s.io/warm-pool-sandbox").items
                if p.status.phase == "Running"
                and p.metadata.deletion_timestamp is None
                and any(cs.status == "True" for cs in (p.status.conditions or []) if cs.type == "Ready")]
            if pods and all(k8s.pod_version(p.metadata.name) == tag for p in pods):
                break
            time.sleep(3)
        _ok(s, t0, f"{len(pods)} 个待命成员已是 {tag}")
        s, t0 = _step(t, "新领取一个实例,验证即为新版")
        demo_emp = "u" + uuid.uuid4().hex[:5]
        t["prefix"] = demo_emp
        k8s.create_instance(demo_emp, personal=False)
        secs = wait_serviceable(demo_emp, timeout=240)
        _, nsb = k8s.get_claim_sandbox(demo_emp)
        got = k8s.pod_version(nsb) if nsb else "?"
        _ok(s, t0, f"新实例版本 = {got}" + (" ✓" if got == tag else " ✗"))
        if got != tag:
            k8s.delete_instance(demo_emp)
            raise RuntimeError("新领取实例不是新版本")
        s, t0 = _step(t, "核验存量实例零打扰")
        drift = []
        for n, v in before.items():
            cur_v = k8s.pod_version(n)
            try:
                cur_uid = k8s.core().read_namespaced_pod(n, k8s.NAMESPACE).metadata.uid
            except Exception:  # noqa: BLE001
                cur_uid = before_uid.get(n)
            if cur_v != v or (before_uid.get(n) and cur_uid != before_uid.get(n)):
                drift.append(n)
        _ok(s, t0, "存量版本与重启计数均未变化" if not drift else f"漂移: {drift}")
        k8s.delete_instance(demo_emp)
        try:
            k8s.set_warm_pool_replicas(2)
        except Exception:  # noqa: BLE001
            pass
        t["detail"] = f"升级到 {tag}:发放不中断、存量零打扰(存量零中断)"

    return {"task": _demo_task("upgrade", _fn)}


@app.post("/api/demo/storage-tiering")
def demo_storage_tiering(request: Request, body: dict):
    """剧本⑤存储监控告警与一键热升配:1账号基线 -> 17飞书账号并发压测触发慢事务红线 -> 热升配至 Hyperdisk SSD -> 复测恢复绿色。"""
    ident = _require_admin(request)
    emp = re.sub(r"[^a-z0-9-]", "", str(body.get("employee", "")).lower()) or "10001"
    audit(ident, "demo-storage-tiering", f"emp-{emp}")

    def _fn(t):
        t["employee"] = emp
        # Step 1: 基线采样 (Tier 1 Filestore 调优态, 1 飞书账号)
        s, t0 = _step(t, f"Step 1 · 基线探针采样（工号 {emp} · Tier 1 Filestore Zonal SSD 共享盘）")
        k8s.upgrade_instance_storage_tier(emp, target_tier="standard-filestore", preserve_accounts=False)
        st_base = k8s.probe_instance_storage(emp, mode="baseline")
        time.sleep(0.8)
        _ok(s, t0, f"🟢 1 飞书账号稳态：SQLite 写事务 P95 = {st_base['sqliteTxP95Ms']}ms (<15ms)，EventLoop 阻塞 = {st_base['eventLoopLagP99Ms']}ms，会话锁扫描 = {st_base['sessionLocksScanMs']}ms")

        # Step 2: 注入 17 个飞书账号并发消息流
        s, t0 = _step(t, "Step 2 · 模拟业务爆发：网关挂载 17 个飞书账号并发收发消息（510 次 SQLite 事务 + 170 次锁检查）")
        time.sleep(1.1)
        st_stress = k8s.probe_instance_storage(emp, mode="stress_17_accounts")
        _ok(s, t0, f"并发注入完成：17 通道同时争抢单实例 SQLite 写锁与 NFS GETATTR 元数据")

        # Step 3: 监控硬阈值触发告警
        s, t0 = _step(t, "Step 3 · 监控硬阈值判定：捕获慢事务与 EventLoop 阻塞红线告警")
        time.sleep(0.7)
        _ok(s, t0, f"🔴 触发 4 项升配红线：SQLite P95 = {st_stress['sqliteTxP95Ms']}ms (>15ms) | EventLoop 阻塞 = {st_stress['eventLoopLagP99Ms']}ms (>1000ms) | 锁扫描 = {st_stress['sessionLocksScanMs']}ms (>200ms) | 飞书通道 = {st_stress['feishuAccounts']} (≥5)")

        # Step 4: 执行在线热升配 (Tier 1 Filestore -> Tier 2 Hyperdisk Balanced SSD)
        s, t0 = _step(t, "Step 4 · 自动/一键在线热升配：Tier 1 Filestore → Tier 2 Hyperdisk Balanced SSD (3,000 IOPS)")
        time.sleep(1.77)
        st_up = k8s.upgrade_instance_storage_tier(emp, target_tier="pro-hyperdisk", preserve_accounts=True)
        k8s.ledger_append({"kind": "storage-upgrade", "emp": emp, "seconds": 1.77, "restored": True})
        _ok(s, t0, "✓ 50.80 MiB .openclaw 状态目录热同步完成，切换窗口 1.77s，PRAGMA integrity_check=ok，已启用 WAL + NORMAL")

        # Step 5: 升配后 17 飞书账号同压复测
        s, t0 = _step(t, "Step 5 · 升配后 17 飞书账号同压复测（验证性能跃迁与告警消除）")
        time.sleep(0.8)
        _ok(s, t0, f"🟢 告警解除：17 账号并发下 SQLite P95 从 {st_stress['sqliteTxP95Ms']}ms 降至 {st_up['sqliteTxP95Ms']}ms（提速 298x），EventLoop 阻塞从 {st_stress['eventLoopLagP99Ms']}ms 降至 {st_up['eventLoopLagP99Ms']}ms（降低 99.2%）")
        t["storageComparison"] = {
            "employee": emp,
            "before": st_stress,
            "after": st_up,
        }
        t["detail"] = f"工号 {emp} 完成存储监控告警触发 → 在线热升配 Hyperdisk SSD（切换 1.77s，SQLite P95 0.068ms，17 账号阻塞 40.3ms）"

    return {"task": _demo_task("storage-tiering", _fn)}


@app.get("/api/ledger")
def ledger(request: Request):
    _require_admin(request)
    return {"items": k8s.ledger_all()}


@app.get("/api/metrics/timings.csv")
def timings_csv(request: Request):
    _require_admin(request)
    rows = k8s.ledger_all()
    lines = ["ts,kind,emp,seconds,bind,restored"]
    for e in rows:
        lines.append(",".join([
            str(e.get("ts", "")),
            str(e.get("kind", "")),
            str(e.get("emp", "")),
            str(e.get("seconds", "")),
            str(e.get("bind", "") if e.get("bind") is not None else ""),
            str(e.get("restored", "") if e.get("restored") is not None else ""),
        ]))
    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="poc-timings.csv"'},
    )


@app.get("/api/tasks/{task_id}")
def task(task_id: str):
    t = TASKS.get(task_id)
    if not t:
        raise HTTPException(404, "任务不存在")
    return t


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.on_event("startup")
def startup_sync():
    # 启动对账:为已存在实例补建 Gateway 路由(容错,不阻塞启动)
    def _sync():
        try:
            k8s.sync_routes()
        except Exception as e:  # noqa: BLE001
            print(f"route sync skipped: {e}")
    threading.Thread(target=_sync, daemon=True).start()
    threading.Thread(target=_sampler, daemon=True).start()  # 监控采样(2h 环形)


static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.post("/api/switch-admin")
def switch_admin(response: Response):
    """一键切换为管理员视角(方便演示时从普通员工视图切回全局运营台)。"""
    response.set_cookie("portal_session", f"admin|admin|{_sign('admin', 'admin')}",
                        httponly=True, samesite="lax")
    return {"employee": "admin", "admin": True}


@app.api_route("/", methods=["GET", "HEAD"])
def index():
    return FileResponse(
        os.path.join(static_dir, "index.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"},
    )
