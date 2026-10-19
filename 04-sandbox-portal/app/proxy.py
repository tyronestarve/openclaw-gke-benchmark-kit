"""沙箱 UI 反代（独立端口 8081）。

按 portal 登录会话把根路径流量转发到该用户自己的沙箱（HTTP + WebSocket），
根路径直出——不加路径前缀，避免破坏 UI 的绝对路径资源。
管理员可经 open_emp cookie 打开任意工号的实例。
"""
import asyncio
import re

import hashlib
import time as _time

import httpx
import websockets
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import PlainTextResponse, RedirectResponse, Response

from . import k8s
from .main import ADMIN_EMPLOYEES, SANDBOX_TOKEN, _SECRET, _sign
import hmac as _hmac

proxy = FastAPI()

_HOP = {"host", "connection", "keep-alive", "transfer-encoding", "upgrade",
        "proxy-authorization", "proxy-connection", "te", "trailers",
        "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions",
        "content-length", "accept-encoding"}


def _who(cookies: dict) -> tuple[str | None, bool]:
    tok = cookies.get("portal_session", "")
    parts = tok.split("|")
    if len(parts) == 3 and _hmac.compare_digest(parts[2], _sign(parts[0], parts[1])):
        return parts[0], parts[1] == "admin"
    return None, False


def _target_emp(cookies: dict) -> str | None:
    emp, admin = _who(cookies)
    if not emp:
        return None
    want = cookies.get("open_emp", "")
    return want if want and (admin or want == emp) else emp


# 会话反代直连沙箱 Service(需配套 NetworkPolicy 放行本组件)。
# 不走 router 的原因(实测):router 会剥离 Origin 头,而网关的 control-ui
# 连接强制要求 Origin 存在("origin missing or invalid",allowedOrigins=["*"]
# 也救不了)——已列为向上游反馈项。直连时把 Origin 设为与 Host 一致,
# 命中网关内置的 host-fallback 校验,确定性通过。


def _sandbox_host(emp: str) -> str:
    # 优先直连已认领沙箱的 Pod IP，彻底绕开刚认领 0~5 秒内 K8s Service Endpoint 传播与 DNS 负缓存窗口
    try:
        ip = k8s.claim_pod_ip(emp)
        if ip:
            return f"{ip}:{k8s.SANDBOX_PORT}"
    except Exception:  # noqa: BLE001
        pass
    return f"emp-{emp}.{k8s.NAMESPACE}.svc.cluster.local:{k8s.SANDBOX_PORT}"


def _upstream(emp: str, path: str, query: str = "") -> str:
    return f"{_sandbox_host(emp)}/{path}" + (f"?{query}" if query else "")


def _route_headers(emp: str) -> dict:
    # Origin 与 Host 保持一致(host-fallback 放行条件)
    h = _sandbox_host(emp)
    return {"Host": h, "Origin": f"http://{h}"}


@proxy.get("/healthz")
async def healthz():
    return PlainTextResponse("ok", 200)


@proxy.get("/probe")
async def probe(ip: str = "", exp: str = "", sig: str = ""):
    """探活直拨(签名校验):管理台递入 pod IP,跳过 Service DNS/端点传播,
    消除观察者延迟;返回 200=网关可服务。"""
    expect = _hmac.new(_SECRET, f"probe|{ip}|{exp}".encode(),
                       hashlib.sha256).hexdigest()[:32]
    if not (_hmac.compare_digest(sig, expect) and exp.isdigit()
            and int(exp) > _time.time()):
        return PlainTextResponse("bad sig", 403)
    if not re.fullmatch(r"[0-9.]+:[0-9]+", ip):
        return PlainTextResponse("bad ip", 400)
    try:
        async with httpx.AsyncClient(timeout=3) as cl:
            r = await cl.get(f"http://{ip}/", headers={"Origin": f"http://{ip}"})
        ok = r.status_code == 200
        return PlainTextResponse("ok" if ok else "no", 200 if ok else 503)
    except httpx.HTTPError:
        return PlainTextResponse("no", 503)


@proxy.get("/open")
async def open_link(t: str = ""):
    """Cloud Run 管理台签发的一次性签名链接:验签 → 落会话 cookie → 进沙箱 UI。"""
    parts = t.split(".")
    if len(parts) == 3:
        emp, exp, sig = parts
        expect = _hmac.new(_SECRET, f"open|{emp}|{exp}".encode(),
                           hashlib.sha256).hexdigest()[:32]
        if _hmac.compare_digest(sig, expect) and exp.isdigit() and int(exp) > _time.time():
            resp = RedirectResponse(url=f"/#token={SANDBOX_TOKEN}", status_code=302)
            resp.set_cookie("portal_session", f"{emp}|user|{_sign(emp, 'user')}",
                            httponly=True, samesite="lax")
            resp.set_cookie("open_emp", emp)
            return resp
    return PlainTextResponse("链接无效或已过期，请回管理台重新点击\"打开\"", 403)


@proxy.api_route("/{path:path}",
                 methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def http_proxy(request: Request, path: str):
    emp = _target_emp(request.cookies)
    if not emp:
        return PlainTextResponse("请先在管理台登录，再通过实例列表的\"打开\"按钮进入", 401)
    url = "http://" + _upstream(emp, path, request.url.query)
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in _HOP and k.lower() != "origin"}
    headers.update(_route_headers(emp))
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = None
            for attempt in range(4):
                try:
                    url = "http://" + _upstream(emp, path, request.url.query)
                    headers.update(_route_headers(emp))
                    r = await c.request(request.method, url, headers=headers,
                                        content=await request.body())
                    break
                except httpx.HTTPError:
                    if attempt == 3:
                        raise
                    await asyncio.sleep(0.6)
    except httpx.HTTPError as e:
        return PlainTextResponse(f"沙箱不可达(可能休眠中,请先在管理台唤醒): {e}", 502)
    resp_headers = {k: v for k, v in r.headers.items()
                    if k.lower() not in _HOP and k.lower() != "content-encoding"}
    return Response(r.content, r.status_code, headers=resp_headers)


@proxy.websocket("/{path:path}")
async def ws_proxy(ws: WebSocket, path: str):
    emp = _target_emp(ws.cookies)
    if not emp:
        await ws.close(code=4401)
        return
    subproto = ws.headers.get("sec-websocket-protocol")
    await ws.accept(subprotocol=subproto.split(",")[0].strip() if subproto else None)
    query = ws.url.query or ""
    uri = "ws://" + _upstream(emp, path, query)
    try:
        async with websockets.connect(
            uri, subprotocols=subproto.split(",") if subproto else None,
            origin=f"http://{_sandbox_host(emp)}",   # Origin==Host,host-fallback 放行
            max_size=32 * 1024 * 1024,
        ) as up:
            async def c2s():
                while True:
                    msg = await ws.receive()
                    if msg["type"] == "websocket.disconnect":
                        return
                    if msg.get("text") is not None:
                        await up.send(msg["text"])
                    elif msg.get("bytes") is not None:
                        await up.send(msg["bytes"])

            async def s2c():
                async for m in up:
                    if isinstance(m, str):
                        await ws.send_text(m)
                    else:
                        await ws.send_bytes(m)

            done, pending = await asyncio.wait(
                [asyncio.create_task(c2s()), asyncio.create_task(s2c())],
                return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
    except Exception:  # noqa: BLE001 —— 上游断开/不可达,正常关闭客户端侧
        pass
    try:
        await ws.close()
    except RuntimeError:
        pass
