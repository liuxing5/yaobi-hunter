"""HTTP API v3 - Dashboard后端"""
import aiohttp, base64, hmac, json, logging, os, glob
from aiohttp import web
from pathlib import Path
from config import PARAM_SCHEMA
from collections import deque

logger = logging.getLogger(__name__)


def create_app(config, scanner, trader):
    app = web.Application()

    @web.middleware
    async def cors(request, handler):
        if request.method == "OPTIONS":
            return web.Response(headers={
                "Access-Control-Allow-Origin":  "*",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
            })
        resp = await handler(request)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    app.middlewares.append(cors)

    # 可选 HTTP Basic 认证：设置 DASHBOARD_USER/DASHBOARD_PASSWORD 后生效。
    # 公网部署时强烈建议开启，否则任何拿到网址的人都能改参数/平仓。
    dashboard_user     = os.getenv("DASHBOARD_USER", "")
    dashboard_password = os.getenv("DASHBOARD_PASSWORD", "")

    @web.middleware
    async def basic_auth(request, handler):
        if not (dashboard_user and dashboard_password):
            return await handler(request)
        if request.path == "/health":
            return await handler(request)
        header = request.headers.get("Authorization", "")
        if header.startswith("Basic "):
            try:
                decoded = base64.b64decode(header[6:], validate=True).decode("utf-8")
                username, sep, password = decoded.partition(":")
                if (sep and hmac.compare_digest(username, dashboard_user)
                        and hmac.compare_digest(password, dashboard_password)):
                    return await handler(request)
            except Exception:
                pass
        return web.Response(
            status=401,
            text="Unauthorized",
            headers={"WWW-Authenticate": 'Basic realm="Yaobi Hunter", charset="UTF-8"'},
        )
    app.middlewares.append(basic_auth)

    async def handle_health(req):
        return _json({"ok": True, "service": "yaobi-hunter"})

    async def handle_binance_diag(req):
        """不带 API Key 探测 Binance 可达性，用于区分 IP 封禁与密钥问题。"""
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get("https://fapi.binance.com/fapi/v1/time") as r:
                    body = (await r.text())[:300]
                    return _json({
                        "ok": r.status == 200,
                        "status": r.status,
                        "retry_after": r.headers.get("Retry-After"),
                        "body": body,
                    })
        except Exception as e:
            return _json({"ok": False, "error": str(e)}, 502)

    async def handle_status(req):
        acct = await trader.get_account_summary()
        return _json({
            "scan_total":    scanner.total_signals,
            "positions":     trader.get_positions_summary(),
            "account":       acct,
            "account_ok":    acct.get("ok", True),
            "trade_enabled": config.TRADE_ENABLED,
            "cooldown_count":len(scanner.cooldown),
            "binance":       scanner.binance_status(),
        })

    async def handle_signals(req):
        path = config.SIGNAL_LOG_PATH
        signals = []
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                # lines = f.readlines()[-300:]
                lines = deque(f, maxlen=300)
            for l in reversed(lines):
                try:
                    signals.append(json.loads(l))
                except Exception:
                    pass
        return _json({"signals": signals})

    async def handle_observe_signals(req):
        data_dir = "data"
        signals = []
        for fpath in sorted(glob.glob(os.path.join(data_dir, "observe_signals_*.jsonl")), reverse=True):
            try:
                with open(fpath, encoding="utf-8") as f:
                    lines = deque(f, maxlen=200)
                for l in reversed(lines):
                    try:
                        signals.append(json.loads(l))
                    except Exception:
                        pass
            except Exception:
                pass
            if len(signals) >= 200:
                break
        return _json({"signals": signals[:200]})

    async def handle_params_get(req):
        result = {}
        for k, (default, typ, lo, hi, desc) in PARAM_SCHEMA.items():
            result[k] = {
                "value":   getattr(config, k),
                "default": default,
                "type":    typ,
                "min":     lo,
                "max":     hi,
                "desc":    desc,
            }
        return _json(result)

    async def handle_params_set(req):
        try:
            body = await req.json()
        except Exception:
            return _json({"ok": False, "msg": "invalid json"}, 400)
        results = {}
        for k, v in body.items():
            ok, msg = config.update(k, v)
            results[k] = {"ok": ok, "msg": msg, "value": getattr(config, k, None)}
        return _json({"results": results})

    async def handle_param_single(req):
        key  = req.match_info["key"]
        body = await req.json()
        ok, msg = config.update(key, body.get("value"))
        return _json({"ok": ok, "msg": msg, "value": getattr(config, key, None)})

    async def handle_close_position(req):
        sym = req.match_info["symbol"]
        pos = trader.positions.get(sym)
        if not pos:
            return _json({"ok": False, "msg": "持仓不存在"}, 404)
        try:
            mark_data = await trader.client.get("/fapi/v1/premiumIndex", {"symbol": sym})
            cur    = float(mark_data["markPrice"])
            pnl    = pos.pnl_pct(cur)
            await trader._close_position(pos, cur, pnl, "手动平仓(Dashboard)")
            return _json({"ok": True})
        except Exception as e:
            return _json({"ok": False, "msg": str(e)}, 500)

    async def handle_dashboard(req):
        html_path = Path(__file__).parent / "dashboard.html"
        if not html_path.exists():
            return web.Response(text="dashboard.html not found", status=404)
        return web.FileResponse(html_path)

    async def handle_options(req):
        return web.Response(status=204, headers={
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        })

    app.router.add_get ("/",                         handle_dashboard)
    app.router.add_get ("/dashboard.html",           handle_dashboard)
    app.router.add_get ("/health",                   handle_health)
    app.router.add_get ("/api/diag/binance",         handle_binance_diag)
    app.router.add_get ("/api/status",               handle_status)
    app.router.add_get ("/api/signals",                  handle_signals)
    app.router.add_get ("/api/observe_signals",          handle_observe_signals)
    app.router.add_get ("/api/params",                   handle_params_get)
    app.router.add_post("/api/params",                   handle_params_set)
    app.router.add_post("/api/params/{key}",             handle_param_single)
    app.router.add_post("/api/positions/{symbol}/close", handle_close_position)
    app.router.add_route("OPTIONS", "/{path_info:.*}",   handle_options)
    return app


def _json(data, status=200):
    return web.Response(
        text=json.dumps(data, ensure_ascii=False),
        content_type="application/json",
        status=status,
    )


async def start_server(config, scanner, trader, host="0.0.0.0", port=8765):
    app    = create_app(config, scanner, trader)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info(f"Dashboard API: http://{host}:{port}")
    return runner
