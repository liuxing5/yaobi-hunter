#!/usr/bin/env python3
"""
妖币猎手 v3 · 主程序
- 独立监控协程：持仓监控15~30秒一次，不受扫描间隔影响
- 每60秒扫描中小币种妖币信号
- 信号触发后自动开仓（TRADE_ENABLED=True时）
- HTTP API供Dashboard实时读写参数
"""
import os

# 代理不再硬编码：本地可在 .env 设置 SCANNER_PROXY / TG_PROXY，
# Render 等云平台不设置即可直连（避免 127.0.0.1 代理导致连接失败）。
import asyncio, logging, json, time, glob
from datetime import datetime
from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler("yaobi_hunter.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

from config import Config
from scanner import YaoBiScanner
from trader import Trader, is_tradable_signal
from notifier import Notifier
from api_server import start_server


async def monitor_loop(trader: Trader, config: Config):
    while True:
        try:
            has_half = any(p.half_closed for p in trader.positions.values())
            interval = 15 if has_half else 30
            await trader.monitor_positions()
        except Exception as e:
            logger.error(f"持仓监控异常: {e}", exc_info=True)
            interval = 30
        await asyncio.sleep(interval)


def cleanup_old_logs(max_days: int = 7):
    now = time.time()
    for pattern in ["data/signals.jsonl", "data/observe_signals_*.jsonl"]:
        for fpath in glob.glob(pattern):
            try:
                if os.path.getmtime(fpath) < now - max_days * 86400:
                    os.remove(fpath)
                    logger.info(f"清理过期日志: {fpath}")
            except Exception:
                pass


async def main():
    config = Config()
    try:
        config.validate()
    except ValueError as e:
        logger.critical(str(e))
        return

    notifier = Notifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID, config.PUSHPLUS_TOKEN)
    scanner  = YaoBiScanner(config, notifier)
    trader   = Trader(config, notifier)

    # 预加载交易所精度信息（一次请求缓存所有币种的tick/step）
    await scanner.client.load_exchange_info()
    await trader.client.load_exchange_info()

    await trader.audit_positions()

    # 启动Dashboard API（Render 通过 PORT 环境变量指定端口）
    port = int(os.getenv("PORT", "8765"))
    await start_server(config, scanner, trader, host="0.0.0.0", port=port)

    # 启动独立监控协程
    monitor_task = asyncio.create_task(monitor_loop(trader, config))

    # 启动通知
    server_ip = os.getenv("SERVER_IP", "YOUR_SERVER_IP")
    dashboard_url = os.getenv("RENDER_EXTERNAL_URL") or f"http://{server_ip}:{port}"
    await notifier.send_message(
        f"🟢 *妖币猎手 v3 已启动*\n\n"
        f"自动下单: `{'开启' if config.TRADE_ENABLED else '❌关闭(仅报警)'}`\n"
        f"杠杆: `{config.LEVERAGE}x`  每笔: `{config.USDT_PER_TRADE}U`\n"
        f"止损: `-{config.STOP_LOSS_PCT}%`  止盈: `+{config.TAKE_PROFIT_PCT}%`\n"
        f"移动止损: `{'开启' if config.TRAIL_STOP_ENABLED else '关闭'}`\n"
        f"最低评分: `{config.MIN_SCORE}`  扫描: `{config.TOP_N_SYMBOLS}`币\n"
        f"24H涨幅上限(多): `{config.MAX_24H_GAIN_LONG}%`\n"
        f"冷却时间: `{config.COOLDOWN_SECONDS//60}`分钟\n"
        f"Dashboard: `{dashboard_url}`"
    )

    scan_count = 0
    last_report_signals = 0

    while True:
        try:
            os.makedirs("data", exist_ok=True)

            # 信号扫描
            scan_count += 1
            if scan_count % 100 == 1:
                cleanup_old_logs()
            logger.info(f"=== 第{scan_count}轮扫描 | 持仓:{len(trader.positions)}/{config.MAX_OPEN_POSITIONS} | 冷却:{len(scanner.cooldown)} ===")

            signals = await scanner.scan()
            logger.info(f"本轮触发 {len(signals)} 个信号")

            ideal_signals = []
            observe_signals = []
            for sig in signals:
                if is_tradable_signal(sig):
                    ideal_signals.append(sig)
                else:
                    observe_signals.append(sig)

            if len(ideal_signals) >= 3 and not config.TRADE_ENABLED:
                summary = f"📡 *本轮{len(ideal_signals)}个理想信号*\n"
                for s in ideal_signals[:5]:
                    summary += f"  • `{s['symbol']}` {s['direction']} 评分{s['score']}\n"
                await notifier.send_message(summary)
                for sig in ideal_signals:
                    await notifier.send_signal(sig)
            else:
                for sig in ideal_signals:
                    await notifier.send_signal(sig)
                    if config.TRADE_ENABLED:
                        await trader.try_open(sig)

            obs_min = getattr(config, 'OBSERVE_TG_MIN_SCORE', 70)
            if len(observe_signals) >= 3:
                summary = f"👀 *本轮{len(observe_signals)}个观察信号*\n"
                for s in observe_signals[:5]:
                    summary += f"  • `{s['symbol']}` {s['direction']} 评分{s['score']}\n"
                await notifier.send_message(summary)
            else:
                for sig in observe_signals:
                    if sig["score"] >= obs_min:
                        await notifier.send_signal({**sig, "quality": "OBSERVE"})

            for sig in observe_signals:
                try:
                    os.makedirs("data", exist_ok=True)
                    date_str = datetime.now().strftime("%Y%m%d")
                    with open(f"data/observe_signals_{date_str}.jsonl", "a", encoding="utf-8") as f:
                        f.write(json.dumps({**sig, "observed_at": time.time()}, ensure_ascii=False) + "\n")
                except Exception:
                    pass

            # 每100轮状态播报（约100分钟）
            if scan_count % 100 == 0:
                acct = await trader.get_account_summary()
                delta = scanner.total_signals - last_report_signals
                last_report_signals = scanner.total_signals
                pos_lines = "\n".join(
                    f"  • {p['symbol']} {p['direction']} 评分{p['score']} 峰值{p['peak_pnl']:.1f}%"
                    for p in trader.get_positions_summary()
                ) or "  无持仓"
                await notifier.send_message(
                    f"📊 *最近100轮报告*\n"
                    f"扫描: {scan_count}轮  本期信号: {delta}个 (累计{scanner.total_signals})\n"
                    f"余额: `{acct['wallet']:.2f}U`  浮盈: `{acct['unrealized']:+.2f}U`\n"
                    f"当前持仓:\n{pos_lines}"
                )

            await asyncio.sleep(config.SCAN_INTERVAL)

        except KeyboardInterrupt:
            logger.info("用户中断")
            monitor_task.cancel()
            await notifier.send_message("🔴 *妖币猎手已停止*")
            await scanner.client.close()
            await trader.client.close()
            break
        except Exception as e:
            logger.error(f"主循环异常: {e}", exc_info=True)
            await notifier.send_message(f"⚠️ *系统异常*\n```\n{str(e)[:400]}\n```")
            await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())
