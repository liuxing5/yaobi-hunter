"""推送模块 v4 - Telegram + PushPlus(微信)"""
import aiohttp, logging, os
logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.base    = f"https://api.telegram.org/bot{token}"
        # 未单独配置 TG_PROXY 时复用 SCANNER_PROXY；云端两者都为空则直连
        self.proxy   = os.getenv("TG_PROXY") or os.getenv("SCANNER_PROXY") or None

    async def send_message(self, text: str):
        if not self.token or not self.chat_id:
            logger.warning("Telegram未配置，跳过推送")
            return
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as s:
                await s.post(
                    f"{self.base}/sendMessage",
                    json={"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"},
                    proxy=self.proxy,
                    timeout=aiohttp.ClientTimeout(total=15),
                )
        except Exception as e:
            logger.error(f"Telegram发送失败: {e}")

    async def send_signal(self, sig: dict):
        score = sig["score"]
        quality = sig.get("quality", "TRADE")
        emoji = "🔥🔥🔥" if score >= 80 else "🔥🔥" if score >= 65 else "🔥"
        d     = sig.get("direction", "LONG")
        f     = sig.get("factors", {})

        lines = []
        for key, label in [
            ("oi_surge",       None),
            ("volume_spike",   None),
            ("funding_extreme",None),
            ("price_squeeze",  None),
            ("ls_ratio_signal",None),
            ("basis_signal",   None),
            ("momentum_signal",None),
            ("ob_imbalance",   None),
        ]:
            if key in f:
                val = f.get(key, "")
                extra = ""
                if key == "oi_surge":
                    extra = f"  OI变化: {f.get('oi_change_5m','?')}  规模: {f.get('oi_usd','?')}"
                elif key == "volume_spike":
                    extra = f"  量比: {f.get('vol_ratio','?')}"
                elif key == "funding_extreme":
                    extra = f"  费率: {f.get('funding_rate','?')}"
                lines.append(f"  • {val}{extra}")

        quality_tag = " 📋观察" if quality == "OBSERVE" else " ✅交易"
        msg = (
            f"{emoji} *妖币信号 | {sig['symbol'].replace('USDT','/USDT')}*{quality_tag}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🎯 综合评分: *{score}*\n"
            f"方向: {'📈 做多' if d=='LONG' else '📉 做空'}\n"
            f"💰 价格: `{sig['price']}`  24H: `{sig['price_change_24h']}`\n"
            f"成交额: `{sig['volume_24h_usd']}`\n"
            f"━━━━━━━━━━━━━━━━\n"
            + ("\n".join(lines) if lines else "  无详情") + "\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"⏰ `{sig['timestamp']}`"
        )
        await self.send_message(msg)


class PushPlusNotifier:
    def __init__(self, token: str):
        self.token = token
        self.api   = "https://www.pushplus.plus/send"

    async def send_message(self, title: str, content: str, template: str = "txt"):
        if not self.token:
            logger.warning("PushPlus未配置，跳过微信推送")
            return
        try:
            async with aiohttp.ClientSession() as s:
                r = await s.post(
                    self.api,
                    json={
                        "token":    self.token,
                        "title":    title,
                        "content":  content,
                        "template": template,
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                data = await r.json()
                if data.get("code") != 200:
                    logger.error(f"PushPlus发送失败: {data.get('msg', data)}")
        except Exception as e:
            logger.error(f"PushPlus发送失败: {e}")

    async def send_signal(self, sig: dict):
        score = sig["score"]
        quality = sig.get("quality", "TRADE")
        d = sig.get("direction", "LONG")
        f = sig.get("factors", {})

        lines = []
        for key in ("oi_surge", "volume_spike", "funding_extreme",
                     "price_squeeze", "ls_ratio_signal", "basis_signal",
                     "momentum_signal", "ob_imbalance"):
            if key in f:
                val = f.get(key, "")
                extra = ""
                if key == "oi_surge":
                    extra = f"  OI变化: {f.get('oi_change_5m','?')}  规模: {f.get('oi_usd','?')}"
                elif key == "volume_spike":
                    extra = f"  量比: {f.get('vol_ratio','?')}"
                elif key == "funding_extreme":
                    extra = f"  费率: {f.get('funding_rate','?')}"
                lines.append(f"  • {val}{extra}")

        quality_tag = "📋观察" if quality == "OBSERVE" else "✅交易"
        title = f"妖币信号 | {sig['symbol'].replace('USDT','/USDT')} {quality_tag}"
        content = (
            f"🎯 综合评分: {score}\n"
            f"方向: {'📈 做多' if d=='LONG' else '📉 做空'}\n"
            f"💰 价格: {sig['price']}  24H: {sig['price_change_24h']}\n"
            f"成交额: {sig['volume_24h_usd']}\n"
            f"━━━━━━━━━━━━━━━━\n"
            + ("\n".join(lines) if lines else "  无详情") + "\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"⏰ {sig['timestamp']}"
        )
        await self.send_message(title, content)


class Notifier:
    def __init__(self, tg_token: str, tg_chat_id: str, pp_token: str = ""):
        self.tg = TelegramNotifier(tg_token, tg_chat_id)
        self.wx = PushPlusNotifier(pp_token)

    async def send_message(self, text: str):
        await self.tg.send_message(text)
        title = text.split("\n")[0][:50]
        await self.wx.send_message(title, text)

    async def send_signal(self, sig: dict):
        await self.tg.send_signal(sig)
        await self.wx.send_signal(sig)
