import asyncio, aiohttp

PP_TOKEN = "59710f1c8bf64c50897af99cfd86ff03"

async def test():
    async with aiohttp.ClientSession() as s:
        r = await s.post(
            "https://www.pushplus.plus/send",
            json={
                "token": PP_TOKEN,
                "title": "妖币猎手 · 微信推送测试",
                "content": "✅ PushPlus 微信推送已连通！\n后续信号和通知将同时推送到 TG 和微信。",
                "template": "txt",
            },
            timeout=aiohttp.ClientTimeout(total=15),
        )
        data = await r.json()
        print(f"code={data.get('code')}  msg={data.get('msg')}")

asyncio.run(test())
