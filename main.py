"""
Entry point for the NovaEX AI Telegram bot.

Mode: long-polling only.

FastAPI is started in parallel just to expose `/health` so platforms like
Koyeb (which require an HTTP port for free web services) accept the deployment.
"""

import logging
import os

from contextlib import asynccontextmanager

from dotenv import load_dotenv

from bot import build_application
from fastapi import FastAPI


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("main")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
PORT = int(os.environ.get("PORT", "8000"))

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN env var is required")


def make_app() -> FastAPI:
    application = build_application(BOT_TOKEN)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await application.initialize()
        await application.start()
        try:
            await application.bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await application.updater.start_polling(drop_pending_updates=True)
        log.info("Bot started in POLLING mode")
        yield
        log.info("Stopping polling ...")
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

    app = FastAPI(lifespan=lifespan)

    @app.get("/")
    async def root():
        return {"status": "ok", "service": "novaex-telegram-bot", "mode": "polling"}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = make_app()


def main():
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
