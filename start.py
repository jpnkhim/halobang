"""
Web wrapper untuk NovaEX Telegram Bot - Koyeb Free Web Service
"""

import os
import threading
import subprocess
from flask import Flask, jsonify

app = Flask(__name__)

bot_status = {
    "status": "starting",
    "message": "Bot sedang diinisialisasi..."
}


@app.route("/")
def home():
    return jsonify({
        "app": "NovaEX Telegram Bot",
        "status": bot_status["status"],
        "message": bot_status["message"]
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "bot_status": bot_status["status"]
    }), 200


@app.route("/status")
def status():
    return jsonify(bot_status)


def run_telegram_bot():
    while True:
        try:
            bot_status["status"] = "running"
            bot_status["message"] = "Bot Telegram sedang berjalan"

            subprocess.run(
                ["python", "bot.py"],
                check=True
            )

        except Exception as e:
            bot_status["status"] = "error"
            bot_status["message"] = f"Error: {str(e)}"

            print(f"Error running bot: {e}")


if __name__ == "__main__":
    # Jalankan bot Telegram di background
    bot_thread = threading.Thread(
        target=run_telegram_bot,
        daemon=True
    )
    bot_thread.start()

    # Jalankan Flask web server
    port = int(os.getenv("PORT", 8000))

    print(f"🚀 Starting web server on port {port}")
    print("🤖 Telegram bot running in background")

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )
