import os
import threading
from flask import Flask, jsonify

app = Flask(__name__)

runtime_data = {
    "status": "running",
    "message": "NovaEX Bot Active"
}


@app.route("/")
def home():
    return jsonify({
        "app": "NovaEX Telegram Bot",
        "status": runtime_data["status"],
        "message": runtime_data["message"]
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "healthy"
    })


@app.route("/runtime")
def runtime():
    return jsonify(runtime_data)


def run_web():
    port = int(os.getenv("PORT", 8000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )


# Jalankan Flask di background
threading.Thread(
    target=run_web,
    daemon=True
).start()


# Jalankan bot Telegram di main thread
from bot import main

print("🚀 Runtime Active")
print("🤖 Telegram Bot Running")

main()
