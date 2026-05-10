import os
import threading
from flask import Flask

app = Flask(__name__)

@app.route("/")
def home():
    return "NovaEX Bot Running"

def run_web():
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)

# Flask jalan di background
threading.Thread(target=run_web, daemon=True).start()

# Telegram bot jalan di main thread
from bot import main

main()
