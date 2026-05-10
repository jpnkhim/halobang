import os
import threading
import asyncio
from flask import Flask

from bot import main

app = Flask(__name__)

@app.route("/")
def home():
    return "NovaEX Bot Running"

def run_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main()

threading.Thread(target=run_bot).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
