import os
import threading
from flask import Flask

from bot import main

app = Flask(__name__)

@app.route("/")
def home():
    return "NovaEX Bot Running"

def run_bot():
    main()

threading.Thread(target=run_bot).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)