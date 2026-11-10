# ─────────────────────────────────────────────
#  Chat Test — مینیمال‌ترین چت Socket.IO
#  ⚠️ monkey_patch باید قبل از هر import باشد
# ─────────────────────────────────────────────

import eventlet
eventlet.monkey_patch()

import os
from flask import Flask, render_template
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = "test-secret"

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="eventlet",
)


@app.route("/")
def index():
    return render_template("index.html")


@socketio.on("connect")
def on_connect():
    print(f"[connect] {request_sid()}")
    emit("system", {"msg": "به چت تست خوش آمدی!"})


@socketio.on("disconnect")
def on_disconnect():
    print(f"[disconnect] {request_sid()}")


@socketio.on("chat_message")
def on_chat_message(data):
    """دریافت پیام و broadcast به همه."""
    print(f"[chat_message] {data}")
    username = (data or {}).get("username", "ناشناس")[:20]
    text = (data or {}).get("text", "")[:500]
    if not text.strip():
        return
    socketio.emit("chat_message", {
        "username": username,
        "text": text,
    })


def request_sid():
    from flask import request
    return request.sid


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=True)