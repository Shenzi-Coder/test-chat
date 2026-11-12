# ─────────────────────────────────────────────
#  Elshan & Anil — Personal Messenger
#  app.py — Flask + Flask-SocketIO
#  ⚠️ eventlet.monkey_patch() باید قبل از هر import دیگری باشد
# ─────────────────────────────────────────────

import eventlet
eventlet.monkey_patch()

import os
import time
import threading
from collections import deque

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit, join_room

# ─────────────────────────────────────────────
#  تنظیمات
# ─────────────────────────────────────────────
MAX_CONNECTIONS = 4
MAX_HISTORY = 200  # ۲۰۰ پیام آخر نگه داشته می‌شه

# انواع پیام مجاز
ALLOWED_MSG_TYPES = {"text", "image", "video", "audio"}

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "elshan-anil-messenger-2025")

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="eventlet",
    max_http_buffer_size=12 * 1024 * 1024,
)

# ─────────────────────────────────────────────
#  State (in-memory)
# ─────────────────────────────────────────────
lock = threading.Lock()
active_connections = 0                # تعداد اتصال‌های همزمان
online_users = {}                     # sid -> {username, joined_at}
chat_history = deque(maxlen=MAX_HISTORY)  # پیام‌های آخر
typing_users = {}                     # sid -> timestamp آخرین typing


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────
def now_iso():
    """زمان فعلی به صورت ISO (برای نمایش در کلاینت)."""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def get_online_usernames():
    """لیست اسم‌های آنلاین (بدون تکرار)."""
    seen = []
    for info in online_users.values():
        u = info.get("username")
        if u and u != "مهمان" and u not in seen:
            seen.append(u)
    return seen


def system_message(text, msg_type="system"):
    """ساخت پیام سیستمی."""
    return {
        "type": msg_type,
        "text": text,
        "timestamp": now_iso(),
        "system": True,
    }


def broadcast_online():
    """ارسال وضعیت آنلاین به همه."""
    socketio.emit("online_update", {
        "online": active_connections,
        "max": MAX_CONNECTIONS,
        "users": get_online_usernames(),
    }, to="chat")


# ─────────────────────────────────────────────
#  Routes
# ─────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def status():
    with lock:
        return jsonify({
            "online": active_connections,
            "max": MAX_CONNECTIONS,
            "users": get_online_usernames(),
        })


# ─────────────────────────────────────────────
#  SocketIO: Connection
# ─────────────────────────────────────────────
@socketio.on("connect")
def on_connect():
    global active_connections
    with lock:
        if active_connections >= MAX_CONNECTIONS:
            emit("rejected", {"reason": "ظرفیت پر است (حداکثر ۴ نفر)."})
            return False
        active_connections += 1
        online_users[request.sid] = {"username": "مهمان", "joined_at": now_iso()}

    emit("connected", {
        "sid": request.sid,
        "online": active_connections,
        "max": MAX_CONNECTIONS,
        "history": list(chat_history),  # ارسال تاریخچه به کاربر جدید
        "online_users": get_online_usernames(),
    })


@socketio.on("disconnect")
def on_disconnect():
    global active_connections
    sid = request.sid
    username = None

    with lock:
        if sid in online_users:
            username = online_users[sid]["username"]
            del online_users[sid]
            active_connections = max(0, active_connections - 1)
        typing_users.pop(sid, None)

    if username and username != "مهمان":
        msg = system_message(f"{username} از چت خارج شد")
        with lock:
            chat_history.append(msg)
        socketio.emit("chat_message", msg, to="chat")

    broadcast_online()
    socketio.emit("typing_update", {"typing": []}, to="chat")


# ─────────────────────────────────────────────
#  SocketIO: Join
# ─────────────────────────────────────────────
@socketio.on("join_chat")
def on_join_chat(data):
    """کاربر با اسمش وارد چت می‌شه."""
    username = (data or {}).get("username", "").strip()[:20] or "مهمان"

    with lock:
        old_name = online_users.get(request.sid, {}).get("username")
        online_users[request.sid] = {
            "username": username,
            "joined_at": now_iso(),
        }

    join_room("chat")

    # پیام سیستمی ورود
    if username != "مهمان" and old_name != username:
        msg = system_message(f"{username} وارد چت شد")
        with lock:
            chat_history.append(msg)
        socketio.emit("chat_message", msg, to="chat")

    broadcast_online()
    emit("joined", {"username": username})


# ─────────────────────────────────────────────
#  SocketIO: Chat Message (text / image / video / audio)
# ─────────────────────────────────────────────
@socketio.on("chat_message")
def on_chat_message(data):
    """دریافت پیام متنی، عکس، ویدیو یا ویس (base64)."""
    if not data:
        return

    with lock:
        info = online_users.get(request.sid)
        if not info:
            return
        username = info["username"]

    msg_type = data.get("type", "text")
    content = data.get("content", "")
    filename = data.get("filename", "")

    # فقط انواع مجاز
    if msg_type not in ALLOWED_MSG_TYPES:
        emit("chat_error", {"message": "نوع پیام پشتیبانی نمی‌شود."})
        return

    if msg_type == "text":
        content = str(content)[:2000]
        if not content.strip():
            return
    else:
        # حجم base64 تقریباً ۸MB (برای عکس، ویدیو و ویس یکسان)
        if len(content) > 12 * 1024 * 1024:
            emit("chat_error", {"message": "فایل خیلی بزرگ است (حداکثر ۸MB)."})
            return
        if not content:
            return

    msg_id = f"{int(time.time() * 1000)}-{request.sid[:6]}"

    payload = {
        "id": msg_id,
        "type": msg_type,          # text | image | video | audio
        "content": content,
        "filename": filename,
        "username": username,
        "sid": request.sid,
        "timestamp": data.get("timestamp") or now_iso(),
        "read_by": [],             # برای تیک خوانده شدن
        "system": False,
    }

    with lock:
        chat_history.append(payload)
        # کاربر دیگه typing نمی‌کنه
        typing_users.pop(request.sid, None)

    socketio.emit("chat_message", payload, to="chat")

    # به‌روزرسانی typing
    socketio.emit("typing_update", {
        "typing": [online_users[s]["username"] for s in typing_users if s in online_users],
    }, to="chat")


# ─────────────────────────────────────────────
#  SocketIO: Typing Indicator
# ─────────────────────────────────────────────
@socketio.on("typing")
def on_typing(data):
    """کاربر در حال تایپ است."""
    is_typing = (data or {}).get("typing", False)

    with lock:
        if is_typing:
            typing_users[request.sid] = time.time()
        else:
            typing_users.pop(request.sid, None)

        names = [online_users[s]["username"] for s in typing_users if s in online_users]

    # به بقیه بگو (نه خودش)
    socketio.emit("typing_update", {"typing": names}, to="chat", skip_sid=request.sid)


# ─────────────────────────────────────────────
#  SocketIO: Read Receipt
# ─────────────────────────────────────────────
@socketio.on("mark_read")
def on_mark_read(data):
    """کاربر پیامی رو خونده."""
    msg_id = (data or {}).get("id")
    if not msg_id:
        return

    with lock:
        info = online_users.get(request.sid)
        if not info:
            return
        reader = info["username"]

        # پیدا کردن پیام توی history
        for msg in chat_history:
            if msg.get("id") == msg_id and not msg.get("system"):
                if reader not in msg["read_by"]:
                    msg["read_by"].append(reader)
                socketio.emit("message_read", {
                    "id": msg_id,
                    "reader": reader,
                }, to="chat")
                break


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=True)
