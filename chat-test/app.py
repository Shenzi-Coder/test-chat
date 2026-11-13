# ─────────────────────────────────────────────
#  Elshan & Anil — Personal Messenger
#  app.py — Flask + Flask-SocketIO + WebRTC signaling
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
#  Profiles (حساب‌های کاربری)
# ─────────────────────────────────────────────
PROFILES = {
    "anil": {
        "name": "Kawaii Ghost",
        "username": "@purple_kitty",
        "avatar": "https://cdn.imgurl.ir/uploads/144791_.png",
        "bio": "گربه‌های بنفش، استایل کیوت و یه ذره ترسناک... 🎀🖤",
        "theme": "purple",
        "emoji": "🌸",
    },
    "elshan": {
        "name": "GHOSTFACE",
        "username": "@ghostface_official",
        "avatar": "https://cdn.imgurl.ir/uploads/q998430_.jpg",
        "bio": "ترس در تاریکی پنهان است...<br>دنبال من نگرد، خودم پیدات می‌کنم.",
        "theme": "mono",
        "emoji": "⚡",
    },
}


# ─────────────────────────────────────────────
#  State (in-memory)
# ─────────────────────────────────────────────
lock = threading.Lock()
active_connections = 0                # تعداد اتصال‌های همزمان
online_users = {}                     # sid -> {username, joined_at, profile_id}
chat_history = deque(maxlen=MAX_HISTORY)  # پیام‌های آخر
typing_users = {}                     # sid -> timestamp آخرین typing

# Call State (WebRTC signaling)
# sid -> { "peer": sid | None, "state": "idle"|"calling"|"active" }
calls = {}


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


def get_online_profile_ids():
    """لیست id پروفایل‌های آنلاین."""
    seen = []
    for info in online_users.values():
        pid = info.get("profile_id")
        if pid and pid not in seen:
            seen.append(pid)
    return seen


def get_busy_names():
    """اسم کسانی که در حال تماس فعال هستن."""
    result = []
    for s in calls:
        if s in online_users and calls[s]["state"] == "active":
            name = online_users[s]["username"]
            if name not in result:
                result.append(name)
    return result


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
        "profiles": get_online_profile_ids(),
        "busy": get_busy_names(),
    }, to="chat")


def _call_state(sid):
    return calls.get(sid, {"peer": None, "state": "idle"})


def _set_call_state(sid, peer=None, state="idle"):
    calls[sid] = {"peer": peer, "state": state}


def _clear_call(sid):
    if not sid:
        return
    info = calls.pop(sid, None)
    if not info:
        return
    peer = info.get("peer")
    if peer and peer in calls:
        calls.pop(peer, None)


def broadcast_busy():
    socketio.emit("call_busy_update", {
        "busy": get_busy_names()
    }, to="chat")


# ─────────────────────────────────────────────
#  Routes — Chat
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
            "profiles": get_online_profile_ids(),
            "busy": get_busy_names(),
        })


# ─────────────────────────────────────────────
#  Routes — Accounts / Profiles
# ─────────────────────────────────────────────
@app.route("/login")
def login_page():
    """صفحه‌ی انتخاب حساب."""
    return render_template("login.html", profiles=PROFILES)


@app.route("/profile/<user_id>")
def profile_page(user_id):
    """صفحه‌ی پروفایل هر کاربر."""
    profile = PROFILES.get(user_id)
    if not profile:
        return "پروفایل پیدا نشد", 404
    return render_template("profile.html", profile=profile, user_id=user_id)


@app.route("/api/profiles")
def api_profiles():
    """لیست پروفایل‌ها + وضعیت آنلاین برای جاوااسکریپت."""
    with lock:
        online_names = get_online_usernames()
        online_ids = get_online_profile_ids()

    result = {}
    for uid, p in PROFILES.items():
        result[uid] = {
            "name": p["name"],
            "username": p["username"],
            "avatar": p["avatar"],
            "emoji": p["emoji"],
            "theme": p["theme"],
            "online": (uid in online_ids) or (p["name"] in online_names),
        }
    return jsonify(result)


# ─────────────────────────────────────────────
#  ICE servers (STUN + TURN اختیاری)
# ─────────────────────────────────────────────
@app.route("/api/ice")
def api_ice():
    """ICE servers برای WebRTC."""
    servers = [
        {"urls": "stun:stun.l.google.com:19302"},
        {"urls": "stun:stun1.l.google.com:19302"},
        {"urls": "stun:stun2.l.google.com:19302"},
    ]

    turn_url = os.environ.get("TURN_URL")
    turn_user = os.environ.get("TURN_USER")
    turn_pass = os.environ.get("TURN_PASS")
    if turn_url and turn_user and turn_pass:
        servers.append({
            "urls": turn_url,
            "username": turn_user,
            "credential": turn_pass,
        })

    return jsonify({"iceServers": servers})


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
        online_users[request.sid] = {
            "username": "مهمان",
            "joined_at": now_iso(),
            "profile_id": None,
        }

    emit("connected", {
        "sid": request.sid,
        "online": active_connections,
        "max": MAX_CONNECTIONS,
        "history": list(chat_history),
        "online_users": get_online_usernames(),
        "online_profiles": get_online_profile_ids(),
        "busy": get_busy_names(),
        "profiles": {uid: {"name": p["name"], "avatar": p["avatar"], "emoji": p["emoji"]}
                     for uid, p in PROFILES.items()},
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

    # پایان دادن به تماس فعال (اگر بود)
    call_info = calls.get(sid)
    if call_info and call_info.get("peer"):
        peer = call_info["peer"]
        socketio.emit("call_ended", {"by_sid": sid}, to=peer)
        calls.pop(peer, None)
    calls.pop(sid, None)

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
    """کاربر با اسمش (و اختیاراً profile_id) وارد چت می‌شه."""
    data = data or {}
    username = (data.get("username", "") or "").strip()[:20] or "مهمان"
    profile_id = data.get("profile_id")

    if profile_id and profile_id in PROFILES:
        p = PROFILES[profile_id]
        username = p["name"]
    else:
        profile_id = None

    with lock:
        old_name = online_users.get(request.sid, {}).get("username")
        online_users[request.sid] = {
            "username": username,
            "joined_at": now_iso(),
            "profile_id": profile_id,
        }

    join_room("chat")

    if username != "مهمان" and old_name != username:
        msg = system_message(f"{username} وارد چت شد")
        with lock:
            chat_history.append(msg)
        socketio.emit("chat_message", msg, to="chat")

    broadcast_online()
    emit("joined", {"username": username, "profile_id": profile_id})


# ─────────────────────────────────────────────
#  SocketIO: Chat Message
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
        profile_id = info.get("profile_id")

    msg_type = data.get("type", "text")
    content = data.get("content", "")
    filename = data.get("filename", "")

    if msg_type not in ALLOWED_MSG_TYPES:
        emit("chat_error", {"message": "نوع پیام پشتیبانی نمی‌شود."})
        return

    if msg_type == "text":
        content = str(content)[:2000]
        if not content.strip():
            return
    else:
        if len(content) > 12 * 1024 * 1024:
            emit("chat_error", {"message": "فایل خیلی بزرگ است (حداکثر ۸MB)."})
            return
        if not content:
            return

    msg_id = f"{int(time.time() * 1000)}-{request.sid[:6]}"

    avatar = None
    if profile_id and profile_id in PROFILES:
        avatar = PROFILES[profile_id]["avatar"]

    payload = {
        "id": msg_id,
        "type": msg_type,
        "content": content,
        "filename": filename,
        "username": username,
        "sid": request.sid,
        "profile_id": profile_id,
        "avatar": avatar,
        "timestamp": data.get("timestamp") or now_iso(),
        "read_by": [],
        "system": False,
    }

    with lock:
        chat_history.append(payload)
        typing_users.pop(request.sid, None)

    socketio.emit("chat_message", payload, to="chat")

    socketio.emit("typing_update", {
        "typing": [online_users[s]["username"] for s in typing_users if s in online_users],
    }, to="chat")


# ─────────────────────────────────────────────
#  SocketIO: Typing Indicator
# ─────────────────────────────────────────────
@socketio.on("typing")
def on_typing(data):
    is_typing = (data or {}).get("typing", False)

    with lock:
        if is_typing:
            typing_users[request.sid] = time.time()
        else:
            typing_users.pop(request.sid, None)

        names = [online_users[s]["username"] for s in typing_users if s in online_users]

    socketio.emit("typing_update", {"typing": names}, to="chat", skip_sid=request.sid)


# ─────────────────────────────────────────────
#  SocketIO: Read Receipt
# ─────────────────────────────────────────────
@socketio.on("mark_read")
def on_mark_read(data):
    msg_id = (data or {}).get("id")
    if not msg_id:
        return

    with lock:
        info = online_users.get(request.sid)
        if not info:
            return
        reader = info["username"]

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
#  SocketIO: Voice Call Signaling (WebRTC)
# ─────────────────────────────────────────────
@socketio.on("call_request")
def on_call_request(data=None):
    """درخواست تماس صوتی؛ به بقیه‌ی اعضای چت خبر بده."""
    with lock:
        info = online_users.get(request.sid)
        if not info:
            return
        caller_name = info["username"]

    st = _call_state(request.sid)
    if st["state"] != "idle":
        emit("call_error", {"message": "شما در حال حاضر در یک تماس هستید."})
        return

    _set_call_state(request.sid, peer=None, state="calling")

    socketio.emit("incoming_call", {
        "from_sid": request.sid,
        "from_name": caller_name,
    }, to="chat", skip_sid=request.sid)


@socketio.on("call_accept")
def on_call_accept(data):
    """کاربر تماس رو قبول کرد."""
    caller_sid = (data or {}).get("to_sid")
    if not caller_sid or caller_sid not in online_users:
        emit("call_error", {"message": "تماس‌گیرنده در دسترس نیست."})
        return

    with lock:
        callee_name = online_users.get(request.sid, {}).get("username", "?")

    _set_call_state(request.sid, peer=caller_sid, state="active")
    _set_call_state(caller_sid, peer=request.sid, state="active")

    socketio.emit("call_accepted", {
        "by_sid": request.sid,
        "by_name": callee_name,
    }, to=caller_sid)

    broadcast_busy()


@socketio.on("call_reject")
def on_call_reject(data):
    """کاربر تماس رو رد کرد."""
    caller_sid = (data or {}).get("to_sid")
    if caller_sid:
        socketio.emit("call_rejected", {
            "by_sid": request.sid,
        }, to=caller_sid)
    _clear_call(request.sid)
    if caller_sid:
        _clear_call(caller_sid)
    broadcast_busy()


@socketio.on("call_end")
def on_call_end(data=None):
    """یکی از طرفین تماس رو قطع کرد."""
    info = _call_state(request.sid)
    peer = info.get("peer")

    if peer:
        socketio.emit("call_ended", {"by_sid": request.sid}, to=peer)

    _clear_call(request.sid)
    if peer:
        _clear_call(peer)

    broadcast_busy()


@socketio.on("webrtc_offer")
def on_webrtc_offer(data):
    target = (data or {}).get("to_sid")
    offer = (data or {}).get("offer")
    if not target or not offer:
        return
    socketio.emit("webrtc_offer", {
        "from_sid": request.sid,
        "offer": offer,
    }, to=target)


@socketio.on("webrtc_answer")
def on_webrtc_answer(data):
    target = (data or {}).get("to_sid")
    answer = (data or {}).get("answer")
    if not target or not answer:
        return
    socketio.emit("webrtc_answer", {
        "from_sid": request.sid,
        "answer": answer,
    }, to=target)


@socketio.on("webrtc_ice")
def on_webrtc_ice(data):
    target = (data or {}).get("to_sid")
    candidate = (data or {}).get("candidate")
    if not target or not candidate:
        return
    socketio.emit("webrtc_ice", {
        "from_sid": request.sid,
        "candidate": candidate,
    }, to=target)


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=True)
