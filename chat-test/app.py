# ─────────────────────────────────────────────
#  Elshan & Anil — Personal Messenger
#  app.py — Flask + Flask-SocketIO + WebRTC + Web Push
#  ⚠️ eventlet.monkey_patch() باید قبل از هر import دیگری باشد
# ─────────────────────────────────────────────

import eventlet
eventlet.monkey_patch()

import os
import time
import threading
import uuid
import json
from collections import deque

from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit, join_room

# ─────────────────────────────────────────────
#  Web Push (اختیاری — اگر نصب باشد)
# ─────────────────────────────────────────────
try:
    from pywebpush import webpush, WebPushException
    from py_vapid import Vapid
    WEBPUSH_AVAILABLE = True
except ImportError:
    WEBPUSH_AVAILABLE = False
    print("[WARN] pywebpush/py-vapid نصب نیست — Push API غیرفعال است")

# ─────────────────────────────────────────────
#  تنظیمات
# ─────────────────────────────────────────────
MAX_CONNECTIONS = 4
MAX_HISTORY = 200
EDIT_WINDOW_SECONDS = 15 * 60

ALLOWED_MSG_TYPES = {"text", "image", "video", "audio"}
ALLOWED_REACTIONS = {"❤️", "😂", "👍", "😮", "😢", "🔥"}

# VAPID
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY_PATH = os.environ.get("VAPID_PRIVATE_KEY_PATH", "private_key.pem")
VAPID_SUBJECT = os.environ.get("VAPID_SUBJECT", "mailto:admin@example.com")

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "elshan-anil-messenger-2025")

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="eventlet",
    max_http_buffer_size=12 * 1024 * 1024,
)

# ─────────────────────────────────────────────
#  Profiles
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
#  State
# ─────────────────────────────────────────────
lock = threading.Lock()
active_connections = 0
online_users = {}
chat_history = deque(maxlen=MAX_HISTORY)
typing_users = {}

# Call state
calls = {}
call_modes = {}

# Push subscriptions: sid -> subscription info
push_subscriptions = {}


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────
def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def now_ts():
    return time.time()


def get_online_usernames():
    seen = []
    for info in online_users.values():
        u = info.get("username")
        if u and u != "مهمان" and u not in seen:
            seen.append(u)
    return seen


def get_online_profile_ids():
    seen = []
    for info in online_users.values():
        pid = info.get("profile_id")
        if pid and pid not in seen:
            seen.append(pid)
    return seen


def get_busy_names():
    result = []
    for s in calls:
        if s in online_users and calls[s]["state"] == "active":
            name = online_users[s]["username"]
            if name not in result:
                result.append(name)
    return result


def system_message(text, msg_type="system"):
    return {
        "id": f"sys-{uuid.uuid4().hex[:10]}",
        "type": msg_type,
        "text": text,
        "timestamp": now_iso(),
        "system": True,
    }


def broadcast_online():
    socketio.emit("online_update", {
        "online": active_connections,
        "max": MAX_CONNECTIONS,
        "users": get_online_usernames(),
        "profiles": get_online_profile_ids(),
        "busy": get_busy_names(),
    }, to="chat")


def broadcast_busy():
    socketio.emit("call_busy_update", {"busy": get_busy_names()}, to="chat")


def find_message(msg_id):
    for msg in chat_history:
        if msg.get("id") == msg_id:
            return msg
    return None


def _call_state(sid):
    return calls.get(sid, {"peer": None, "state": "idle"})


def _set_call_state(sid, peer=None, state="idle"):
    calls[sid] = {"peer": peer, "state": state}


def _set_call_mode(sid, mode="audio"):
    if sid:
        call_modes[sid] = mode if mode in ("audio", "video") else "audio"


def _get_call_mode(sid):
    return call_modes.get(sid, "audio")


def _clear_call(sid):
    if not sid:
        return
    info = calls.pop(sid, None)
    call_modes.pop(sid, None)
    if not info:
        return
    peer = info.get("peer")
    if peer and peer in calls:
        calls.pop(peer, None)
        call_modes.pop(peer, None)


# ─────────────────────────────────────────────
#  Web Push
# ─────────────────────────────────────────────
def send_web_push(subscription_info, payload, ttl=86400):
    """ارسال push به یک subscription."""
    if not WEBPUSH_AVAILABLE:
        return False
    if not VAPID_PUBLIC_KEY or not os.path.exists(VAPID_PRIVATE_KEY_PATH):
        return False

    try:
        webpush(
            subscription_info=subscription_info,
            data=json.dumps(payload, ensure_ascii=False),
            vapid_private_key=VAPID_PRIVATE_KEY_PATH,
            vapid_claims={"sub": VAPID_SUBJECT},
            ttl=ttl,
        )
        return True
    except WebPushException as e:
        # اگر subscription منقضی شده (410)، حذفش کن
        status = getattr(e.response, "status_code", None)
        if status in (404, 410):
            return False  # نامعتبر
        print(f"[WebPush] خطا: {e}")
        return False
    except Exception as e:
        print(f"[WebPush] خطای غیرمنتظره: {e}")
        return False


def push_to_all_except(sender_sid, payload):
    """ارسال push به همه‌ی subscriptionها به‌جز فرستنده."""
    if not WEBPUSH_AVAILABLE:
        return
    with lock:
        items = [(sid, sub) for sid, sub in push_subscriptions.items() if sid != sender_sid]

    invalid = []
    for sid, sub in items:
        ok = send_web_push(sub, payload)
        if not ok:
            # اگر نامعتبر بود (410/404)، برای حذف علامت بزن
            invalid.append(sid)

    if invalid:
        with lock:
            for sid in invalid:
                push_subscriptions.pop(sid, None)


# ─────────────────────────────────────────────
#  Routes — Chat
# ─────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/sw.js")
def service_worker():
    response = send_from_directory(
        os.path.join(app.root_path, "static"),
        "sw.js",
        mimetype="application/javascript",
    )
    response.headers["Service-Worker-Allowed"] = "/"
    response.headers["Cache-Control"] = "no-cache"
    return response


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


@app.route("/api/history")
def api_history():
    with lock:
        return jsonify({"history": list(chat_history)})


# ─────────────────────────────────────────────
#  Routes — Push API
# ─────────────────────────────────────────────
@app.route("/api/push/vapid-public-key")
def api_vapid_public_key():
    """کلید عمومی VAPID برای کلاینت."""
    if not VAPID_PUBLIC_KEY:
        return jsonify({"error": "VAPID public key تنظیم نشده"}), 500
    return jsonify({"publicKey": VAPID_PUBLIC_KEY})


@app.route("/api/push/subscribe", methods=["POST"])
def api_push_subscribe():
    """ذخیره‌ی subscription."""
    data = request.get_json(silent=True) or {}
    subscription = data.get("subscription")
    sid = data.get("sid") or "anonymous"

    if not subscription or not isinstance(subscription, dict):
        return jsonify({"error": "subscription نامعتبر"}), 400

    endpoint = subscription.get("endpoint")
    if not endpoint:
        return jsonify({"error": "endpoint ندارد"}), 400

    with lock:
        push_subscriptions[sid] = subscription

    print(f"[Push] subscribed: sid={sid[:8]}...")
    return jsonify({"ok": True, "count": len(push_subscriptions)})


@app.route("/api/push/unsubscribe", methods=["POST"])
def api_push_unsubscribe():
    data = request.get_json(silent=True) or {}
    sid = data.get("sid")
    with lock:
        if sid and sid in push_subscriptions:
            push_subscriptions.pop(sid, None)
    return jsonify({"ok": True})


@app.route("/api/push/test", methods=["POST"])
def api_push_test():
    """ارسال یک push تستی به همه."""
    payload = {
        "title": "تست Push 💜",
        "body": "اگر این را می‌بینی، Push کار می‌کند!",
        "icon": "/favicon.ico",
        "tag": "test",
        "data": {"url": "/"},
    }
    push_to_all_except(None, payload)
    return jsonify({"ok": True, "count": len(push_subscriptions)})


# ─────────────────────────────────────────────
#  Routes — Accounts
# ─────────────────────────────────────────────
@app.route("/login")
def login_page():
    return render_template("login.html", profiles=PROFILES)


@app.route("/profile/<user_id>")
def profile_page(user_id):
    profile = PROFILES.get(user_id)
    if not profile:
        return "پروفایل پیدا نشد", 404
    return render_template("profile.html", profile=profile, user_id=user_id)


@app.route("/api/profiles")
def api_profiles():
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
#  ICE servers
# ─────────────────────────────────────────────
@app.route("/api/ice")
def api_ice():
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
        "call_modes": ["audio", "video"],
        "allowed_reactions": list(ALLOWED_REACTIONS),
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
        push_subscriptions.pop(sid, None)

    call_info = calls.get(sid)
    if call_info and call_info.get("peer"):
        peer = call_info["peer"]
        socketio.emit("call_ended", {"by_sid": sid}, to=peer)
        _clear_call(peer)
    _clear_call(sid)

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

    reply_to = data.get("reply_to")
    reply_payload = None
    if reply_to and isinstance(reply_to, dict) and reply_to.get("id"):
        reply_payload = {
            "id": str(reply_to.get("id"))[:64],
            "username": str(reply_to.get("username", ""))[:20],
            "snippet": str(reply_to.get("snippet", ""))[:120],
            "type": str(reply_to.get("type", "text"))[:20],
        }

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
        "reactions": {},
        "reply_to": reply_payload,
        "edited_at": None,
        "deleted": False,
        "system": False,
        "created_ts": now_ts(),
    }

    with lock:
        chat_history.append(payload)
        typing_users.pop(request.sid, None)

    socketio.emit("chat_message", payload, to="chat")

    # Notification (in-page — فقط وقتی سایت باز است)
    notify_payload = {
        "id": msg_id,
        "username": username,
        "avatar": avatar,
        "type": msg_type,
        "text": content if msg_type == "text" else "",
        "filename": filename,
        "timestamp": payload["timestamp"],
        "sender_sid": request.sid,
    }
    socketio.emit("notification", notify_payload, to="chat", skip_sid=request.sid)

    # Web Push (وقتی سایت بسته است)
    push_title = username or "پیام جدید"
    if msg_type == "text":
        push_body = content[:120]
    elif msg_type == "image":
        push_body = "🖼 عکس فرستاد"
    elif msg_type == "video":
        push_body = "🎬 ویدیو فرستاد"
    elif msg_type == "audio":
        push_body = "🎙 پیام صوتی فرستاد"
    else:
        push_body = "پیام جدید"

    push_payload = {
        "title": push_title,
        "body": push_body,
        "icon": avatar or "/favicon.ico",
        "tag": f"msg-{msg_id}",
        "data": {"url": "/", "msg_id": msg_id},
    }
    # در thread جدا تا بلاک نشه
    eventlet.spawn_n(push_to_all_except, request.sid, push_payload)

    socketio.emit("typing_update", {
        "typing": [online_users[s]["username"] for s in typing_users if s in online_users],
    }, to="chat")


# ─────────────────────────────────────────────
#  SocketIO: Typing
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

        msg = find_message(msg_id)
        if msg and not msg.get("system"):
            read_by = msg.setdefault("read_by", [])
            if reader not in read_by:
                read_by.append(reader)
            socketio.emit("message_read", {
                "id": msg_id,
                "reader": reader,
            }, to="chat")


# ─────────────────────────────────────────────
#  SocketIO: Reactions
# ─────────────────────────────────────────────
@socketio.on("react_message")
def on_react_message(data):
    data = data or {}
    msg_id = data.get("id")
    emoji = data.get("emoji")
    if not msg_id or emoji not in ALLOWED_REACTIONS:
        return

    with lock:
        info = online_users.get(request.sid)
        if not info:
            return
        username = info["username"]

        msg = find_message(msg_id)
        if not msg or msg.get("deleted"):
            return

        reactions = msg.setdefault("reactions", {})
        users = reactions.setdefault(emoji, [])

        if username in users:
            users.remove(username)
            action = "removed"
        else:
            users.append(username)
            action = "added"

        if not users:
            reactions.pop(emoji, None)

        current = {e: list(u) for e, u in reactions.items()}

    socketio.emit("message_reaction", {
        "id": msg_id,
        "reactions": current,
        "by": username,
        "action": action,
        "emoji": emoji,
    }, to="chat")


# ─────────────────────────────────────────────
#  SocketIO: Delete
# ─────────────────────────────────────────────
@socketio.on("delete_message")
def on_delete_message(data):
    msg_id = (data or {}).get("id")
    scope = (data or {}).get("scope", "everyone")

    if not msg_id:
        return

    with lock:
        info = online_users.get(request.sid)
        if not info:
            return
        username = info["username"]

        msg = find_message(msg_id)
        if not msg or msg.get("system"):
            return

        if scope == "everyone" and msg.get("username") != username:
            emit("chat_error", {"message": "فقط فرستنده می‌تواند پیام را برای همه حذف کند."})
            return

        msg["deleted"] = True
        msg["deleted_by"] = username
        msg["deleted_scope"] = scope
        msg["deleted_at"] = now_iso()

        if scope == "everyone":
            msg["content"] = ""
            msg["filename"] = ""
            msg["type"] = "deleted"
            msg["reactions"] = {}

    socketio.emit("message_deleted", {
        "id": msg_id,
        "scope": scope,
        "by": username,
    }, to="chat")


# ─────────────────────────────────────────────
#  SocketIO: Edit
# ─────────────────────────────────────────────
@socketio.on("edit_message")
def on_edit_message(data):
    data = data or {}
    msg_id = data.get("id")
    new_content = str(data.get("content", ""))[:2000]

    if not msg_id or not new_content.strip():
        return

    with lock:
        info = online_users.get(request.sid)
        if not info:
            return
        username = info["username"]

        msg = find_message(msg_id)
        if not msg or msg.get("system") or msg.get("deleted"):
            return

        if msg.get("username") != username:
            emit("chat_error", {"message": "فقط فرستنده می‌تواند پیام را ویرایش کند."})
            return

        if msg.get("type") != "text":
            emit("chat_error", {"message": "فقط پیام‌های متنی قابل ویرایش هستند."})
            return

        created = msg.get("created_ts")
        if created and (now_ts() - created) > EDIT_WINDOW_SECONDS:
            emit("chat_error", {"message": "زمان ویرایش (۱۵ دقیقه) گذشته است."})
            return

        msg["content"] = new_content
        msg["edited_at"] = now_iso()

        updated = {
            "id": msg_id,
            "content": new_content,
            "edited_at": msg["edited_at"],
        }

    socketio.emit("message_edited", updated, to="chat")


# ─────────────────────────────────────────────
#  SocketIO: Call Signaling
# ─────────────────────────────────────────────
@socketio.on("call_request")
def on_call_request(data=None):
    data = data or {}
    mode = data.get("mode", "audio")
    if mode not in ("audio", "video"):
        mode = "audio"

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
    _set_call_mode(request.sid, mode)

    socketio.emit("incoming_call", {
        "from_sid": request.sid,
        "from_name": caller_name,
        "mode": mode,
    }, to="chat", skip_sid=request.sid)


@socketio.on("call_accept")
def on_call_accept(data):
    data = data or {}
    caller_sid = data.get("to_sid")
    callee_mode = data.get("mode", "audio")
    if callee_mode not in ("audio", "video"):
        callee_mode = "audio"

    if not caller_sid or caller_sid not in online_users:
        emit("call_error", {"message": "تماس‌گیرنده در دسترس نیست."})
        return

    caller_mode = _get_call_mode(caller_sid)

    with lock:
        callee_name = online_users.get(request.sid, {}).get("username", "?")

    _set_call_state(request.sid, peer=caller_sid, state="active")
    _set_call_state(caller_sid, peer=request.sid, state="active")
    _set_call_mode(request.sid, callee_mode)
    _set_call_mode(caller_sid, caller_mode)

    socketio.emit("call_accepted", {
        "by_sid": request.sid,
        "by_name": callee_name,
        "caller_mode": caller_mode,
        "callee_mode": callee_mode,
        "mode": "video" if (caller_mode == "video" or callee_mode == "video") else "audio",
    }, to=caller_sid)

    broadcast_busy()


@socketio.on("call_reject")
def on_call_reject(data):
    caller_sid = (data or {}).get("to_sid")
    if caller_sid:
        socketio.emit("call_rejected", {"by_sid": request.sid}, to=caller_sid)
    _clear_call(request.sid)
    if caller_sid:
        _clear_call(caller_sid)
    broadcast_busy()


@socketio.on("call_end")
def on_call_end(data=None):
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
