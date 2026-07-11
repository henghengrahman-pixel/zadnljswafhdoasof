
import os
import sqlite3
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from html import escape

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

DB_PATH = os.getenv("DB_PATH", "/data/deposit_monitor.db")
API_KEY = os.getenv("API_KEY", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()

DEFAULT_LATE_MINUTES = int(os.getenv("LATE_MINUTES", "5"))
DEFAULT_SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "10"))
LEADER_TTL_SECONDS = int(os.getenv("LEADER_TTL_SECONDS", "30"))
MAX_DEVICES = int(os.getenv("MAX_DEVICES", "0"))  # 0 = tanpa batas

WIB = ZoneInfo("Asia/Jakarta")
db_lock = threading.RLock()


def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_lock, db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            device_name TEXT NOT NULL,
            last_seen INTEGER NOT NULL,
            page_url TEXT,
            form_count INTEGER DEFAULT 0,
            late_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS sent_forms (
            form_id TEXT PRIMARY KEY,
            username TEXT,
            game_id TEXT,
            form_time TEXT,
            amount TEXT,
            bank TEXT,
            destination TEXT,
            sent_at INTEGER NOT NULL,
            device_id TEXT
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """)

        columns = [
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(sent_forms)"
            ).fetchall()
        ]

        if "destination" not in columns:
            conn.execute(
                "ALTER TABLE sent_forms ADD COLUMN destination TEXT"
            )

        defaults = {
            "late_minutes": str(DEFAULT_LATE_MINUTES),
            "scan_seconds": str(DEFAULT_SCAN_SECONDS),
            "enabled": "1"
        }
        for key, value in defaults.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                (key, value)
            )
        conn.commit()


def require_key():
    key = request.headers.get("X-API-Key", "")
    return bool(API_KEY) and key == API_KEY


def get_settings(conn):
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    data = {r["key"]: r["value"] for r in rows}
    return {
        "enabled": data.get("enabled", "1") == "1",
        "lateMinutes": int(data.get("late_minutes", DEFAULT_LATE_MINUTES)),
        "scanSeconds": int(data.get("scan_seconds", DEFAULT_SCAN_SECONDS)),
        "leaderTtlSeconds": LEADER_TTL_SECONDS,
        "maxDevices": MAX_DEVICES
    }


def choose_leader(conn, now_ts):
    cutoff = now_ts - LEADER_TTL_SECONDS
    row = conn.execute(
        """
        SELECT device_id
        FROM devices
        WHERE last_seen >= ?
        ORDER BY last_seen DESC, device_id ASC
        LIMIT 1
        """,
        (cutoff,)
    ).fetchone()
    return row["device_id"] if row else None


def telegram_send(text):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("BOT_TOKEN atau CHAT_ID belum diatur di Railway")

    response = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        },
        timeout=20
    )
    data = response.json()
    if not response.ok or not data.get("ok"):
        raise RuntimeError(data.get("description") or f"Telegram HTTP {response.status_code}")
    return data


@app.get("/")
def home():
    with db_lock, db() as conn:
        settings = get_settings(conn)
        now_ts = int(time.time())
        leader = choose_leader(conn, now_ts)
        online = conn.execute(
            "SELECT COUNT(*) c FROM devices WHERE last_seen >= ?",
            (now_ts - LEADER_TTL_SECONDS,)
        ).fetchone()["c"]

    return jsonify({
        "status": "ok",
        "service": "deposit-monitor-sync",
        "timeWib": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
        "telegramReady": bool(BOT_TOKEN and CHAT_ID),
        "apiKeyReady": bool(API_KEY),
        "onlineDevices": online,
        "leaderDeviceId": leader,
        "settings": settings
    })


@app.post("/api/heartbeat")
def heartbeat():
    if not require_key():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401

    data = request.get_json(silent=True) or {}
    device_id = str(data.get("deviceId", "")).strip()
    device_name = str(data.get("deviceName", "")).strip() or "Perangkat"
    page_url = str(data.get("pageUrl", "")).strip()
    form_count = int(data.get("formCount", 0) or 0)
    late_count = int(data.get("lateCount", 0) or 0)

    if not device_id:
        return jsonify({"ok": False, "error": "deviceId wajib"}), 400

    now_ts = int(time.time())

    with db_lock, db() as conn:
        existing = conn.execute(
            "SELECT device_id FROM devices WHERE device_id = ?",
            (device_id,)
        ).fetchone()

        if not existing and MAX_DEVICES > 0:
            total = conn.execute("SELECT COUNT(*) c FROM devices").fetchone()["c"]
            if total >= MAX_DEVICES:
                return jsonify({
                    "ok": False,
                    "error": f"Batas perangkat tercapai ({MAX_DEVICES})"
                }), 403

        conn.execute(
            """
            INSERT INTO devices(device_id, device_name, last_seen, page_url, form_count, late_count)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
              device_name=excluded.device_name,
              last_seen=excluded.last_seen,
              page_url=excluded.page_url,
              form_count=excluded.form_count,
              late_count=excluded.late_count
            """,
            (device_id, device_name, now_ts, page_url, form_count, late_count)
        )
        conn.commit()

        leader = choose_leader(conn, now_ts)
        settings = get_settings(conn)

    return jsonify({
        "ok": True,
        "isLeader": leader == device_id,
        "leaderDeviceId": leader,
        "settings": settings,
        "serverTimeWib": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
    })


@app.post("/api/form-alert")
def form_alert():
    if not require_key():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401

    data = request.get_json(silent=True) or {}
    required = ["formId", "deviceId", "username", "formTime", "ageMinutes"]
    missing = [k for k in required if data.get(k) in (None, "")]
    if missing:
        return jsonify({"ok": False, "error": f"Field kurang: {', '.join(missing)}"}), 400

    form_id = str(data["formId"])
    device_id = str(data["deviceId"])
    username = str(data["username"])
    game_id = str(data.get("gameId", "-"))
    form_time = str(data["formTime"])
    amount = str(data.get("amount", "-"))
    bank = str(data.get("bank", "-"))
    destination = str(data.get("destination", "-")).strip() or "-"
    age_minutes = int(data.get("ageMinutes", 0))
    now_ts = int(time.time())

    with db_lock, db() as conn:
        leader = choose_leader(conn, now_ts)
        settings = get_settings(conn)

        if not settings["enabled"]:
            return jsonify({"ok": True, "sent": False, "reason": "Monitor dinonaktifkan"})

        if leader != device_id:
            return jsonify({"ok": True, "sent": False, "reason": "Perangkat bukan leader"})

        if age_minutes < settings["lateMinutes"]:
            return jsonify({"ok": True, "sent": False, "reason": "Belum lewat batas waktu"})

        old = conn.execute(
            "SELECT form_id FROM sent_forms WHERE form_id = ?",
            (form_id,)
        ).fetchone()
        if old:
            return jsonify({"ok": True, "sent": False, "reason": "Sudah pernah dikirim"})

        conn.execute(
            """
            INSERT INTO sent_forms(
                form_id,
                username,
                game_id,
                form_time,
                amount,
                bank,
                destination,
                sent_at,
                device_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                form_id,
                username,
                game_id,
                form_time,
                amount,
                bank,
                destination,
                now_ts,
                device_id
            )
        )
        conn.commit()

    message = (
        "⚠️ <b>FORM DEPOSIT TERLAMBAT</b>\n\n"
        f"{escape(game_id)} - {escape(destination)} "
        f"(ID: <b>{escape(username)}</b>)\n"
        f"Sudah lebih dari <b>{settings['lateMinutes']} menit</b>. "
        "Silakan dicek.\n\n"
        f"🕒 Waktu form: <b>{escape(form_time)}</b>\n"
        f"⏳ Umur form: <b>{age_minutes} menit</b>\n"
        f"💰 Amount: <b>{escape(amount)}</b>"
    )

    try:
        telegram_send(message)
    except Exception as exc:
        with db_lock, db() as conn:
            conn.execute("DELETE FROM sent_forms WHERE form_id = ?", (form_id,))
            conn.commit()
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({"ok": True, "sent": True})


@app.get("/api/status")
def status():
    if not require_key():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401

    now_ts = int(time.time())
    with db_lock, db() as conn:
        leader = choose_leader(conn, now_ts)
        devices = [
            dict(r) for r in conn.execute(
                """
                SELECT device_id, device_name, last_seen, page_url, form_count, late_count
                FROM devices
                ORDER BY last_seen DESC
                """
            ).fetchall()
        ]
        sent_count = conn.execute("SELECT COUNT(*) c FROM sent_forms").fetchone()["c"]
        settings = get_settings(conn)

    for d in devices:
        d["online"] = d["last_seen"] >= now_ts - LEADER_TTL_SECONDS
        d["isLeader"] = d["device_id"] == leader

    return jsonify({
        "ok": True,
        "leaderDeviceId": leader,
        "devices": devices,
        "sentForms": sent_count,
        "settings": settings
    })


@app.post("/api/settings")
def update_settings():
    if not require_key():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401

    data = request.get_json(silent=True) or {}
    updates = {}

    if "enabled" in data:
        updates["enabled"] = "1" if bool(data["enabled"]) else "0"
    if "lateMinutes" in data:
        updates["late_minutes"] = str(max(1, int(data["lateMinutes"])))
    if "scanSeconds" in data:
        updates["scan_seconds"] = str(max(5, int(data["scanSeconds"])))

    with db_lock, db() as conn:
        for key, value in updates.items():
            conn.execute(
                """
                INSERT INTO settings(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (key, value)
            )
        conn.commit()
        settings = get_settings(conn)

    return jsonify({"ok": True, "settings": settings})


@app.post("/api/reset-sent")
def reset_sent():
    if not require_key():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401

    with db_lock, db() as conn:
        count = conn.execute("SELECT COUNT(*) c FROM sent_forms").fetchone()["c"]
        conn.execute("DELETE FROM sent_forms")
        conn.commit()

    return jsonify({"ok": True, "deleted": count})


init_db()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
