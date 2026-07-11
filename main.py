
import os
import sqlite3
import threading
import time
from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

DB_PATH = os.getenv("DB_PATH", "/data/deposit_monitor.db")
API_KEY = os.getenv("API_KEY", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
DEFAULT_LATE_MINUTES = int(os.getenv("LATE_MINUTES", "5"))
DEFAULT_SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "5"))
LEADER_TTL_SECONDS = int(os.getenv("LEADER_TTL_SECONDS", "30"))
MAX_DEVICES = int(os.getenv("MAX_DEVICES", "0"))

WIB = ZoneInfo("Asia/Jakarta")
db_lock = threading.RLock()


def connect_db():
    directory = os.path.dirname(DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db():
    with db_lock, connect_db() as conn:
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
            destination TEXT,
            destination_account TEXT,
            destination_owner TEXT,
            form_time TEXT,
            amount TEXT,
            bank TEXT,
            sent_at INTEGER NOT NULL,
            device_id TEXT
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """)

        columns = {row["name"] for row in conn.execute("PRAGMA table_info(sent_forms)")}
        migrations = {
            "destination": "TEXT",
            "destination_account": "TEXT",
            "destination_owner": "TEXT"
        }
        for name, sql_type in migrations.items():
            if name not in columns:
                conn.execute(f"ALTER TABLE sent_forms ADD COLUMN {name} {sql_type}")

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


def authorized():
    return bool(API_KEY) and request.headers.get("X-API-Key", "") == API_KEY


def get_settings(conn):
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    data = {row["key"]: row["value"] for row in rows}
    return {
        "enabled": data.get("enabled", "1") == "1",
        "lateMinutes": int(data.get("late_minutes", DEFAULT_LATE_MINUTES)),
        "scanSeconds": int(data.get("scan_seconds", DEFAULT_SCAN_SECONDS)),
        "leaderTtlSeconds": LEADER_TTL_SECONDS,
        "maxDevices": MAX_DEVICES
    }


def choose_leader(conn, now_ts):
    # FIX: leader stabil. Jangan urutkan berdasarkan last_seen DESC karena itu
    # membuat leader berpindah setiap heartbeat perangkat lain.
    cutoff = now_ts - LEADER_TTL_SECONDS
    row = conn.execute(
        """
        SELECT device_id
        FROM devices
        WHERE last_seen >= ?
        ORDER BY device_id ASC
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
        timeout=25
    )
    data = response.json()
    if not response.ok or not data.get("ok"):
        raise RuntimeError(data.get("description") or f"Telegram HTTP {response.status_code}")
    return data


@app.get("/")
def home():
    now_ts = int(time.time())
    with db_lock, connect_db() as conn:
        settings = get_settings(conn)
        leader = choose_leader(conn, now_ts)
        online = conn.execute(
            "SELECT COUNT(*) c FROM devices WHERE last_seen >= ?",
            (now_ts - LEADER_TTL_SECONDS,)
        ).fetchone()["c"]

    return jsonify({
        "status": "ok",
        "service": "deposit-monitor-sync-stable-v3",
        "timeWib": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
        "telegramReady": bool(BOT_TOKEN and CHAT_ID),
        "apiKeyReady": bool(API_KEY),
        "onlineDevices": online,
        "leaderDeviceId": leader,
        "settings": settings
    })


@app.post("/api/heartbeat")
def heartbeat():
    if not authorized():
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

    with db_lock, connect_db() as conn:
        exists = conn.execute(
            "SELECT 1 FROM devices WHERE device_id = ?",
            (device_id,)
        ).fetchone()

        if not exists and MAX_DEVICES > 0:
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
    if not authorized():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401

    data = request.get_json(silent=True) or {}
    required = ["formId", "deviceId", "username", "formTime", "ageMinutes"]
    missing = [key for key in required if data.get(key) in (None, "")]
    if missing:
        return jsonify({"ok": False, "error": f"Field kurang: {', '.join(missing)}"}), 400

    form_id = str(data["formId"])
    device_id = str(data["deviceId"])
    username = str(data["username"])
    game_id = str(data.get("gameId", "-"))
    destination = str(data.get("destination", "-")).strip() or "-"
    destination_account = str(data.get("destinationAccount", "-")).strip() or "-"
    destination_owner = str(data.get("destinationOwner", "-")).strip() or "-"
    form_time = str(data["formTime"])
    amount = str(data.get("amount", "-"))
    bank = str(data.get("bank", "-"))
    age_minutes = int(data.get("ageMinutes", 0))
    now_ts = int(time.time())

    with db_lock, connect_db() as conn:
        settings = get_settings(conn)

        if not settings["enabled"]:
            return jsonify({"ok": True, "sent": False, "reason": "Monitor dinonaktifkan"})

        if age_minutes < settings["lateMinutes"]:
            return jsonify({"ok": True, "sent": False, "reason": "Belum lewat batas waktu"})

        # FIX: semua perangkat boleh submit; SQLite PRIMARY KEY melakukan
        # anti-duplikat global. Ini menghilangkan race leader saat banyak PC aktif.
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO sent_forms(
              form_id, username, game_id, destination,
              destination_account, destination_owner,
              form_time, amount, bank, sent_at, device_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                form_id, username, game_id, destination,
                destination_account, destination_owner,
                form_time, amount, bank, now_ts, device_id
            )
        )
        conn.commit()

        if cursor.rowcount == 0:
            return jsonify({"ok": True, "sent": False, "reason": "Sudah pernah dikirim"})

    message = (
        "⚠️ <b>FORM DEPOSIT TERLAMBAT</b>\n\n"
        f"{escape(game_id)} - {escape(destination)} "
        f"(ID: <b>{escape(username)}</b>)\n"
        f"Sudah lebih dari <b>{settings['lateMinutes']} menit</b>. Silakan dicek.\n\n"
        f"🕒 Waktu form: <b>{escape(form_time)}</b>\n"
        f"⏳ Umur form: <b>{age_minutes} menit</b>\n"
        f"💰 Amount: <b>{escape(amount)}</b>\n"
        f"🎯 Tujuan: <b>{escape(destination)}</b> - "
        f"{escape(destination_account)} - {escape(destination_owner)}"
    )

    try:
        telegram_send(message)
    except Exception as exc:
        with db_lock, connect_db() as conn:
            conn.execute("DELETE FROM sent_forms WHERE form_id = ?", (form_id,))
            conn.commit()
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({"ok": True, "sent": True})


@app.get("/api/status")
def status():
    if not authorized():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401

    now_ts = int(time.time())
    with db_lock, connect_db() as conn:
        leader = choose_leader(conn, now_ts)
        devices = [dict(row) for row in conn.execute(
            """
            SELECT device_id, device_name, last_seen, page_url, form_count, late_count
            FROM devices ORDER BY device_name ASC
            """
        ).fetchall()]
        sent_count = conn.execute("SELECT COUNT(*) c FROM sent_forms").fetchone()["c"]
        settings = get_settings(conn)

    for device in devices:
        device["online"] = device["last_seen"] >= now_ts - LEADER_TTL_SECONDS
        device["isLeader"] = device["device_id"] == leader

    return jsonify({
        "ok": True,
        "leaderDeviceId": leader,
        "devices": devices,
        "sentForms": sent_count,
        "settings": settings
    })


@app.post("/api/test-telegram")
def test_telegram():
    if not authorized():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401
    telegram_send(
        "✅ <b>TES SERVER DEPOSIT MONITOR BERHASIL</b>\n\n"
        "Railway sudah terhubung ke grup Telegram."
    )
    return jsonify({"ok": True})


@app.post("/api/reset-sent")
def reset_sent():
    if not authorized():
        return jsonify({"ok": False, "error": "API key tidak valid"}), 401
    with db_lock, connect_db() as conn:
        count = conn.execute("SELECT COUNT(*) c FROM sent_forms").fetchone()["c"]
        conn.execute("DELETE FROM sent_forms")
        conn.commit()
    return jsonify({"ok": True, "deleted": count})


init_db()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
