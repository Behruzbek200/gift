# ==================== SOVGAMARKET BOT — QISM 1/20 ====================
# Importlar, sozlamalar, config, env
# Render + Webhook + Supabase PostgreSQL + Telethon
# =====================================================================

import os
import re
import time
import json
import asyncio
import io
import csv
import shutil
from datetime import datetime, timedelta
from threading import Thread

from flask import Flask, request
import telebot
from telebot import types
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from dotenv import load_dotenv

# ==================== ENV YUKLASH ====================
load_dotenv()

# ==================== ASOSIY SOZLAMALAR ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]
DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
PORT = int(os.getenv("PORT", "10000"))

# ==================== TELETHON (HUMO avto) ====================
API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "")
HUMO_BOT_USERNAME = os.getenv("HUMO_BOT_USERNAME", "HUMOcardbot")

# ==================== TO'LOV (USER HUMO orqali) ====================
CARD_NUMBER = os.getenv("CARD_NUMBER", "8600123456789012")
CARD_OWNER = os.getenv("CARD_OWNER", "HUMO CARD")

# ==================== GIFT YUBORISH USULI ====================
# "bot"     — botning o'z Stars balansidan ← TAVSIYA
# "userbot" — shaxsiy akkaunt (Telethon)
# "auto"    — avtomatik tanlash
# "manual"  — admin qo'lda
GIFT_METHOD = os.getenv("GIFT_METHOD", "bot").lower()

# ==================== DEFAULT SOZLAMALAR ====================
DEFAULT_SETTINGS = {
    # To'lov (HUMO)
    "payment_time_minutes": 15,
    "start_amount": 5000,
    "card_number": CARD_NUMBER,
    "card_owner": CARD_OWNER,

    # Referal (4 xil tizim)
    "referral_percent": 10,           # B) Do'st sovg'a olsa %
    "referral_bonus": 500,            # A) Do'st kelganda pul
    "free_gift_referrals": 20,        # C) N ta do'st = 1 tekin (global)
    "ref_orders_needed": 3,           # D) N ta buyurtma = 1 tekin
    "ref_bonus_on": True,             # A) toggle
    "ref_percent_on": True,           # B) toggle
    "ref_invites_on": True,           # C) toggle
    "ref_orders_on": False,           # D) toggle

    # Gift
    "free_gift_id": 0,                # eski: 1 ta tekin sovg'a
    "gift_method": GIFT_METHOD,       # bot/userbot/auto/manual

    # Isbotlar kanali
    "proof_channel_id": "",
    "proof_channel_username": "",
    "proof_enabled": True,
    "proof_format": "full",

    # Bot holati
    "bot_active": True,
    "maintenance": False,
}

# ==================== BOT VA FLASK OBYEKTLARI ====================
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=True)
app = Flask(__name__)

# ==================== GLOBAL O'ZGARUVCHILAR ====================
telethon_client = None
_db_pool = None
_settings_cache = {}
_settings_cache_time = 0
_SETTINGS_TTL = 30
_admin_cache = set(ADMIN_IDS)
_recent_humo = {}

# ==================== QISM 1 TUGADI ====================
# Keyingi QISM 2: Database (Supabase PostgreSQL) — jadvallar, migratsiyalar
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 2/20 ====================
# Supabase PostgreSQL pool + jadvallar + migratsiyalar
# =====================================================================

# ==================== DB CONNECTION POOL ====================
def init_pool():
    """PostgreSQL connection pool (Supabase uchun)"""
    global _db_pool
    if _db_pool is None and DATABASE_URL:
        _db_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL,
            cursor_factory=RealDictCursor,
        )
        print("✅ DB pool (Supabase) tayyor")


def get_db():
    """Pool'dan connection olish"""
    if _db_pool is None:
        init_pool()
    conn = _db_pool.getconn()
    conn.autocommit = False
    return conn


def put_db(conn):
    """Connection'ni pool'ga qaytarish"""
    if not conn:
        return
    if _db_pool:
        try:
            _db_pool.putconn(conn)
            return
        except Exception:
            pass
    try:
        conn.close()
    except Exception:
        pass


# ==================== DB YARATISH (AVTO) ====================
def init_db():
    """Barcha jadvallarni avtomatik yaratish (Supabase PostgreSQL)"""
    conn = get_db()
    c = conn.cursor()

    # ---------- USERS ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            balance REAL DEFAULT 0,
            referral_balance REAL DEFAULT 0,
            referred_by BIGINT DEFAULT 0,
            referrals_count INTEGER DEFAULT 0,
            free_gifts INTEGER DEFAULT 0,
            referral_order_progress INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0,
            joined_date TEXT,
            last_active TEXT
        )
    """)

    # ---------- SETTINGS ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # ---------- PAYMENTS ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount REAL,
            unique_amount REAL,
            status TEXT DEFAULT 'pending',
            method TEXT DEFAULT 'humo',
            created_at TEXT,
            paid_at TEXT,
            expires_at TEXT
        )
    """)

    # ---------- GIFTS (YANGILANGAN — referral_needed + cheksiz stock) ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS gifts (
            id SERIAL PRIMARY KEY,
            name TEXT,
            emoji TEXT DEFAULT '🎁',
            price REAL DEFAULT 0,
            stars_price INTEGER DEFAULT 0,
            stock INTEGER DEFAULT 999999999,
            description TEXT DEFAULT '',
            tg_gift_id TEXT DEFAULT '',
            referral_needed INTEGER DEFAULT 20,
            is_active INTEGER DEFAULT 1,
            created_at TEXT
        )
    """)

    # ---------- GIFT ORDERS ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS gift_orders (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            gift_id INTEGER,
            gift_name TEXT,
            price REAL,
            is_free INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            tg_gift_sent INTEGER DEFAULT 0,
            tg_error TEXT DEFAULT '',
            created_at TEXT,
            delivered_at TEXT
        )
    """)

    # ---------- CHANNELS ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            channel_id TEXT,
            channel_username TEXT,
            channel_title TEXT,
            is_private INTEGER DEFAULT 0,
            invite_link TEXT DEFAULT ''
        )
    """)

    # ---------- ADMINS ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id BIGINT PRIMARY KEY
        )
    """)

    # ---------- REFERRAL HISTORY ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS referral_history (
            id SERIAL PRIMARY KEY,
            from_user BIGINT,
            to_user BIGINT,
            amount REAL,
            created_at TEXT
        )
    """)

    # ---------- REVIEWS ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            full_name TEXT,
            text TEXT,
            created_at TEXT,
            is_approved INTEGER DEFAULT 1
        )
    """)

    # ---------- TRANSACTIONS ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            type TEXT,
            amount REAL,
            source TEXT,
            description TEXT,
            created_at TEXT
        )
    """)

    # ---------- ADMIN LOG ----------
    c.execute("""
        CREATE TABLE IF NOT EXISTS admin_log (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            action TEXT,
            details TEXT,
            created_at TEXT
        )
    """)

    # ========== MIGRATSIYALAR (mavjud jadvalga yangi ustun) ==========
    migrations = [
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_order_progress INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS free_gifts INTEGER DEFAULT 0",
        "ALTER TABLE gifts ADD COLUMN IF NOT EXISTS tg_gift_id TEXT DEFAULT ''",
        "ALTER TABLE gifts ADD COLUMN IF NOT EXISTS stars_price INTEGER DEFAULT 0",
        "ALTER TABLE gifts ADD COLUMN IF NOT EXISTS referral_needed INTEGER DEFAULT 20",
        "ALTER TABLE gift_orders ADD COLUMN IF NOT EXISTS tg_gift_sent INTEGER DEFAULT 0",
        "ALTER TABLE gift_orders ADD COLUMN IF NOT EXISTS tg_error TEXT DEFAULT ''",
    ]
    for sql in migrations:
        try:
            c.execute(sql)
        except Exception:
            pass

    # ========== MAVJUD GIFTLARNI CHEKSIZ QILISH ==========
    try:
        c.execute("UPDATE gifts SET stock = 999999999 WHERE stock < 999999999")
    except Exception:
        pass

    try:
        c.execute("UPDATE gifts SET referral_needed = 20 WHERE referral_needed IS NULL")
    except Exception:
        pass

    # ========== DEFAULT QIYMATLAR YANGI GIFTLAR UCHUN ==========
    try:
        c.execute("ALTER TABLE gifts ALTER COLUMN stock SET DEFAULT 999999999")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE gifts ALTER COLUMN referral_needed SET DEFAULT 20")
    except Exception:
        pass

    # ========== DEFAULT SOZLAMALAR ==========
    for k, v in DEFAULT_SETTINGS.items():
        c.execute(
            "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
            (k, str(v))
        )

    # ========== ADMINLAR ==========
    for a in ADMIN_IDS:
        c.execute("INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (a,))

    conn.commit()
    put_db(conn)
    print("✅ Supabase PostgreSQL DB tayyor")


# ==================== QISM 2 TUGADI ====================
# Keyingi QISM 3: Settings cache, user helper, format, transaction log
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 3/20 ====================
# Settings cache, user helper, format, transaction, admin log
# =====================================================================

# ==================== SOZLAMA QIYMATINI PARSE QILISH ====================
def _pv(val):
    """Sozlama qiymatini to'g'ri turga o'tkazish"""
    if val is None:
        return None
    if str(val).lower() in ("true", "false"):
        return str(val).lower() == "true"
    try:
        return float(val) if "." in str(val) else int(val)
    except Exception:
        return val


# ==================== SETTINGS (CACHE BILAN) ====================
def get_setting(key, default=None):
    """Sozlamani olish (30 sekund kesh)"""
    global _settings_cache, _settings_cache_time
    if time.time() - _settings_cache_time > _SETTINGS_TTL or not _settings_cache:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("SELECT key, value FROM settings")
            _settings_cache = {r["key"]: r["value"] for r in c.fetchall()}
            _settings_cache_time = time.time()
        finally:
            put_db(conn)
    v = _settings_cache.get(key)
    return default if v is None else _pv(v)


def set_setting(key, value):
    """Sozlamani yangilash (DB + cache)"""
    global _settings_cache, _settings_cache_time
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO settings (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (key, str(value))
        )
        conn.commit()
        _settings_cache[key] = str(value)
        _settings_cache_time = time.time()
    finally:
        put_db(conn)


def refresh_settings_cache():
    """Cache'ni majburiy yangilash"""
    global _settings_cache, _settings_cache_time
    _settings_cache = {}
    _settings_cache_time = 0
    get_setting("bot_active")


# ==================== ADMIN TEKSHIRISH ====================
def is_admin(uid):
    """Admin ekanligini tekshirish (kesh bilan)"""
    if uid in _admin_cache:
        return True
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT user_id FROM admins WHERE user_id = %s", (uid,))
        r = c.fetchone()
        if r:
            _admin_cache.add(uid)
            return True
        return False
    finally:
        put_db(conn)


def add_admin_cache(uid):
    """Admin cache'ga qo'shish"""
    _admin_cache.add(uid)


def remove_admin_cache(uid):
    """Admin cache'dan o'chirish"""
    _admin_cache.discard(uid)


# ==================== FOYDALANUVCHI ====================
def get_user(uid):
    """Foydalanuvchini olish"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE user_id = %s", (uid,))
        return c.fetchone()
    finally:
        put_db(conn)


def update_balance(uid, amount, is_referral=False):
    """Balansni yangilash (asosiy yoki referal)"""
    conn = get_db()
    try:
        c = conn.cursor()
        col = "referral_balance" if is_referral else "balance"
        c.execute(f"UPDATE users SET {col} = {col} + %s WHERE user_id = %s", (amount, uid))
        conn.commit()
    finally:
        put_db(conn)


def update_last_active(uid):
    """Oxirgi faollik vaqtini yangilash"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            "UPDATE users SET last_active = %s WHERE user_id = %s",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), uid)
        )
        conn.commit()
    finally:
        put_db(conn)


# ==================== FORMAT MONEY ====================
def format_money(amount):
    """Pulni formatlash: 15000 → '15 000 soʻm'"""
    try:
        return f"{float(amount):,.0f}".replace(",", " ") + " soʻm"
    except Exception:
        return f"{amount} soʻm"


# ==================== TRANZAKSIYA YOZISH ====================
def write_transaction(uid, ttype, amount, source, description=""):
    """
    Kirim/chiqim yozish
    ttype: "kirim" yoki "chiqim"
    source: humo/stars/referal/gift/referal o'tkazma
    """
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            """INSERT INTO transactions (user_id, type, amount, source, description, created_at)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (uid, ttype, amount, source, description,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
    finally:
        put_db(conn)


# ==================== ADMIN LOG ====================
def log_admin_action(admin_id, action, details=""):
    """Admin harakatini yozish"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            """INSERT INTO admin_log (admin_id, action, details, created_at)
               VALUES (%s, %s, %s, %s)""",
            (admin_id, action, details, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()
    finally:
        put_db(conn)


# ==================== USER STATISTIKA ====================
def get_user_stats(uid):
    """Foydalanuvchi to'liq statistikasi"""
    u = get_user(uid)
    if not u:
        return {}
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE user_id = %s", (uid,))
        orders = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM payments WHERE user_id = %s AND status = 'paid'", (uid,))
        pays = c.fetchone()["n"]
        c.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id = %s AND type = 'kirim'", (uid,))
        total_in = c.fetchone()["s"]
        c.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id = %s AND type = 'chiqim'", (uid,))
        total_out = c.fetchone()["s"]
        return {
            "user": u,
            "orders_count": orders,
            "payments_count": pays,
            "total_in": total_in,
            "total_out": total_out,
        }
    finally:
        put_db(conn)


# ==================== KANALLAR (OBUNA) ====================
def get_channels():
    """Barcha majburiy obuna kanallari"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT channel_id, channel_username, channel_title, invite_link FROM channels")
        return c.fetchall()
    finally:
        put_db(conn)


def check_subscription(user_id):
    """Foydalanuvchi barcha kanallarga obuna bo'lganmi?"""
    chans = get_channels()
    if not chans:
        return True, []
    missing = []
    for ch in chans:
        try:
            mb = bot.get_chat_member(ch["channel_id"], user_id)
            if mb.status in ["left", "kicked"]:
                raise Exception()
        except Exception:
            display = ch["channel_title"] or ch["channel_username"] or ch["channel_id"]
            url = ch["invite_link"] or (
                f"https://t.me/{ch['channel_username'].lstrip('@')}" if ch["channel_username"] else None
            )
            missing.append((display, url))
    return len(missing) == 0, missing


# ==================== ISBOT KANAL YORDAMCHISI ====================
def get_proof_channel():
    """Isbotlar kanal ID sini olish"""
    return get_setting("proof_channel_id", "")


def is_proof_enabled():
    """Isbotlar yoqilganmi?"""
    return get_setting("proof_enabled", True)


# ==================== QISM 3 TUGADI ====================
# Keyingi QISM 4: User ro'yxatdan o'tkazish, referal 4 tizim
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 4/20 ====================
# User ro'yxatdan o'tkazish + Referal 4 tizim (A, B, C, D)
# =====================================================================

# ==================== USER RO'YXATDAN O'TKAZISH ====================
def register_user(uid, un, fn, ref=0):
    """
    Yangi foydalanuvchini ro'yxatdan o'tkazish.
    Referal A tizim (do'st kelganda pul) avtomatik ishga tushadi.
    C tizim (har gift uchun referal) — msg_free_gift() da ishlaydi.
    """
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------- USERNI QO'SHISH ----------
    c.execute(
        """INSERT INTO users (user_id, username, full_name, referred_by, joined_date, last_active)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT (user_id) DO NOTHING""",
        (uid, un, fn, ref, now, now)
    )

    # ---------- REFERAL A TIZIM ----------
    if ref and ref != uid and un and str(un).strip():
        # Referal egasi mavjudmi?
        c.execute("SELECT user_id FROM users WHERE user_id = %s", (ref,))
        if c.fetchone():
            # Bu user avval referal bo'lganmi? (takror bo'lmasin)
            c.execute("SELECT id FROM referral_history WHERE to_user = %s", (uid,))
            if not c.fetchone():

                # Referallar sonini +1
                c.execute(
                    "UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id = %s",
                    (ref,)
                )

                # ========== A) DO'ST KELGANDA PUL ==========
                if get_setting("ref_bonus_on", True):
                    bonus = get_setting("referral_bonus", 500)

                    c.execute(
                        "UPDATE users SET referral_balance = referral_balance + %s WHERE user_id = %s",
                        (bonus, ref)
                    )
                    c.execute(
                        """INSERT INTO referral_history (from_user, to_user, amount, created_at)
                           VALUES (%s, %s, %s, %s)""",
                        (ref, uid, bonus, now)
                    )
                    conn.commit()

                    # Referal egasiga xabar
                    try:
                        bot.send_message(ref,
                            f"🎁 <b>Yangi doʻst!</b>\n\n"
                            f"👤 {fn} (@{un})\n"
                            f"💰 <b>+{format_money(bonus)}</b> referal balansiga tushdi.")
                    except Exception:
                        pass

                    # Tranzaksiya yozish
                    try:
                        write_transaction(ref, "kirim", bonus, "referal", "Doʻst bonus")
                    except Exception:
                        pass
                else:
                    # O'chirilgan bo'lsa ham referal tarixga yoziladi
                    c.execute(
                        """INSERT INTO referral_history (from_user, to_user, amount, created_at)
                           VALUES (%s, %s, 0, %s)""",
                        (ref, uid, now)
                    )

    conn.commit()
    put_db(conn)


# ==================== SOTIB OLGANDA REFERAL BONUS ====================
def referral_on_purchase(buyer_id, price):
    """
    Foydalanuvchi sovg'a sotib olganda referal egasiga bonus.
    B) Foiz va D) Buyurtma soni
    """
    user = get_user(buyer_id)
    if not user or not user.get("referred_by"):
        return

    ref_id = user["referred_by"]

    # ========== B) FOIZ ==========
    if get_setting("ref_percent_on", True):
        percent = float(get_setting("referral_percent", 10) or 0)
        if percent > 0 and price > 0:
            bonus = price * percent / 100

            update_balance(ref_id, bonus, is_referral=True)

            try:
                bot.send_message(ref_id,
                    f"💰 <b>Referal foiz</b>\n\n"
                    f"👤 Doʻstingiz sovgʻa sotib oldi.\n"
                    f"➕ <b>{format_money(bonus)}</b> ({percent}%)")
            except Exception:
                pass

            try:
                write_transaction(ref_id, "kirim", bonus, "referal foiz",
                                  f"Doʻst xaridi {percent}%")
            except Exception:
                pass

    # ========== D) N TA BUYURTMA = 1 TEKIN ==========
    if get_setting("ref_orders_on", False):
        need = int(get_setting("ref_orders_needed", 3) or 3)
        if need <= 0:
            return

        conn = get_db()
        c = conn.cursor()
        try:
            c.execute(
                """UPDATE users
                   SET referral_order_progress = COALESCE(referral_order_progress, 0) + 1
                   WHERE user_id = %s
                   RETURNING referral_order_progress""",
                (ref_id,)
            )
            prog = c.fetchone()["referral_order_progress"]

            if prog >= need:
                c.execute(
                    """UPDATE users
                       SET free_gifts = free_gifts + 1,
                           referral_order_progress = referral_order_progress - %s
                       WHERE user_id = %s""",
                    (need, ref_id)
                )
                conn.commit()

                try:
                    bot.send_message(ref_id,
                        f"🎁 <b>Tekin sovgʻa (referal)</b>\n\n"
                        f"👥 Doʻstlaringiz jami <b>{need}</b> ta sovgʻa sotib oldi.\n"
                        f"Sizga <b>1 ta tekin sovgʻa</b> huquqi berildi!\n\n"
                        f"👉 «🎁 Tekin sovgʻa» dan oling.")
                except Exception:
                    pass
            else:
                conn.commit()
                try:
                    bot.send_message(ref_id,
                        f"📊 <b>Referal progress</b>\n\n"
                        f"🛒 Buyurtmalar: <b>{prog}/{need}</b>\n"
                        f"Yana <b>{need - prog}</b> ta sovgʻa olsa — tekin olasiz.")
                except Exception:
                    pass
        finally:
            put_db(conn)


# ==================== REFERAL STATISTIKASI ====================
def get_referral_stats(uid):
    """Foydalanuvchi referal statistikasi"""
    u = get_user(uid)
    if not u:
        return {
            "referrals_count": 0,
            "free_gifts": 0,
            "referral_balance": 0,
            "order_progress": 0,
            "needed_invites": get_setting("free_gift_referrals", 20),
            "needed_orders": get_setting("ref_orders_needed", 3),
            "ref_bonus_on": get_setting("ref_bonus_on", True),
            "ref_percent_on": get_setting("ref_percent_on", True),
            "ref_invites_on": get_setting("ref_invites_on", True),
            "ref_orders_on": get_setting("ref_orders_on", False),
        }
    return {
        "referrals_count": u["referrals_count"],
        "free_gifts": u["free_gifts"],
        "referral_balance": u["referral_balance"],
        "order_progress": u.get("referral_order_progress", 0),
        "needed_invites": get_setting("free_gift_referrals", 20),
        "needed_orders": get_setting("ref_orders_needed", 3),
        "ref_bonus_on": get_setting("ref_bonus_on", True),
        "ref_percent_on": get_setting("ref_percent_on", True),
        "ref_invites_on": get_setting("ref_invites_on", True),
        "ref_orders_on": get_setting("ref_orders_on", False),
    }


# ==================== REFERAL HAVOLA YARATISH ====================
def get_referral_link(uid):
    """Referal havolani yaratish"""
    try:
        info = bot.get_me()
        return f"https://t.me/{info.username}?start=ref{uid}"
    except Exception:
        return ""


# ==================== REFERAL BALANSNI ASOSIYGA O'TKAZISH ====================
def move_referral_to_main(uid):
    """Referal balansni asosiy balansga o'tkazish"""
    u = get_user(uid)
    if not u or (u["referral_balance"] or 0) <= 0:
        return 0
    amt = u["referral_balance"]

    conn = get_db()
    c = conn.cursor()
    c.execute(
        """UPDATE users SET balance = balance + %s, referral_balance = 0
           WHERE user_id = %s""",
        (amt, uid)
    )
    conn.commit()
    put_db(conn)

    try:
        write_transaction(uid, "kirim", amt, "referal o'tkazma", "Ref → asosiy")
    except Exception:
        pass

    return amt


# ==================== QISM 4 TUGADI ====================
# Keyingi QISM 5: Sovg'alar CRUD (get, add, update, delete, link TG)
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 5/20 ====================
# Sovg'alar CRUD (get, add, update, delete, link TG, pagination)
# =====================================================================

# ==================== SOVG'ALARNI OLISH ====================
def get_active_gifts():
    """Faol sovg'alar ro'yxati (stock > 0)"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT * FROM gifts
                     WHERE is_active = 1 AND stock > 0
                     ORDER BY price ASC""")
        return [dict(x) for x in c.fetchall()]
    finally:
        put_db(conn)


def get_all_gifts():
    """Barcha sovg'alar (admin uchun — faol + nofaol)"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM gifts ORDER BY id DESC")
        return [dict(x) for x in c.fetchall()]
    finally:
        put_db(conn)


def get_gift(gid):
    """Bitta sovg'ani olish"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM gifts WHERE id = %s", (gid,))
        r = c.fetchone()
        return dict(r) if r else None
    finally:
        put_db(conn)


# ==================== SOVG'A QO'SHISH ====================
def add_gift(name, emoji, price, stars_price, stock=999999999, description="", referral_needed=20):
    """
    Yangi sovg'a qo'shish.
    stock — default 999999999 (cheksiz)
    referral_needed — nechta referal kerak tekin olish uchun
    """
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT INTO gifts
                 (name, emoji, price, stars_price, stock, description, 
                  referral_needed, is_active, created_at)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s)
                 RETURNING id""",
              (name, emoji or "🎁", price, stars_price, stock, 
               description, referral_needed, now))
    gid = c.fetchone()["id"]
    conn.commit()
    put_db(conn)
    return gid


# ==================== SOVG'A YANGILASH ====================
def update_gift(gid, field, value):
    """
    Sovg'a maydonini yangilash.
    Ruxsat etilgan field'lar:
    price, stock, stars_price, name, emoji, description, 
    tg_gift_id, referral_needed
    """
    allowed = ("price", "stock", "stars_price", "name", "emoji", 
               "description", "tg_gift_id", "referral_needed")
    if field not in allowed:
        return False
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(f"UPDATE gifts SET {field} = %s WHERE id = %s", (value, gid))
        n = c.rowcount
        conn.commit()
        return n > 0
    finally:
        put_db(conn)


# ==================== SOVG'A O'CHIRISH (SOFT DELETE) ====================
def delete_gift(gid):
    """Sovg'ani o'chirish (soft delete — is_active = 0)"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("UPDATE gifts SET is_active = 0 WHERE id = %s", (gid,))
        n = c.rowcount
        conn.commit()
        return n > 0
    finally:
        put_db(conn)


# ==================== TG GIFTNI BOG'LASH ====================
def link_gift_tg(local_id, tg_gift_id):
    """Lokal sovg'ani TG gift ID bilan bog'lash"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("UPDATE gifts SET tg_gift_id = %s WHERE id = %s", (tg_gift_id, local_id))
        n = c.rowcount
        conn.commit()
        return n > 0
    finally:
        put_db(conn)


# ==================== SOVG'ANI QIDIRISH ====================
def find_gift_by_tg_id(tg_gift_id):
    """TG gift ID bo'yicha lokal sovg'ani topish"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM gifts WHERE tg_gift_id = %s LIMIT 1", (tg_gift_id,))
        r = c.fetchone()
        return dict(r) if r else None
    finally:
        put_db(conn)


# ==================== SOVG'ALAR KLAVIATURASI ====================
def build_gifts_kb(page=0, per_page=8):
    """
    Sovg'alar katalogi uchun sahifalangan klaviatura.
    Qaytaradi: (markup, total, page)
    
    Tugma formati (UZUN):
    ⭐ Yulduzcha sovg'asi — 15 000 soʻm
    """
    gs = get_active_gifts()
    total = len(gs)
    start = page * per_page
    end = start + per_page
    chunk = gs[start:end]

    markup = types.InlineKeyboardMarkup(row_width=1)

    # Sovg'a tugmalari — UZUN FORMAT
    for g in chunk:
        emoji = g["emoji"] or "🎁"
        # Uzun format: emoji + nom + "sovg'asi" + narx
        btn_text = f"{emoji} {g['name']} sovg'asi — {format_money(g['price'])}"
        markup.add(types.InlineKeyboardButton(
            btn_text, 
            callback_data=f"buy_g_{g['id']}"
        ))

    # Sahifalash navigatsiyasi
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("⬅️ Oldingi", callback_data=f"gpage_{page-1}"))
    if end < total:
        nav.append(types.InlineKeyboardButton("Keyingi ➡️", callback_data=f"gpage_{page+1}"))
    if nav:
        markup.row(*nav)

    # Sahifa raqami
    pages = max(1, (total + per_page - 1) // per_page)
    markup.add(types.InlineKeyboardButton(f"📄 {page+1}/{pages}", callback_data="noop"))

    # Yopish
    markup.add(types.InlineKeyboardButton("🔙 Yopish", callback_data="close_gifts"))

    return markup, total, page


# ==================== SOVG'A STATISTIKASI ====================
def get_gift_stats(gid):
    """Sovg'a statistikasi (sotilgan, kutilmoqda)"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT COUNT(*) as n FROM gift_orders
                     WHERE gift_id = %s AND status = 'delivered'""", (gid,))
        delivered = c.fetchone()["n"]
        c.execute("""SELECT COUNT(*) as n FROM gift_orders
                     WHERE gift_id = %s AND status = 'pending'""", (gid,))
        pending = c.fetchone()["n"]
        c.execute("""SELECT COUNT(*) as n FROM gift_orders
                     WHERE gift_id = %s AND is_free = 1""", (gid,))
        free_cnt = c.fetchone()["n"]
        return {
            "delivered": delivered,
            "pending": pending,
            "free_count": free_cnt,
        }
    finally:
        put_db(conn)


# ==================== QISM 5 TUGADI ====================
# Keyingi QISM 6: To'lov funksiyalari (unikal summa, expire, confirm, reject)
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 6/20 ====================
# To'lov: unikal summa, expire, confirm, reject
# =====================================================================

# ==================== SUMMA BANDMI? ====================
def is_amount_busy(amount):
    """Bu summa boshqa pending to'lovda bandmi?"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT id FROM payments WHERE status = 'pending' AND unique_amount = %s", (amount,))
        row = c.fetchone()
        return row is not None
    finally:
        put_db(conn)


# ==================== UNIKAL SUMMA YARATISH ====================
def get_unique_amount(desired=None):
    """
    Unikal summa yaratish (boshqa pending to'lov bilan to'qnashmasin).
    Masalan: 10 000 band → 10 001 → 10 002 → ...
    """
    start = get_setting("start_amount", 5000)
    amount = int(desired) if desired is not None else int(start)
    if amount < 1000:
        amount = int(start)
    for _ in range(50):
        if not is_amount_busy(amount):
            return amount
        amount += 1
    return amount


# ==================== TO'LOV YARATISH ====================
def create_payment(uid, amount):
    """
    Yangi to'lov yozuvi yaratish.
    Qaytaradi: (payment_id, unique_amount, expires_datetime)
    """
    # Avval muddati o'tganlarni tozalash
    expire_pending()

    # Unikal summa
    amt = get_unique_amount(amount)
    mins = int(get_setting("payment_time_minutes", 15))
    now_dt = datetime.now()
    exp_dt = now_dt + timedelta(minutes=mins)

    conn = get_db()
    c = conn.cursor()
    c.execute(
        """INSERT INTO payments
           (user_id, amount, unique_amount, status, method, created_at, expires_at)
           VALUES (%s, %s, %s, 'pending', 'humo', %s, %s)
           RETURNING id""",
        (uid, amt, amt,
         now_dt.strftime("%Y-%m-%d %H:%M:%S"),
         exp_dt.strftime("%Y-%m-%d %H:%M:%S"))
    )
    pid = c.fetchone()["id"]
    conn.commit()
    put_db(conn)
    return pid, amt, exp_dt


# ==================== MUDDATI O'TGAN TO'LOVLAR ====================
def expire_pending():
    """
    Muddati o'tgan pending to'lovlarni 'expired' qilish.
    Foydalanuvchiga xabar yuboradi.
    """
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute(
        """SELECT id, user_id, unique_amount FROM payments
           WHERE status = 'pending'
           AND expires_at IS NOT NULL
           AND expires_at < %s""",
        (now,)
    )
    rows = c.fetchall()

    for row in rows:
        c.execute("UPDATE payments SET status = 'expired' WHERE id = %s", (row["id"],))
        try:
            bot.send_message(row["user_id"],
                f"⏰ <b>Toʻlov vaqti tugadi!</b>\n\n"
                f"💰 Summa: <b>{format_money(row['unique_amount'])}</b>\n\n"
                f"Yangi toʻlov uchun «➕ Toʻldirish» ni bosing.")
        except Exception:
            pass

    conn.commit()
    put_db(conn)
    return len(rows)


# ==================== EXPIRE CHECKER (THREAD) ====================
def start_expire_checker():
    """Har 30 sekundda muddati o'tgan to'lovlarni tekshiruvchi thread"""
    def worker():
        while True:
            try:
                n = expire_pending()
                if n:
                    print(f"⏰ {n} ta to'lov muddati o'tdi")
            except Exception as e:
                print("Expire error:", e)
            time.sleep(30)

    Thread(target=worker, daemon=True).start()
    print("🔄 Expire checker ishga tushdi")


# ==================== TO'LOVNI QO'LDA TASDIQLASH ====================
def confirm_payment(pid, admin_id=None):
    """
    To'lovni qo'lda tasdiqlash (admin uchun).
    Qaytaradi: (success: bool, user_id | error_message)
    """
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id, unique_amount, status FROM payments WHERE id = %s", (pid,))
    r = c.fetchone()

    if not r or r["status"] == "paid":
        put_db(conn)
        return False, "Topilmadi yoki allaqachon toʻlangan"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("UPDATE payments SET status = 'paid', paid_at = %s WHERE id = %s", (now, pid))
    c.execute(
        "UPDATE users SET balance = balance + %s WHERE user_id = %s",
        (r["unique_amount"], r["user_id"])
    )
    conn.commit()
    put_db(conn)

    # Tranzaksiya yozish
    try:
        write_transaction(r["user_id"], "kirim", r["unique_amount"], "humo", "Qoʻlda tasdiq")
    except Exception:
        pass

    # Admin log
    if admin_id:
        try:
            log_admin_action(admin_id, "confirm_payment", f"#{pid} — {r['unique_amount']}")
        except Exception:
            pass

    # Userga xabar
    try:
        bot.send_message(r["user_id"],
            f"✅ <b>Toʻlov tasdiqlandi!</b>\n\n"
            f"💰 {format_money(r['unique_amount'])}\n"
            f"🧾 #{pid}\n\n"
            f"Endi «🎁 Sovgʻalar» orqali xarid qiling.")
    except Exception:
        pass

    return True, r["user_id"]


# ==================== TO'LOVNI RAD ETISH ====================
def reject_payment(pid, admin_id=None):
    """To'lovni rad etish (admin uchun)"""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id, unique_amount, status FROM payments WHERE id = %s", (pid,))
    r = c.fetchone()

    if not r or r["status"] != "pending":
        put_db(conn)
        return False

    c.execute("UPDATE payments SET status = 'expired' WHERE id = %s", (pid,))
    conn.commit()
    put_db(conn)

    if admin_id:
        try:
            log_admin_action(admin_id, "reject_payment", f"#{pid}")
        except Exception:
            pass

    try:
        bot.send_message(r["user_id"],
            f"❌ <b>Toʻlov rad etildi</b>\n\n"
            f"🧾 #{pid}\n\n"
            f"Muammo boʻlsa — «📞 Admin» orqali yozing.")
    except Exception:
        pass

    return True


# ==================== TO'LOV HOLATINI OLISH ====================
def get_payment(pid):
    """Bitta to'lovni olish"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM payments WHERE id = %s", (pid,))
        r = c.fetchone()
        return dict(r) if r else None
    finally:
        put_db(conn)


# ==================== FOYDALANUVCHI TO'LOVLARI ====================
def get_user_payments(uid, limit=20):
    """Foydalanuvchi to'lovlari"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            """SELECT id, unique_amount, status, method, created_at, paid_at
               FROM payments WHERE user_id = %s
               ORDER BY id DESC LIMIT %s""",
            (uid, limit)
        )
        return [dict(x) for x in c.fetchall()]
    finally:
        put_db(conn)


# ==================== TO'LOV STATISTIKASI ====================
def get_payment_stats():
    """Umumiy to'lov statistikasi (admin uchun)"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s
                     FROM payments WHERE status = 'paid'""")
        paid = c.fetchone()
        c.execute("""SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s
                     FROM payments WHERE status = 'pending'""")
        pending = c.fetchone()
        c.execute("""SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s
                     FROM payments WHERE status = 'expired'""")
        expired = c.fetchone()
        c.execute("SELECT COUNT(*) as n FROM payments")
        total = c.fetchone()["n"]
        c.execute("""SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s
                     FROM payments WHERE status = 'paid'
                     AND DATE(paid_at) = CURRENT_DATE""")
        today = c.fetchone()
        return {
            "paid": paid,
            "pending": pending,
            "expired": expired,
            "total": total,
            "today": today,
        }
    finally:
        put_db(conn)


# ==================== TO'LOVNI TEKSHIRISH (USER UCHUN) ====================
def get_user_payment_summary(uid):
    """User to'lovlar xulosasi"""
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s
                     FROM payments WHERE user_id = %s AND status = 'paid'""", (uid,))
        paid = c.fetchone()
        c.execute("""SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s
                     FROM payments WHERE user_id = %s AND status = 'pending'""", (uid,))
        pending = c.fetchone()
        c.execute("""SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s
                     FROM payments WHERE user_id = %s AND status = 'expired'""", (uid,))
        expired = c.fetchone()
        return {
            "paid": paid,
            "pending": pending,
            "expired": expired,
        }
    finally:
        put_db(conn)


# ==================== QISM 6 TUGADI ====================
# Keyingi QISM 7: HUMO parser + process_humo (avto-to'lov)
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 7/20 ====================
# HUMO xabaridan summani ajratish + avtomatik to'lov tasdiqlash
# =====================================================================

# ==================== HUMO XABARIDAN SUMMANI AJRATISH ====================
def parse_humo(text):
    """
    HUMO / bank xabaridan summani aniq ajratib olish.
    Misol:
        "➕ 5.000,00 UZS"  → 5000
        "💰 35.000,00 UZS" → 35000
    """
    if not text:
        return None

    # Normalize — turli bo'shliqlar va apostroflar
    text_n = (
        text.replace("\u00a0", " ")
        .replace(" ", " ")
        .replace("'", "'")
    )

    candidates = []

    patterns = [
        # ➕ 5.000,00 UZS | ➕ 5.000,00 сўм
        r'[\+➕]\s*([\d\s]+[.,]\d{2})\s*(?:UZS|сўм|so[ʻ\']?m|сум|som)',
        # kirim/приход/поступление 5.000,00
        r'(?:kirim|приход|income|поступление)[^\d]{0,20}([\d\s]+[.,]\d{2})',
        # сумма/summa/amount: 5.000,00
        r'(?:сумма|summa|amount|суммаси|miqdor)[:\s]*([\d\s]+[.,]?\d*)',
        # 💰 5.000,00 UZS
        r'💰\s*([\d\s]+[.,]\d{2})\s*UZS',
        # 5.000,00 UZS
        r'([\d\s]+[.,]\d{2})\s*UZS',
        # ➕ 5000 (UZS ixtiyoriy)
        r'[\+➕]\s*([\d\s.,]+)\s*(?:UZS|so[ʻ\']?m)?',
        # 5000,00 (vergul + 00)
        r'(\d{3,7})[.,]00\b',
        # 5000 UZS / 5000 сўм
        r'\b(\d{4,7})\b\s*(?:UZS|so[ʻ\']?m|сум)',
    ]

    for pat in patterns:
        for m in re.finditer(pat, text_n, re.IGNORECASE):
            raw = m.group(1).replace(" ", "").replace(",", ".")
            try:
                # Agar 2+ nuqta bo'lsa — oxirgisini kasr qilib qoldirish
                if raw.count(".") > 1:
                    parts = raw.split(".")
                    raw = "".join(parts[:-1]) + "." + parts[-1]
                val = int(round(float(raw)))
                if 500 <= val <= 50_000_000:
                    candidates.append(val)
            except Exception:
                continue

    # Birinchi topilgan summani qaytarish
    if candidates:
        return candidates[0]

    # Fallback — 4-7 xonali oddiy raqam
    for m in re.finditer(r'\b(\d{4,7})\b', text_n.replace(" ", "")):
        try:
            val = int(m.group(1))
            if 1000 <= val <= 50_000_000:
                return val
        except Exception:
            pass

    return None


# ==================== TAKRORIY XABARLARNI BLOKLASH ====================
_recent_humo = {}  # {amount: timestamp}


# ==================== HUMO TO'LOVNI AVTO TASDIQLASH ====================
def process_humo(amount, src="HUMO"):
    """
    HUMO xabaridan kelgan summani pending to'lovlar bilan solishtiradi
    va topilsa — avtomatik tasdiqlaydi.
    """
    amount = float(amount)
    now_ts = time.time()

    # Takroriy xabar (30 sekund ichida) — o'tkazib yuborish
    if amount in _recent_humo and now_ts - _recent_humo[amount] < 30:
        print(f"⏭ Takroriy HUMO o'tkazib yuborildi: {amount}")
        return False
    _recent_humo[amount] = now_ts

    # Eski yozuvlarni tozalash (5 daqiqadan oshgan)
    for k in list(_recent_humo.keys()):
        if now_ts - _recent_humo[k] > 300:
            del _recent_humo[k]

    conn = get_db()
    try:
        c = conn.cursor()

        # 1) Aniq summani qidirish
        c.execute("""SELECT id, user_id, unique_amount, created_at, expires_at
                     FROM payments
                     WHERE status = 'pending' AND unique_amount = %s
                     ORDER BY id DESC LIMIT 1""", (amount,))
        row = c.fetchone()

        # 2) Topilmasa — ±2 diapazonda qidirish
        if not row:
            c.execute("""SELECT id, user_id, unique_amount, created_at, expires_at
                         FROM payments
                         WHERE status = 'pending'
                         AND unique_amount BETWEEN %s AND %s
                         ORDER BY id DESC LIMIT 1""", (amount - 2, amount + 2))
            row = c.fetchone()

        # 3) Ham topilmasa — adminga xabar
        if not row:
            print(f"⚠️ [{src}] Mos pending yoʻq: {amount}")
            for admin_id in ADMIN_IDS:
                try:
                    bot.send_message(admin_id,
                        f"⚠️ <b>Avto-toʻlov: mos pending topilmadi</b>\n\n"
                        f"💰 Kelgan summa: <b>{format_money(amount)}</b>\n"
                        f"📡 Manba: <b>{src}</b>\n\n"
                        f"User hali «➕ Toʻldirish» qilmagan yoki summa mos emas.\n\n"
                        f"Qoʻlda: /confirm ID yoki «💵 Balans +-»")
                except Exception:
                    pass
            return False

        # ========== TO'LOVNI TASDIQLASH ==========
        pay_id = row["id"]
        user_id = row["user_id"]
        unique_amount = row["unique_amount"]
        created_at = row.get("created_at") or "—"

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        c.execute("UPDATE payments SET status = 'paid', paid_at = %s, method = 'humo' WHERE id = %s",
                  (now, pay_id))
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s",
                  (unique_amount, user_id))

        c.execute("SELECT balance, full_name, username FROM users WHERE user_id = %s", (user_id,))
        u = c.fetchone()
        new_bal = u["balance"] if u else unique_amount
        uname = f"@{u['username']}" if u and u.get("username") else "—"
        fname = (u.get("full_name") if u else "") or "—"

        conn.commit()
    finally:
        put_db(conn)

    # ========== TRANZAKSIYA YOZISH ==========
    try:
        write_transaction(user_id, "kirim", unique_amount, "humo", f"Avto ({src})")
    except Exception:
        pass

    # ========== USERGA XABAR ==========
    try:
        bot.send_message(user_id,
            f"✅ <b>Toʻlov muvaffaqiyatli tasdiqlandi!</b>\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💰 Toʻldirildi: <b>{format_money(unique_amount)}</b>\n"
            f"💵 Joriy balans: <b>{format_money(new_bal)}</b>\n"
            f"🧾 Chek №: <code>{pay_id}</code>\n"
            f"🕒 Vaqt: {now}\n"
            f"📡 Usul: avtomatik ({src})\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"Endi «🎁 Sovgʻalar» orqali xarid qiling.")
    except Exception as e:
        print("Notify user error:", e)

    # ========== ADMINLARGA XABAR ==========
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id,
                f"💳 <b>AVTO-TOʻLOV TASDIQLANDI</b>\n\n"
                f"👤 User: {fname} ({uname})\n"
                f"🆔 ID: <code>{user_id}</code>\n"
                f"💰 Summa: <b>{format_money(unique_amount)}</b>\n"
                f"💵 Yangi balans: <b>{format_money(new_bal)}</b>\n"
                f"🧾 Payment #: <code>{pay_id}</code>\n"
                f"📅 Yaratilgan: {created_at}\n"
                f"✅ Tasdiq: {now}\n"
                f"📡 Manba: {src}")
        except Exception:
            pass

    print(f"✅ [{src}] Avto-to'lov OK: user={user_id} amount={unique_amount} pay=#{pay_id}")
    return True


# ==================== HUMO SUMMANI QAYTA TEKSHIRISH ====================
def parse_humo_debug(text):
    """
    Debug rejimi — HUMO xabarnining turli qismlaridan summalarni ko'rsatadi.
    """
    if not text:
        return []
    text_n = text.replace("\u00a0", " ").replace(" ", " ").replace("'", "'")
    results = []

    patterns = [
        r'[\+➕]\s*([\d\s]+[.,]\d{2})\s*(?:UZS|сўм|so[ʻ\']?m|сум|som)',
        r'💰\s*([\d\s]+[.,]\d{2})\s*UZS',
        r'([\d\s]+[.,]\d{2})\s*UZS',
        r'\b(\d{4,7})\b',
    ]

    for pat in patterns:
        for m in re.finditer(pat, text_n, re.IGNORECASE):
            raw = m.group(1).replace(" ", "").replace(",", ".")
            try:
                if raw.count(".") > 1:
                    parts = raw.split(".")
                    raw = "".join(parts[:-1]) + "." + parts[-1]
                val = int(round(float(raw)))
                if 500 <= val <= 50_000_000:
                    results.append({
                        "value": val,
                        "raw": m.group(0),
                        "pattern": pat[:40]
                    })
            except Exception:
                continue

    return results


# ==================== HUMO XABARNI TEKSHIRISH (TEST) ====================
def test_humo_parse():
    """HUMO parser uchun test misollari"""
    tests = [
        "🎉 To'ldirish\n➕ 5.000,00 UZS\n📍 ZOOMRAD\n💰 35.000,00 UZS",
        "💰 10.003 UZS",
        "+15 000,00 soʻm",
        "приход 25.000,00 UZS",
        "5000 UZS",
        "➕ 100 000,00 UZS",
    ]
    print("=" * 50)
    print("🧪 HUMO PARSER TEST")
    print("=" * 50)
    for t in tests:
        result = parse_humo(t)
        print(f"📩 {t[:60]}...")
        print(f"💰 → {result}")
        print("-" * 30)
    print("=" * 50)


# ==================== QISM 7 TUGADI ====================
# Keyingi QISM 8: Telethon listener + Gift yuborish + Isbotlar kanal
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 8/20 ====================
# Telethon listener (HUMO 24/7), gift yuborish (bot/userbot/auto), isbot kanal
# =====================================================================

# ==================== TELETHON LISTENER (HUMO KUZATUV) ====================
def start_telethon():
    """
    @HUMOcardbot xabarlarini 24/7 kuzatuvchi Telethon thread.
    Render uchun: TELETHON_STRING_SESSION env (kod so'ralmaydi).
    """
    global telethon_client
    if not API_ID or not API_HASH:
        print("⚠️ API_ID/API_HASH yoʻq — Telethon oʻchiq")
        return
    try:
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession
    except ImportError:
        print("⚠️ telethon oʻrnatilmagan: pip install telethon")
        return

    string_session = (os.getenv("TELETHON_STRING_SESSION") or "").strip()
    humo_names = {
        HUMO_BOT_USERNAME.lower().lstrip("@"),
        "humocardbot",
        "humo",
    }

    if not string_session:
        print("=" * 50)
        print("❌ TELETHON_STRING_SESSION yoʻq!")
        print("=" * 50)
        return

    async def run_listener():
        global telethon_client
        client = TelegramClient(
            StringSession(string_session),
            API_ID,
            API_HASH,
            device_model="SovgaMarket",
            system_version="1.0",
            app_version="1.0",
        )
        print("🔌 Telethon StringSession bilan ulanmoqda...")
        await client.connect()

        if not await client.is_user_authorized():
            print("❌ StringSession yaroqsiz yoki muddati oʻtgan.")
            await client.disconnect()
            return

        me = await client.get_me()
        telethon_client = client
        print(f"✅ Telethon OK: id={me.id} username=@{getattr(me, 'username', None)}")

        humo_entity = None
        try:
            humo_entity = await client.get_entity(HUMO_BOT_USERNAME)
            print(f"✅ HUMO entity: {humo_entity.id}")
        except Exception as e:
            print(f"⚠️ HUMO entity topilmadi ({HUMO_BOT_USERNAME}): {e}")

        async def handle_text(text, tag="HUMO"):
            text = text or ""
            if not text.strip():
                return
            print(f"📩 [{tag}] {text[:150].replace(chr(10), ' ')}")
            amount = parse_humo(text)
            if amount:
                print(f"💰 [{tag}] Summa: {amount}")
                process_humo(amount, source=tag)
            else:
                print(f"⚠️ [{tag}] Summa aniqlanmadi")

        if humo_entity:
            @client.on(events.NewMessage(from_users=humo_entity))
            async def on_humo_entity(event):
                await handle_text(event.message.message, "HUMOcardbot")

        @client.on(events.NewMessage(from_users=HUMO_BOT_USERNAME))
        async def on_humo_name(event):
            await handle_text(event.message.message, "HUMOcardbot")

        @client.on(events.NewMessage())
        async def on_all_messages(event):
            try:
                sender = await event.get_sender()
                if not sender:
                    return
                uname = (getattr(sender, "username", None) or "").lower()
                if uname not in humo_names:
                    return
                await handle_text(event.message.message, f"@{uname}")
            except Exception as e:
                print("on_all_messages error:", e)

        @client.on(events.NewMessage(incoming=True, pattern=re.compile(
            r'(UZS|\+[\d\s.,]+|сумма|kirim|поступление|💰)', re.I)))
        async def on_pattern(event):
            try:
                sender = await event.get_sender()
                uname = (getattr(sender, "username", None) or "").lower()
                if uname not in humo_names and not (
                    humo_entity and sender and
                    getattr(sender, "id", None) == getattr(humo_entity, "id", None)
                ):
                    return
                await handle_text(event.message.message, "HUMO-pattern")
            except Exception as e:
                print("on_pattern error:", e)

        print("👁 HUMO kuzatuv faol...")
        await client.run_until_disconnected()

    def run_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        backoff = 5
        while True:
            try:
                loop.run_until_complete(run_listener())
                backoff = 5
            except Exception as e:
                print(f"❌ Telethon uzildi: {e}")
            print(f"🔄 Telethon qayta ulanish: {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    Thread(target=run_in_thread, daemon=True).start()
    print("🚀 Telethon avto-toʻlov ishga tushdi")


# ==================== GIFT YUBORISH — BOT ORQALI ====================
def send_gift_bot(uid, tg_gift_id, text=None):
    """
    Botning o'z Stars balansidan gift yuborish.
    Qaytaradi: (success: bool, info: str)
    """
    try:
        kwargs = {
            "user_id": int(uid),
            "gift_id": str(tg_gift_id),
            "pay_for_upgrade": True,
        }
        if text:
            kwargs["text"] = text[:255]
        bot.send_gift(**kwargs)
        return True, "OK"
    except Exception as e:
        err = str(e)
        if "BALANCE_TOO_LOW" in err:
            return False, "BOT_STARS_YETARLI_EMAS"
        if "STARGIFT_USAGE_LIMITED" in err:
            return False, "GIFT_TUGAGAN"
        if "GIFT_STARS_INVALID" in err or "STARGIFT_INVALID" in err:
            return False, "GIFT_ID_XATO"
        if "USER_IS_BOT" in err:
            return False, "USER_IS_BOT"
        return False, err[:200]


# ==================== GIFT YUBORISH — TELETHON (USERBOT) ====================
async def _tg_send_gift(uid, gid, text):
    """Telethon orqali shaxsiy akkauntdan gift (async)"""
    global telethon_client
    if not telethon_client:
        return False, "Telethon ulanmagan"
    try:
        from telethon import functions
        await telethon_client(functions.payments.SendStarsGiftRequest(
            user_id=int(uid),
            gift_id=int(gid),
            text=text[:255] if text else None,
            pay_for_upgrade=True,
        ))
        return True, "OK"
    except Exception as e:
        return False, str(e)[:200]


def send_gift_userbot(uid, gid, text=None):
    """Telethon orqali shaxsiy akkauntdan gift yuborish"""
    global telethon_client
    if not telethon_client:
        return False, "Telethon ulanmagan"
    try:
        fut = asyncio.run_coroutine_threadsafe(
            _tg_send_gift(uid, gid, text), telethon_client.loop)
        return fut.result(timeout=30)
    except Exception as e:
        return False, str(e)[:200]


# ==================== GIFT YUBORISH — AVTO ====================
def send_gift_auto(uid, tg_gift_id, text=None):
    """
    GIFT_METHOD ga qarab avtomatik tanlaydi:
    - bot     → botning Stars balansidan ← TAVSIYA
    - userbot → shaxsiy akkaunt (Telethon)
    - auto    → bot → userbot fallback
    - manual  → admin qo'lda
    """
    m = get_setting("gift_method", GIFT_METHOD)

    if m == "manual":
        return False, "MANUAL"

    if m == "auto":
        try:
            r = bot.get_my_star_balance()
            bal = r.get("amount", 0) if isinstance(r, dict) else getattr(r, "amount", 0)
            m = "bot" if bal >= 1 else "userbot"
        except Exception:
            m = "userbot"

    if m == "bot":
        ok, info = send_gift_bot(uid, tg_gift_id, text)
        if not ok and info == "BOT_STARS_YETARLI_EMAS" and telethon_client:
            print("⚠️ Bot stars yoʻq → userbot'ga oʻtildi")
            return send_gift_userbot(uid, tg_gift_id, text)
        return ok, info

    if m == "userbot":
        return send_gift_userbot(uid, tg_gift_id, text)

    return False, "NOMA'LUM"


# ==================== ISBOTLAR KANALI ====================
def send_proof(user_id, gift_name, price, order_id, is_free=False, gift_emoji="🎁"):
    """Isbotlar kanaliga xarid haqida xabar yuborish"""
    if not get_setting("proof_enabled", True):
        return
    channel = get_setting("proof_channel_id", "")
    if not channel:
        return

    try:
        u = get_user(user_id)
        un = f"@{u['username']}" if u and u.get("username") else "—"
        fn = (u["full_name"] if u else "—") or "—"

        fmt_type = get_setting("proof_format", "full")
        now = datetime.now().strftime("%d.%m.%Y %H:%M")

        if fmt_type == "minimal":
            text = f"✅ #{order_id} — {gift_emoji} {gift_name} — {fn[:20]}"

        elif fmt_type == "short":
            text = (
                f"🎉 <b>Yangi xarid!</b>\n\n"
                f"👤 {fn}\n"
                f"🎁 {gift_emoji} {gift_name}\n"
                f"💵 {format_money(price) if not is_free else 'TEKIN'}\n"
                f"🧾 #{order_id}"
            )

        else:  # full
            tip = "🎁 TEKIN" if is_free else f"💵 {format_money(price)}"
            try:
                bot_username = (bot.get_me()).username
            except Exception:
                bot_username = "SovgaMarketBot"
            text = (
                f"🎉 <b>YANGI XARID!</b>\n\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"👤 User: {fn}\n"
                f"🆔 {un}\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"🎁 Sovgʻa: {gift_emoji} <b>{gift_name}</b>\n"
                f"💰 Narx: <b>{tip}</b>\n"
                f"📅 Vaqt: {now}\n"
                f"🧾 Buyurtma: #{order_id}\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"✅ Yetkazildi\n\n"
                f"🤖 @{bot_username}"
            )

        bot.send_message(channel, text)
    except Exception as e:
        print(f"⚠️ Isbot yuborishda xato: {e}")


# ==================== REFERAL ISBOT ====================
def send_referral_proof(ref_id, new_user_id, bonus):
    """Referal isbotini kanalga yuborish"""
    if not get_setting("proof_enabled", True):
        return
    channel = get_setting("proof_channel_id", "")
    if not channel:
        return
    try:
        u1 = get_user(ref_id)
        u2 = get_user(new_user_id)
        n1 = f"@{u1['username']}" if u1 and u1.get("username") else "—"
        n2 = f"@{u2['username']}" if u2 and u2.get("username") else "—"
        text = (
            f"🎁 <b>YANGI REFERAL!</b>\n\n"
            f"👤 {n1} → {n2}\n"
            f"💰 Bonus: {format_money(bonus)}\n"
            f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )
        bot.send_message(channel, text)
    except Exception as e:
        print(f"⚠️ Referal isbot xato: {e}")


# ==================== STARS BALANSI ====================
def get_bot_stars_balance():
    """Botning Stars balansi"""
    try:
        r = bot.get_my_star_balance()
        return r.get("amount", 0) if isinstance(r, dict) else getattr(r, "amount", 0)
    except Exception:
        return 0


# ==================== TG GIFTLAR RO'YXATI ====================
def get_available_tg_gifts():
    """Bot yubora oladigan TG giftlar"""
    try:
        r = bot.get_available_gifts()
        return r.get("gifts", []) if isinstance(r, dict) else (getattr(r, "gifts", []) or [])
    except Exception as e:
        print(f"getAvailableGifts xato: {e}")
        return []


# ==================== TEST USERBOT ====================
async def _test_tg_gift():
    """Telethon orqali test"""
    global telethon_client
    if not telethon_client:
        return False, "Telethon ulanmagan"
    try:
        me = await telethon_client.get_me()
        return True, f"Telethon OK: @{getattr(me, 'username', None)}"
    except Exception as e:
        return False, str(e)[:200]


def test_userbot():
    """Userbot ishlashini tekshirish"""
    global telethon_client
    if not telethon_client:
        return False, "Telethon ulanmagan"
    try:
        fut = asyncio.run_coroutine_threadsafe(_test_tg_gift(), telethon_client.loop)
        return fut.result(timeout=10)
    except Exception as e:
        return False, str(e)[:200]


# ==================== QISM 8 TUGADI ====================
# Keyingi QISM 9: Klaviaturalar — USER (7 tugma)
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 9/20 ====================
# USER klaviaturalar (7 tugma, tekin shartli ko'rinadi)
# =====================================================================

# ==================== USER ASOSIY MENYU ====================
def kb_user_main(uid):
    """
    User asosiy menyusi.
    - 7 ta asosiy tugma
    - "🎁 Tekin sovgʻa" — faqat C yoki D tizim yoqilganda ko'rinadi
    - "⭐ Stars to'plash" — user menyusida YO'Q (faqat admin panelda)
    """
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("🎁 Sovgʻalar", "💰 Hisobim")
    m.add("👥 Referal", "📋 Buyurtmalarim")
    m.add("💬 Sharhlar", "📞 Admin")
    m.add("📢 Isbotlar")

    # Tekin sovg'a tugmasi — C yoki D tizim yoqilgan bo'lsa
    invites_on = get_setting("ref_invites_on", True)
    orders_on = get_setting("ref_orders_on", False)
    if invites_on or orders_on:
        m.add("🎁 Tekin sovgʻa")

    return m


# ==================== USER HISOBIM MENYU ====================
def kb_user_acc():
    """Hisobim submenyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("💵 Balansim", "➕ Toʻldirish")
    m.add("📜 Tarix", "📊 Kirim/Chiqim")
    m.add("🔙 Orqaga")
    return m


# ==================== USER REFERAL MENYU ====================
def kb_user_ref():
    """Referal submenyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("🔗 Havolam", "👤 Doʻstlarim")
    m.add("💸 Ref balans", "💸 Asosiyga oʻtkazish")
    m.add("📊 Statistika", "🔙 Orqaga")
    return m


# ==================== ORQAGA ====================
def kb_back():
    """Faqat Orqaga"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.add("🔙 Orqaga")
    return m


# ==================== INLINE KLAVIATURALAR ====================

def kb_confirm_gift(gid):
    """Sovg'ani sotib olishni tasdiqlash"""
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(
        types.InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"ok_g_{gid}"),
        types.InlineKeyboardButton("❌ Bekor", callback_data="noop"),
    )
    m.add(types.InlineKeyboardButton("🔙 Katalog", callback_data="back_gifts"))
    return m


def kb_no_balance():
    """Balans yetarli emas — to'ldirishga o'tish"""
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(
        types.InlineKeyboardButton("➕ Toʻldirish", callback_data="goto_topup"),
        types.InlineKeyboardButton("🔙 Orqaga", callback_data="back_gifts"),
    )
    return m


def kb_sub_check(missing):
    """
    Majburiy obuna tekshirish klaviaturasi.
    missing: [(display_name, url), ...]
    """
    m = types.InlineKeyboardMarkup(row_width=1)
    for display, url in missing:
        if url:
            m.add(types.InlineKeyboardButton(f"📢 {display}", url=url))
        else:
            m.add(types.InlineKeyboardButton(f"📢 {display}", callback_data="noop"))
    m.add(types.InlineKeyboardButton("✅ Tekshirish", callback_data="chk_sub"))
    return m


def kb_proof_link(channel_url):
    """
    Isbotlar kanal linki klaviaturasi.
    User bosganda DARHOL kanalga o'tadi.
    """
    m = types.InlineKeyboardMarkup()
    m.add(types.InlineKeyboardButton("📢 Kanalga oʻtish", url=channel_url))
    return m


# ==================== QISM 9 TUGADI ====================
# Keyingi QISM 10: Klaviaturalar — ADMIN (14 tugma + Stars to'plash)
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 10/20 ====================
# ADMIN klaviaturalar (15 tugma, Stars to'plash)
# =====================================================================

# ==================== ADMIN ASOSIY MENYU ====================
def kb_admin_main():
    """
    Admin asosiy menyusi — 15 tugma.
    - ⭐ Bot Stars to'plash — YANGI tugma
    - 📢 Isbotlar — boshqaruv paneli
    - 💬 Sharhlar — o'chirish + yozish
    """
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("📊 Statistika", "💰 Toʻlovlar")
    m.add("🎁 Sovgʻalar", "👥 Foydalanuvchilar")
    m.add("📢 Xabar", "🎁 Referal sozlama")
    m.add("⏱ Toʻlov sozlama", "📢 Majburiy obuna")
    m.add("📢 Isbotlar", "⭐ Stars sozlama")
    m.add("⭐ Bot Stars toʻplash", "💰 Bot Stars balansi")  # ← YANGI
    m.add("💬 Sharhlar", "👨‍💼 Adminlar")
    m.add("⚙️ Boshqa", "🔙 Asosiy menyu")
    return m


# ==================== SOVG'ALAR BOSHQARUVI ====================
def kb_admin_gifts():
    """Sovg'alar CRUD menyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("➕ Sovgʻa qoʻshish", "📋 Roʻyxat")
    m.add("💵 Narx", "📦 Miqdor")
    m.add("⭐ Stars narxi", "🎯 Tekin soni")
    m.add("🎁 TG giftlar", "🔗 Gift bogʻlash")
    m.add("❌ Oʻchirish", "🧾 Yetkazilmaganlar")
    m.add("🔙 Admin Panel")
    return m


# ==================== FOYDALANUVCHILAR ====================
def kb_admin_users():
    """Userlar boshqaruvi menyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("📋 Userlar", "🔍 Qidirish")
    m.add("💵 Balans +-", "🚫 Blok")
    m.add("📊 Top userlar", "📥 Excel export")
    m.add("🔙 Admin Panel")
    return m


# ==================== TO'LOV SOZLAMA ====================
def kb_admin_pay_set():
    """To'lov sozlamalari menyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("⏱ Vaqt", "💵 Start summa")
    m.add("💳 Karta", "👤 Karta egasi")
    m.add("🔙 Admin Panel")
    return m


# ==================== REFERAL SOZLAMA ====================
def kb_admin_ref_set():
    """Referal sozlamalari menyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("📊 Foiz", "💰 Bonus")
    m.add("⚙️ Toggle", "📊 Ref statistika")
    m.add("🔙 Admin Panel")
    return m


# ==================== MAJBURIY OBUNA ====================
def kb_admin_channels():
    """Obuna kanallar boshqaruvi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("➕ Kanal qoʻshish", "📋 Kanallar")
    m.add("❌ Kanal oʻchirish", "🔄 Test")
    m.add("🔙 Admin Panel")
    return m


# ==================== ISBOTLAR (ADMIN — BOSHQARUV) ====================
def kb_admin_proof():
    """Isbotlar kanali boshqaruvi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("➕ Kanal belgilash", "📋 Format tanlash")
    m.add("⚙️ Yoq/Oʻchirish", "📤 Test xabar")
    m.add("❌ Kanal oʻchirish", "🔙 Admin Panel")
    return m


# ==================== STARS SOZLAMA ====================
def kb_admin_stars():
    """Stars sozlamalari menyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("⭐ Bot Stars balansi", "🎁 TG giftlar")
    m.add("🔗 Gift bogʻlash", "🔄 Method")
    m.add("🔙 Admin Panel")
    return m


# ==================== ADMINLAR ====================
def kb_admin_admins():
    """Adminlar boshqaruvi menyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("➕ Admin qoʻshish", "❌ Admin oʻchirish")
    m.add("📋 Adminlar", "📊 Harakatlar")
    m.add("🔙 Admin Panel")
    return m


# ==================== SHARHLAR (ADMIN) ====================
def kb_admin_reviews():
    """Sharhlar boshqaruvi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("🗑 Sharh oʻchirish", "✍️ O'zi sharh yozish")
    m.add("🔙 Admin Panel")
    return m


# ==================== BOSHQA SOZLAMALAR ====================
def kb_admin_other():
    """Boshqa sozlamalar menyusi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    m.add("🟢 Bot yoqish", "🔴 Bot oʻchirish")
    m.add("🛠 Texnik ishlar", "💾 Backup")
    m.add("🔙 Admin Panel")
    return m


# ==================== INLINE KLAVIATURALAR (ADMIN) ====================

def kb_admin_gift_method():
    """Gift method tanlash"""
    m = types.InlineKeyboardMarkup(row_width=2)
    cur = get_setting("gift_method", GIFT_METHOD)
    for x in ["bot", "userbot", "auto", "manual"]:
        st = "✅" if x == cur else "⬜"
        m.add(types.InlineKeyboardButton(f"{st} {x}", callback_data=f"setgm_{x}"))
    return m


def kb_admin_proof_format():
    """Isbot formati tanlash"""
    m = types.InlineKeyboardMarkup(row_width=1)
    cur = get_setting("proof_format", "full")
    items = [
        ("full", "📄 To'liq (matn + emoji)"),
        ("short", "📝 Qisqa"),
        ("minimal", "⚡ Minimal"),
    ]
    for k, name in items:
        st = "✅" if k == cur else "⬜"
        m.add(types.InlineKeyboardButton(f"{st} {name}", callback_data=f"pfmt_{k}"))
    return m


def kb_admin_ref_toggle():
    """Referal toggle menyusi (4 xil)"""
    m = types.InlineKeyboardMarkup(row_width=1)
    items = [
        ("ref_bonus_on", "💰 Do'st kelganda pul"),
        ("ref_percent_on", "📊 Sovg'a foizi"),
        ("ref_invites_on", "👥 Har gift uchun ref"),
        ("ref_orders_on", "🛒 Buyurtma soni → tekin"),
    ]
    for k, t in items:
        on = get_setting(k, k != "ref_orders_on")
        if k == "ref_orders_on":
            on = get_setting(k, False)
        m.add(types.InlineKeyboardButton(f"{'✅' if on else '❌'} {t}", callback_data=f"rt_{k}"))
    return m


def kb_admin_user_actions(uid):
    """User ustiga bosilganda — amallar"""
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(
        types.InlineKeyboardButton("💬 Xabar", callback_data=f"am_{uid}"),
        types.InlineKeyboardButton("💵 +Balans", callback_data=f"ab_{uid}"),
    )
    m.add(
        types.InlineKeyboardButton("🚫 Blok/Ochish", callback_data=f"abl_{uid}"),
        types.InlineKeyboardButton("📋 Buyurtmalar", callback_data=f"ao_{uid}"),
    )
    return m


def kb_admin_gift_actions(gid):
    """Sovg'a ustiga bosilganda — amallar"""
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(
        types.InlineKeyboardButton("💵 Narx", callback_data=f"gp_{gid}"),
        types.InlineKeyboardButton("📦 Miqdor", callback_data=f"gs_{gid}"),
    )
    m.add(
        types.InlineKeyboardButton("⭐ Stars", callback_data=f"gsp_{gid}"),
        types.InlineKeyboardButton("🎯 Tekin ref", callback_data=f"gref_{gid}"),
    )
    m.add(types.InlineKeyboardButton("❌ O'chirish", callback_data=f"gd_{gid}"))
    return m


def kb_admin_confirm_pay(pid):
    """To'lovni tasdiqlash"""
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(
        types.InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"cp_{pid}"),
        types.InlineKeyboardButton("❌ Rad etish", callback_data=f"rp_{pid}"),
    )
    return m


def kb_admin_retry_gift(oid):
    """Qayta yuborish klaviaturasi"""
    m = types.InlineKeyboardMarkup(row_width=1)
    m.add(types.InlineKeyboardButton("🔄 Qayta yuborish", callback_data=f"retry_{oid}"))
    m.add(types.InlineKeyboardButton("✅ Qoʻlda yuborildi", callback_data=f"dlv_{oid}"))
    return m


def kb_admin_back():
    """Faqat Admin Panel tugmasi"""
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.add("🔙 Admin Panel")
    return m


# ==================== QISM 10 TUGADI ====================
# Keyingi QISM 11: USER handlerlar (start, help, hisobim, to'ldirish)
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 11/20 ====================
# USER: /start, /help, hisobim, to'ldirish, tarix, kirim/chiqim
# =====================================================================

# ==================== /start ====================
@bot.message_handler(commands=['start'])
def cmd_start(m):
    uid = m.from_user.id
    un = m.from_user.username or ""
    fn = m.from_user.full_name or ""

    # Referal kod
    ref = 0
    if len(m.text.split()) > 1:
        try:
            rc = m.text.split()[1]
            if rc.startswith("ref"):
                ref = int(rc[3:])
        except Exception:
            pass

    # Ro'yxatdan o'tkazish
    register_user(uid, un, fn, ref)
    u = get_user(uid)

    # Bloklangan
    if u and u["is_blocked"] == 1:
        bot.send_message(uid, "🚫 Siz bloklangansiz.")
        return

    # Bot holati
    if not get_setting("bot_active", True):
        bot.send_message(uid, "🔴 Bot vaqtincha oʻchirilgan.")
        return

    if get_setting("maintenance", False) and not is_admin(uid):
        bot.send_message(uid, "🛠 Texnik ishlar olib borilmoqda. Keyinroq urinib koʻring.")
        return

    # Majburiy obuna
    is_sub, missing = check_subscription(uid)
    if not is_sub:
        bot.send_message(uid,
            "⚠️ <b>Kanallarga obuna boʻling:</b>\n\n"
            "Keyin «✅ Tekshirish» ni bosing.",
            reply_markup=kb_sub_check(missing))
        return

    bal = format_money(u["balance"]) if u else "0 soʻm"

    # ADMIN bo'lsa — admin panelga
    if is_admin(uid):
        bot.send_message(uid,
            f"👑 <b>Admin panel tayyor, {fn}!</b>\n\n"
            f"💵 Balans: <b>{bal}</b>\n"
            f"🆔 ID: <code>{uid}</code>\n\n"
            f"Siz admin sifatida kirdingiz 👇",
            reply_markup=kb_admin_main())
    else:
        bot.send_message(uid,
            f"🎉 <b>Assalomu alaykum, {fn}!</b>\n\n"
            f"🎁 <b>SovgaMarket</b> — Telegram sovgʻalar doʻkoni\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💵 Balans: <b>{bal}</b>\n"
            f"🆔 ID: <code>{uid}</code>\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"Pastdagi menyudan tanlang 👇",
            reply_markup=kb_user_main(uid))


# ==================== /help ====================
@bot.message_handler(commands=['help'])
def cmd_help(m):
    uid = m.from_user.id
    text = (
        f"📖 <b>Yordam</b>\n\n"
        f"🎁 <b>SovgaMarket</b> — Telegram sovgʻalar doʻkoni\n\n"
        f"<b>Asosiy buyruqlar:</b>\n"
        f"• /start — Botni ishga tushirish\n"
        f"• /help — Yordam\n"
        f"• /cancel — Bekor qilish\n"
    )
    if is_admin(uid):
        text += (
            f"\n<b>Admin buyruqlar:</b>\n"
            f"• /confirm ID — toʻlovni tasdiqlash\n"
            f"• /reject ID — toʻlovni rad etish\n"
            f"• /stats — statistika\n"
            f"• /stars — bot Stars balansi\n"
        )
    rm = kb_admin_main() if is_admin(uid) else kb_user_main(uid)
    bot.send_message(uid, text, reply_markup=rm)


# ==================== /cancel ====================
@bot.message_handler(commands=['cancel'])
def cmd_cancel(m):
    uid = m.from_user.id
    rm = kb_admin_main() if is_admin(uid) else kb_user_main(uid)
    bot.send_message(m.chat.id, "✅ Bekor qilindi.", reply_markup=rm)


# ==================== OBUNA TEKSHIRISH ====================
@bot.callback_query_handler(func=lambda c: c.data == "chk_sub")
def cb_chk_sub(call):
    uid = call.from_user.id
    is_sub, missing = check_subscription(uid)
    if is_sub:
        bot.answer_callback_query(call.id, "✅ Tasdiqlandi!")
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass

        # /start ni qayta chaqirish
        class FakeMsg:
            def __init__(s, u):
                s.from_user = u
                s.text = "/start"
                s.chat = type("o", (), {"id": u.id})()
        cmd_start(FakeMsg(call.from_user))
    else:
        bot.answer_callback_query(call.id, "❌ Obuna boʻlmadingiz!", show_alert=True)


@bot.callback_query_handler(func=lambda c: c.data == "noop")
def cb_noop(call):
    bot.answer_callback_query(call.id)


# ==================== ORQAGA / BEKOR ====================
@bot.message_handler(func=lambda m: m.text in ["🔙 Orqaga", "🔙 Asosiy menyu"])
def msg_back(m):
    uid = m.from_user.id
    if is_admin(uid):
        bot.send_message(m.chat.id, "🔐 Admin Panel", reply_markup=kb_admin_main())
    else:
        bot.send_message(m.chat.id, "🏠 Asosiy menyu", reply_markup=kb_user_main(uid))


@bot.message_handler(func=lambda m: m.text == "❌ Bekor")
def msg_cancel(m):
    uid = m.from_user.id
    rm = kb_admin_main() if is_admin(uid) else kb_user_main(uid)
    bot.send_message(m.chat.id, "Bekor qilindi.", reply_markup=rm)


@bot.message_handler(func=lambda m: m.text == "🔙 Admin Panel")
def msg_back_admin(m):
    if not is_admin(m.from_user.id):
        bot.send_message(m.chat.id, "🏠", reply_markup=kb_user_main(m.from_user.id))
        return
    bot.send_message(m.chat.id, "🔐 <b>Admin Panel</b>", reply_markup=kb_admin_main())


# ==================== 💰 HISOBIM (user) ====================
@bot.message_handler(func=lambda m: m.text == "💰 Hisobim" and not is_admin(m.from_user.id))
def msg_acc(m):
    bot.send_message(m.chat.id, "💰 <b>Hisobim</b>", reply_markup=kb_user_acc())


@bot.message_handler(func=lambda m: m.text == "💵 Balansim" and not is_admin(m.from_user.id))
def msg_balance(m):
    uid = m.from_user.id
    u = get_user(uid)
    if not u:
        return
    stats = get_user_stats(uid)
    bot.send_message(m.chat.id,
        f"💵 <b>Balans va statistika</b>\n\n"
        f"💰 Asosiy: <b>{format_money(u['balance'])}</b>\n"
        f"🎁 Referal: <b>{format_money(u['referral_balance'])}</b>\n"
        f"🎁 Tekin sovgʻa: <b>{u['free_gifts']} ta</b>\n"
        f"👥 Referallar: <b>{u['referrals_count']}</b>\n"
        f"📋 Buyurtmalar: <b>{stats.get('orders_count', 0)}</b>\n"
        f"✅ Toʻlovlar: <b>{stats.get('payments_count', 0)}</b>",
        reply_markup=kb_user_acc())


# ==================== ➕ TO'LDIRISH (user) ====================
@bot.message_handler(func=lambda m: m.text == "➕ Toʻldirish" and not is_admin(m.from_user.id))
def msg_topup(m):
    start_amt = get_setting("start_amount", 5000)
    msg = bot.send_message(m.chat.id,
        f"➕ <b>Hisob toʻldirish</b>\n\n"
        f"Qancha summa?\n"
        f"Masalan: <code>{start_amt}</code>\n\n"
        f"❌ Bekor — bekor qilish",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_topup)


def proc_topup(m):
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_user_acc())
        return

    try:
        desired = int(str(m.text).replace(" ", "").replace(",", "").replace(".", ""))
        if desired < 1000:
            bot.send_message(m.chat.id, "❌ Minimal 1000 soʻm.", reply_markup=kb_back())
            bot.register_next_step_handler(m, proc_topup)
            return
    except Exception:
        bot.send_message(m.chat.id, "❌ Faqat raqam (masalan 5000):", reply_markup=kb_back())
        bot.register_next_step_handler(m, proc_topup)
        return

    uid = m.from_user.id
    pid, amt, exp_dt = create_payment(uid, desired)
    mins = int(get_setting("payment_time_minutes", 15))
    card = get_setting("card_number", "")
    owner = get_setting("card_owner", "")

    note = ""
    if amt != desired:
        note = f"\n⚠️ {format_money(desired)} band edi. Sizga: <b>{format_money(amt)}</b>\n"

    bot.send_message(m.chat.id,
        f"➕ <b>Toʻlov maʼlumotlari</b>{note}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💳 Summa: <code>{amt}</code> soʻm\n"
        f"🏦 Karta: <code>{card}</code>\n"
        f"👤 {owner}\n"
        f"⏱ Vaqt: {mins} daqiqa\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"📌 <b>Qoidalar:</b>\n"
        f"1. Aynan <b>{amt}</b> soʻm tashlang\n"
        f"2. ⚠️ Boshqa summa — hisobga tushmaydi\n"
        f"3. Toʻlov tushishi bilan balans <b>avtomatik</b> toʻladi\n"
        f"4. Vaqt tugasa — qaytadan\n\n"
        f"🧾 Toʻlov raqami: <code>#{pid}</code>",
        reply_markup=kb_user_acc())


# ==================== 📜 TARIX (user) ====================
@bot.message_handler(func=lambda m: m.text == "📜 Tarix" and not is_admin(m.from_user.id))
def msg_history(m):
    uid = m.from_user.id
    expire_pending()
    rows = get_user_payments(uid, limit=20)

    if not rows:
        bot.send_message(m.chat.id, "📜 Toʻlovlar yoʻq.", reply_markup=kb_user_acc())
        return

    text = "📜 <b>Toʻlov tarixi</b>\n\n"
    for r in rows:
        st = {"paid": "✅", "expired": "⏰", "pending": "⏳"}.get(r["status"], "❓")
        text += (f"{st} #{r['id']} | {r['method']} | {format_money(r['unique_amount'])}\n"
                 f"📅 {r['created_at']}\n\n")
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_user_acc())


# ==================== 📊 KIRIM/CHIQIM ====================
@bot.message_handler(func=lambda m: m.text == "📊 Kirim/Chiqim" and not is_admin(m.from_user.id))
def msg_transactions(m):
    uid = m.from_user.id
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT type, amount, source, description, created_at
                     FROM transactions WHERE user_id = %s
                     ORDER BY id DESC LIMIT 30""", (uid,))
        rows = c.fetchall()

        c.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id = %s AND type='kirim'", (uid,))
        kin = c.fetchone()["s"]
        c.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id = %s AND type='chiqim'", (uid,))
        kout = c.fetchone()["s"]
    finally:
        put_db(conn)

    u = get_user(uid)
    bal = u["balance"] if u else 0

    text = (f"📊 <b>Hisob harakati</b>\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💰 Kirim: <b>{format_money(kin)}</b>\n"
            f"🎁 Chiqim: <b>{format_money(kout)}</b>\n"
            f"💵 Hozir: <b>{format_money(bal)}</b>\n"
            f"━━━━━━━━━━━━━━━━\n\n")

    if not rows:
        text += "Hali harakat yoʻq."
    else:
        for r in rows:
            emoji = "✅" if r["type"] == "kirim" else "❌"
            sign = "+" if r["type"] == "kirim" else "−"
            text += (f"{emoji} {r['created_at'][:16]}\n"
                     f"   {sign}{format_money(r['amount'])} | {r['source']}\n"
                     f"   {r['description'] or ''}\n\n")

    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_user_acc())


# ==================== QISM 11 TUGADI ====================
# Keyingi QISM 12: USER handlerlar — Referal + Sovg'alar
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 12/20 ====================
# USER: Referal + Sovg'alar + Sotib olish + Buyurtmalar
# =====================================================================

# ==================== 👥 REFERAL MENYU ====================
@bot.message_handler(func=lambda m: m.text == "👥 Referal" and not is_admin(m.from_user.id))
def msg_ref(m):
    bot.send_message(m.chat.id, "👥 <b>Referal tizimi</b>", reply_markup=kb_user_ref())


# ==================== 🔗 HAVOLAM ====================
@bot.message_handler(func=lambda m: m.text == "🔗 Havolam" and not is_admin(m.from_user.id))
def msg_ref_link(m):
    uid = m.from_user.id
    link = get_referral_link(uid)
    s = get_referral_stats(uid)

    rules = []
    n = 1
    if s["ref_bonus_on"]:
        rules.append(f"{n}️⃣ Doʻst kelganda → <b>{format_money(get_setting('referral_bonus', 500))}</b>")
        n += 1
    if s["ref_percent_on"]:
        rules.append(f"{n}️⃣ Doʻst sovgʻa olsa → <b>{get_setting('referral_percent', 10)}%</b>")
        n += 1
    if s["ref_invites_on"]:
        rules.append(f"{n}️⃣ <b>Har bir sovgʻa</b> — oʻz referal soni bilan tekin olinadi")
        n += 1
    if s["ref_orders_on"]:
        rules.append(f"{n}️⃣ <b>{s['needed_orders']}</b> ta buyurtma → <b>1 tekin</b>")
        n += 1
    if not rules:
        rules.append("Hozircha bonuslar oʻchirilgan.")

    text = (f"🔗 <b>Sizning referal havolangiz</b>\n\n"
            f"<code>{link}</code>\n\n"
            f"📋 <b>Qanday ishlaydi?</b>\n" + "\n".join(rules) +
            f"\n\n📊 <b>Sizning natijangiz</b>\n"
            f"👥 Taklif qilganlar: <b>{s['referrals_count']}</b> ta\n"
            f"🎁 Tekin sovgʻa huquqi: <b>{s['free_gifts']}</b> ta\n"
            f"💸 Referal balansi: <b>{format_money(s['referral_balance'])}</b>\n\n"
            f"💡 <b>Tekin sovgʻa uchun:</b>\n"
            f"🎁 Tekin sovgʻa boʻlimidan kerakli sovgʻani tanlang.\n"
            f"Har bir sovgʻa uchun referal soni har xil!")

    bot.send_message(m.chat.id, text, reply_markup=kb_user_ref())


# ==================== 👤 DO'STLARIM ====================
@bot.message_handler(func=lambda m: m.text == "👤 Doʻstlarim" and not is_admin(m.from_user.id))
def msg_ref_friends(m):
    uid = m.from_user.id
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT full_name, username, joined_date FROM users
                     WHERE referred_by = %s ORDER BY joined_date DESC LIMIT 20""", (uid,))
        rows = c.fetchall()
    finally:
        put_db(conn)

    if not rows:
        bot.send_message(m.chat.id, "👤 Hali taklif qilganlar yoʻq.", reply_markup=kb_user_ref())
        return

    text = "👤 <b>Taklif qilganlarim</b>\n\n"
    for r in rows:
        text += f"• {r['full_name']} (@{r['username'] or 'yoq'}) — {r['joined_date']}\n"
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_user_ref())


# ==================== 💸 REF BALANS ====================
@bot.message_handler(func=lambda m: m.text == "💸 Ref balans" and not is_admin(m.from_user.id))
def msg_ref_balance(m):
    u = get_user(m.from_user.id)
    bot.send_message(m.chat.id,
        f"💸 <b>Referal balans</b>\n\n"
        f"💰 {format_money(u['referral_balance'] if u else 0)}\n\n"
        f"Bu balansni asosiy balansga oʻtkazishingiz mumkin.",
        reply_markup=kb_user_ref())


# ==================== 💸 ASOSIYGA O'TKAZISH ====================
@bot.message_handler(func=lambda m: m.text == "💸 Asosiyga oʻtkazish" and not is_admin(m.from_user.id))
def msg_ref_to_main(m):
    uid = m.from_user.id
    amt = move_referral_to_main(uid)
    if amt <= 0:
        bot.send_message(m.chat.id, "❌ Referal balans boʻsh.", reply_markup=kb_user_ref())
        return
    bot.send_message(m.chat.id,
        f"✅ <b>Oʻtkazildi!</b>\n\n"
        f"💰 {format_money(amt)} asosiy balansga oʻtdi.",
        reply_markup=kb_user_ref())


# ==================== 📊 STATISTIKA ====================
@bot.message_handler(func=lambda m: m.text == "📊 Statistika" and not is_admin(m.from_user.id))
def msg_ref_stats(m):
    s = get_referral_stats(m.from_user.id)
    text = (f"📊 <b>Referal statistikasi</b>\n\n"
            f"👥 Taklif qilganlar: <b>{s['referrals_count']}</b>\n"
            f"🎁 Tekin huquq: <b>{s['free_gifts']}</b>\n"
            f"💸 Ref balans: <b>{format_money(s['referral_balance'])}</b>\n")
    if s["ref_orders_on"]:
        text += f"🛒 Progress: <b>{s['order_progress']}/{s['needed_orders']}</b>\n"
    bot.send_message(m.chat.id, text, reply_markup=kb_user_ref())


# ==================== 🎁 SOVG'ALAR KATALOG ====================
@bot.message_handler(func=lambda m: m.text == "🎁 Sovgʻalar" and not is_admin(m.from_user.id))
def msg_gifts(m):
    uid = m.from_user.id
    gs = get_active_gifts()
    if not gs:
        bot.send_message(m.chat.id, "❌ <b>Hozircha sovgʻalar yoʻq.</b>",
                         reply_markup=kb_user_main(uid))
        return

    mk, total, _ = build_gifts_kb(0)
    bot.send_message(m.chat.id,
        f"🎁 <b>Sovgʻalar katalogi</b>\n"
        f"Jami: <b>{total}</b> ta\n\n"
        f"Tanlang:",
        reply_markup=mk)


# ==================== SAHIFALASH ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("gpage_"))
def cb_gifts_page(call):
    page = int(call.data.replace("gpage_", ""))
    mk, total, page = build_gifts_kb(page)
    try:
        bot.edit_message_text(
            f"🎁 <b>Sovgʻalar katalogi</b>\nJami: <b>{total}</b>\nSahifa: {page+1}",
            call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        pass
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data == "close_gifts")
def cb_close_gifts(call):
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data == "back_gifts")
def cb_back_gifts(call):
    mk, total, _ = build_gifts_kb(0)
    try:
        bot.edit_message_text(
            f"🎁 <b>Sovgʻalar katalogi</b>\nJami: <b>{total}</b>\n\nTanlang:",
            call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        pass
    bot.answer_callback_query(call.id)


# ==================== SOVG'A TANLASH ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("buy_g_"))
def cb_buy_gift(call):
    gid = int(call.data.replace("buy_g_", ""))
    uid = call.from_user.id

    u = get_user(uid)
    if not u or u["is_blocked"] == 1:
        bot.answer_callback_query(call.id, "🚫 Bloklangansiz.", show_alert=True)
        return

    g = get_gift(gid)
    if not g or g["is_active"] != 1 or g["stock"] <= 0:
        bot.answer_callback_query(call.id, "❌ Sovgʻa topilmadi yoki tugagan.", show_alert=True)
        return

    price = g["price"]
    bal = u["balance"] or 0
    emoji = g["emoji"] or "🎁"

    # Balans yetarli emas
    if bal < price:
        try:
            bot.edit_message_text(
                f"{emoji} <b>{g['name']}</b>\n\n"
                f"💵 Narxi: <b>{format_money(price)}</b>\n"
                f"💰 Sizning balans: <b>{format_money(bal)}</b>\n\n"
                f"❌ <b>Hisobingizda yetarli mablagʻ yoʻq.</b>\n"
                f"Kerak: <b>{format_money(price - bal)}</b> yana toʻldiring.",
                call.message.chat.id, call.message.message_id,
                reply_markup=kb_no_balance())
        except Exception:
            pass
        bot.answer_callback_query(call.id, "Hisobingizda yetarli mablagʻ yoʻq.", show_alert=True)
        return

    # Tasdiqlash ekrani
    desc = f"\n\n{g['description']}" if g.get("description") else ""
    try:
        bot.edit_message_text(
            f"{emoji} <b>{g['name']}</b>{desc}\n\n"
            f"💵 Narxi: <b>{format_money(price)}</b>\n"
            f"💰 Balans: <b>{format_money(bal)}</b>\n\n"
            f"Shu sovgʻani sotib olasizmi?\n\n"
            f"⚠️ <b>Diqqat:</b>\n"
            f"• Toʻlovdan soʻng buyurtma avtomatik yuboriladi\n"
            f"• Muammo boʻlsa — pul balansga qaytariladi",
            call.message.chat.id, call.message.message_id,
            reply_markup=kb_confirm_gift(gid))
    except Exception:
        pass
    bot.answer_callback_query(call.id)


# ==================== TO'LDIRISHGA O'TISH ====================
@bot.callback_query_handler(func=lambda c: c.data == "goto_topup")
def cb_goto_topup(call):
    bot.answer_callback_query(call.id)
    msg_topup(call.message)


# ==================== SOTIB OLISHNI TASDIQLASH ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("ok_g_"))
def cb_confirm_buy(call):
    gid = int(call.data.replace("ok_g_", ""))
    uid = call.from_user.id
    u = get_user(uid)

    if not u or u["is_blocked"] == 1:
        bot.answer_callback_query(call.id, "🚫 Bloklangansiz.", show_alert=True)
        return

    conn = get_db()
    c = conn.cursor()
    try:
        # Sovg'ani lock bilan olish
        c.execute("SELECT * FROM gifts WHERE id = %s AND is_active = 1 FOR UPDATE", (gid,))
        g = c.fetchone()

        if not g or g["stock"] <= 0:
            conn.rollback()
            bot.answer_callback_query(call.id, "❌ Sovgʻa tugagan.", show_alert=True)
            return

        price = g["price"]

        # Balansni lock bilan olish
        c.execute("SELECT balance FROM users WHERE user_id = %s FOR UPDATE", (uid,))
        uu = c.fetchone()
        if not uu or uu["balance"] < price:
            conn.rollback()
            bot.answer_callback_query(call.id, "❌ Balans yetarli emas.", show_alert=True)
            return

        # Balansdan yechish + buyurtma yaratish
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE users SET balance = balance - %s WHERE user_id = %s", (price, uid))
        c.execute("UPDATE gifts SET stock = stock - 1 WHERE id = %s", (gid,))
        c.execute("""INSERT INTO gift_orders
                     (user_id, gift_id, gift_name, price, is_free, status, created_at)
                     VALUES (%s, %s, %s, %s, 0, 'pending', %s)
                     RETURNING id""",
                  (uid, gid, g["name"], price, now))
        oid = c.fetchone()["id"]
        conn.commit()
    except Exception as e:
        try: conn.rollback()
        except: pass
        print("Buy error:", e)
        bot.answer_callback_query(call.id, "❌ Xato.", show_alert=True)
        return
    finally:
        put_db(conn)

    # Tranzaksiya yozish
    try:
        write_transaction(uid, "chiqim", price, "gift", f"Xarid: {g['name']}")
    except Exception:
        pass

    # Referal bonus (do'stga foiz)
    try:
        referral_on_purchase(uid, price)
    except Exception as e:
        print("Referral error:", e)

    # ========== AVTO GIFT YUBORISH ==========
    tg_gift_id = (g["tg_gift_id"] or "").strip()
    auto_sent = False
    err = ""

    if tg_gift_id:
        ok, info = send_gift_auto(uid, tg_gift_id, f"🎁 {g['name']} — SovgaMarket")
        if ok:
            auto_sent = True
            conn2 = get_db()
            c2 = conn2.cursor()
            try:
                c2.execute("""UPDATE gift_orders
                              SET status = 'delivered', delivered_at = %s, tg_gift_sent = 1
                              WHERE id = %s""",
                           (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), oid))
                conn2.commit()
            finally:
                put_db(conn2)
        else:
            err = str(info)[:200]
            conn2 = get_db()
            c2 = conn2.cursor()
            try:
                c2.execute("UPDATE gift_orders SET tg_error = %s WHERE id = %s", (err, oid))
                conn2.commit()
            finally:
                put_db(conn2)

    # Foydalanuvchiga xabar
    emoji = g["emoji"] or "🎁"
    if auto_sent:
        try:
            bot.edit_message_text(
                f"🎉 <b>Sovgʻa avto yuborildi!</b>\n\n"
                f"{emoji} <b>{g['name']}</b>\n"
                f"💵 {format_money(price)}\n"
                f"🧾 Buyurtma №: <code>{oid}</code>\n\n"
                f"Telegram profilingizni tekshiring!",
                call.message.chat.id, call.message.message_id)
        except Exception:
            pass
    else:
        try:
            bot.edit_message_text(
                f"✅ <b>Buyurtma qabul qilindi!</b>\n\n"
                f"{emoji} <b>{g['name']}</b>\n"
                f"💵 {format_money(price)}\n"
                f"🧾 Buyurtma №: <code>{oid}</code>\n\n"
                f"⏳ Admin tez orada sovgʻani yuboradi.",
                call.message.chat.id, call.message.message_id)
        except Exception:
            pass

    bot.send_message(uid, "Menyu", reply_markup=kb_user_main(uid))

    # Isbotlar kanaliga
    try:
        send_proof(uid, g["name"], price, oid, is_free=False, gift_emoji=emoji)
    except Exception:
        pass

    # Adminlarga xabar
    st = "✅ AVTO YUBORILDI" if auto_sent else f"⚠️ {err or 'tg_gift_id yoʻq'}"
    un = f"@{call.from_user.username}" if call.from_user.username else "—"
    for a in ADMIN_IDS:
        try:
            mk = types.InlineKeyboardMarkup()
            if not auto_sent:
                mk.add(types.InlineKeyboardButton("🔄 Qayta yuborish", callback_data=f"retry_{oid}"))
                mk.add(types.InlineKeyboardButton("✅ Qoʻlda yuborildi", callback_data=f"dlv_{oid}"))
            bot.send_message(a,
                f"🧾 <b>Yangi buyurtma</b>\n\n"
                f"{emoji} <b>{g['name']}</b>\n"
                f"💵 {format_money(price)}\n"
                f"👤 {call.from_user.full_name} ({un})\n"
                f"🆔 <code>{uid}</code>\n"
                f"🧾 № <code>{oid}</code>\n\n"
                f"{st}",
                reply_markup=mk if not auto_sent else None)
        except Exception:
            pass


# ==================== YETKAZILDI BELGILASH ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("dlv_"))
def cb_deliver(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Faqat admin", show_alert=True)
        return
    oid = int(call.data.replace("dlv_", ""))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM gift_orders WHERE id = %s", (oid,))
        o = c.fetchone()
        if not o or o["status"] == "delivered":
            bot.answer_callback_query(call.id, "Allaqachon belgilangan.", show_alert=True)
            return
        c.execute("UPDATE gift_orders SET status = 'delivered', delivered_at = %s WHERE id = %s",
                  (now, oid))
        conn.commit()
    finally:
        put_db(conn)

    try:
        bot.send_message(o["user_id"],
            f"🎉 <b>Sovgʻangiz yuborildi!</b>\n\n"
            f"🎁 {o['gift_name']}\n"
            f"🧾 № <code>{oid}</code>")
    except Exception:
        pass

    bot.answer_callback_query(call.id, "✅ Yetkazildi", show_alert=True)
    try:
        bot.edit_message_text(call.message.text + "\n\n✅ <b>YETKAZILDI</b>",
                              call.message.chat.id, call.message.message_id)
    except Exception:
        pass


# ==================== QAYTA YUBORISH (RETRY) ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("retry_"))
def cb_retry_gift(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Faqat admin", show_alert=True)
        return
    oid = int(call.data.replace("retry_", ""))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM gift_orders WHERE id = %s", (oid,))
        o = c.fetchone()
        if not o:
            bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
            return
        if o["status"] == "delivered":
            bot.answer_callback_query(call.id, "Allaqachon yuborilgan", show_alert=True)
            return

        # Sovg'ani olish
        c.execute("SELECT * FROM gifts WHERE id = %s", (o["gift_id"],))
        g = c.fetchone()
        if not g or not g.get("tg_gift_id"):
            bot.answer_callback_query(call.id, "TG ID yoʻq", show_alert=True)
            return

        tg_id = g["tg_gift_id"]
    finally:
        put_db(conn)

    # Qayta urinish
    ok, info = send_gift_auto(o["user_id"], tg_id, f"🎁 {o['gift_name']}")

    conn2 = get_db()
    c2 = conn2.cursor()
    try:
        if ok:
            c2.execute("""UPDATE gift_orders SET status='delivered', delivered_at=%s,
                          tg_gift_sent=1, tg_error='' WHERE id=%s""",
                       (now, oid))
            conn2.commit()
            try:
                bot.send_message(o["user_id"], f"🎉 Sovgʻangiz yuborildi: {o['gift_name']}")
            except: pass
            bot.answer_callback_query(call.id, "✅ Yuborildi!", show_alert=True)
            try:
                bot.edit_message_text(call.message.text + "\n\n✅ QAYTA YUBORILDI",
                                      call.message.chat.id, call.message.message_id)
            except: pass
        else:
            c2.execute("UPDATE gift_orders SET tg_error=%s WHERE id=%s",
                       (str(info)[:200], oid))
            conn2.commit()
            bot.answer_callback_query(call.id, f"❌ {str(info)[:100]}", show_alert=True)
    finally:
        put_db(conn2)


# ==================== 📋 BUYURTMALARIM ====================
@bot.message_handler(func=lambda m: m.text == "📋 Buyurtmalarim" and not is_admin(m.from_user.id))
def msg_my_orders(m):
    uid = m.from_user.id
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT gift_name, price, is_free, status, created_at FROM gift_orders
                     WHERE user_id = %s ORDER BY id DESC LIMIT 20""", (uid,))
        rows = c.fetchall()

        c.execute("SELECT COALESCE(SUM(price),0) as s FROM gift_orders WHERE user_id = %s", (uid,))
        total_spent = c.fetchone()["s"]
        c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE user_id = %s", (uid,))
        total_count = c.fetchone()["n"]
    finally:
        put_db(conn)

    if not rows:
        bot.send_message(m.chat.id, "📋 Buyurtmalar yoʻq.", reply_markup=kb_user_main(uid))
        return

    text = "📋 <b>Buyurtmalarim</b>\n\n"
    for r in rows:
        tip = "🎁 Tekin" if r["is_free"] else f"💵 {format_money(r['price'])}"
        st = "✅" if r["status"] == "delivered" else "⏳"
        text += f"🎁 <b>{r['gift_name']}</b>\n{tip} | {st}\n📅 {r['created_at']}\n\n"

    text += (f"━━━━━━━━━━━━━━━━\n"
             f"📊 Jami: <b>{total_count}</b> ta buyurtma\n"
             f"💰 Sarflangan: <b>{format_money(total_spent)}</b>")
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_user_main(uid))


# ==================== QISM 12 TUGADI ====================
# Keyingi QISM 13: USER — Sharhlar, Admin yozish, Isbotlar (kanal link)
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 13/20 ====================
# USER: Sharhlar, Admin, Isbotlar + TEKIN SOVG'A (har gift uchun)
# =====================================================================

# ==================== 💬 SHARHLAR (USER) ====================
@bot.message_handler(func=lambda m: m.text == "💬 Sharhlar" and not is_admin(m.from_user.id))
def user_reviews(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT id, full_name, username, text FROM reviews
                     WHERE is_approved = 1 ORDER BY id DESC LIMIT 15""")
        rows = c.fetchall()
    finally:
        put_db(conn)

    text = "💬 <b>Sharhlar</b>\n\n"
    if not rows:
        text += "Hali sharhlar yoʻq. Birinchi boʻling!"
    else:
        for r in rows:
            text += f"👤 {r['full_name']} (@{r['username'] or 'yoq'})\n{r['text']}\n\n"

    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton("✍️ Sharh qoldirish", callback_data="add_rev"))
    bot.send_message(m.chat.id, text[:3500], reply_markup=mk)


# ==================== SHARH QO'SHISH (USER) ====================
@bot.callback_query_handler(func=lambda c: c.data == "add_rev")
def cb_add_rev(call):
    msg = bot.send_message(call.from_user.id, "✍️ Sharhingizni yozing:\n\n❌ Bekor — bekor qilish")
    bot.register_next_step_handler(msg, proc_add_rev)
    bot.answer_callback_query(call.id)


def proc_add_rev(m):
    if m.text in ["❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_user_main(m.from_user.id))
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""INSERT INTO reviews
                     (user_id, username, full_name, text, created_at, is_approved)
                     VALUES (%s, %s, %s, %s, %s, 1)""",
                  (m.from_user.id, m.from_user.username or "",
                   m.from_user.full_name or "", m.text[:500], now))
        conn.commit()
    finally:
        put_db(conn)

    bot.send_message(m.chat.id, "✅ <b>Sharh qoʻshildi!</b>",
                     reply_markup=kb_user_main(m.from_user.id))


# ==================== 📞 ADMIN YOZISH (USER) ====================
@bot.message_handler(func=lambda m: m.text == "📞 Admin" and not is_admin(m.from_user.id))
def user_to_admin(m):
    msg = bot.send_message(m.chat.id,
        "📞 <b>Adminga xabar</b>\n\nXabaringizni yozing:\n\n❌ Bekor — bekor qilish",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_user_admin)


def proc_user_admin(m):
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_user_main(m.from_user.id))
        return

    un = f"@{m.from_user.username}" if m.from_user.username else "—"
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton("↩️ Javob berish", callback_data=f"reply_u_{m.from_user.id}"))

    for a in ADMIN_IDS:
        try:
            bot.send_message(a,
                f"📞 <b>Yangi xabar</b>\n\n"
                f"👤 {m.from_user.full_name} ({un})\n"
                f"🆔 <code>{m.from_user.id}</code>\n\n"
                f"{m.text}",
                reply_markup=mk)
        except Exception:
            pass

    bot.send_message(m.chat.id,
        "✅ <b>Yuborildi!</b>\n\nTez orada javob beramiz.",
        reply_markup=kb_user_main(m.from_user.id))


# ==================== ADMIN JAVOB BERISH ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("reply_u_"))
def cb_reply_user(call):
    if not is_admin(call.from_user.id):
        return
    tid = int(call.data.replace("reply_u_", ""))
    msg = bot.send_message(call.from_user.id,
        f"💬 <code>{tid}</code> ga javob yozing:\n\n❌ Bekor — bekor qilish")
    bot.register_next_step_handler(msg, lambda m: proc_admin_reply(m, tid))
    bot.answer_callback_query(call.id)


def proc_admin_reply(m, target_id):
    """Admin reply — QISM 14 da ham ishlatiladi"""
    if not is_admin(m.from_user.id):
        return
    if m.text in ["❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_main())
        return
    try:
        bot.send_message(target_id, f"📞 <b>Admin javobi:</b>\n\n{m.text}")
        bot.send_message(m.chat.id, "✅ Yuborildi.", reply_markup=kb_admin_main())
    except Exception as e:
        bot.send_message(m.chat.id, f"❌ Xato: {e}", reply_markup=kb_admin_main())


# ==================== 📢 ISBOTLAR (USER — KANAL LINK) ====================
@bot.message_handler(func=lambda m: m.text == "📢 Isbotlar" and not is_admin(m.from_user.id))
def user_proof(m):
    """
    User «📢 Isbotlar» bosadi → DARHOL kanalga o'tadi.
    """
    un = get_setting("proof_channel_username", "")
    if not un:
        bot.send_message(m.chat.id,
            "📢 <b>Isbotlar kanali</b>\n\nHozircha kanal sozlanmagan.",
            reply_markup=kb_user_main(m.from_user.id))
        return

    # URL yasash
    if un.startswith("http"):
        link = un
    elif un.startswith("@"):
        link = f"https://t.me/{un.lstrip('@')}"
    else:
        link = f"https://t.me/{un}"

    # Inline URL tugma
    mk = types.InlineKeyboardMarkup()
    mk.add(types.InlineKeyboardButton("📢 Kanalga oʻtish", url=link))

    bot.send_message(m.chat.id,
        f"📢 <b>Isbotlar kanali</b>\n\n"
        f"Bu yerda barcha xaridlar isbotlari\n"
        f"koʻrsatiladi. Ishonch uchun koʻring!\n\n"
        f"👉 {un}",
        reply_markup=mk)


# ==================== 🎁 TEKIN SOVG'A (HAR GIFT UCHUN) ====================
@bot.message_handler(func=lambda m: m.text == "🎁 Tekin sovgʻa" and not is_admin(m.from_user.id))
def msg_free_gift(m):
    """
    Har bir sovg'a uchun alohida referal soni.
    User 5 ta do'st chaqirsa → Ayiqcha (5 ref)
    User 7 ta do'st chaqirsa → Yulduzcha (7 ref)
    """
    uid = m.from_user.id
    u = get_user(uid)
    if not u:
        return

    refs = u["referrals_count"]

    # Barcha faol sovg'alarni olish
    gifts = get_active_gifts()
    if not gifts:
        bot.send_message(m.chat.id, "❌ Hozircha sovgʻalar yoʻq.",
                         reply_markup=kb_user_main(uid))
        return

    text = f"🎁 <b>Tekin sovgʻalar</b>\n\n"
    text += f"👥 Sizning referallaringiz: <b>{refs}</b> ta\n\n"
    text += "Sovgʻani tanlang:\n\n"

    mk = types.InlineKeyboardMarkup(row_width=1)
    has_any = False

    for g in gifts:
        needed = g.get("referral_needed", 20) or 20
        emoji = g["emoji"] or "🎁"

        if refs >= needed:
            # Olish mumkin
            status = "✅"
            text += f"{status} {emoji} <b>{g['name']}</b> — {needed} ta referal\n"
            mk.add(types.InlineKeyboardButton(
                f"{status} {emoji} {g['name']} olish",
                callback_data=f"take_free_{g['id']}"))
            has_any = True
        else:
            # Yana kerak
            left = needed - refs
            status = "🔒"
            text += f"{status} {emoji} <b>{g['name']}</b> — yana <b>{left}</b> ta\n"

    if not has_any:
        text += "\n\n💡 <b>Do'st chaqiring!</b>\n"
        text += f"👉 «👥 Referal» → «🔗 Havolam»\n"
        text += "Har bir do'st = +1 referal"

    bot.send_message(m.chat.id, text[:4000], reply_markup=mk)


# ==================== TEKIN SOVG'A OLISH ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("take_free_"))
def cb_take_free(call):
    gid = int(call.data.replace("take_free_", ""))
    uid = call.from_user.id
    u = get_user(uid)
    if not u:
        return

    g = get_gift(gid)
    if not g:
        bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
        return

    needed = g.get("referral_needed", 20) or 20
    refs = u["referrals_count"]

    # Yetarli emasmi?
    if refs < needed:
        bot.answer_callback_query(call.id,
            f"❌ Yana {needed - refs} ta referal kerak!", show_alert=True)
        return

    # Stock tekshirish
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM gifts WHERE id = %s AND is_active = 1 FOR UPDATE", (gid,))
        g_check = c.fetchone()
        if not g_check or g_check["stock"] <= 0:
            bot.answer_callback_query(call.id, "❌ Sovgʻa hozircha omborda yoʻq.", show_alert=True)
            return

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Referal sonini kamaytirish
        c.execute("UPDATE users SET referrals_count = referrals_count - %s WHERE user_id = %s",
                  (needed, uid))
        # Stock kamaytirish
        c.execute("UPDATE gifts SET stock = stock - 1 WHERE id = %s", (gid,))
        # Buyurtma yaratish (is_free = 1)
        c.execute("""INSERT INTO gift_orders
                     (user_id, gift_id, gift_name, price, is_free, status, created_at)
                     VALUES (%s, %s, %s, 0, 1, 'pending', %s)
                     RETURNING id""",
                  (uid, gid, g["name"], now))
        oid = c.fetchone()["id"]
        conn.commit()
    except Exception as e:
        try: conn.rollback()
        except: pass
        print("Take free error:", e)
        bot.answer_callback_query(call.id, "❌ Xato.", show_alert=True)
        return
    finally:
        put_db(conn)

    # Avto gift yuborish
    tg_gift_id = (g["tg_gift_id"] or "").strip()
    auto_sent = False
    err = ""

    if tg_gift_id:
        ok, info = send_gift_auto(uid, tg_gift_id, f"🎁 {g['name']} (tekin)")
        if ok:
            auto_sent = True
            conn2 = get_db()
            c2 = conn2.cursor()
            try:
                c2.execute("""UPDATE gift_orders
                              SET status = 'delivered', delivered_at = %s, tg_gift_sent = 1
                              WHERE id = %s""",
                           (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), oid))
                conn2.commit()
            finally:
                put_db(conn2)
        else:
            err = str(info)[:200]
            conn2 = get_db()
            c2 = conn2.cursor()
            try:
                c2.execute("UPDATE gift_orders SET tg_error = %s WHERE id = %s", (err, oid))
                conn2.commit()
            finally:
                put_db(conn2)

    # Userga xabar
    emoji = g["emoji"] or "🎁"
    if auto_sent:
        try:
            bot.edit_message_text(
                f"🎉 <b>Tekin sovgʻa avto yuborildi!</b>\n\n"
                f"{emoji} <b>{g['name']}</b>\n"
                f"🧾 № <code>{oid}</code>\n\n"
                f"Telegram profilingizni tekshiring!",
                call.message.chat.id, call.message.message_id)
        except Exception:
            pass
    else:
        try:
            bot.edit_message_text(
                f"🎉 <b>Tekin sovgʻa buyurtmasi qabul qilindi!</b>\n\n"
                f"{emoji} <b>{g['name']}</b>\n"
                f"🧾 № <code>{oid}</code>\n\n"
                f"⏳ Admin tez orada yuboradi.",
                call.message.chat.id, call.message.message_id)
        except Exception:
            pass

    bot.send_message(uid, "Menyu", reply_markup=kb_user_main(uid))

    # Isbotlar kanaliga
    try:
        send_proof(uid, g["name"], 0, oid, is_free=True, gift_emoji=emoji)
    except Exception:
        pass

    # Adminlarga xabar
    un = f"@{call.from_user.username}" if call.from_user.username else "—"
    for a in ADMIN_IDS:
        try:
            mk = types.InlineKeyboardMarkup()
            if not auto_sent:
                mk.add(types.InlineKeyboardButton("🔄 Qayta yuborish", callback_data=f"retry_{oid}"))
                mk.add(types.InlineKeyboardButton("✅ Qoʻlda yuborildi", callback_data=f"dlv_{oid}"))
            bot.send_message(a,
                f"🧾 <b>Yangi TEKIN buyurtma</b>\n\n"
                f"{emoji} <b>{g['name']}</b>\n"
                f"👤 {call.from_user.full_name} ({un})\n"
                f"🆔 <code>{uid}</code>\n"
                f"🧾 № <code>{oid}</code>\n"
                f"👥 {needed} ref ishlatildi\n\n"
                f"{'✅ AVTO' if auto_sent else '⚠️ Qoʻlda yuboring'}",
                reply_markup=mk if not auto_sent else None)
        except Exception:
            pass


# ==================== QISM 13 TUGADI ====================
# Keyingi QISM 14: ADMIN — Statistika, To'lovlar, Sovg'alar CRUD
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 14/20 ====================
# ADMIN: Statistika, to'lovlar, sovg'alar CRUD (6 qator + ref soni)
# =====================================================================

# ==================== 🔐 ADMIN PANEL ====================
@bot.message_handler(func=lambda m: m.text == "🔐 Admin Panel" and is_admin(m.from_user.id))
def adm_panel(m):
    bot.send_message(m.chat.id, "🔐 <b>Admin Panel</b>", reply_markup=kb_admin_main())


# ==================== 📊 STATISTIKA ====================
@bot.message_handler(func=lambda m: m.text == "📊 Statistika" and is_admin(m.from_user.id))
def adm_stats(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) as n FROM users"); tot_u = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM users WHERE is_blocked = 1"); blk_u = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM users WHERE DATE(joined_date) = CURRENT_DATE"); today_u = c.fetchone()["n"]
        c.execute("SELECT COALESCE(SUM(balance),0) as s FROM users"); bal_sum = c.fetchone()["s"]

        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='paid'"); p = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='pending'"); pd = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='expired'"); ex = c.fetchone()
        c.execute("""SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments
                     WHERE status='paid' AND DATE(paid_at) = CURRENT_DATE"""); tp = c.fetchone()

        c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE status='delivered'"); od = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE status='pending'"); op = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE is_free=1"); of_n = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM gifts WHERE is_active=1"); ga = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM referral_history"); refs = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM channels"); chn = c.fetchone()["n"]
    finally:
        put_db(conn)

    stars = get_bot_stars_balance()

    text = (
        f"📊 <b>Toʻliq statistika</b>\n\n"
        f"👥 <b>Foydalanuvchilar</b>\n"
        f"• Jami: <b>{tot_u}</b>\n"
        f"• Bloklangan: <b>{blk_u}</b>\n"
        f"• Bugun qoʻshilgan: <b>{today_u}</b>\n"
        f"• Balans jami: <b>{format_money(bal_sum)}</b>\n\n"
        f"💰 <b>Toʻlovlar</b>\n"
        f"• ✅ Tasdiqlangan: <b>{p['n']}</b> — {format_money(p['s'])}\n"
        f"• ⏳ Kutilmoqda: <b>{pd['n']}</b> — {format_money(pd['s'])}\n"
        f"• ⏰ Muddati oʻtgan: <b>{ex['n']}</b> — {format_money(ex['s'])}\n"
        f"• 📅 Bugun: <b>{tp['n']}</b> — {format_money(tp['s'])}\n\n"
        f"🎁 <b>Buyurtmalar</b>\n"
        f"• ✅ Yetkazilgan: <b>{od}</b>\n"
        f"• ⏳ Kutilmoqda: <b>{op}</b>\n"
        f"• 🎁 Tekin: <b>{of_n}</b>\n"
        f"• Faol sovgʻalar: <b>{ga}</b>\n\n"
        f"👥 Referallar: <b>{refs}</b>\n"
        f"📢 Kanallar: <b>{chn}</b>\n"
        f"⭐ Bot Stars: <b>{stars}</b>\n"
        f"🔄 Method: <b>{get_setting('gift_method', GIFT_METHOD)}</b>"
    )
    bot.send_message(m.chat.id, text, reply_markup=kb_admin_main())


# ==================== 💰 TO'LOVLAR ====================
@bot.message_handler(func=lambda m: m.text == "💰 Toʻlovlar" and is_admin(m.from_user.id))
def adm_pays(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='paid'"); p = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='pending'"); pd = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='expired'"); ex = c.fetchone()
        c.execute("""SELECT id, user_id, unique_amount, status, method, created_at
                     FROM payments ORDER BY id DESC LIMIT 15""")
        rows = c.fetchall()
    finally:
        put_db(conn)

    text = (f"💰 <b>Toʻlovlar</b>\n\n"
            f"✅ Tasdiqlangan: <b>{p['n']}</b> — {format_money(p['s'])}\n"
            f"⏳ Kutilmoqda: <b>{pd['n']}</b> — {format_money(pd['s'])}\n"
            f"⏰ Muddati oʻtgan: <b>{ex['n']}</b> — {format_money(ex['s'])}\n\n"
            f"━━━━━━━━━━━━━━━━\n")

    if not rows:
        text += "Hali toʻlov yoʻq."
    else:
        for r in rows:
            st = {"paid": "✅", "pending": "⏳", "expired": "⏰"}.get(r["status"], "❓")
            text += (f"{st} #{r['id']} | <code>{r['user_id']}</code>\n"
                     f"{r['method']} | {format_money(r['unique_amount'])}\n"
                     f"📅 {r['created_at']}\n")

    text += "\n\n<b>Buyruqlar:</b>\n/confirm ID\n/reject ID"
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_admin_main())


# ==================== /confirm ====================
@bot.message_handler(commands=['confirm'])
def cmd_confirm(m):
    if not is_admin(m.from_user.id):
        return
    parts = m.text.split()
    if len(parts) != 2:
        bot.send_message(m.chat.id, "Format: <code>/confirm ID</code>")
        return
    try:
        pid = int(parts[1])
        ok, info = confirm_payment(pid, m.from_user.id)
        if ok:
            bot.send_message(m.chat.id,
                f"✅ <b>Toʻlov tasdiqlandi!</b>\n\n"
                f"🆔 #{pid}\n👤 User: <code>{info}</code>")
        else:
            bot.send_message(m.chat.id, f"❌ {info}")
    except Exception as e:
        bot.send_message(m.chat.id, f"❌ {e}")


# ==================== /reject ====================
@bot.message_handler(commands=['reject'])
def cmd_reject(m):
    if not is_admin(m.from_user.id):
        return
    parts = m.text.split()
    if len(parts) != 2:
        bot.send_message(m.chat.id, "Format: <code>/reject ID</code>")
        return
    try:
        pid = int(parts[1])
        if reject_payment(pid, m.from_user.id):
            bot.send_message(m.chat.id, f"❌ <b>Toʻlov #{pid} rad etildi.</b>")
        else:
            bot.send_message(m.chat.id, "Topilmadi yoki allaqachon toʻlangan.")
    except Exception as e:
        bot.send_message(m.chat.id, f"❌ {e}")


# ==================== /stats ====================
@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if is_admin(m.from_user.id):
        adm_stats(m)


# ==================== /stars ====================
@bot.message_handler(commands=['stars'])
def cmd_stars(m):
    if not is_admin(m.from_user.id):
        return
    bal = get_bot_stars_balance()
    bot.send_message(m.chat.id, f"⭐ Bot Stars: <b>{bal}</b>")


# ==================== 🎁 SOVG'ALAR MENYU ====================
@bot.message_handler(func=lambda m: m.text == "🎁 Sovgʻalar" and is_admin(m.from_user.id))
def adm_gifts_menu(m):
    rows = get_all_gifts()
    text = "🎁 <b>Sovgʻalar boshqaruvi</b>\n\n"
    if not rows:
        text += "📋 <b>Hozircha sovgʻalar yoʻq</b>\n\n"
        text += "👉 «➕ Sovgʻa qoʻshish» bilan boshlang"
    else:
        text += f"📊 Jami: <b>{len(rows)}</b> ta\n\n"
        text += "━━━━━━━━━━━━━━━━\n"
        for r in rows[:10]:
            st = "✅" if r["is_active"] else "🚫"
            tg = r["tg_gift_id"] or "—"
            ref = r.get("referral_needed", 20) or 20
            text += (f"{st} #{r['id']} {r['emoji'] or '🎁'} <b>{r['name']}</b>\n"
                     f"💵 {format_money(r['price'])} | ⭐ {r['stars_price']} | 📦 {r['stock']}\n"
                     f"🔗 TG: <code>{tg}</code>\n"
                     f"🎯 Tekin: <b>{ref}</b> referal\n\n")
        if len(rows) > 10:
            text += f"\n📋 Yana {len(rows) - 10} ta — «📋 Roʻyxat» da koʻring"
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_admin_gifts())


# ==================== ➕ SOVG'A QO'SHISH (6 QATOR) ====================
@bot.message_handler(func=lambda m: m.text == "➕ Sovgʻa qoʻshish" and is_admin(m.from_user.id))
def adm_add_gift(m):
    msg = bot.send_message(m.chat.id,
        "➕ <b>Yangi sovgʻa</b>\n\n"
        "6 qator:\n"
        "<code>Nomi\nEmoji\nNarx (soʻm)\nStars narxi\nReferal soni\nMiqdor (ixtiyoriy)</code>\n\n"
        "Misol:\n"
        "<code>Ayiqcha\n🧸\n3500\n15\n5\n999999999</code>\n\n"
        "❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_add_gift)


def proc_add_gift(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_gifts())
        return

    ls = [l.strip() for l in m.text.split("\n") if l.strip()]
    if len(ls) < 5:
        bot.send_message(m.chat.id, "❌ Kamida 5 qator kerak.", reply_markup=kb_admin_gifts())
        return

    try:
        name = ls[0]
        emoji = ls[1] or "🎁"
        price = float(ls[2].replace(" ", "").replace(",", ""))
        sp = int(ls[3])
        ref_needed = int(ls[4]) if len(ls) > 4 else 20
        # Stock — agar kiritilmagan bo'lsa, cheksiz
        stock = int(ls[5]) if len(ls) > 5 else 999999999
    except Exception as e:
        bot.send_message(m.chat.id, f"❌ Xato: {e}", reply_markup=kb_admin_gifts())
        return

    gid = add_gift(name, emoji, price, sp, stock, "", ref_needed)
    bot.send_message(m.chat.id,
        f"✅ <b>Qoʻshildi!</b>\n\n"
        f"🆔 #{gid}\n"
        f"{emoji} <b>{name}</b>\n"
        f"💵 {format_money(price)} | ⭐ {sp}\n"
        f"🎯 Tekin: <b>{ref_needed}</b> referal\n"
        f"📦 Stock: <b>{stock}</b>\n\n"
        f"💡 Endi TG gift bilan bogʻlang:\n"
        f"⭐ Stars sozlama → 🔗 Gift bogʻlash",
        reply_markup=kb_admin_gifts())


# ==================== 📋 RO'YXAT ====================
@bot.message_handler(func=lambda m: m.text == "📋 Roʻyxat" and is_admin(m.from_user.id))
def adm_list_gifts(m):
    rows = get_all_gifts()
    if not rows:
        bot.send_message(m.chat.id, "📋 Sovgʻalar yoʻq.", reply_markup=kb_admin_gifts())
        return
    text = "📋 <b>Barcha sovgʻalar</b>\n\n"
    for r in rows:
        st = "✅" if r["is_active"] else "🚫"
        tg = r["tg_gift_id"] or "—"
        ref = r.get("referral_needed", 20) or 20
        text += (f"{st} #{r['id']} {r['emoji'] or '🎁'} <b>{r['name']}</b>\n"
                 f"💵 {format_money(r['price'])} | ⭐ {r['stars_price']} | 📦 {r['stock']}\n"
                 f"🔗 TG: <code>{tg}</code>\n"
                 f"🎯 Tekin: <b>{ref}</b> referal\n\n")
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_admin_gifts())


# ==================== 💵 NARX ====================
@bot.message_handler(func=lambda m: m.text == "💵 Narx" and is_admin(m.from_user.id))
def adm_set_price(m):
    msg = bot.send_message(m.chat.id, "Format: <code>ID YANGI_NARX</code>\n/cancel", reply_markup=kb_back())
    bot.register_next_step_handler(msg, lambda x: _adm_gift_field(x, "price"))


# ==================== 📦 MIQDOR ====================
@bot.message_handler(func=lambda m: m.text == "📦 Miqdor" and is_admin(m.from_user.id))
def adm_set_stock(m):
    msg = bot.send_message(m.chat.id, "Format: <code>ID MIQDOR</code>\n/cancel", reply_markup=kb_back())
    bot.register_next_step_handler(msg, lambda x: _adm_gift_field(x, "stock"))


# ==================== ⭐ STARS NARXI ====================
@bot.message_handler(func=lambda m: m.text == "⭐ Stars narxi" and is_admin(m.from_user.id))
def adm_set_sp(m):
    msg = bot.send_message(m.chat.id, "Format: <code>ID STARS</code>\n/cancel", reply_markup=kb_back())
    bot.register_next_step_handler(msg, lambda x: _adm_gift_field(x, "stars_price"))


# ==================== 🎯 TEKIN SONI (REFERAL) ====================
@bot.message_handler(func=lambda m: m.text == "🎯 Tekin soni" and is_admin(m.from_user.id))
def adm_set_ref(m):
    msg = bot.send_message(m.chat.id,
        "🎯 <b>Tekin referal soni</b>\n\n"
        "Format: <code>ID REFERAL_SONI</code>\n"
        "Misol: <code>1 5</code>\n\n"
        "Bunda #1 sovg'a uchun 5 ta referal kerak bo'ladi.\n\n"
        "❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, lambda x: _adm_gift_field(x, "referral_needed"))


def _adm_gift_field(m, field):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_gifts())
        return
    ps = m.text.strip().split()
    if len(ps) != 2:
        bot.send_message(m.chat.id, "❌ Format xato.", reply_markup=kb_admin_gifts())
        return
    try:
        gid = int(ps[0])
        if field == "price":
            v = float(ps[1].replace(",", ""))
        else:
            v = int(ps[1])
        if update_gift(gid, field, v):
            bot.send_message(m.chat.id,
                f"✅ #{gid} {field} → <b>{v}</b>",
                reply_markup=kb_admin_gifts())
        else:
            bot.send_message(m.chat.id, "Topilmadi.", reply_markup=kb_admin_gifts())
    except Exception:
        bot.send_message(m.chat.id, "Faqat son.", reply_markup=kb_admin_gifts())


# ==================== 🎯 TEKIN BELGILASH (eski) ====================
@bot.message_handler(func=lambda m: m.text == "🎯 Tekin belgilash" and is_admin(m.from_user.id))
def adm_set_free(m):
    msg = bot.send_message(m.chat.id, "Gift ID:\n/cancel", reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_set_free)


def proc_set_free(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_gifts())
        return
    try:
        gid = int(m.text.strip())
        g = get_gift(gid)
        if not g:
            bot.send_message(m.chat.id, "Topilmadi.", reply_markup=kb_admin_gifts())
            return
        set_setting("free_gift_id", gid)
        bot.send_message(m.chat.id, f"✅ {g['emoji']} {g['name']}", reply_markup=kb_admin_gifts())
    except Exception:
        bot.send_message(m.chat.id, "Faqat ID.", reply_markup=kb_admin_gifts())


# ==================== ❌ O'CHIRISH ====================
@bot.message_handler(func=lambda m: m.text == "❌ Oʻchirish" and is_admin(m.from_user.id))
def adm_del_gift(m):
    msg = bot.send_message(m.chat.id, "Gift ID:\n/cancel", reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_del)


def proc_del(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_gifts())
        return
    try:
        gid = int(m.text.strip())
        if delete_gift(gid):
            bot.send_message(m.chat.id, f"✅ #{gid} oʻchirildi.", reply_markup=kb_admin_gifts())
        else:
            bot.send_message(m.chat.id, "Topilmadi.", reply_markup=kb_admin_gifts())
    except Exception:
        bot.send_message(m.chat.id, "Faqat ID.", reply_markup=kb_admin_gifts())


# ==================== 🧾 YETKAZILMAGANLAR ====================
@bot.message_handler(func=lambda m: m.text == "🧾 Yetkazilmaganlar" and is_admin(m.from_user.id))
def adm_pending(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT id, user_id, gift_name, price, is_free, tg_error, created_at
                     FROM gift_orders WHERE status='pending'
                     ORDER BY id ASC LIMIT 30""")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(m.chat.id, "✅ Yetkazilmagan yoʻq.", reply_markup=kb_admin_gifts())
        return
    for r in rows:
        tip = "🎁 Tekin" if r["is_free"] else format_money(r["price"])
        err = (r.get("tg_error") or "")[:80]
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("🔄 Qayta yuborish", callback_data=f"retry_{r['id']}"))
        mk.add(types.InlineKeyboardButton("✅ Qoʻlda yuborildi", callback_data=f"dlv_{r['id']}"))
        text = (f"🧾 №<code>{r['id']}</code>\n"
                f"🎁 {r['gift_name']} | {tip}\n"
                f"👤 <code>{r['user_id']}</code>\n"
                f"📅 {r['created_at']}")
        if err:
            text += f"\n⚠️ <code>{err}</code>"
        bot.send_message(m.chat.id, text, reply_markup=mk)
    bot.send_message(m.chat.id, "👆 Barcha kutilayotganlar.", reply_markup=kb_admin_gifts())


# ==================== QISM 14 TUGADI ====================
# Keyingi QISM 15: ADMIN — Userlar, Xabar, Referal sozlama, To'lov sozlama
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 15/20 ====================
# ADMIN: Userlar, Xabar, Referal sozlama, To'lov sozlama
# =================================    # ==================== SOVGAMARKET BOT — QISM 15A/20 ====================
# ADMIN: Foydalanuvchilar boshqaruvi + Xabar yuborish
# =====================================================================

# ==================== 👥 FOYDALANUVCHILAR MENYU ====================
@bot.message_handler(func=lambda m: m.text == "👥 Foydalanuvchilar" and is_admin(m.from_user.id))
def adm_users(m):
    bot.send_message(m.chat.id, "👥 <b>Foydalanuvchilar</b>", reply_markup=kb_admin_users())


# ==================== 📋 USERLAR RO'YXATI ====================
@bot.message_handler(func=lambda m: m.text == "📋 Userlar" and is_admin(m.from_user.id))
def adm_list_users(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) as n FROM users")
        tot = c.fetchone()["n"]
        c.execute("""SELECT user_id, full_name, username, balance, is_blocked
                     FROM users ORDER BY joined_date DESC LIMIT 20""")
        rows = c.fetchall()
    finally:
        put_db(conn)

    text = f"📋 <b>Barcha userlar</b> (jami {tot})\n\n"
    mk = types.InlineKeyboardMarkup(row_width=1)

    if not rows:
        text += "Userlar yoʻq."
    else:
        for r in rows:
            st = "🚫" if r["is_blocked"] else "✅"
            un = f"@{r['username']}" if r["username"] else "—"
            text += (f"{st} <code>{r['user_id']}</code> | "
                     f"{(r['full_name'] or '—')[:20]} ({un})\n"
                     f"💰 {format_money(r['balance'])}\n")
            mk.add(types.InlineKeyboardButton(
                f"{st} {r['user_id']} — {(r['full_name'] or '')[:16]}",
                callback_data=f"au_{r['user_id']}"))

    bot.send_message(m.chat.id, text[:3500], reply_markup=mk)


# ==================== USER KARTASI ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("au_"))
def cb_adm_user(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("au_", ""))
    u = get_user(uid)
    if not u:
        bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
        return

    st = "🚫 Bloklangan" if u["is_blocked"] else "✅ Faol"
    text = (
        f"👤 <b>User</b>\n"
        f"ID: <code>{u['user_id']}</code>\n"
        f"👤 {u['full_name']} (@{u['username'] or 'yoq'})\n"
        f"💰 Balans: <b>{format_money(u['balance'])}</b>\n"
        f"🎁 Referal: <b>{format_money(u['referral_balance'])}</b>\n"
        f"👥 Referallar: <b>{u['referrals_count']}</b>\n"
        f"🎁 Tekin: <b>{u['free_gifts']}</b>\n"
        f"📅 Qoʻshilgan: {u['joined_date']}\n"
        f"Holat: {st}"
    )
    mk = kb_admin_user_actions(uid)
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=mk)
    except Exception:
        bot.send_message(call.from_user.id, text, reply_markup=mk)
    bot.answer_callback_query(call.id)


# ==================== 💬 XABAR YUBORISH (bitta user) ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("am_"))
def cb_adm_msg(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("am_", ""))
    msg = bot.send_message(call.from_user.id,
        f"💬 <code>{uid}</code> ga xabar:\n\n❌ Bekor")
    bot.register_next_step_handler(msg, lambda m: proc_admin_reply(m, uid))
    bot.answer_callback_query(call.id)


# ==================== 💵 BALANS O'ZGARTIRISH (callback) ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("ab_"))
def cb_adm_bal(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("ab_", ""))
    msg = bot.send_message(call.from_user.id,
        f"💵 <code>{uid}</code> uchun summa:\n"
        f"Format: <code>+5000</code> yoki <code>-2000</code>\n\n❌ Bekor")
    bot.register_next_step_handler(msg, lambda m: _proc_adm_bal(m, uid))
    bot.answer_callback_query(call.id)


def _proc_adm_bal(m, uid):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_users())
        return
    try:
        amount = float(m.text.strip().replace(" ", "").replace("+", ""))
        update_balance(uid, amount)
        try:
            bot.send_message(uid, f"💵 Balansingiz oʻzgardi: <b>{amount:+.0f}</b> soʻm")
        except Exception:
            pass
        bot.send_message(m.chat.id, f"✅ {uid} → {amount:+.0f}", reply_markup=kb_admin_users())
    except Exception:
        bot.send_message(m.chat.id, "❌ Faqat son (+5000/-2000).", reply_markup=kb_admin_users())


# ==================== 🚫 BLOK/OCHISH (callback) ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("abl_"))
def cb_adm_block(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("abl_", ""))
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT is_blocked FROM users WHERE user_id = %s", (uid,))
        row = c.fetchone()
        if not row:
            bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
            return
        new_st = 0 if row["is_blocked"] else 1
        c.execute("UPDATE users SET is_blocked = %s WHERE user_id = %s", (new_st, uid))
        conn.commit()
        log_admin_action(call.from_user.id, "block_user" if new_st else "unblock_user", str(uid))
    finally:
        put_db(conn)
    bot.answer_callback_query(call.id, "🚫 Bloklandi" if new_st else "✅ Ochildi", show_alert=True)


# ==================== 📋 USER BUYURTMALARI ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("ao_"))
def cb_adm_orders(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("ao_", ""))
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT gift_name, price, is_free, status, created_at
                     FROM gift_orders WHERE user_id = %s
                     ORDER BY id DESC LIMIT 10""", (uid,))
        rows = c.fetchall()
    finally:
        put_db(conn)

    if not rows:
        bot.answer_callback_query(call.id, "Buyurtma yoʻq", show_alert=True)
        return

    text = f"📋 <code>{uid}</code> buyurtmalari:\n\n"
    for r in rows:
        tip = "🎁 Tekin" if r["is_free"] else format_money(r["price"])
        st = "✅" if r["status"] == "delivered" else "⏳"
        text += f"{st} {r['gift_name']} | {tip}\n📅 {r['created_at']}\n"
    bot.send_message(call.from_user.id, text[:3000])
    bot.answer_callback_query(call.id)


# ==================== 💵 BALANS +- (matn) ====================
@bot.message_handler(func=lambda m: m.text == "💵 Balans +-" and is_admin(m.from_user.id))
def adm_bal_adj(m):
    msg = bot.send_message(m.chat.id,
        "💵 <b>Balans oʻzgartirish</b>\n\n"
        "Format: <code>USER_ID +5000</code> yoki <code>USER_ID -2000</code>\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_bal_adj)


def proc_bal_adj(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_users())
        return
    parts = m.text.strip().split()
    if len(parts) != 2:
        bot.send_message(m.chat.id, "❌ Format: ID SUMMA", reply_markup=kb_admin_users())
        return
    try:
        uid = int(parts[0])
        amount = float(parts[1].replace("+", ""))
        update_balance(uid, amount)
        try:
            bot.send_message(uid, f"💵 Balansingiz: <b>{amount:+.0f}</b> soʻm")
        except Exception:
            pass
        bot.send_message(m.chat.id, f"✅ {uid} → {amount:+.0f}", reply_markup=kb_admin_users())
    except Exception:
        bot.send_message(m.chat.id, "❌ Faqat: ID SUMMA", reply_markup=kb_admin_users())


# ==================== 🚫 BLOK (matn) ====================
@bot.message_handler(func=lambda m: m.text == "🚫 Blok" and is_admin(m.from_user.id))
def adm_blk(m):
    msg = bot.send_message(m.chat.id, "User ID:\n\n❌ Bekor", reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_blk)


def proc_blk(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_users())
        return
    try:
        uid = int(m.text.strip())
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("SELECT is_blocked FROM users WHERE user_id = %s", (uid,))
            r = c.fetchone()
            if not r:
                bot.send_message(m.chat.id, "Topilmadi.", reply_markup=kb_admin_users())
                return
            ns = 0 if r["is_blocked"] else 1
            c.execute("UPDATE users SET is_blocked = %s WHERE user_id = %s", (ns, uid))
            conn.commit()
        finally:
            put_db(conn)
        bot.send_message(m.chat.id,
            f"✅ {uid} {'bloklandi' if ns else 'ochildi'}.",
            reply_markup=kb_admin_users())
    except Exception:
        bot.send_message(m.chat.id, "Faqat ID.", reply_markup=kb_admin_users())


# ==================== 🔍 QIDIRISH ====================
@bot.message_handler(func=lambda m: m.text == "🔍 Qidirish" and is_admin(m.from_user.id))
def adm_srch(m):
    msg = bot.send_message(m.chat.id, "ID yoki @username:\n\n❌ Bekor", reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_srch)


def proc_srch(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_users())
        return
    q = m.text.strip().replace("@", "")
    conn = get_db()
    c = conn.cursor()
    try:
        try:
            c.execute("SELECT * FROM users WHERE user_id = %s", (int(q),))
        except Exception:
            c.execute("SELECT * FROM users WHERE username = %s", (q,))
        r = c.fetchone()
    finally:
        put_db(conn)

    if not r:
        bot.send_message(m.chat.id, "❌ Topilmadi.", reply_markup=kb_admin_users())
        return

    bot.send_message(m.chat.id,
        f"👤 <b>User</b>\n"
        f"ID: <code>{r['user_id']}</code>\n"
        f"{r['full_name']} (@{r['username'] or 'yoq'})\n"
        f"💰 {format_money(r['balance'])}\n"
        f"🎁 {format_money(r['referral_balance'])}\n"
        f"👥 {r['referrals_count']} | 🎁 {r['free_gifts']}\n"
        f"Blok: {'Ha' if r['is_blocked'] else 'Yoʻq'}",
        reply_markup=kb_admin_users())


# ==================== 📊 TOP USERLAR ====================
@bot.message_handler(func=lambda m: m.text == "📊 Top userlar" and is_admin(m.from_user.id))
def adm_top(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT user_id, full_name, balance FROM users
                     ORDER BY balance DESC LIMIT 10""")
        rows = c.fetchall()
    finally:
        put_db(conn)

    if not rows:
        bot.send_message(m.chat.id, "Userlar yoʻq.", reply_markup=kb_admin_users())
        return

    text = "🏆 <b>Top userlar (balans)</b>\n\n"
    for i, r in enumerate(rows, 1):
        text += (f"{i}. <code>{r['user_id']}</code> — "
                 f"{(r['full_name'] or '—')[:20]}\n"
                 f"   💰 {format_money(r['balance'])}\n")
    bot.send_message(m.chat.id, text, reply_markup=kb_admin_users())


# ==================== 📥 EXCEL EXPORT ====================
@bot.message_handler(func=lambda m: m.text == "📥 Excel export" and is_admin(m.from_user.id))
def adm_export(m):
    try:
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("""SELECT user_id, username, full_name, balance,
                                referral_balance, referrals_count, is_blocked, joined_date
                         FROM users ORDER BY user_id""")
            rows = c.fetchall()
        finally:
            put_db(conn)

        if not rows:
            bot.send_message(m.chat.id, "❌ Userlar yoʻq.", reply_markup=kb_admin_users())
            return

        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["ID", "Username", "Full name", "Balance",
                    "Referral balance", "Referrals", "Blocked", "Joined"])
        for r in rows:
            w.writerow([
                r["user_id"], r["username"] or "", r["full_name"] or "",
                r["balance"], r["referral_balance"], r["referrals_count"],
                r["is_blocked"], r["joined_date"]
            ])

        output.seek(0)
        f = io.BytesIO(output.getvalue().encode("utf-8"))
        f.name = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        bot.send_document(m.chat.id, f,
            caption=f"📥 <b>Userlar export</b>\n\nJami: {len(rows)} ta",
            reply_markup=kb_admin_users())
    except Exception as e:
        bot.send_message(m.chat.id, f"❌ Xato: {e}", reply_markup=kb_admin_users())


# ==================== 📢 XABAR YUBORISH (BROADCAST) ====================
@bot.message_handler(func=lambda m: m.text == "📢 Xabar" and is_admin(m.from_user.id))
def adm_bcast(m):
    msg = bot.send_message(m.chat.id,
        "📢 <b>Xabar yuborish</b>\n\n"
        "Matn/rasm/video/fayl yuboring.\n\n❌ Bekor",
        reply_markup=types.ReplyKeyboardRemove())
    bot.register_next_step_handler(msg, proc_bcast)


def proc_bcast(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_main())
        return

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT user_id FROM users WHERE is_blocked = 0")
        users = c.fetchall()
    finally:
        put_db(conn)

    bot.send_message(m.chat.id, f"⏳ Yuborilmoqda... ({len(users)} ta user)")

    ok = fail = 0
    for u in users:
        try:
            if m.content_type == "text":
                bot.send_message(u["user_id"], m.text)
            else:
                bot.copy_message(u["user_id"], m.chat.id, m.message_id)
            ok += 1
            time.sleep(0.04)
        except Exception:
            fail += 1

    bot.send_message(m.chat.id,
        f"✅ <b>Yuborildi!</b>\n\n"
        f"✅ Muvaffaqiyatli: <b>{ok}</b>\n"
        f"❌ Xato: <b>{fail}</b>",
        reply_markup=kb_admin_main())


@bot.message_handler(commands=['broadcast'])
def cmd_broadcast(m):
    if is_admin(m.from_user.id):
        adm_bcast(m)


# ==================== QISM 15A TUGADI ====================
# Keyingi QISM 15B: Referal sozlama + To'lov sozlama
# =====================================================================
# ==================== SOVGAMARKET BOT — QISM 15B/20 ====================
# ADMIN: Referal sozlama (4 xil) + To'lov sozlama
# =====================================================================

# ==================== 🎁 REFERAL SOZLAMA ====================
@bot.message_handler(func=lambda m: m.text == "🎁 Referal sozlama" and is_admin(m.from_user.id))
def adm_ref_set(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) as n FROM users WHERE referrals_count > 0")
        active = c.fetchone()["n"]
        c.execute("SELECT COALESCE(SUM(referral_balance),0) as s FROM users")
        total_bal = c.fetchone()["s"]
        c.execute("SELECT COALESCE(SUM(amount),0) as s FROM referral_history")
        total_paid = c.fetchone()["s"]
    finally:
        put_db(conn)

    text = (
        f"🎁 <b>Referal sozlamalari</b>\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💰 Doʻst kelganda: <b>{format_money(get_setting('referral_bonus', 500))}</b> — "
        f"{'✅' if get_setting('ref_bonus_on', True) else '❌'}\n"
        f"📊 Foiz: <b>{get_setting('referral_percent', 10)}%</b> — "
        f"{'✅' if get_setting('ref_percent_on', True) else '❌'}\n"
        f"👥 Har gift uchun ref: <b>{'✅' if get_setting('ref_invites_on', True) else '❌'}</b>\n"
        f"🛒 Buyurtma soni: <b>{get_setting('ref_orders_needed', 3)}</b> — "
        f"{'✅' if get_setting('ref_orders_on', False) else '❌'}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Statistika:</b>\n"
        f"• Faol referrerlar: <b>{active}</b>\n"
        f"• Ref balans: <b>{format_money(total_bal)}</b>\n"
        f"• Jami bonus: <b>{format_money(total_paid)}</b>"
    )
    bot.send_message(m.chat.id, text, reply_markup=kb_admin_ref_set())


# ==================== 📊 FOIZ ====================
@bot.message_handler(func=lambda m: m.text == "📊 Foiz" and is_admin(m.from_user.id))
def r_f(m):
    msg = bot.send_message(m.chat.id,
        f"📊 <b>Referal foiz</b>\n\n"
        f"Hozirgi: <b>{get_setting('referral_percent', 10)}%</b>\n\n"
        f"Yangi foiz (0-100):\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, lambda x: _si(x, "referral_percent", 0, 100, kb_admin_ref_set))


# ==================== 💰 BONUS ====================
@bot.message_handler(func=lambda m: m.text == "💰 Bonus" and is_admin(m.from_user.id))
def r_b(m):
    msg = bot.send_message(m.chat.id,
        f"💰 <b>Doʻst kelganda bonus</b>\n\n"
        f"Hozirgi: <b>{format_money(get_setting('referral_bonus', 500))}</b>\n\n"
        f"Yangi summa (soʻm):\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, lambda x: _si(x, "referral_bonus", 0, 10_000_000, kb_admin_ref_set))


# ==================== ⚙️ TOGGLE ====================
@bot.message_handler(func=lambda m: m.text == "⚙️ Toggle" and is_admin(m.from_user.id))
def r_t(m):
    bot.send_message(m.chat.id,
        "⚙️ <b>Referal tizimlarini yoqish / oʻchirish</b>\n\n"
        "Tugmani bosing — holat oʻzgaradi:",
        reply_markup=kb_admin_ref_toggle())


@bot.callback_query_handler(func=lambda c: c.data.startswith("rt_"))
def cb_rt(call):
    if not is_admin(call.from_user.id):
        return
    k = call.data.replace("rt_", "")
    cur = get_setting(k, False)
    set_setting(k, not cur)
    bot.answer_callback_query(call.id,
        "✅ Yoqildi" if not cur else "❌ Oʻchirildi",
        show_alert=True)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                      reply_markup=kb_admin_ref_toggle())
    except Exception:
        pass


# ==================== 📊 REF STATISTIKA ====================
@bot.message_handler(func=lambda m: m.text == "📊 Ref statistika" and is_admin(m.from_user.id))
def r_s(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT user_id, full_name, referrals_count, referral_balance
                     FROM users WHERE referrals_count > 0
                     ORDER BY referrals_count DESC LIMIT 10""")
        rows = c.fetchall()
    finally:
        put_db(conn)

    if not rows:
        bot.send_message(m.chat.id, "📊 Hali referrerlar yoʻq.", reply_markup=kb_admin_ref_set())
        return

    text = "🏆 <b>Top referrerlar</b>\n\n"
    for i, r in enumerate(rows, 1):
        text += (f"{i}. <code>{r['user_id']}</code> — {(r['full_name'] or '—')[:20]}\n"
                 f"   👥 {r['referrals_count']} ta | "
                 f"💸 {format_money(r['referral_balance'])}\n\n")
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_admin_ref_set())


# ==================== ⏱ TO'LOV SOZLAMA ====================
@bot.message_handler(func=lambda m: m.text == "⏱ Toʻlov sozlama" and is_admin(m.from_user.id))
def adm_pay_set(m):
    text = (
        f"⏱ <b>Toʻlov sozlamalari</b>\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"⏱ Vaqt: <b>{get_setting('payment_time_minutes', 15)} daqiqa</b>\n"
        f"💵 Start summa: <b>{format_money(get_setting('start_amount', 5000))}</b>\n"
        f"💳 Karta: <code>{get_setting('card_number', '')}</code>\n"
        f"👤 Egasi: <b>{get_setting('card_owner', '')}</b>\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"Bu sozlamalar user «➕ Toʻldirish» bosganda koʻrsatiladi."
    )
    bot.send_message(m.chat.id, text, reply_markup=kb_admin_pay_set())


# ==================== ⏱ VAQT ====================
@bot.message_handler(func=lambda m: m.text == "⏱ Vaqt" and is_admin(m.from_user.id))
def p_v(m):
    msg = bot.send_message(m.chat.id,
        f"⏱ <b>Toʻlov vaqti</b>\n\n"
        f"Hozirgi: <b>{get_setting('payment_time_minutes', 15)} daqiqa</b>\n\n"
        f"Yangi vaqt (1-120 daqiqa):\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg,
        lambda x: _si(x, "payment_time_minutes", 1, 120, kb_admin_pay_set))


# ==================== 💵 START SUMMA ====================
@bot.message_handler(func=lambda m: m.text == "💵 Start summa" and is_admin(m.from_user.id))
def p_s(m):
    msg = bot.send_message(m.chat.id,
        f"💵 <b>Start summa</b>\n\n"
        f"Hozirgi: <b>{format_money(get_setting('start_amount', 5000))}</b>\n\n"
        f"Yangi summa (min 1000):\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg,
        lambda x: _si(x, "start_amount", 1000, 100_000_000, kb_admin_pay_set))


# ==================== 💳 KARTA ====================
@bot.message_handler(func=lambda m: m.text == "💳 Karta" and is_admin(m.from_user.id))
def p_c(m):
    msg = bot.send_message(m.chat.id,
        f"💳 <b>Karta raqami</b>\n\n"
        f"Hozirgi: <code>{get_setting('card_number', '')}</code>\n\n"
        f"Yangi karta raqami (16 raqam):\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_card)


def proc_card(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_pay_set())
        return
    card = m.text.strip().replace(" ", "").replace("-", "")
    if len(card) != 16 or not card.isdigit():
        bot.send_message(m.chat.id, "❌ Karta 16 raqamdan iborat boʻlishi kerak.",
                         reply_markup=kb_admin_pay_set())
        return
    set_setting("card_number", card)
    bot.send_message(m.chat.id, f"✅ Karta → <code>{card}</code>",
                     reply_markup=kb_admin_pay_set())


# ==================== 👤 KARTA EGASI ====================
@bot.message_handler(func=lambda m: m.text == "👤 Karta egasi" and is_admin(m.from_user.id))
def p_o(m):
    msg = bot.send_message(m.chat.id,
        f"👤 <b>Karta egasi</b>\n\n"
        f"Hozirgi: <b>{get_setting('card_owner', '')}</b>\n\n"
        f"Yangi ism (F.I.Sh):\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_owner)


def proc_owner(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_pay_set())
        return
    name = m.text.strip()[:50]
    set_setting("card_owner", name)
    bot.send_message(m.chat.id, f"✅ Egasi → <b>{name}</b>",
                     reply_markup=kb_admin_pay_set())


# ==================== YORDAMCHI: INT SETTER ====================
def _si(m, key, mi, ma, mf):
    """Universal int setter"""
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=mf())
        return
    try:
        v = int(m.text.strip().replace(" ", "").replace(",", ""))
        if v < mi or v > ma:
            raise Exception()
        set_setting(key, v)
        bot.send_message(m.chat.id, f"✅ → <b>{v}</b>", reply_markup=mf())
    except Exception:
        bot.send_message(m.chat.id, f"❌ {mi}-{ma} orasida son.", reply_markup=mf())


# ==================== QISM 15B TUGADI ====================
# Keyingi QISM 16: ADMIN — Obuna, Isbotlar, Stars sozlama
# =====================================================================
# ==================== SOVGAMARKET BOT — QISM 16A/20 ====================
# ADMIN: Majburiy obuna + Isbotlar kanal boshqaruvi
# =====================================================================

# ==================== 📢 MAJBURIY OBUNA ====================
@bot.message_handler(func=lambda m: m.text == "📢 Majburiy obuna" and is_admin(m.from_user.id))
def adm_channels_menu(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) as n FROM channels")
        n = c.fetchone()["n"]
    finally:
        put_db(conn)
    bot.send_message(m.chat.id,
        f"📢 <b>Majburiy obuna</b>\n\n"
        f"Kanallar soni: <b>{n}</b>\n\n"
        f"User /start bosganda kanallarga obuna boʻlishi shart.",
        reply_markup=kb_admin_channels())


# ==================== ➕ KANAL QO'SHISH ====================
@bot.message_handler(func=lambda m: m.text == "➕ Kanal qoʻshish" and is_admin(m.from_user.id))
def adm_add_ch(m):
    msg = bot.send_message(m.chat.id,
        "📢 <b>Kanal qoʻshish</b>\n\n"
        "Kanal @username, ID yoki forward qiling.\n\n"
        "⚠️ Bot kanalda <b>admin</b> boʻlishi kerak!\n\n"
        "❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_add_ch)


def proc_add_ch(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_channels())
        return

    if m.forward_from_chat:
        chat = m.forward_from_chat
        cid = str(chat.id)
        title = chat.title or cid
        un = f"@{chat.username}" if chat.username else ""
        priv = 0 if chat.username else 1
        invite = ""
    else:
        lines = [l.strip() for l in (m.text or "").split("\n") if l.strip()]
        if not lines:
            bot.send_message(m.chat.id, "❌ Xato format.", reply_markup=kb_admin_channels())
            return
        ch = lines[0]
        invite = lines[1] if len(lines) > 1 else ""
        cid = ch
        title = ch
        un = ch if ch.startswith("@") else ""
        priv = 0
        try:
            info = bot.get_chat(ch)
            cid = str(info.id)
            title = info.title or ch
            un = f"@{info.username}" if info.username else ""
            priv = 0 if info.username else 1
        except Exception:
            pass

    # Test — bot kanalga yoza oladimi?
    try:
        bot.send_message(cid, "✅ Kanal muvaffaqiyatli ulandi!")
    except Exception as e:
        bot.send_message(m.chat.id,
            f"❌ Bot kanalga admin emas!\n\nXato: {e}",
            reply_markup=kb_admin_channels())
        return

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""INSERT INTO channels
                     (channel_id, channel_username, channel_title, is_private, invite_link)
                     VALUES (%s, %s, %s, %s, %s)""",
                  (cid, un, title, priv, invite))
        conn.commit()
    finally:
        put_db(conn)

    log_admin_action(m.from_user.id, "add_channel", title)
    bot.send_message(m.chat.id, f"✅ Qoʻshildi: <b>{title}</b>",
                     reply_markup=kb_admin_channels())


# ==================== 📋 KANALLAR RO'YXATI ====================
@bot.message_handler(func=lambda m: m.text == "📋 Kanallar" and is_admin(m.from_user.id))
def adm_list_ch(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM channels ORDER BY id")
        rows = c.fetchall()
    finally:
        put_db(conn)

    if not rows:
        bot.send_message(m.chat.id, "📋 Kanallar yoʻq.", reply_markup=kb_admin_channels())
        return

    text = "📋 <b>Kanallar:</b>\n\n"
    for r in rows:
        text += (f"• {r['channel_title'] or r['channel_username'] or r['channel_id']}\n"
                 f"  🆔 <code>{r['channel_id']}</code>\n"
                 f"  {r['channel_username'] or '—'}\n\n")
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_admin_channels())


# ==================== ❌ KANAL O'CHIRISH ====================
@bot.message_handler(func=lambda m: m.text == "❌ Kanal oʻchirish" and is_admin(m.from_user.id))
def adm_del_ch(m):
    msg = bot.send_message(m.chat.id,
        "❌ <b>Kanal oʻchirish</b>\n\nKanal @username yoki ID:\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_del_ch)


def proc_del_ch(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_channels())
        return
    ch = m.text.strip()
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""DELETE FROM channels
                     WHERE channel_id = %s OR channel_username = %s OR channel_username = %s""",
                  (ch, ch, f"@{ch.lstrip('@')}"))
        n = c.rowcount
        conn.commit()
    finally:
        put_db(conn)
    bot.send_message(m.chat.id, "✅ Oʻchirildi." if n else "❌ Topilmadi.",
                     reply_markup=kb_admin_channels())


# ==================== 🔄 TEST ====================
@bot.message_handler(func=lambda m: m.text == "🔄 Test" and is_admin(m.from_user.id))
def adm_test_ch(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT channel_id, channel_title FROM channels")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(m.chat.id, "📢 Kanallar yoʻq.", reply_markup=kb_admin_channels())
        return
    text = "🔄 <b>Tekshiruv:</b>\n\n"
    for r in rows:
        try:
            mb = bot.get_chat_member(r["channel_id"], m.from_user.id)
            text += f"✅ {r['channel_title']} — {mb.status}\n"
        except Exception as e:
            text += f"❌ {r['channel_title']} — {str(e)[:50]}\n"
    bot.send_message(m.chat.id, text, reply_markup=kb_admin_channels())


# ==================== 📢 ISBOTLAR (ADMIN) ====================
@bot.message_handler(func=lambda m: m.text == "📢 Isbotlar" and is_admin(m.from_user.id))
def adm_proof_menu(m):
    ch = get_setting("proof_channel_id", "")
    un = get_setting("proof_channel_username", "")
    st = "✅ Yoqilgan" if get_setting("proof_enabled", True) else "❌ Oʻchirilgan"
    fmt_names = {"full": "Toʻliq", "short": "Qisqa", "minimal": "Minimal"}
    fmt_type = get_setting("proof_format", "full")

    text = (f"📢 <b>Isbotlar kanali</b>\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Kanal: <code>{ch or 'belgilanmagan'}</code>\n"
            f"Username: {un or '—'}\n"
            f"Holat: {st}\n"
            f"Format: <b>{fmt_names.get(fmt_type, 'Toʻliq')}</b>\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"Har bir xarid avtomatik kanalga yuboriladi.\n"
            f"<b>User</b> esa «📢 Isbotlar» bosganda kanal linki oladi.")
    bot.send_message(m.chat.id, text, reply_markup=kb_admin_proof())


# ==================== ➕ KANAL BELGILASH ====================
@bot.message_handler(func=lambda m: m.text == "➕ Kanal belgilash" and is_admin(m.from_user.id))
def adm_set_proof(m):
    msg = bot.send_message(m.chat.id,
        "📢 <b>Isbotlar kanali</b>\n\n"
        "Kanal @username, ID yoki forward qiling.\n\n"
        "⚠️ Bot kanalda <b>admin</b> boʻlishi kerak!\n\n"
        "❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_set_proof)


def proc_set_proof(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_proof())
        return

    if m.forward_from_chat:
        chat = m.forward_from_chat
        cid = chat.id
        un = f"@{chat.username}" if chat.username else ""
        title = chat.title or str(cid)
    else:
        try:
            chat = bot.get_chat(m.text.strip())
            cid = chat.id
            un = f"@{chat.username}" if chat.username else ""
            title = chat.title or str(cid)
        except Exception as e:
            bot.send_message(m.chat.id, f"❌ Topilmadi: {e}", reply_markup=kb_admin_proof())
            return

    # Test — bot kanalga yoza oladimi?
    try:
        bot.send_message(cid, "✅ Isbotlar kanali ulandi!")
    except Exception as e:
        bot.send_message(m.chat.id,
            f"❌ Bot kanalga admin emas!\n\nXato: {e}",
            reply_markup=kb_admin_proof())
        return

    set_setting("proof_channel_id", str(cid))
    set_setting("proof_channel_username", un or title)
    set_setting("proof_enabled", True)

    log_admin_action(m.from_user.id, "set_proof", title)
    bot.send_message(m.chat.id,
        f"✅ <b>Isbotlar kanali ulandi!</b>\n\n"
        f"📢 {title}\n🆔 <code>{cid}</code>",
        reply_markup=kb_admin_proof())


# ==================== 📋 FORMAT TANLASH ====================
@bot.message_handler(func=lambda m: m.text == "📋 Format tanlash" and is_admin(m.from_user.id))
def adm_proof_fmt(m):
    bot.send_message(m.chat.id, "📋 <b>Isbot formati</b>\n\nTanlang:",
                     reply_markup=kb_admin_proof_format())


@bot.callback_query_handler(func=lambda c: c.data.startswith("pfmt_"))
def cb_set_pfmt(call):
    if not is_admin(call.from_user.id):
        return
    fmt = call.data.replace("pfmt_", "")
    set_setting("proof_format", fmt)
    bot.answer_callback_query(call.id, f"✅ {fmt}", show_alert=True)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                      reply_markup=kb_admin_proof_format())
    except Exception:
        pass


# ==================== ⚙️ YOQ/O'CHIRISH ====================
@bot.message_handler(func=lambda m: m.text == "⚙️ Yoq/Oʻchirish" and is_admin(m.from_user.id))
def adm_proof_tog(m):
    cur = get_setting("proof_enabled", True)
    set_setting("proof_enabled", not cur)
    bot.send_message(m.chat.id,
        f"📢 Isbotlar: {'✅ Yoqildi' if not cur else '❌ Oʻchirildi'}",
        reply_markup=kb_admin_proof())


# ==================== 📤 TEST XABAR ====================
@bot.message_handler(func=lambda m: m.text == "📤 Test xabar" and is_admin(m.from_user.id))
def adm_proof_test(m):
    ch = get_setting("proof_channel_id", "")
    if not ch:
        bot.send_message(m.chat.id, "❌ Kanal belgilanmagan.", reply_markup=kb_admin_proof())
        return
    try:
        send_proof(m.from_user.id, "Test sovgʻa", 15000, 9999,
                   is_free=False, gift_emoji="🎁")
        bot.send_message(m.chat.id, "✅ Test yuborildi.", reply_markup=kb_admin_proof())
    except Exception as e:
        bot.send_message(m.chat.id, f"❌ Xato: {e}", reply_markup=kb_admin_proof())


# ==================== ❌ KANAL O'CHIRISH (isbotlar) ====================
@bot.message_handler(func=lambda m: m.text == "❌ Kanal oʻchirish" and is_admin(m.from_user.id) and
                     get_setting("proof_channel_id", "") != "")
def adm_del_proof(m):
    set_setting("proof_channel_id", "")
    set_setting("proof_channel_username", "")
    log_admin_action(m.from_user.id, "del_proof")
    bot.send_message(m.chat.id, "✅ Isbotlar kanali oʻchirildi.",
                     reply_markup=kb_admin_proof())


# ==================== QISM 16A TUGADI ====================
# Keyingi QISM 16B: Stars, Adminlar, Sharh o'chirish, Boshqa
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 16B/20 ====================
# ADMIN: Stars sozlama, Adminlar, Sharh o'chirish, Boshqa sozlamalar
# =====================================================================

# ==================== ⭐ STARS SOZLAMA ====================
@bot.message_handler(func=lambda m: m.text == "⭐ Stars sozlama" and is_admin(m.from_user.id))
def adm_stars_menu(m):
    stars = get_bot_stars_balance()
    bot.send_message(m.chat.id,
        f"⭐ <b>Stars sozlamalari</b>\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Bot Stars: <b>{stars}</b>\n"
        f"Method: <b>{get_setting('gift_method', GIFT_METHOD)}</b>\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"• <b>bot</b> — botning Stars balansidan ← tavsiya\n"
        f"• <b>userbot</b> — shaxsiy akkaunt (Telethon)\n"
        f"• <b>auto</b> — avtomatik tanlash\n"
        f"• <b>manual</b> — admin qoʻlda",
        reply_markup=kb_admin_stars())


# ==================== 💰 BOT STARS BALANSI (asosiy menyu) ====================
@bot.message_handler(func=lambda m: m.text == "💰 Bot Stars balansi" and is_admin(m.from_user.id))
def adm_stars_bal_main(m):
    stars = get_bot_stars_balance()
    bot.send_message(m.chat.id,
        f"💰 <b>Bot Stars balansi</b>\n\n"
        f"⭐ Joriy balans: <b>{stars}</b> Stars\n\n"
        f"Bu Stars'lardan gift yuboriladi.\n"
        f"To'ldirish uchun: <b>⭐ Bot Stars toʻplash</b>",
        reply_markup=kb_admin_main())


# ==================== ⭐ BOT STARS BALANSI (submenyu) ====================
@bot.message_handler(func=lambda m: m.text == "⭐ Bot Stars balansi" and is_admin(m.from_user.id))
def adm_stars_bal(m):
    stars = get_bot_stars_balance()
    bot.send_message(m.chat.id,
        f"⭐ <b>Bot Stars balansi: {stars}</b>\n\n"
        f"Toʻldirish uchun «⭐ Bot Stars toʻplash» tugmasini bosing.",
        reply_markup=kb_admin_stars())


# ==================== ⭐ BOT STARS TO'PLASH ====================
@bot.message_handler(func=lambda m: m.text == "⭐ Bot Stars toʻplash" and is_admin(m.from_user.id))
def adm_stars_topup(m):
    stars = get_bot_stars_balance()
    msg = bot.send_message(m.chat.id,
        f"⭐ <b>Bot Stars toʻplash</b>\n\n"
        f"💰 Joriy balans: <b>{stars}</b> Stars\n\n"
        f"Nechta Stars to'lamoqchisiz?\n"
        f"Masalan: <code>1000</code>\n\n"
        f"❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_stars_topup)


def proc_stars_topup(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_main())
        return
    try:
        stars = int(m.text.strip().replace(" ", "").replace(",", ""))
        if stars < 1 or stars > 1_000_000:
            raise Exception()
    except Exception:
        bot.send_message(m.chat.id, "❌ 1-1 000 000 orasida son:",
                         reply_markup=kb_back())
        bot.register_next_step_handler(m, proc_stars_topup)
        return

    # Telegram Stars invoice yaratish
    try:
        bot.send_invoice(
    chat_id=m.chat.id,
    title="⭐ Bot Stars toʻplash",
    description=f"{stars} Stars bot balansiga tushadi",
    invoice_payload=f"botstars_{m.from_user.id}_{stars}_{int(time.time())}",
    provider_token="",
    currency="XTR",
    prices=[types.LabeledPrice(
        label="⭐ Stars toʻplash",
        amount=int(stars)
    )]
)
        bot.send_message(m.chat.id,
            f"⭐ <b>Invoice yuborildi!</b>\n\n"
            f"Miqdor: <b>{stars} Stars</b>\n\n"
            f"To'lovni amalga oshiring →",
            reply_markup=kb_admin_main())
    except Exception as e:
        print("Stars invoice xato:", e)
        bot.send_message(m.chat.id, f"❌ Xato: {e}", reply_markup=kb_admin_main())


# ==================== PRE-CHECKOUT ====================
@bot.pre_checkout_query_handler(func=lambda q: True)
def pre_checkout(q):
    bot.answer_pre_checkout_query(q.id, ok=True)


# ==================== SUCCESSFUL PAYMENT (STARS) ====================
@bot.message_handler(content_types=['successful_payment'])
def successful_payment(m):
    """Stars to'lov muvaffaqiyatli o'tganda"""
    try:
        sp = m.successful_payment
        stars = sp.total_amount
        uid = m.from_user.id
        payload = sp.invoice_payload or ""

        # Faqat bot stars to'plash uchun
        if not payload.startswith("botstars_"):
            return

        # Stars bot balansiga TUSHDI ✅
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # DB'ga yozish
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("""INSERT INTO payments
                         (user_id, amount, unique_amount, status, method, created_at, paid_at)
                         VALUES (%s, 0, 0, 'paid', 'stars', %s, %s)""",
                      (uid, now, now))
            conn.commit()
        finally:
            put_db(conn)

        # Yangi balans
        new_bal = get_bot_stars_balance()

        # Adminga xabar
        bot.send_message(uid,
            f"✅ <b>Bot Stars to'lov qabul qilindi!</b>\n\n"
            f"⭐ Miqdor: <b>{stars} Stars</b>\n"
            f"💰 Bot balansi: <b>{new_bal}</b> Stars\n\n"
            f"Endi bot bu Stars'lardan gift yuboradi.")

        print(f"⭐ Bot Stars to'lov: user={uid} stars={stars} balans={new_bal}")
    except Exception as e:
        print(f"Stars payment error: {e}")


# ==================== 🎁 TG GIFTLAR ====================
@bot.message_handler(func=lambda m: m.text == "🎁 TG giftlar" and is_admin(m.from_user.id))
def adm_tg_gifts(m):
    gs = get_available_tg_gifts()
    if not gs:
        bot.send_message(m.chat.id,
            "❌ Giftlar olinmadi. pyTelegramBotAPI yangilang:\n"
            "<code>pip install -U pyTelegramBotAPI</code>",
            reply_markup=kb_admin_stars())
        return

    text = "🎁 <b>Bot yubora oladigan giftlar:</b>\n\n"
    for g in gs[:40]:
        gid = g.get("id") if isinstance(g, dict) else getattr(g, "id", "?")
        sc = g.get("star_count") if isinstance(g, dict) else getattr(g, "star_count", "?")
        rm = g.get("remaining_count") if isinstance(g, dict) else getattr(g, "remaining_count", "?")
        text += f"• <code>{gid}</code> — ⭐ {sc} | qoldi: {rm}\n"
    text += "\n📋 ID ni nusxalab «🔗 Gift bogʻlash» orqali ulang."
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_admin_stars())


# ==================== 🔗 GIFT BOG'LASH ====================
@bot.message_handler(func=lambda m: m.text == "🔗 Gift bogʻlash" and is_admin(m.from_user.id))
def adm_link_gift(m):
    msg = bot.send_message(m.chat.id,
        "🔗 <b>Gift bogʻlash</b>\n\n"
        "Format: <code>LOCAL_ID TG_GIFT_ID</code>\n"
        "Uzish: <code>LOCAL_ID 0</code>\n\n"
        "Misol: <code>1 5170145012310081615</code>\n\n"
        "❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_link_gift)


def proc_link_gift(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_stars())
        return
    ps = m.text.strip().split()
    if len(ps) != 2:
        bot.send_message(m.chat.id,
            "❌ <b>Format xato</b>\n\n"
            "Toʻgʻri format: <code>LOCAL_ID TG_GIFT_ID</code>\n"
            "Misol: <code>1 5170145012310081615</code>",
            reply_markup=kb_admin_stars())
        return
    try:
        lid = int(ps[0])
        tg = "" if ps[1] == "0" else ps[1].strip()
        if link_gift_tg(lid, tg):
            bot.send_message(m.chat.id,
                f"✅ <b>Bogʻlandi!</b>\n\n"
                f"🆔 Lokal: #{lid}\n"
                f"🔗 TG ID: <code>{tg or 'uzildi'}</code>",
                reply_markup=kb_admin_stars())
            log_admin_action(m.from_user.id, "link_gift", f"#{lid} → {tg}")
        else:
            bot.send_message(m.chat.id,
                f"❌ <b>#{lid} topilmadi</b>\n\n"
                f"Avval «🎁 Sovgʻalar» → «➕ Sovgʻa qoʻshish» qiling,\n"
                f"soʻngra uni bogʻlang.",
                reply_markup=kb_admin_stars())
    except ValueError:
        bot.send_message(m.chat.id,
            "❌ <b>Faqat raqamlar</b> kiriting.",
            reply_markup=kb_admin_stars())
    except Exception as e:
        bot.send_message(m.chat.id, f"❌ Xato: {e}", reply_markup=kb_admin_stars())


# ==================== 🔄 METHOD ====================
@bot.message_handler(func=lambda m: m.text == "🔄 Method" and is_admin(m.from_user.id))
def adm_method(m):
    cur = get_setting("gift_method", GIFT_METHOD)
    bot.send_message(m.chat.id,
        f"🔄 <b>Gift method: {cur}</b>\n\nTanlang:",
        reply_markup=kb_admin_gift_method())


@bot.callback_query_handler(func=lambda c: c.data.startswith("setgm_"))
def cb_setgm(call):
    if not is_admin(call.from_user.id):
        return
    method = call.data.replace("setgm_", "")
    set_setting("gift_method", method)
    bot.answer_callback_query(call.id, f"✅ {method}", show_alert=True)
    try:
        bot.edit_message_text(
            f"🔄 Gift method: <b>{method}</b>",
            call.message.chat.id, call.message.message_id,
            reply_markup=kb_admin_gift_method())
    except Exception:
        pass


# ==================== 👨‍💼 ADMINLAR ====================
@bot.message_handler(func=lambda m: m.text == "👨‍💼 Adminlar" and is_admin(m.from_user.id))
def adm_admins(m):
    bot.send_message(m.chat.id, "👨‍💼 <b>Adminlar</b>", reply_markup=kb_admin_admins())


@bot.message_handler(func=lambda m: m.text == "➕ Admin qoʻshish" and is_admin(m.from_user.id))
def adm_add_admin(m):
    msg = bot.send_message(m.chat.id,
        "➕ <b>Yangi admin</b>\n\nUser ID:\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_add_admin)


def proc_add_admin(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_admins())
        return
    try:
        uid = int(m.text.strip())
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (uid,))
            conn.commit()
            add_admin_cache(uid)
        finally:
            put_db(conn)
        log_admin_action(m.from_user.id, "add_admin", str(uid))
        bot.send_message(m.chat.id, f"✅ <code>{uid}</code> admin qilindi.",
                         reply_markup=kb_admin_admins())
    except Exception:
        bot.send_message(m.chat.id, "❌ Faqat son.", reply_markup=kb_admin_admins())


@bot.message_handler(func=lambda m: m.text == "❌ Admin oʻchirish" and is_admin(m.from_user.id))
def adm_rm_admin(m):
    msg = bot.send_message(m.chat.id,
        "❌ <b>Admin oʻchirish</b>\n\nAdmin ID:\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_rm_admin)


def proc_rm_admin(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_admins())
        return
    try:
        uid = int(m.text.strip())
        if uid in ADMIN_IDS:
            bot.send_message(m.chat.id,
                "❌ Asosiy adminni oʻchirib boʻlmaydi.",
                reply_markup=kb_admin_admins())
            return
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("DELETE FROM admins WHERE user_id = %s", (uid,))
            n = c.rowcount
            conn.commit()
            remove_admin_cache(uid)
        finally:
            put_db(conn)
        log_admin_action(m.from_user.id, "rm_admin", str(uid))
        bot.send_message(m.chat.id,
            f"✅ <code>{uid}</code> adminlikdan olinди." if n else "❌ Topilmadi.",
            reply_markup=kb_admin_admins())
    except Exception:
        bot.send_message(m.chat.id, "❌ Faqat son.", reply_markup=kb_admin_admins())


@bot.message_handler(func=lambda m: m.text == "📋 Adminlar" and is_admin(m.from_user.id))
def adm_list_admins(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT user_id FROM admins ORDER BY user_id")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(m.chat.id, "📋 Adminlar yoʻq.", reply_markup=kb_admin_admins())
        return
    text = "📋 <b>Adminlar:</b>\n\n"
    for r in rows:
        tag = " 👑 Asosiy" if r["user_id"] in ADMIN_IDS else ""
        text += f"• <code>{r['user_id']}</code>{tag}\n"
    bot.send_message(m.chat.id, text, reply_markup=kb_admin_admins())


@bot.message_handler(func=lambda m: m.text == "📊 Harakatlar" and is_admin(m.from_user.id))
def adm_log_view(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT admin_id, action, details, created_at
                     FROM admin_log ORDER BY id DESC LIMIT 20""")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(m.chat.id, "📊 Log boʻsh.", reply_markup=kb_admin_admins())
        return
    text = "📊 <b>Admin harakatlari</b>\n\n"
    for r in rows:
        text += (f"👤 <code>{r['admin_id']}</code>\n"
                 f"⚡ {r['action']}\n"
                 f"📝 {r['details'] or '—'}\n"
                 f"📅 {r['created_at']}\n\n")
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_admin_admins())


# ==================== 💬 SHARHLAR (ADMIN) ====================
@bot.message_handler(func=lambda m: m.text == "💬 Sharhlar" and is_admin(m.from_user.id))
def adm_reviews(m):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT id, full_name, username, text FROM reviews
                     WHERE is_approved = 1 ORDER BY id DESC LIMIT 15""")
        rows = c.fetchall()
    finally:
        put_db(conn)

    text = "💬 <b>Sharhlar boshqaruvi</b>\n\n"
    mk = types.InlineKeyboardMarkup(row_width=1)

    if not rows:
        text += "Hali sharhlar yoʻq."
    else:
        for r in rows:
            text += f"👤 {r['full_name']} (@{r['username'] or 'yoq'})\n{r['text']}\n\n"
            mk.add(types.InlineKeyboardButton(
                f"🗑 #{r['id']} oʻchirish",
                callback_data=f"drv_{r['id']}"))

    mk.add(types.InlineKeyboardButton("✍️ Sharh yozish", callback_data="add_rev"))
    bot.send_message(m.chat.id, text[:3500], reply_markup=mk)


@bot.callback_query_handler(func=lambda c: c.data.startswith("drv_"))
def cb_del_review(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Faqat admin", show_alert=True)
        return
    rid = int(call.data.replace("drv_", ""))
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("DELETE FROM reviews WHERE id = %s", (rid,))
        conn.commit()
    finally:
        put_db(conn)
    log_admin_action(call.from_user.id, "del_review", f"#{rid}")
    bot.answer_callback_query(call.id, "✅ Oʻchirildi", show_alert=True)
    try:
        bot.edit_message_text(
            f"✅ Sharh #{rid} oʻchirildi.",
            call.message.chat.id, call.message.message_id)
    except Exception:
        pass


@bot.message_handler(func=lambda m: m.text == "✍️ O'zi sharh yozish" and is_admin(m.from_user.id))
def adm_self_rev(m):
    msg = bot.send_message(m.chat.id,
        "Sharh matni:\n\n❌ Bekor",
        reply_markup=kb_back())
    bot.register_next_step_handler(msg, proc_self_rev)


def proc_self_rev(m):
    if not is_admin(m.from_user.id):
        return
    if m.text in ["🔙 Orqaga", "❌ Bekor", "/cancel"]:
        bot.send_message(m.chat.id, "Bekor.", reply_markup=kb_admin_reviews())
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""INSERT INTO reviews
                     (user_id, username, full_name, text, created_at, is_approved)
                     VALUES (%s, %s, %s, %s, %s, 1)""",
                  (m.from_user.id, m.from_user.username or "",
                   m.from_user.full_name or "", m.text[:500], now))
        conn.commit()
    finally:
        put_db(conn)
    bot.send_message(m.chat.id, "✅ Qoʻshildi.", reply_markup=kb_admin_reviews())


# ==================== ⚙️ BOSHQA SOZLAMALAR ====================
@bot.message_handler(func=lambda m: m.text == "⚙️ Boshqa" and is_admin(m.from_user.id))
def adm_other(m):
    status = "🟢 Yoqilgan" if get_setting("bot_active", True) else "🔴 Oʻchirilgan"
    maint = "🛠 Yoqilgan" if get_setting("maintenance", False) else "✅ Normal"
    stars = get_bot_stars_balance()
    text = (f"⚙️ <b>Boshqa sozlamalar</b>\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"Bot holati: <b>{status}</b>\n"
            f"Texnik ishlar: <b>{maint}</b>\n"
            f"⭐ Bot Stars: <b>{stars}</b>\n"
            f"🔄 Method: <b>{get_setting('gift_method', GIFT_METHOD)}</b>\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"👑 Adminlar: <code>{', '.join(str(a) for a in ADMIN_IDS)}</code>")
    bot.send_message(m.chat.id, text, reply_markup=kb_admin_other())


@bot.message_handler(func=lambda m: m.text == "🟢 Bot yoqish" and is_admin(m.from_user.id))
def adm_bot_on(m):
    set_setting("bot_active", True)
    log_admin_action(m.from_user.id, "bot_on")
    bot.send_message(m.chat.id, "✅ <b>Bot yoqildi!</b>", reply_markup=kb_admin_other())


@bot.message_handler(func=lambda m: m.text == "🔴 Bot oʻchirish" and is_admin(m.from_user.id))
def adm_bot_off(m):
    set_setting("bot_active", False)
    log_admin_action(m.from_user.id, "bot_off")
    bot.send_message(m.chat.id, "🔴 <b>Bot oʻchirildi!</b>", reply_markup=kb_admin_other())


@bot.message_handler(func=lambda m: m.text == "🛠 Texnik ishlar" and is_admin(m.from_user.id))
def adm_maint(m):
    cur = get_setting("maintenance", False)
    set_setting("maintenance", not cur)
    log_admin_action(m.from_user.id, "maintenance_on" if not cur else "maintenance_off")
    bot.send_message(m.chat.id,
        f"🛠 {'Yoqildi' if not cur else 'Oʻchirildi'}",
        reply_markup=kb_admin_other())


@bot.message_handler(func=lambda m: m.text == "💾 Backup" and is_admin(m.from_user.id))
def adm_backup(m):
    bot.send_message(m.chat.id,
        "💾 <b>Backup</b>\n\n"
        "Supabase PostgreSQL — avtomatik backup qiladi.\n"
        "Supabase panelida «Database → Backups» boʻlimida koʻring.",
        reply_markup=kb_admin_other())


# ==================== ADMIN HUMO QO'LDA ====================
@bot.message_handler(func=lambda m: is_admin(m.from_user.id) and m.text and
                     ("UZS" in m.text.upper() or "💰" in m.text) and
                     ("soʻm" in m.text.lower() or "сум" in m.text.lower() or "UZS" in m.text.upper()))
def adm_humo_manual(m):
    """Admin HUMO chekni qo'lda yuborsa — avto tasdiqlash"""
    a = parse_humo(m.text)
    if not a:
        return
    if process_humo(a, "admin-manual"):
        bot.send_message(m.chat.id, f"✅ {format_money(a)} tasdiqlandi")
    else:
        bot.send_message(m.chat.id,
            f"⚠️ {format_money(a)} — pending topilmadi.\n"
            f"User «➕ Toʻldirish» qilmagan boʻlishi mumkin.")


# ==================== QISM 16B TUGADI ====================
# Keyingi QISM 17: Flask webhook + startup + main
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 17/20 ====================
# Flask webhook + Render + startup
# =====================================================================

# ==================== FLASK ROUTES ====================

@app.route("/", methods=["GET"])
def flask_index():
    """Root — sog'liq tekshiruvi"""
    return {
        "status": "ok",
        "bot": "SovgaMarket",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "gift_method": get_setting("gift_method", GIFT_METHOD),
        "bot_active": get_setting("bot_active", True),
    }, 200


@app.route("/health", methods=["GET"])
def flask_health():
    """Health check — Render uchun"""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT 1")
        c.fetchone()
        put_db(conn)
        db_ok = True
    except Exception as e:
        db_ok = False
        print("Health DB error:", e)

    tg_ok = telethon_client is not None

    try:
        stars = get_bot_stars_balance()
    except Exception:
        stars = 0

    return {
        "status": "healthy" if db_ok else "degraded",
        "db": db_ok,
        "telethon": tg_ok,
        "stars": stars,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }, (200 if db_ok else 500)


@app.route(WEBHOOK_PATH, methods=["POST"])
def flask_webhook():
    """Telegram webhook endpoint."""
    if request.headers.get("content-type") == "application/json":
        try:
            json_string = request.get_data().decode("utf-8")
            update = types.Update.de_json(json_string)
            bot.process_new_updates([update])
            return "OK", 200
        except Exception as e:
            print(f"❌ Webhook process xato: {e}")
            return "ERROR", 500
    return "OK", 200


@app.route("/set_webhook", methods=["GET"])
def flask_set_webhook():
    """Webhook'ni qo'lda o'rnatish (bir marta)"""
    try:
        if not WEBHOOK_URL:
            return {"error": "WEBHOOK_URL env yoʻq"}, 400
        url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=url, drop_pending_updates=False)
        info = bot.get_webhook_info()
        return {
            "ok": True,
            "url": url,
            "webhook_info": {
                "url": info.url,
                "has_custom_certificate": info.has_custom_certificate,
                "pending_update_count": info.pending_update_count,
                "last_error_date": info.last_error_date,
                "last_error_message": info.last_error_message,
            }
        }, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


@app.route("/delete_webhook", methods=["GET"])
def flask_del_webhook():
    """Webhook'ni o'chirish (polling uchun)"""
    try:
        bot.remove_webhook()
        return {"ok": True, "message": "Webhook oʻchirildi"}, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


@app.route("/webhook_info", methods=["GET"])
def flask_webhook_info():
    """Webhook holati"""
    try:
        info = bot.get_webhook_info()
        return {
            "url": info.url,
            "pending_update_count": info.pending_update_count,
            "last_error_date": info.last_error_date,
            "last_error_message": info.last_error_message,
        }, 200
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/stats", methods=["GET"])
def flask_stats():
    """Qisqa statistika (JSON)"""
    try:
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("SELECT COUNT(*) as n FROM users")
            users = c.fetchone()["n"]
            c.execute("SELECT COUNT(*) as n FROM payments WHERE status='paid'")
            paid = c.fetchone()["n"]
            c.execute("SELECT COUNT(*) as n FROM gift_orders")
            orders = c.fetchone()["n"]
        finally:
            put_db(conn)

        return {
            "users": users,
            "paid_payments": paid,
            "gift_orders": orders,
            "stars": get_bot_stars_balance(),
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }, 200
    except Exception as e:
        return {"error": str(e)}, 500


# ==================== WEBHOOK O'RNATISH ====================
def setup_webhook():
    """Webhook'ni avtomatik o'rnatish (Render uchun)"""
    if not WEBHOOK_URL:
        print("⚠️ WEBHOOK_URL yoʻq — webhook oʻrnatilmadi")
        return False
    try:
        url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(
            url=url,
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query", "pre_checkout_query",
                             "inline_query", "chosen_inline_result"]
        )
        info = bot.get_webhook_info()
        print("=" * 50)
        print(f"✅ Webhook oʻrnatildi: {info.url}")
        print(f"📬 Kutilayotgan: {info.pending_update_count}")
        if info.last_error_message:
            print(f"⚠️ Oxirgi xato: {info.last_error_message}")
        print("=" * 50)
        return True
    except Exception as e:
        print(f"❌ Webhook oʻrnatishda xato: {e}")
        return False


# ==================== QISM 17 TUGADI ====================
# Keyingi QISM 18: Startup, default sovg'alar, run_flask
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 18/20 ====================
# Startup, default sovg'alar, run_flask, background tasklar
# =====================================================================

# ==================== STARTUP ====================
def startup():
    """Bot ishga tushganda bajariladigan vazifalar"""
    print("=" * 50)
    print("🚀 SOVGAMARKET BOT ishga tushmoqda...")
    print("=" * 50)

    # 1) DB
    try:
        init_db()
    except Exception as e:
        print(f"❌ DB xato: {e}")
        return False

    # 2) Default sovg'alar (agar bo'sh bo'lsa)
    try:
        seed_default_gifts()
    except Exception as e:
        print(f"⚠️ Default sovgʻalar xato: {e}")

    # 3) Settings cache
    try:
        refresh_settings_cache()
        print("✅ Settings cache yangilandi")
    except Exception as e:
        print(f"⚠️ Settings cache xato: {e}")

    # 4) Muddati o'tgan to'lovlar
    try:
        n = expire_pending()
        if n:
            print(f"⏰ {n} ta to'lov muddati oʻtdi")
    except Exception as e:
        print(f"⚠️ Expire xato: {e}")

    # 5) Expire checker thread
    try:
        start_expire_checker()
    except Exception as e:
        print(f"⚠️ Expire checker xato: {e}")

    # 6) Telethon listener
    try:
        start_telethon()
    except Exception as e:
        print(f"⚠️ Telethon xato: {e}")

    # 7) Bot ma'lumoti
    try:
        me = bot.get_me()
        print(f"✅ Bot: @{me.username} (id={me.id})")
    except Exception as e:
        print(f"⚠️ Bot get_me xato: {e}")

    # 8) Stars balans
    try:
        stars = get_bot_stars_balance()
        print(f"⭐ Bot Stars: {stars}")
    except Exception as e:
        print(f"⚠️ Stars balans xato: {e}")

    print("=" * 50)
    print("✅ Barcha tizimlar tayyor!")
    print("=" * 50)
    return True


# ==================== DEFAULT SOVG'ALAR ====================
def seed_default_gifts():
    """
    Agar gifts jadvali bo'sh bo'lsa, default sovg'alarni qo'shish.
    Faqat 1 marta ishlaydi.
    """
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(*) as n FROM gifts")
        n = c.fetchone()["n"]
        if n > 0:
            print(f"📦 {n} ta sovgʻa mavjud — seed kerak emas")
            return

        defaults = [
            # (nom, emoji, narx, stars, referral_needed)
            ("Ayiqcha", "🧸", 5000, 25, 5),
            ("Yulduzcha", "⭐", 15000, 50, 7),
            ("Yurakcha", "❤️", 25000, 75, 10),
            ("Gul", "🌹", 35000, 100, 12),
            ("Tort", "🎂", 50000, 150, 15),
            ("Olmos", "💎", 100000, 300, 20),
            ("Raketa", "🚀", 150000, 500, 25),
            ("Toj", "👑", 250000, 800, 30),
        ]

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for name, emoji, price, stars, ref in defaults:
            c.execute("""INSERT INTO gifts
                         (name, emoji, price, stars_price, stock, description,
                          tg_gift_id, referral_needed, is_active, created_at)
                         VALUES (%s, %s, %s, %s, 999999999, '', '', %s, 1, %s)""",
                      (name, emoji, price, stars, ref, now))
        conn.commit()
        print(f"✅ {len(defaults)} ta default sovgʻa qoʻshildi")
    except Exception as e:
        print(f"❌ Seed xato: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        put_db(conn)


# ==================== FLASK RUN (THREAD) ====================
def run_flask():
    """Flask'ni alohida thread'da ishga tushirish"""
    try:
        print(f"🌐 Flask ishga tushmoqda (port {PORT})...")
        app.run(
            host="0.0.0.0",
            port=PORT,
            debug=False,
            use_reloader=False,
            threaded=True,
        )
    except Exception as e:
        print(f"❌ Flask xato: {e}")


# ==================== BOT POLLING (FALLBACK) ====================
def run_polling():
    """
    Webhook o'rnatilmasa — polling rejimida ishlash.
    Render Free tier uchun zaxira variant.
    """
    try:
        print("🔄 Polling rejimi ishga tushmoqda...")
        bot.remove_webhook()
        time.sleep(1)
        bot.infinity_polling(
            timeout=60,
            long_polling_timeout=60,
            skip_pending=True,
            restart_on_change=False,
        )
    except Exception as e:
        print(f"❌ Polling xato: {e}")
        time.sleep(5)
        # Qayta urinish
        try:
            bot.infinity_polling(timeout=60, skip_pending=True)
        except Exception as e2:
            print(f"❌ Polling qayta urinish xato: {e2}")


# ==================== BACKGROUND KEEP-ALIVE ====================
def keep_alive_ping():
    """
    Render Free tier uxlab qolmasligi uchun har 10 daqiqada
    o'z-o'ziga ping yuborish.
    """
    if not WEBHOOK_URL:
        return

    def worker():
        time.sleep(60)  # Startdan keyin 1 daqiqa kutish
        while True:
            try:
                import urllib.request
                url = f"{WEBHOOK_URL}/health"
                req = urllib.request.Request(url, headers={"User-Agent": "SovgaMarket-Ping"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    _ = resp.read()
                print(f"💓 Keep-alive OK: {datetime.now().strftime('%H:%M:%S')}")
            except Exception as e:
                print(f"⚠️ Keep-alive xato: {str(e)[:100]}")
            time.sleep(600)  # 10 daqiqa

    Thread(target=worker, daemon=True).start()
    print("💓 Keep-alive thread ishga tushdi")


# ==================== QISM 18 TUGADI ====================
# Keyingi QISM 19: Global exception handler + admin buyruqlar
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 19/20 ====================
# Global exception handler + qo'shimcha admin buyruqlar
# =====================================================================

# ==================== GLOBAL XATO USHLAGICH ====================
@bot.message_handler(func=lambda m: True, content_types=[
    "text", "photo", "video", "document", "audio", "voice",
    "sticker", "location", "contact", "animation", "video_note"
])
def global_fallback(m):
    """
    Fallback handler — boshqa handlerlar ishlamasa.
    """
    uid = m.from_user.id

    # Admin uchun
    if is_admin(uid):
        bot.send_message(m.chat.id,
            "❓ <b>Buyruq topilmadi</b>\n\n"
            "Pastdagi menyudan tanlang yoki /help ni bosing.",
            reply_markup=kb_admin_main())
        return

    # User uchun
    u = get_user(uid)
    if not u:
        # Ro'yxatdan o'tmagan
        bot.send_message(m.chat.id,
            "👋 Iltimos, /start buyrugʻini yuboring.",
            reply_markup=types.ReplyKeyboardRemove())
        return

    if u["is_blocked"] == 1:
        bot.send_message(m.chat.id, "🚫 Siz bloklangansiz.")
        return

    if not get_setting("bot_active", True):
        bot.send_message(m.chat.id, "🔴 Bot vaqtincha oʻchirilgan.")
        return

    if get_setting("maintenance", False):
        bot.send_message(m.chat.id,
            "🛠 Texnik ishlar olib borilmoqda. Keyinroq urinib koʻring.")
        return

    bot.send_message(m.chat.id,
        "❓ <b>Buyruq topilmadi</b>\n\n"
        "Pastdagi menyudan tanlang yoki /help ni bosing.",
        reply_markup=kb_user_main(uid))


# ==================== ADMIN BUYRUQLAR ====================

@bot.message_handler(commands=['admin'])
def cmd_admin(m):
    """Tezkor admin panel"""
    if not is_admin(m.from_user.id):
        return
    bot.send_message(m.chat.id, "🔐 <b>Admin Panel</b>", reply_markup=kb_admin_main())


@bot.message_handler(commands=['ping'])
def cmd_ping(m):
    """Bot ishlashini tekshirish"""
    try:
        t0 = time.time()
        bot.send_message(m.chat.id, "🏓 Pong...")
        t1 = time.time()
        bot.send_message(m.chat.id,
            f"🏓 <b>Pong!</b>\n\n"
            f"⚡ Javob vaqti: <b>{(t1-t0)*1000:.0f} ms</b>")
    except Exception as e:
        print(f"Ping xato: {e}")


@bot.message_handler(commands=['id'])
def cmd_id(m):
    """Foydalanuvchi ID sini ko'rsatish"""
    bot.send_message(m.chat.id,
        f"🆔 <b>Sizning ID:</b>\n"
        f"<code>{m.from_user.id}</code>\n\n"
        f"👤 {m.from_user.full_name}\n"
        f"📛 @{m.from_user.username or 'yoq'}")


@bot.message_handler(commands=['myid'])
def cmd_myid(m):
    """Chat ID ni ko'rsatish (kanal ulash uchun)"""
    bot.send_message(m.chat.id,
        f"🆔 <b>Chat ID:</b>\n"
        f"<code>{m.chat.id}</code>\n\n"
        f"Tur: <b>{m.chat.type}</b>")


@bot.message_handler(commands=['broadcast'])
def cmd_bc(m):
    """Broadcast (admin)"""
    if not is_admin(m.from_user.id):
        return
    adm_bcast(m)


@bot.message_handler(commands=['reload'])
def cmd_reload(m):
    """Settings cache'ni yangilash (admin)"""
    if not is_admin(m.from_user.id):
        return
    try:
        refresh_settings_cache()
        bot.send_message(m.chat.id,
            "✅ <b>Settings cache yangilandi!</b>",
            reply_markup=kb_admin_main())
    except Exception as e:
        bot.send_message(m.chat.id, f"❌ Xato: {e}")


@bot.message_handler(commands=['telethon'])
def cmd_telethon_check(m):
    """Telethon holatini tekshirish (admin)"""
    if not is_admin(m.from_user.id):
        return

    if not API_ID or not API_HASH:
        bot.send_message(m.chat.id,
            "⚠️ API_ID / API_HASH yoʻq.\n"
            "Env'ga qoʻshing va qayta deploy qiling.",
            reply_markup=kb_admin_main())
        return

    ss = (os.getenv("TELETHON_STRING_SESSION") or "").strip()
    if not ss:
        bot.send_message(m.chat.id,
            "⚠️ <b>TELETHON_STRING_SESSION</b> yoʻq!\n\n"
            "Gift method userbot ishlamaydi.",
            reply_markup=kb_admin_main())
        return

    ok, info = test_userbot()
    if ok:
        bot.send_message(m.chat.id,
            f"✅ <b>Telethon ishlayapti</b>\n\n{info}",
            reply_markup=kb_admin_main())
    else:
        bot.send_message(m.chat.id,
            f"❌ <b>Telethon muammosi</b>\n\n{info}",
            reply_markup=kb_admin_main())


@bot.message_handler(commands=['humotest'])
def cmd_humo_test(m):
    """HUMO parser test (admin)"""
    if not is_admin(m.from_user.id):
        return
    test_humo_parse()
    bot.send_message(m.chat.id,
        "✅ Test natijalari konsolda (Render logs).",
        reply_markup=kb_admin_main())


@bot.message_handler(commands=['humo'])
def cmd_humo_parse(m):
    """HUMO matnini parse qilib ko'rsatish (admin)"""
    if not is_admin(m.from_user.id):
        return
    # /humo 5000,00 UZS
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(m.chat.id,
            "Format: <code>/humo 5.000,00 UZS</code>")
        return
    text = parts[1]
    amount = parse_humo(text)
    debug = parse_humo_debug(text)

    if not amount:
        bot.send_message(m.chat.id,
            f"❌ Summa aniqlanmadi.\n\n"
            f"Topilganlar: {debug}")
        return

    bot.send_message(m.chat.id,
        f"💰 <b>Aniqlangan summa:</b> {format_money(amount)}\n"
        f"🔢 <code>{amount}</code>\n\n"
        f"📊 <b>Barcha topilganlar:</b>\n" +
        "\n".join([f"• {d['value']} ← <code>{d['raw']}</code>"
                   for d in debug[:8]]))


@bot.message_handler(commands=['settlement'])
def cmd_settlement(m):
    """Tezkor settlement (admin) — barcha pending'larni ko'rish"""
    if not is_admin(m.from_user.id):
        return

    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""SELECT id, user_id, unique_amount, created_at
                     FROM payments WHERE status = 'pending'
                     ORDER BY id DESC LIMIT 20""")
        rows = c.fetchall()
    finally:
        put_db(conn)

    if not rows:
        bot.send_message(m.chat.id, "✅ Pending to'lov yo'q.",
                         reply_markup=kb_admin_main())
        return

    text = "⏳ <b>Pending to'lovlar:</b>\n\n"
    for r in rows:
        text += (f"#{r['id']} | <code>{r['user_id']}</code>\n"
                 f"💰 <b>{format_money(r['unique_amount'])}</b>\n"
                 f"📅 {r['created_at']}\n"
                 f"✅ /confirm {r['id']}\n\n")
    bot.send_message(m.chat.id, text[:4000], reply_markup=kb_admin_main())


@bot.message_handler(commands=['top'])
def cmd_top(m):
    """Top userlar (admin)"""
    if not is_admin(m.from_user.id):
        return
    adm_top(m)


@bot.message_handler(commands=['pending'])
def cmd_pending(m):
    """Yetkazilmagan buyurtmalar (admin)"""
    if not is_admin(m.from_user.id):
        return
    adm_pending(m)


# ==================== ESKI FREE GIFT TUGMASI (COMPAT) ====================
@bot.message_handler(func=lambda m: m.text == "🎁 Tekin" and not is_admin(m.from_user.id))
def msg_free_gift_compat(m):
    """Eski tugma — yangi funksiyaga yo'naltirish"""
    msg_free_gift(m)


# ==================== STARS TO'LOVNI QAYTA ISHLASH ====================
@bot.message_handler(content_types=['successful_payment'])
def on_successful_payment(m):
    """
    Stars to'lovi muvaffaqiyatli — duplicate handler uchun zaxira.
    (QISM 16B da ham bor, lekin bu yerda ham tasdiq)
    """
    try:
        sp = m.successful_payment
        payload = sp.invoice_payload or ""
        if payload.startswith("botstars_"):
            return  # QISM 16B handler ishlaydi
    except Exception:
        pass


# ==================== QISM 19 TUGADI ====================
# Keyingi QISM 20: MAIN — ishga tushirish (Render uchun)
# =====================================================================# ==================== SOVGAMARKET BOT — QISM 20/20 ====================
# MAIN — Render uchun ishga tushirish
# =====================================================================

# ==================== ASOSIY ISHGA TUSHIRISH ====================
def main():
    """
    Render uchun asosiy ishga tushirish funksiyasi.
    - Flask webhook server
    - Webhook o'rnatish
    - Startup vazifalar
    - Keep-alive ping
    """
    print("=" * 60)
    print("🎁 SOVGAMARKET BOT — START")
    print("=" * 60)
    print(f"🌐 Port: {PORT}")
    print(f"🔗 Webhook URL: {WEBHOOK_URL or 'yoʻq'}")
    print(f"🔗 Webhook Path: {WEBHOOK_PATH}")
    print(f"💾 DB: {'✅' if DATABASE_URL else '❌'}")
    print(f"⭐ Telethon: {'✅' if (API_ID and API_HASH) else '❌'}")
    print(f"🎁 Gift Method: {GIFT_METHOD}")
    print(f"👑 Adminlar: {ADMIN_IDS}")
    print("=" * 60)

    # ========== 1) STARTUP ==========
    ok = startup()
    if not ok:
        print("❌ Startup xato — davom etilmoqda (Flask baribir ishga tushadi)")

    # ========== 2) WEBHOOK yoki POLLING ==========
    if WEBHOOK_URL:
        # Webhook rejimi (Render uchun TAVSIYA)
        print("=" * 60)
        print("🔗 WEBHOOK REJIMI")
        print("=" * 60)

        # Webhook o'rnatish
        webhook_ok = setup_webhook()

        # Keep-alive ping
        keep_alive_ping()

        # Flask'ni ishga tushirish (asosiy thread)
        try:
            app.run(
                host="0.0.0.0",
                port=PORT,
                debug=False,
                use_reloader=False,
                threaded=True,
            )
        except Exception as e:
            print(f"❌ Flask xato: {e}")
            # Flask ishlamasa — polling'ga o'tish
            print("🔄 Polling'ga oʻtilmoqda...")
            run_polling()
    else:
        # Polling rejimi (lokal test uchun)
        print("=" * 60)
        print("🔄 POLLING REJIMI (WEBHOOK_URL yoʻq)")
        print("=" * 60)

        # Flask'ni alohida thread'da (ixtiyoriy)
        Thread(target=run_flask, daemon=True).start()
        time.sleep(2)

        # Polling'ni asosiy thread'da
        run_polling()


# ==================== EXCEPTION HANDLER ====================
def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Global xato ushlagich"""
    if issubclass(exc_type, KeyboardInterrupt):
        print("\n👋 Bot to'xtatildi (KeyboardInterrupt)")
        return

    print("=" * 60)
    print(f"❌ GLOBAL XATO: {exc_type.__name__}")
    print(f"📝 {exc_value}")
    print("=" * 60)

    import traceback
    traceback.print_exception(exc_type, exc_value, exc_traceback)

    # Adminga xabar (xato bo'lsa)
    try:
        for admin_id in ADMIN_IDS:
            if admin_id and admin_id != 0:
                try:
                    bot.send_message(admin_id,
                        f"❌ <b>Bot xatosi</b>\n\n"
                        f"📝 <code>{str(exc_value)[:200]}</code>")
                except Exception:
                    pass
    except Exception:
        pass


# ==================== ISHGA TUSHIRISH ====================
if __name__ == "__main__":
    import sys

    # Global xato ushlagich
    sys.excepthook = global_exception_handler

    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Bot to'xtatildi")
    except Exception as e:
        print(f"❌ FATAL: {e}")
        import traceback
        traceback.print_exc()


# ==================== RENDER UCHUN ====================
# Agar gunicorn ishlatilsa:
#   gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120
#
# Agar to'g'ridan-to'g'ri python bilan:
#   python app.py
#
# MUHIM ENV O'ZGARUVCHILAR (Render panelida):
#   BOT_TOKEN               — bot tokeni (@BotFather)
#   ADMIN_IDS               — admin ID'lar (vergul bilan: 123,456)
#   DATABASE_URL            — Supabase PostgreSQL URL
#   WEBHOOK_URL             — Render URL (https://your-app.onrender.com)
#   WEBHOOK_PATH            — /webhook
#   PORT                    — 10000
#   API_ID                  — my.telegram.org dan
#   API_HASH                — my.telegram.org dan
#   TELETHON_STRING_SESSION — userbot session (StringSession)
#   HUMO_BOT_USERNAME       — HUMOcardbot
#   CARD_NUMBER             — HUMO karta raqami
#   CARD_OWNER              — Karta egasi F.I.Sh
#   GIFT_METHOD             — bot / userbot / auto / manual
# =============================================================

# ==================== SOVGAMARKET BOT — TUGADI ====================
# 20/20 QISM TAYYOR ✅
# Bot to'liq ishlaydi:
#   1. Importlar + config
#   2. Database (Supabase)
#   3. Settings + helpers
#   4. User register + referal (4 tizim)
#   5. Sovg'alar CRUD
#   6. To'lovlar (unikal summa)
#   7. HUMO parser + avto-to'lov
#   8. Telethon + gift yuborish + isbot
#   9. User klaviaturalar
#  10. Admin klaviaturalar (15 tugma)
#  11. User: start, hisobim, to'ldirish
#  12. User: referal, sovg'alar, sotib olish
#  13. User: sharh, admin, isbotlar, tekin sovg'a
#  14. Admin: statistika, to'lovlar, sovg'alar
#  15. Admin: userlar, xabar, referal, to'lov sozlama
#  16. Admin: obuna, isbotlar, stars, adminlar
#  17. Flask webhook + Render
#  18. Startup + default sovg'alar
#  19. Global fallback + admin buyruqlar
#  20. MAIN (Render)
# =====================================================================