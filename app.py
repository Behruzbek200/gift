# ==================== SOVGAMARKET BOT ====================
# Flask + Webhook + PostgreSQL (Supabase) | Telegram sovg'alar (gift) sotuvi
# O'rnatish: pip install -r requirements.txt
# Ishga tushirish (lokal): python app.py
# Render: gunicorn app:app

import os
import re
import time
import json
import asyncio
from datetime import datetime, timedelta
from threading import Thread

from flask import Flask, request
import telebot
from telebot import types
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

# ==================== SOZLAMALAR ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]
DATABASE_URL = os.getenv("DATABASE_URL", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
PORT = int(os.getenv("PORT", "10000"))

API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "")
TELETHON_PHONE = os.getenv("TELETHON_PHONE", "")
HUMO_BOT_USERNAME = os.getenv("HUMO_BOT_USERNAME", "HUMOcardbot")

DEFAULT_SETTINGS = {
    "payment_time_minutes": 15,
    "start_amount": 5000,
    "referral_percent": 10,
    "referral_bonus": 500,
    "free_gift_referrals": 20,
    # Referal tizimlari yoq/o'ch (True/False)
    "ref_bonus_on": True,       # A) do'st kelganda pul
    "ref_percent_on": True,     # B) do'st sovg'a olsa foiz
    "ref_invites_on": True,     # C) N ta do'st = 1 tekin sovg'a huquqi
    "ref_orders_on": False,     # D) do'stlar N ta sovg'a olsa = 1 tekin
    "ref_orders_needed": 3,     # D uchun kerakli buyurtma soni
    "free_gift_id": 0,          # tekin sovg'a sifatida beriladigan gift id
    "bot_active": True,
    "maintenance": False,
    "card_number": os.getenv("DEFAULT_CARD", "8600123456789012"),
    "card_owner": os.getenv("DEFAULT_CARD_OWNER", "HUMO CARD"),
}

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=True)
app = Flask(__name__)

# ==================== DATABASE POOL + KESH ====================
_db_pool = None
_settings_cache = {}
_settings_cache_time = 0
_SETTINGS_TTL = 30  # soniya
_admin_cache = set(ADMIN_IDS)

def init_pool():
    global _db_pool
    if _db_pool is None and DATABASE_URL:
        _db_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=8,
            dsn=DATABASE_URL,
            cursor_factory=RealDictCursor,
        )
        print("✅ DB pool tayyor")

def get_db():
    if _db_pool is None:
        init_pool()
    conn = _db_pool.getconn()
    conn.autocommit = False
    return conn

def put_db(conn):
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

def init_db():
    conn = get_db()
    c = conn.cursor()
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
    try:
        c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_order_progress INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS free_gifts INTEGER DEFAULT 0")
    except Exception:
        pass
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount REAL,
            unique_amount REAL,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            paid_at TEXT,
            expires_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS gifts (
            id SERIAL PRIMARY KEY,
            name TEXT,
            emoji TEXT DEFAULT '🎁',
            price REAL DEFAULT 0,
            stock INTEGER DEFAULT 0,
            description TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS gift_orders (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            gift_id INTEGER,
            gift_name TEXT,
            price REAL,
            is_free INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            delivered_at TEXT
        )
    """)
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
    c.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id BIGINT PRIMARY KEY
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS referral_history (
            id SERIAL PRIMARY KEY,
            from_user BIGINT,
            to_user BIGINT,
            amount REAL,
            created_at TEXT
        )
    """)
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
    for key, value in DEFAULT_SETTINGS.items():
        c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", (key, str(value)))
    for admin_id in ADMIN_IDS:
        c.execute("INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (admin_id,))
    conn.commit()
    put_db(conn)
    print("✅ PostgreSQL database tayyor")

def _parse_setting_val(val):
    if val is None:
        return None
    if str(val).lower() in ("true", "false"):
        return str(val).lower() == "true"
    try:
        if "." in str(val):
            return float(val)
        return int(val)
    except Exception:
        return val

def get_setting(key, default=None):
    """Settings xotirada keshlanadi — tezroq javob"""
    global _settings_cache, _settings_cache_time
    now = time.time()
    if now - _settings_cache_time > _SETTINGS_TTL or not _settings_cache:
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("SELECT key, value FROM settings")
            rows = c.fetchall()
            _settings_cache = {r["key"]: r["value"] for r in rows}
            _settings_cache_time = now
        finally:
            put_db(conn)
    val = _settings_cache.get(key)
    if val is None:
        return default
    return _parse_setting_val(val)

def set_setting(key, value):
    global _settings_cache, _settings_cache_time
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, str(value)))
        conn.commit()
        _settings_cache[key] = str(value)
        _settings_cache_time = time.time()
    finally:
        put_db(conn)

def is_admin(user_id):
    if user_id in _admin_cache:
        return True
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT user_id FROM admins WHERE user_id = %s", (user_id,))
        result = c.fetchone()
        if result:
            _admin_cache.add(user_id)
            return True
        return False
    finally:
        put_db(conn)

def get_user(user_id):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        return c.fetchone()
    finally:
        put_db(conn)

def register_user(user_id, username, full_name, referred_by=0):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT INTO users (user_id, username, full_name, referred_by, joined_date, last_active)
                 VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING""",
              (user_id, username, full_name, referred_by, now, now))
    if referred_by and referred_by != user_id and username and str(username).strip():
        c.execute("SELECT user_id FROM users WHERE user_id = %s", (referred_by,))
        if c.fetchone():
            c.execute("SELECT id FROM referral_history WHERE to_user = %s", (user_id,))
            if not c.fetchone():
                c.execute("UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id = %s", (referred_by,))
                # A) Do'st kelganda pul
                if get_setting("ref_bonus_on", True):
                    bonus = get_setting("referral_bonus", 500)
                    c.execute("UPDATE users SET referral_balance = referral_balance + %s WHERE user_id = %s", (bonus, referred_by))
                    c.execute("INSERT INTO referral_history (from_user, to_user, amount, created_at) VALUES (%s, %s, %s, %s)",
                              (referred_by, user_id, bonus, now))
                    try:
                        bot.send_message(referred_by,
                            f"🎁 <b>Yangi doʻst!</b>\n\n"
                            f"+{format_money(bonus)} referal balansiga tushdi.")
                    except Exception:
                        pass
                else:
                    c.execute("INSERT INTO referral_history (from_user, to_user, amount, created_at) VALUES (%s, %s, %s, %s)",
                              (referred_by, user_id, 0, now))
                # C) N ta do'st = 1 tekin sovg'a
                if get_setting("ref_invites_on", True):
                    c.execute("SELECT referrals_count FROM users WHERE user_id = %s", (referred_by,))
                    count = c.fetchone()["referrals_count"]
                    needed = int(get_setting("free_gift_referrals", 20) or 20)
                    if needed > 0 and count >= needed:
                        c.execute("UPDATE users SET free_gifts = free_gifts + 1, referrals_count = referrals_count - %s WHERE user_id = %s",
                                  (needed, referred_by))
                        try:
                            bot.send_message(referred_by,
                                f"🎉 <b>Tekin sovgʻa huquqi!</b>\n\n"
                                f"{needed} ta doʻst uchun 1 ta tekin sovgʻa berildi.\n"
                                f"«🎁 Tekin sovgʻa» boʻlimidan oling.")
                        except Exception:
                            pass
    conn.commit()
    put_db(conn)

def process_referral_on_purchase(buyer_id, price):
    """Sovg'a sotib olinganda referal egasiga foiz + (D) tizim"""
    user = get_user(buyer_id)
    if not user or not user.get("referred_by"):
        return
    ref_id = user["referred_by"]
    # B) Foiz
    if get_setting("ref_percent_on", True):
        percent = float(get_setting("referral_percent", 10) or 0)
        if percent > 0 and price > 0:
            bonus = price * percent / 100
            update_balance(ref_id, bonus, is_referral=True)
            try:
                bot.send_message(ref_id,
                    f"💰 <b>Referal foiz</b>\n\n"
                    f"Doʻstingiz sovgʻa oldi.\n"
                    f"+{format_money(bonus)} ({percent}%)")
            except Exception:
                pass
    # D) Do'stlar N ta sovg'a olsa — tekin sovg'a
    if get_setting("ref_orders_on", False):
        needed = int(get_setting("ref_orders_needed", 3) or 3)
        if needed <= 0:
            return
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("""UPDATE users SET referral_order_progress = COALESCE(referral_order_progress,0) + 1
                         WHERE user_id = %s RETURNING referral_order_progress""", (ref_id,))
            row = c.fetchone()
            progress = row["referral_order_progress"] if row else 0
            if progress >= needed:
                c.execute("""UPDATE users SET free_gifts = free_gifts + 1,
                             referral_order_progress = referral_order_progress - %s
                             WHERE user_id = %s""", (needed, ref_id))
                conn.commit()
                try:
                    bot.send_message(ref_id,
                        f"🎁 <b>Tekin sovgʻa (referal)</b>\n\n"
                        f"Doʻstlaringiz jami <b>{needed}</b> ta sovgʻa sotib oldi.\n"
                        f"Sizga <b>1 ta tekin sovgʻa</b> huquqi berildi!\n"
                        f"👉 «🎁 Tekin sovgʻa» dan oling.")
                except Exception:
                    pass
            else:
                conn.commit()
                try:
                    bot.send_message(ref_id,
                        f"📊 Referal progress: <b>{progress}/{needed}</b>\n"
                        f"Doʻstlaringiz yana sovgʻa olsa — tekin sovgʻa olasiz.")
                except Exception:
                    pass
        finally:
            put_db(conn)

def update_balance(user_id, amount, is_referral=False):
    conn = get_db()
    c = conn.cursor()
    if is_referral:
        c.execute("UPDATE users SET referral_balance = referral_balance + %s WHERE user_id = %s", (amount, user_id))
    else:
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (amount, user_id))
    conn.commit()
    put_db(conn)

# ==================== SOVG'ALAR KATALOGI ====================
def get_active_gifts():
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT id, name, emoji, price, stock, description FROM gifts
                     WHERE is_active = 1 AND stock > 0 ORDER BY price ASC""")
        return c.fetchall()
    finally:
        put_db(conn)

def get_gift(gift_id):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM gifts WHERE id = %s", (gift_id,))
        return c.fetchone()
    finally:
        put_db(conn)

def build_gifts_keyboard(page=0, per_page=8):
    gifts = get_active_gifts()
    total = len(gifts)
    start = page * per_page
    end = start + per_page
    chunk = gifts[start:end]
    markup = types.InlineKeyboardMarkup(row_width=1)
    for g in chunk:
        btn_text = f"{g['emoji'] or '🎁'} {g['name']} — {format_money(g['price'])}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"buy_g_{g['id']}"))
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("⬅️ Oldingi", callback_data=f"gpage_{page-1}"))
    if end < total:
        nav.append(types.InlineKeyboardButton("Keyingi ➡️", callback_data=f"gpage_{page+1}"))
    if nav:
        markup.row(*nav)
    pages = max(1, (total + per_page - 1) // per_page)
    markup.add(types.InlineKeyboardButton(f"📄 {page+1}/{pages}", callback_data="noop"))
    markup.add(types.InlineKeyboardButton("🔙 Yopish", callback_data="close_gifts"))
    return markup, total, page

# ==================== KLAVIATURALAR ====================
def main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("🎁 Sovgʻa sotib olish"), types.KeyboardButton("🎁 Tekin sovgʻa"))
    markup.add(types.KeyboardButton("📋 Buyurtmalarim"), types.KeyboardButton("💰 Hisobim"))
    markup.add(types.KeyboardButton("👥 Referal tizimi"), types.KeyboardButton("💬 Sharhlar"))
    markup.add(types.KeyboardButton("📖 Qo'llanma"), types.KeyboardButton("📞 Adminga yozish"))
    markup.add(types.KeyboardButton("⚙️ Sozlamalar"))
    if is_admin(user_id):
        markup.add(types.KeyboardButton("🔐 Admin Panel"))
    return markup

def account_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("💵 Balansim"), types.KeyboardButton("➕ Hisob to'ldirish"))
    markup.add(types.KeyboardButton("📜 To'lov tarixi"), types.KeyboardButton("🔙 Orqaga"))
    return markup

def referral_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("🔗 Mening referal havolam"), types.KeyboardButton("👤 Taklif qilganlarim"))
    markup.add(types.KeyboardButton("💸 Referal balansi"), types.KeyboardButton("📊 Referal statistikasi"))
    markup.add(types.KeyboardButton("🔙 Orqaga"))
    return markup

def admin_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("📊 Statistika"), types.KeyboardButton("💰 To'lovlar"))
    markup.add(types.KeyboardButton("🎁 Sovgʻalar boshqaruvi"), types.KeyboardButton("👥 Foydalanuvchilar"))
    markup.add(types.KeyboardButton("📢 Xabar yuborish"), types.KeyboardButton("🎁 Referal sozlamalari"))
    markup.add(types.KeyboardButton("⏱ To'lov sozlamalari"), types.KeyboardButton("📢 Majburiy obuna"))
    markup.add(types.KeyboardButton("👨‍💼 Adminlar"), types.KeyboardButton("⚙️ Boshqa sozlamalar"))
    markup.add(types.KeyboardButton("🔙 Asosiy menyu"))
    return markup

def gifts_admin_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("➕ Sovgʻa qoʻshish"), types.KeyboardButton("📋 Sovgʻalar roʻyxati"))
    markup.add(types.KeyboardButton("💵 Narxini oʻzgartirish"), types.KeyboardButton("📦 Miqdorini oʻzgartirish"))
    markup.add(types.KeyboardButton("❌ Sovgʻa oʻchirish"), types.KeyboardButton("🎯 Tekin sovgʻani belgilash"))
    markup.add(types.KeyboardButton("🧾 Yetkazilmagan buyurtmalar"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def payment_settings_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("⏱ To'lov vaqtini o'zgartirish"), types.KeyboardButton("💵 Boshlang'ich summa"))
    markup.add(types.KeyboardButton("💳 Karta raqami"), types.KeyboardButton("👤 Karta egasi"))
    markup.add(types.KeyboardButton("🔙 Admin Panel"))
    return markup

def referral_settings_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("📊 Referal foizi"), types.KeyboardButton("💰 1 referal uchun pul"))
    markup.add(types.KeyboardButton("🎁 Tekin sovgʻa uchun soni"), types.KeyboardButton("🛒 Buyurtma soni (yangi)"))
    markup.add(types.KeyboardButton("⚙️ Referal yoq/o'ch"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def channels_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("➕ Kanal qo'shish"), types.KeyboardButton("❌ Kanal o'chirish"))
    markup.add(types.KeyboardButton("📋 Kanallar ro'yxati"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def admins_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("➕ Admin qo'shish"), types.KeyboardButton("❌ Admin o'chirish"))
    markup.add(types.KeyboardButton("📋 Adminlar ro'yxati"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def other_settings_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("🟢 Botni yoqish"), types.KeyboardButton("🔴 Botni o'chirish"))
    markup.add(types.KeyboardButton("🛠 Texnik ishlar"), types.KeyboardButton("🔙 Admin Panel"))
    return markup

def users_admin_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(types.KeyboardButton("📋 Userlar roʻyxati"), types.KeyboardButton("🔍 Foydalanuvchi qidirish"))
    markup.add(types.KeyboardButton("💵 Balans oʻzgartirish"), types.KeyboardButton("🚫 Bloklash / Ochish"))
    markup.add(types.KeyboardButton("🔙 Admin Panel"))
    return markup

def back_only():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("🔙 Orqaga"))
    return markup

# ==================== YORDAMCHI ====================
def format_money(amount):
    try:
        return f"{float(amount):,.0f}".replace(",", " ") + " soʻm"
    except Exception:
        return f"{amount} soʻm"

def is_amount_busy(amount):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM payments WHERE status = 'pending' AND unique_amount = %s", (amount,))
    row = c.fetchone()
    put_db(conn)
    return row is not None

def get_unique_amount(desired=None):
    start = get_setting("start_amount", 5000)
    amount = int(desired) if desired is not None else int(start)
    if amount < 1000:
        amount = int(start)
    for _ in range(50):
        if not is_amount_busy(amount):
            return amount
        amount += 1
    return amount

def expire_pending_payments():
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""SELECT id, user_id, unique_amount FROM payments
                 WHERE status = 'pending' AND expires_at IS NOT NULL AND expires_at < %s""", (now,))
    rows = c.fetchall()
    for row in rows:
        c.execute("UPDATE payments SET status = 'expired' WHERE id = %s", (row["id"],))
        try:
            bot.send_message(row["user_id"],
                f"⏰ <b>Toʻlov vaqti tugadi!</b>\n\n"
                f"💰 Summa: <b>{format_money(row['unique_amount'])}</b>\n"
                f"Vaqt ichida toʻlov qilinmadi.\n"
                f"Yangi toʻlov uchun «➕ Hisob toʻldirish» ni bosing.")
        except Exception:
            pass
    conn.commit()
    put_db(conn)
    return len(rows)

def check_subscription(user_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT channel_id, channel_username, channel_title, is_private, COALESCE(invite_link,'') as invite_link FROM channels")
    channels = c.fetchall()
    put_db(conn)
    if not channels:
        return True, []
    not_subscribed = []
    for ch in channels:
        try:
            member = bot.get_chat_member(ch["channel_id"], user_id)
            if member.status in ["left", "kicked"]:
                display = ch["channel_title"] or ch["channel_username"] or ch["channel_id"]
                url = ch["invite_link"] or (f"https://t.me/{ch['channel_username'].lstrip('@')}" if ch["channel_username"] else None)
                not_subscribed.append((display, url))
        except Exception:
            display = ch["channel_title"] or ch["channel_username"] or ch["channel_id"]
            url = ch["invite_link"] or (f"https://t.me/{ch['channel_username'].lstrip('@')}" if ch["channel_username"] else None)
            not_subscribed.append((display, url))
    return len(not_subscribed) == 0, not_subscribed

# ==================== HUMO TO'LOV (AVTO) ====================
def parse_humo_amount(text):
    """HUMO / bank xabaridan summani aniq ajratib olish"""
    if not text:
        return None
    text_n = (
        text.replace("\u00a0", " ")
        .replace(" ", " ")
        .replace("'", "'")
    )
    candidates = []
    patterns = [
        r'[\+➕]\s*([\d\s]+[.,]\d{2})\s*(?:UZS|сўм|so[ʻ\']?m|сум|som)',
        r'(?:kirim|приход|income|поступление)[^\d]{0,20}([\d\s]+[.,]\d{2})',
        r'(?:сумма|summa|amount|суммаси|miqdor)[:\s]*([\d\s]+[.,]?\d*)',
        r'💰\s*([\d\s]+[.,]\d{2})\s*UZS',
        r'([\d\s]+[.,]\d{2})\s*UZS',
        r'[\+➕]\s*([\d\s.,]+)\s*(?:UZS|so[ʻ\']?m)?',
        r'(\d{3,7})[.,]00\b',
        r'\b(\d{4,7})\b\s*(?:UZS|so[ʻ\']?m|сум)',
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
                    candidates.append(val)
            except Exception:
                continue
    if candidates:
        return candidates[0]
    for m in re.finditer(r'\b(\d{4,7})\b', text_n.replace(" ", "")):
        try:
            val = int(m.group(1))
            if 1000 <= val <= 50_000_000:
                return val
        except Exception:
            pass
    return None

_recent_humo = {}  # amount -> timestamp (takroriy xabarni oldini olish)

def process_humo_payment(amount, source="HUMO"):
    """Pending to'lovni topib avtomatik tasdiqlash — batafsil xabarlar"""
    amount = float(amount)
    now_ts = time.time()
    if amount in _recent_humo and now_ts - _recent_humo[amount] < 30:
        print(f"⏭ Takroriy HUMO o'tkazib yuborildi: {amount}")
        return False
    _recent_humo[amount] = now_ts
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT id, user_id, unique_amount, created_at, expires_at FROM payments
                     WHERE status = 'pending' AND unique_amount = %s
                     ORDER BY id DESC LIMIT 1""", (amount,))
        row = c.fetchone()
        if not row:
            c.execute("""SELECT id, user_id, unique_amount, created_at, expires_at FROM payments
                         WHERE status = 'pending'
                         AND unique_amount BETWEEN %s AND %s
                         ORDER BY id DESC LIMIT 1""", (amount - 2, amount + 2))
            row = c.fetchone()
        if not row:
            print(f"⚠️ [{source}] Mos pending yoʻq: {amount}")
            for admin_id in ADMIN_IDS:
                try:
                    bot.send_message(admin_id,
                        f"⚠️ <b>Avto-toʻlov: mos pending topilmadi</b>\n\n"
                        f"💰 Kelgan summa: <b>{format_money(amount)}</b>\n"
                        f"📡 Manba: {source}\n\n"
                        f"User hali «Hisob toʻldirish» qilmagan yoki summa mos kelmadi.\n"
                        f"Qoʻlda: /confirm ID yoki Balans oʻzgartirish.")
                except Exception:
                    pass
            return False

        pay_id = row["id"]
        user_id = row["user_id"]
        unique_amount = row["unique_amount"]
        created_at = row.get("created_at") or "—"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        c.execute("UPDATE payments SET status = 'paid', paid_at = %s WHERE id = %s", (now, pay_id))
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (unique_amount, user_id))
        c.execute("SELECT balance, full_name, username FROM users WHERE user_id = %s", (user_id,))
        u = c.fetchone()
        new_bal = u["balance"] if u else unique_amount
        uname = f"@{u['username']}" if u and u.get("username") else "—"
        fname = (u.get("full_name") if u else "") or "—"
        conn.commit()
    finally:
        put_db(conn)

    try:
        bot.send_message(user_id,
            f"✅ <b>Toʻlov muvaffaqiyatli tasdiqlandi!</b>\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💰 Toʻldirildi: <b>{format_money(unique_amount)}</b>\n"
            f"💵 Joriy balans: <b>{format_money(new_bal)}</b>\n"
            f"🧾 Chek №: <code>{pay_id}</code>\n"
            f"🕒 Vaqt: {now}\n"
            f"📡 Usul: avtomatik ({source})\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"Endi «🎁 Sovgʻa sotib olish» orqali sovgʻa olishingiz mumkin.\n"
            f"Savol boʻlsa — «📞 Adminga yozish».")
    except Exception as e:
        print("Notify user error:", e)

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
                f"📡 Manba: {source}")
        except Exception:
            pass
    print(f"✅ [{source}] Avto-to'lov OK: user={user_id} amount={unique_amount} pay=#{pay_id}")
    return True

def start_telethon_listener():
    """
    @HUMOcardbot xabarlarini 24/7 kuzatadi.
    Render uchun: TELETHON_STRING_SESSION env (kod so'ralmaydi).
    """
    if not API_ID or not API_HASH:
        print("⚠️ API_ID/API_HASH yoʻq — Telethon avto-toʻlov oʻchiq")
        return
    try:
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession
    except ImportError:
        print("⚠️ telethon yoʻq: pip install telethon")
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
        print("Render da kod kiritib boʻlmaydi.")
        print("Lokalda generate_session.py ni ishga tushiring,")
        print("chiqqan STRING ni Render Environment ga qoʻying.")
        print("=" * 50)
        return

    async def run_listener():
        client = TelegramClient(
            StringSession(string_session),
            API_ID,
            API_HASH,
            device_model="SovgaMarket",
            system_version="1.0",
            app_version="1.0",
        )
        print("🔌 Telethon StringSession bilan ulanmoqda (kod soʻralmaydi)...")
        await client.connect()
        if not await client.is_user_authorized():
            print("❌ StringSession yaroqsiz yoki muddati oʻtgan. Qayta generate_session.py qiling.")
            await client.disconnect()
            return
        me = await client.get_me()
        print(f"✅ Telethon OK: id={me.id} username=@{getattr(me, 'username', None)} (StringSession)")

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
            amount = parse_humo_amount(text)
            if amount:
                print(f"💰 [{tag}] Summa: {amount}")
                process_humo_payment(amount, source=tag)
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
                if uname not in humo_names and not (humo_entity and sender and getattr(sender, "id", None) == getattr(humo_entity, "id", None)):
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

def start_expire_checker():
    def worker():
        while True:
            try:
                n = expire_pending_payments()
                if n:
                    print(f"⏰ {n} ta to'lov muddati o'tdi")
            except Exception as e:
                print("Expire error:", e)
            time.sleep(30)
    Thread(target=worker, daemon=True).start()
    print("🔄 Expire checker ishga tushdi")

# ==================== HANDLERS (asosiy) ====================
@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.from_user.id
    username = message.from_user.username or ""
    full_name = message.from_user.full_name or ""
    referred_by = 0
    if len(message.text.split()) > 1:
        try:
            ref_code = message.text.split()[1]
            if ref_code.startswith("ref"):
                referred_by = int(ref_code[3:])
        except Exception:
            pass
    register_user(user_id, username, full_name, referred_by)
    user = get_user(user_id)
    if user and user["is_blocked"] == 1:
        bot.send_message(user_id, "🚫 Siz bloklangansiz.")
        return
    is_sub, missing = check_subscription(user_id)
    if not is_sub:
        text = "⚠️ <b>Kanallarga obuna boʻling:</b>\n\nKeyin «✅ Tekshirish» ni bosing."
        markup = types.InlineKeyboardMarkup()
        for display, url in missing:
            if url:
                markup.add(types.InlineKeyboardButton(f"📢 {display}", url=url))
            else:
                markup.add(types.InlineKeyboardButton(f"📢 {display}", callback_data="no_link"))
        markup.add(types.InlineKeyboardButton("✅ Tekshirish", callback_data="check_sub"))
        bot.send_message(user_id, text, reply_markup=markup)
        return
    if not get_setting("bot_active", True):
        bot.send_message(user_id, "🔴 Bot vaqtincha o'chirilgan.")
        return
    if get_setting("maintenance", False) and not is_admin(user_id):
        bot.send_message(user_id, "🛠 Texnik ishlar olib borilmoqda.")
        return
    user = get_user(user_id)
    bal = format_money(user["balance"]) if user else "0 soʻm"
    bot.send_message(user_id,
        f"🎉 <b>Assalomu alaykum, {full_name}!</b>\n\n"
        f"🎁 <b>SovgaMarket</b> — Telegram sovgʻalari (gift) doʻkoni\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💵 Balans: <b>{bal}</b>\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"• Sovgʻa sotib olish\n"
        f"• Hisobni avtomatik toʻldirish (HUMO)\n"
        f"• Referal orqali tekin sovgʻa\n\n"
        f"Pastdagi menyudan tanlang 👇",
        reply_markup=main_menu(user_id))

@bot.callback_query_handler(func=lambda call: call.data == "check_sub")
def check_sub_callback(call):
    is_sub, missing = check_subscription(call.from_user.id)
    if is_sub:
        bot.answer_callback_query(call.id, "✅ Obuna tasdiqlandi!")
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass

        class FakeMsg:
            def __init__(self, user):
                self.from_user = user
                self.text = "/start"
                self.chat = type('obj', (object,), {'id': user.id})()
        start(FakeMsg(call.from_user))
    else:
        bot.answer_callback_query(call.id, "❌ Hali obuna bo'lmadingiz!", show_alert=True)

@bot.message_handler(func=lambda m: m.text in ["🔙 Orqaga", "🔙 Asosiy menyu"])
def back_to_main(message):
    bot.send_message(message.chat.id, "🏠 Asosiy menyu", reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "🔙 Admin Panel")
def back_to_admin(message):
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "🏠 Asosiy menyu", reply_markup=main_menu(message.from_user.id))
        return
    bot.send_message(message.chat.id, "🔐 <b>Admin Panel</b>", reply_markup=admin_menu())

@bot.message_handler(func=lambda m: m.text == "💰 Hisobim")
def my_account(message):
    bot.send_message(message.chat.id, "💰 <b>Hisobim</b>", reply_markup=account_menu())

@bot.message_handler(func=lambda m: m.text == "👥 Referal tizimi")
def referral_system(message):
    bot.send_message(message.chat.id, "👥 <b>Referal tizimi</b>", reply_markup=referral_menu())

@bot.message_handler(func=lambda m: m.text == "📖 Qo'llanma")
def guide(message):
    minutes = get_setting("payment_time_minutes", 15)
    bonus = get_setting("referral_bonus", 500)
    percent = get_setting("referral_percent", 10)
    needed = get_setting("free_gift_referrals", 20)
    text = f"""📖 <b>SovgaMarket — toʻliq qoʻllanma</b>

🎁 Telegram sovgʻalarini (gift) xavfsiz sotib oling. Toʻlov — avtomatik, sovgʻa yetkazish — admin tomonidan.

━━━━━━━━━━━━━━━━
💰 <b>1. Hisob toʻldirish (AVTO)</b>
1) «💰 Hisobim» → «➕ Hisob toʻldirish»
2) Summani yozing (masalan 10000)
3) Bot bergan <b>unique summa</b> va kartaga toʻlang
4) {minutes} daqiqa ichida toʻlov tushsa — balans <b>avtomatik</b> toʻldiriladi
5) Taymer tugasa — soʻrov bekor, qayta urinib koʻring

🎁 <b>2. Sovgʻa sotib olish</b>
1) «🎁 Sovgʻa sotib olish»
2) Kataloqdan sovgʻani tanlang
3) Tasdiqlang — balansdan yechiladi, buyurtma admin panelga tushadi
4) Admin sovgʻani Telegram akkauntingizga yuboradi va buyurtmani "yetkazildi" deb belgilaydi

🎁 <b>3. Referal / tekin sovgʻa</b>
• Doʻst taklif: +{format_money(bonus)}
• Doʻst sovgʻa olsa: {percent}%
• {needed} ta haqiqiy doʻst = 1 tekin sovgʻa

📋 <b>4. Buyurtmalar</b> — barcha olingan sovgʻalar tarixi

📞 Muammo boʻlsa — «Adminga yozish»"""
    bot.send_message(message.chat.id, text, reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "⚙️ Sozlamalar")
def settings_user(message):
    bot.send_message(message.chat.id,
        f"⚙️ <b>Sozlamalar</b>\n\n🆔 <code>{message.from_user.id}</code>\n👤 @{message.from_user.username or 'yoq'}",
        reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "💵 Balansim")
def my_balance(message):
    user = get_user(message.from_user.id)
    if not user:
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE user_id = %s", (message.from_user.id,))
    orders = c.fetchone()["n"]
    c.execute("SELECT COUNT(*) as n FROM payments WHERE user_id = %s AND status = 'paid'", (message.from_user.id,))
    pays = c.fetchone()["n"]
    put_db(conn)
    text = (f"💵 <b>Balans va statistika</b>\n\n"
            f"💰 Asosiy: <b>{format_money(user['balance'])}</b>\n"
            f"🎁 Referal: <b>{format_money(user['referral_balance'])}</b>\n"
            f"🎁 Bepul sovgʻa huquqi: <b>{user['free_gifts']} ta</b>\n"
            f"👥 Referallar: <b>{user['referrals_count']}</b>\n"
            f"📋 Buyurtmalar: <b>{orders}</b>\n"
            f"✅ Toʻlovlar: <b>{pays}</b>")
    bot.send_message(message.chat.id, text, reply_markup=account_menu())

@bot.message_handler(func=lambda m: m.text == "➕ Hisob to'ldirish")
def top_up(message):
    start_amt = get_setting("start_amount", 5000)
    msg = bot.send_message(message.chat.id,
        f"➕ <b>Hisob toʻldirish</b>\n\nQancha summa?\nMasalan: <code>{start_amt}</code>\n\n/cancel — bekor",
        reply_markup=back_only())
    bot.register_next_step_handler(msg, process_top_up_amount)

def _payment_text(amount, card, owner, note, remain_sec):
    if remain_sec < 0:
        remain_sec = 0
    mm = remain_sec // 60
    ss = remain_sec % 60
    timer = f"{mm:02d}:{ss:02d}"
    if remain_sec <= 0:
        timer_line = "⏰ <b>Vaqt tugadi!</b> — toʻlov bekor qilindi"
    elif remain_sec <= 60:
        timer_line = f"⏱ Qoldi: <b>{timer}</b> ⚠️ shoshiling!"
    else:
        timer_line = f"⏱ Qoldi: <b>{timer}</b> (daqiqa:soniya)"
    return f"""➕ <b>Hisob toʻldirish</b>
{note}
━━━━━━━━━━━━━━━━
💳 <b>Toʻlov summasi:</b> <code>{int(amount)}</code> soʻm
🏦 <b>Karta raqami:</b> <code>{card}</code>
👤 <b>Karta egasi:</b> {owner}
{timer_line}
━━━━━━━━━━━━━━━━

📌 <b>Qoidalar:</b>
1. Aynan <b>{int(amount)}</b> soʻm tashlang
2. ⚠️ Boshqa summa tashlasangiz — <b>toʻlov hisobingizga tushmaydi!</b>
3. Toʻlov tushishi bilan balans <b>avtomatik</b> toʻldiriladi
4. Vaqt tugasa soʻrov bekor — qayta «Hisob toʻldirish»
5. 1–3 daqiqa kuting (bank kechiksa)"""

def start_payment_timer(chat_id, message_id, user_id, amount, card, owner, note, expires_dt, markup):
    def worker():
        while True:
            remain = int((expires_dt - datetime.now()).total_seconds())
            conn = get_db()
            try:
                c = conn.cursor()
                c.execute("""SELECT status FROM payments
                             WHERE user_id = %s AND unique_amount = %s AND status = 'pending'
                             ORDER BY id DESC LIMIT 1""", (user_id, amount))
                row = c.fetchone()
            finally:
                put_db(conn)
            if not row:
                return
            if remain <= 0:
                try:
                    bot.edit_message_text(
                        f"⏰ <b>Toʻlov vaqti tugadi!</b>\n\n"
                        f"💰 Summa: <b>{format_money(amount)}</b>\n"
                        f"Yangi toʻlov uchun «➕ Hisob toʻldirish» ni bosing.",
                        chat_id, message_id
                    )
                except Exception:
                    pass
                expire_pending_payments()
                return
            text = _payment_text(amount, card, owner, note, remain)
            try:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
            except Exception:
                pass
            time.sleep(10 if remain <= 60 else 15)

    Thread(target=worker, daemon=True).start()

def process_top_up_amount(message):
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "Bekor.", reply_markup=account_menu())
        return
    try:
        desired = int(str(message.text).replace(" ", "").replace(",", "").replace(".", ""))
        if desired < 1000:
            bot.send_message(message.chat.id, "Minimal 1000 soʻm.", reply_markup=back_only())
            bot.register_next_step_handler(message, process_top_up_amount)
            return
    except Exception:
        bot.send_message(message.chat.id, "Faqat raqam (masalan 5000):", reply_markup=back_only())
        bot.register_next_step_handler(message, process_top_up_amount)
        return

    expire_pending_payments()
    user_id = message.from_user.id
    amount = get_unique_amount(desired)
    minutes = int(get_setting("payment_time_minutes", 15))
    card = get_setting("card_number", "8600123456789012")
    owner = get_setting("card_owner", "HUMO CARD")
    now_dt = datetime.now()
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    expires_dt = now_dt + timedelta(minutes=minutes)
    expires = expires_dt.strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO payments (user_id, amount, unique_amount, status, created_at, expires_at)
                     VALUES (%s, %s, %s, 'pending', %s, %s)""", (user_id, amount, amount, now, expires))
        conn.commit()
    finally:
        put_db(conn)

    note = ""
    if amount != desired:
        note = f"\n⚠️ {format_money(desired)} band edi. Sizga: <b>{format_money(amount)}</b>\n"

    remain = minutes * 60
    text = _payment_text(amount, card, owner, note, remain)

    markup = types.InlineKeyboardMarkup(row_width=1)
    try:
        markup.add(types.InlineKeyboardButton(f"📋 Summa ({int(amount)})", copy_text=types.CopyTextButton(text=str(int(amount)))))
        markup.add(types.InlineKeyboardButton("📋 Karta", copy_text=types.CopyTextButton(text=str(card))))
    except Exception:
        markup.add(types.InlineKeyboardButton("📋 Summa", callback_data=f"copy_amt_{int(amount)}"))
        markup.add(types.InlineKeyboardButton("📋 Karta", callback_data="copy_card"))

    sent = bot.send_message(message.chat.id, text, reply_markup=markup)
    bot.send_message(message.chat.id, "Toʻlov qilgach balans avtomatik toʻldiriladi.", reply_markup=account_menu())
    start_payment_timer(message.chat.id, sent.message_id, user_id, amount, card, owner, note, expires_dt, markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_amt_") or call.data == "copy_card")
def copy_fallback(call):
    if call.data.startswith("copy_amt_"):
        amt = call.data.replace("copy_amt_", "")
        bot.answer_callback_query(call.id, f"Summa: {amt}", show_alert=True)
        bot.send_message(call.from_user.id, f"<code>{amt}</code>")
    else:
        card = get_setting("card_number", "")
        bot.send_message(call.from_user.id, f"<code>{card}</code>")
        bot.answer_callback_query(call.id)

def _user_pay_history_page(uid, page=0, per_page=10):
    expire_pending_payments()
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE user_id = %s AND status = 'paid'", (uid,))
        paid = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE user_id = %s AND status = 'pending'", (uid,))
        pend = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE user_id = %s AND status = 'expired'", (uid,))
        exp = c.fetchone()
        c.execute("SELECT COUNT(*) as n FROM payments WHERE user_id = %s", (uid,))
        total = c.fetchone()["n"]
        c.execute("""SELECT id, unique_amount, status, created_at, paid_at FROM payments
                     WHERE user_id = %s ORDER BY id DESC LIMIT %s OFFSET %s""",
                  (uid, per_page, page * per_page))
        rows = c.fetchall()
    finally:
        put_db(conn)
    text = (f"📜 <b>Toʻlov tarixi</b>\n\n"
            f"✅ Toʻlangan: <b>{paid['n']}</b> — {format_money(paid['s'])}\n"
            f"⏳ Kutilmoqda: <b>{pend['n']}</b> — {format_money(pend['s'])}\n"
            f"⏰ Muddati oʻtgan: <b>{exp['n']}</b> — {format_money(exp['s'])}\n"
            f"📊 Jami yozuv: <b>{total}</b>\n\n")
    if not rows:
        text += "Hali toʻlov yoʻq."
    else:
        for r in rows:
            st = {"paid": "✅", "expired": "⏰", "pending": "⏳"}.get(r["status"], "❓")
            text += f"{st} #{r['id']} | <b>{format_money(r['unique_amount'])}</b>\n📅 {r['created_at']}\n\n"
    pages = max(1, (total + per_page - 1) // per_page)
    markup = types.InlineKeyboardMarkup(row_width=3)
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("⬅️", callback_data=f"upay_{page-1}"))
    nav.append(types.InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if (page + 1) * per_page < total:
        nav.append(types.InlineKeyboardButton("➡️", callback_data=f"upay_{page+1}"))
    if nav:
        markup.row(*nav)
    return text[:4000], markup

@bot.message_handler(func=lambda m: m.text == "📜 To'lov tarixi" or m.text == "📜 Toʻlov tarixi")
def payment_history(message):
    text, markup = _user_pay_history_page(message.from_user.id, 0)
    bot.send_message(message.chat.id, text, reply_markup=markup)
    bot.send_message(message.chat.id, "Menyu:", reply_markup=account_menu())

@bot.callback_query_handler(func=lambda call: call.data.startswith("upay_"))
def user_pay_page(call):
    page = int(call.data.replace("upay_", ""))
    text, markup = _user_pay_history_page(call.from_user.id, page)
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception:
        pass
    bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda m: m.text == "🔗 Mening referal havolam")
def my_ref_link(message):
    user_id = message.from_user.id
    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref{user_id}"
    user = get_user(user_id)
    bonus = get_setting("referral_bonus", 500)
    percent = get_setting("referral_percent", 10)
    needed = int(get_setting("free_gift_referrals", 20) or 20)
    orders_needed = int(get_setting("ref_orders_needed", 3) or 3)
    refs = user["referrals_count"] if user else 0
    free = user["free_gifts"] if user else 0
    rbal = format_money(user["referral_balance"]) if user else "0 soʻm"
    progress = (user.get("referral_order_progress") or 0) if user else 0
    left = max(0, needed - refs) if needed else 0

    rules = []
    n = 1
    if get_setting("ref_bonus_on", True):
        rules.append(f"{n}️⃣ Doʻst roʻyxatdan oʻtsa → <b>{format_money(bonus)}</b>")
        n += 1
    if get_setting("ref_percent_on", True):
        rules.append(f"{n}️⃣ Doʻst sovgʻa olsa → sizga <b>{percent}%</b>")
        n += 1
    if get_setting("ref_invites_on", True):
        rules.append(f"{n}️⃣ Har <b>{needed}</b> ta doʻst → <b>1 tekin sovgʻa</b>")
        n += 1
    if get_setting("ref_orders_on", False):
        rules.append(f"{n}️⃣ Doʻstlar jami <b>{orders_needed}</b> ta sovgʻa olsa → <b>1 tekin sovgʻa</b>")
        n += 1
    if not rules:
        rules.append("Hozircha referal bonuslari oʻchirilgan.")

    text = (
        f"🔗 <b>Sizning referal havolangiz</b>\n\n"
        f"<code>{link}</code>\n\n"
        f"📋 <b>Qanday ishlaydi?</b>\n"
        + "\n".join(rules) +
        f"\n\n📊 <b>Sizning natijangiz</b>\n"
        f"👥 Taklif qilganlar: <b>{refs}</b> ta\n"
        f"🎁 Tekin sovgʻa huquqi: <b>{free}</b> ta\n"
        f"💸 Referal balansi: <b>{rbal}</b>\n"
    )
    if get_setting("ref_invites_on", True) and needed:
        text += f"🎯 Keyingi tekin (doʻstlar): yana <b>{left}</b> ta\n"
    if get_setting("ref_orders_on", False):
        text += f"🛒 Buyurtma progress: <b>{progress}/{orders_needed}</b>\n"
    text += "\n⚠️ Faqat username bor akkauntlar hisobga olinadi."
    bot.send_message(message.chat.id, text, reply_markup=referral_menu())

@bot.message_handler(func=lambda m: m.text == "👤 Taklif qilganlarim")
def my_referrals(message):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT full_name, username, joined_date FROM users WHERE referred_by = %s ORDER BY joined_date DESC LIMIT 20",
              (message.from_user.id,))
    rows = c.fetchall()
    put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "👤 Hali yoʻq.", reply_markup=referral_menu())
        return
    text = "👤 <b>Takliflar:</b>\n\n"
    for r in rows:
        text += f"• {r['full_name']} (@{r['username'] or 'yoq'}) — {r['joined_date']}\n"
    bot.send_message(message.chat.id, text, reply_markup=referral_menu())

@bot.message_handler(func=lambda m: m.text == "💸 Referal balansi")
def ref_balance(message):
    user = get_user(message.from_user.id)
    bot.send_message(message.chat.id, f"💸 Referal: <b>{format_money(user['referral_balance'] if user else 0)}</b>",
                     reply_markup=referral_menu())

@bot.message_handler(func=lambda m: m.text == "📊 Referal statistikasi")
def ref_stats(message):
    user = get_user(message.from_user.id)
    needed = get_setting("free_gift_referrals", 20)
    text = f"📊 Referallar: <b>{user['referrals_count'] if user else 0}</b>\n🎁 Bepul: <b>{user['free_gifts'] if user else 0}</b>\n🎯 Kerakli: <b>{needed}</b>"
    bot.send_message(message.chat.id, text, reply_markup=referral_menu())

# ==================== SOVG'A SOTIB OLISH ====================
@bot.message_handler(func=lambda m: m.text == "🎁 Sovgʻa sotib olish")
def select_gift(message):
    chat_id = message.chat.id
    gifts = get_active_gifts()
    if not gifts:
        text = "❌ <b>Hozircha kataloqda sovgʻa yoʻq.</b>"
        if is_admin(message.from_user.id):
            text += "\n\n👉 Admin: «🎁 Sovgʻalar boshqaruvi» → «➕ Sovgʻa qoʻshish»"
        bot.send_message(chat_id, text, reply_markup=main_menu(message.from_user.id))
        return
    markup, total, page = build_gifts_keyboard(0)
    text = f"🎁 <b>Sovgʻalar kataloqi</b>\nJami: <b>{total}</b> ta\n\nTanlang:"
    bot.send_message(chat_id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("gpage_"))
def gifts_page(call):
    page = int(call.data.replace("gpage_", ""))
    markup, total, page = build_gifts_keyboard(page)
    try:
        bot.edit_message_text(f"🎁 <b>Sovgʻalar kataloqi</b>\nJami: <b>{total}</b>\nSahifa: {page+1}",
                              call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception:
        pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "close_gifts")
def close_gifts(call):
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "noop")
def noop_cb(call):
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("buy_g_"))
def process_buy_gift(call):
    gift_id = int(call.data.replace("buy_g_", ""))
    user_id = call.from_user.id
    user = get_user(user_id)
    if not user or user["is_blocked"] == 1:
        bot.answer_callback_query(call.id, "🚫 Bloklangansiz.", show_alert=True)
        return
    g = get_gift(gift_id)
    if not g or g["is_active"] != 1 or g["stock"] <= 0:
        bot.answer_callback_query(call.id, "Sovgʻa topilmadi yoki tugagan.", show_alert=True)
        return
    price = g["price"]
    bal = user["balance"] or 0
    emoji = g["emoji"] or "🎁"

    markup = types.InlineKeyboardMarkup(row_width=2)
    if bal < price:
        markup.add(types.InlineKeyboardButton("➕ Hisob toʻldirish", callback_data="goto_topup"))
        markup.add(types.InlineKeyboardButton("🔙 Orqaga", callback_data="back_gifts"))
        try:
            bot.edit_message_text(
                f"{emoji} <b>{g['name']}</b>\n\n"
                f"💵 Narxi: <b>{format_money(price)}</b>\n"
                f"💰 Sizning balans: <b>{format_money(bal)}</b>\n\n"
                f"❌ <b>Hisobingizda yetarli mablagʻ yoʻq.</b>\n"
                f"Kerak: <b>{format_money(price - bal)}</b> yana toʻldiring.",
                call.message.chat.id, call.message.message_id, reply_markup=markup)
        except Exception:
            pass
        bot.answer_callback_query(call.id, "Hisobingizda yetarli mablagʻ yoʻq.", show_alert=True)
        return

    markup.add(
        types.InlineKeyboardButton("✅ Tasdiqlash", callback_data=f"confirm_buy_g_{gift_id}"),
        types.InlineKeyboardButton("❌ Bekor", callback_data="cancel_buy"),
    )
    markup.add(types.InlineKeyboardButton("🔙 Kataloq", callback_data="back_gifts"))
    desc = f"\n\n{g['description']}" if g.get("description") else ""
    bot.edit_message_text(
        f"{emoji} <b>{g['name']}</b>{desc}\n\n"
        f"💵 Narxi: <b>{format_money(price)}</b>\n"
        f"💰 Balans: <b>{format_money(bal)}</b>\n\n"
        f"Shu sovgʻani sotib olasizmi?\n\n"
        f"⚠️ <b>Diqqat:</b>\n"
        f"• Toʻlovdan soʻng buyurtma admin panelga tushadi\n"
        f"• Admin sovgʻani Telegram akkauntingizga yuboradi\n"
        f"• Muammo boʻlsa — pul balansga qaytariladi",
        call.message.chat.id, call.message.message_id, reply_markup=markup)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "back_gifts")
def back_gifts(call):
    markup, total, page = build_gifts_keyboard(0)
    try:
        bot.edit_message_text(
            f"🎁 <b>Sovgʻalar kataloqi</b>\nJami: <b>{total}</b> ta\n\nTanlang:",
            call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception:
        pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "goto_topup")
def goto_topup(call):
    bot.answer_callback_query(call.id)
    bot.send_message(call.from_user.id,
                     "➕ <b>Hisob toʻldirish</b>\n\nQancha summa tashlamoqchisiz?\nMasalan: <code>10000</code>\n\n/cancel — bekor",
                     reply_markup=back_only())
    bot.register_next_step_handler_by_chat_id(call.from_user.id, process_top_up_amount)

@bot.callback_query_handler(func=lambda call: call.data == "cancel_buy")
def cancel_buy(call):
    bot.edit_message_text("❌ Bekor qilindi.", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_buy_g_"))
def confirm_buy_gift(call):
    gift_id = int(call.data.replace("confirm_buy_g_", ""))
    user_id = call.from_user.id
    user = get_user(user_id)
    if not user or user["is_blocked"] == 1:
        bot.answer_callback_query(call.id, "🚫 Bloklangansiz.", show_alert=True)
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM gifts WHERE id = %s AND is_active = 1 FOR UPDATE", (gift_id,))
        g = c.fetchone()
        if not g or g["stock"] <= 0:
            conn.rollback()
            put_db(conn)
            bot.answer_callback_query(call.id, "Sovgʻa tugagan.", show_alert=True)
            return
        price = g["price"]
        c.execute("SELECT balance FROM users WHERE user_id = %s FOR UPDATE", (user_id,))
        u = c.fetchone()
        bal = u["balance"] if u else 0
        if bal < price:
            conn.rollback()
            put_db(conn)
            bot.answer_callback_query(call.id, "Hisobingizda yetarli mablagʻ yoʻq.", show_alert=True)
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE users SET balance = balance - %s WHERE user_id = %s", (price, user_id))
        c.execute("UPDATE gifts SET stock = stock - 1 WHERE id = %s", (gift_id,))
        c.execute("""INSERT INTO gift_orders (user_id, gift_id, gift_name, price, is_free, status, created_at)
                     VALUES (%s, %s, %s, %s, 0, 'pending', %s) RETURNING id""",
                  (user_id, gift_id, g["name"], price, now))
        order_id = c.fetchone()["id"]
        conn.commit()
    finally:
        put_db(conn)

    try:
        process_referral_on_purchase(user_id, price)
    except Exception as e:
        print("referral on purchase:", e)

    emoji = g["emoji"] or "🎁"
    uname = f"@{call.from_user.username}" if call.from_user.username else "—"
    bot.edit_message_text(
        f"✅ <b>Buyurtma qabul qilindi!</b>\n\n"
        f"{emoji} <b>{g['name']}</b>\n"
        f"💵 {format_money(price)}\n"
        f"🧾 Buyurtma №: <code>{order_id}</code>\n\n"
        f"Sovgʻa tez orada Telegram akkauntingizga yuboriladi.\n"
        f"Admin siz bilan bogʻlanadi.",
        call.message.chat.id, call.message.message_id)
    bot.send_message(user_id, "Asosiy menyu", reply_markup=main_menu(user_id))

    for admin_id in ADMIN_IDS:
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("✅ Yuborildi deb belgilash", callback_data=f"deliver_g_{order_id}"))
            bot.send_message(admin_id,
                f"🧾 <b>Yangi sovgʻa buyurtmasi</b>\n\n"
                f"{emoji} <b>{g['name']}</b>\n"
                f"💵 {format_money(price)}\n"
                f"👤 {call.from_user.full_name} ({uname})\n"
                f"🆔 <code>{user_id}</code>\n"
                f"🧾 Buyurtma №: <code>{order_id}</code>\n\n"
                f"Sovgʻani foydalanuvchining Telegram akkauntiga qoʻlda yuboring, keyin tugmani bosing.",
                reply_markup=markup)
        except Exception:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("deliver_g_"))
def deliver_gift_cb(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Faqat admin", show_alert=True)
        return
    order_id = int(call.data.replace("deliver_g_", ""))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM gift_orders WHERE id = %s", (order_id,))
        order = c.fetchone()
        if not order:
            put_db(conn)
            bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
            return
        if order["status"] == "delivered":
            put_db(conn)
            bot.answer_callback_query(call.id, "Allaqachon belgilangan.", show_alert=True)
            return
        c.execute("UPDATE gift_orders SET status = 'delivered', delivered_at = %s WHERE id = %s", (now, order_id))
        conn.commit()
    finally:
        put_db(conn)
    try:
        bot.send_message(order["user_id"],
            f"🎉 <b>Sovgʻangiz yuborildi!</b>\n\n"
            f"🎁 {order['gift_name']}\n"
            f"🧾 Buyurtma №: <code>{order_id}</code>\n\n"
            f"Rahmat! Sharh qoldirishni unutmang 💬")
    except Exception:
        pass
    bot.answer_callback_query(call.id, "✅ Yetkazildi deb belgilandi.", show_alert=True)
    try:
        bot.edit_message_text(
            call.message.text + "\n\n✅ <b>YETKAZILDI</b>",
            call.message.chat.id, call.message.message_id)
    except Exception:
        pass

# ==================== TEKIN SOVG'A ====================
@bot.message_handler(func=lambda m: m.text == "🎁 Tekin sovgʻa")
def free_gift(message):
    user = get_user(message.from_user.id)
    if not user:
        return
    needed = get_setting("free_gift_referrals", 20)
    rights = user["free_gifts"]
    gift_id = int(get_setting("free_gift_id", 0) or 0)
    g = get_gift(gift_id) if gift_id else None
    if rights < 1:
        bot.send_message(message.chat.id,
            f"❌ Huquq yoʻq. {needed} ta doʻst kerak (hozir: {user['referrals_count']})",
            reply_markup=main_menu(message.from_user.id))
        return
    if not g or g["is_active"] != 1:
        bot.send_message(message.chat.id,
            "🎁 Sizda tekin sovgʻa huquqi bor, lekin admin hali tekin sovgʻani belgilamagan.\n"
            "«📞 Adminga yozish» orqali murojaat qiling.",
            reply_markup=main_menu(message.from_user.id))
        return
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(f"✅ {g['emoji'] or '🎁'} {g['name']} olish", callback_data=f"take_free_{gift_id}"))
    bot.send_message(message.chat.id,
        f"🎁 <b>Tekin sovgʻa</b>\n\n✅ Huquq: <b>{rights}</b> ta\n\nSizga: {g['emoji'] or '🎁'} <b>{g['name']}</b>",
        reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("take_free_"))
def take_free_gift(call):
    gift_id = int(call.data.replace("take_free_", ""))
    user_id = call.from_user.id
    user = get_user(user_id)
    if not user or user["free_gifts"] < 1:
        bot.answer_callback_query(call.id, "Huquq yetarli emas!", show_alert=True)
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT * FROM gifts WHERE id = %s AND is_active = 1 FOR UPDATE", (gift_id,))
        g = c.fetchone()
        if not g or g["stock"] <= 0:
            put_db(conn)
            bot.answer_callback_query(call.id, "Sovgʻa hozircha omborda yoʻq.", show_alert=True)
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE users SET free_gifts = free_gifts - 1 WHERE user_id = %s", (user_id,))
        c.execute("UPDATE gifts SET stock = stock - 1 WHERE id = %s", (gift_id,))
        c.execute("""INSERT INTO gift_orders (user_id, gift_id, gift_name, price, is_free, status, created_at)
                     VALUES (%s, %s, %s, 0, 1, 'pending', %s) RETURNING id""",
                  (user_id, gift_id, g["name"], now))
        order_id = c.fetchone()["id"]
        conn.commit()
    finally:
        put_db(conn)
    emoji = g["emoji"] or "🎁"
    uname = f"@{call.from_user.username}" if call.from_user.username else "—"
    bot.edit_message_text(
        f"🎉 <b>Tekin sovgʻa buyurtmasi qabul qilindi!</b>\n\n"
        f"{emoji} {g['name']}\n🧾 №<code>{order_id}</code>\n\nTez orada yuboriladi.",
        call.message.chat.id, call.message.message_id)
    bot.send_message(user_id, "Asosiy menyu", reply_markup=main_menu(user_id))
    for admin_id in ADMIN_IDS:
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("✅ Yuborildi deb belgilash", callback_data=f"deliver_g_{order_id}"))
            bot.send_message(admin_id,
                f"🧾 <b>Yangi TEKIN sovgʻa buyurtmasi</b>\n\n"
                f"{emoji} <b>{g['name']}</b>\n"
                f"👤 {call.from_user.full_name} ({uname})\n"
                f"🆔 <code>{user_id}</code>\n"
                f"🧾 №<code>{order_id}</code>",
                reply_markup=markup)
        except Exception:
            pass

@bot.message_handler(func=lambda m: m.text == "📋 Buyurtmalarim")
def my_orders(message):
    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT gift_name, price, is_free, status, created_at FROM gift_orders
                 WHERE user_id = %s ORDER BY id DESC LIMIT 20""", (message.from_user.id,))
    rows = c.fetchall()
    put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "📋 Buyurtmalar yoʻq.", reply_markup=main_menu(message.from_user.id))
        return
    text = "📋 <b>Buyurtmalar:</b>\n\n"
    for r in rows:
        tip = "🎁 Tekin" if r["is_free"] else f"💵 {format_money(r['price'])}"
        st = "✅ Yetkazildi" if r["status"] == "delivered" else "⏳ Kutilmoqda"
        text += f"🎁 <b>{r['gift_name']}</b>\n{tip} | {st}\n📅 {r['created_at']}\n\n"
    bot.send_message(message.chat.id, text[:4000], reply_markup=main_menu(message.from_user.id))

@bot.message_handler(func=lambda m: m.text == "📞 Adminga yozish")
def write_admin(message):
    msg = bot.send_message(message.chat.id, "📞 Xabaringizni yozing:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_write_admin)

def process_write_admin(message):
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "Bekor.", reply_markup=main_menu(message.from_user.id))
        return
    uname = f"@{message.from_user.username}" if message.from_user.username else "—"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("↩️ Javob berish", callback_data=f"reply_user_{message.from_user.id}"))
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id,
                f"📞 <b>Yangi xabar</b>\n\n👤 {message.from_user.full_name} ({uname})\n🆔 <code>{message.from_user.id}</code>\n\n{message.text}",
                reply_markup=markup)
        except Exception:
            pass
    bot.send_message(message.chat.id, "✅ Xabaringiz yuborildi. Tez orada javob beramiz.", reply_markup=main_menu(message.from_user.id))

@bot.callback_query_handler(func=lambda call: call.data.startswith("reply_user_"))
def admin_reply_start(call):
    if not is_admin(call.from_user.id):
        return
    target_id = int(call.data.replace("reply_user_", ""))
    msg = bot.send_message(call.from_user.id, f"💬 <code>{target_id}</code> ga javob:\n/cancel")
    bot.register_next_step_handler(msg, lambda m: process_admin_reply(m, target_id))
    bot.answer_callback_query(call.id)

def process_admin_reply(message, target_id):
    if not is_admin(message.from_user.id):
        return
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=admin_menu())
        return
    try:
        bot.send_message(target_id, f"📞 <b>Admin javobi:</b>\n\n{message.text}")
        bot.send_message(message.chat.id, "✅ Yuborildi.", reply_markup=admin_menu())
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Xato: {e}", reply_markup=admin_menu())

# ==================== ADMIN PANEL ====================
@bot.message_handler(func=lambda m: m.text == "🔐 Admin Panel")
def admin_panel(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, "🔐 <b>Admin Panel</b>", reply_markup=admin_menu())

@bot.message_handler(func=lambda m: m.text == "📊 Statistika")
def admin_stats(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM users")
        total = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM users WHERE username IS NOT NULL AND username != ''")
        real = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM users WHERE is_blocked = 1")
        blocked = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status = 'paid'")
        paid = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status = 'pending'")
        pend = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status = 'expired'")
        exp = c.fetchone()
        c.execute("SELECT COUNT(*) as n FROM payments")
        all_p = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE status = 'delivered'")
        delivered = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE status = 'pending'")
        pending_orders = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM gift_orders WHERE is_free = 1")
        free_n = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n FROM gifts WHERE is_active = 1")
        gifts_count = c.fetchone()["n"]
        c.execute("SELECT COALESCE(SUM(balance),0) as s FROM users")
        bal_sum = c.fetchone()["s"]
        c.execute("SELECT COUNT(*) as n FROM users WHERE DATE(joined_date) = CURRENT_DATE")
        today_u = c.fetchone()["n"]
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='paid' AND DATE(paid_at)=CURRENT_DATE")
        today_p = c.fetchone()
    finally:
        put_db(conn)
    text = f"""📊 <b>Toʻliq statistika</b>

👥 <b>Foydalanuvchilar</b>
• Jami: <b>{total}</b>
• Haqiqiy (@username): <b>{real}</b>
• Bloklangan: <b>{blocked}</b>
• Bugun qoʻshilgan: <b>{today_u}</b>
• Userlar balansi jami: <b>{format_money(bal_sum)}</b>

💰 <b>Toʻlovlar (butun davr)</b>
• ✅ Tasdiqlangan: <b>{paid['n']}</b> — {format_money(paid['s'])}
• ⏳ Kutilayotgan: <b>{pend['n']}</b> — {format_money(pend['s'])}
• ⏰ Muddati oʻtgan: <b>{exp['n']}</b> — {format_money(exp['s'])}
• 📋 Jami yozuv: <b>{all_p}</b>
• 📅 Bugun tushgan: <b>{today_p['n']}</b> — {format_money(today_p['s'])}

🎁 <b>Sovgʻalar</b>
• Yetkazilgan buyurtma: <b>{delivered}</b>
• Kutilayotgan buyurtma: <b>{pending_orders}</b>
• Tekin berilgan: <b>{free_n}</b>
• Faol sovgʻa turi: <b>{gifts_count}</b>"""
    bot.send_message(message.chat.id, text, reply_markup=admin_menu())

# --- Sovg'alar boshqaruvi ---
@bot.message_handler(func=lambda m: m.text == "🎁 Sovgʻalar boshqaruvi")
def gifts_admin(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id,
                     "🎁 <b>Sovgʻalar boshqaruvi</b>\n\n"
                     "➕ Yangi sovgʻa qoʻshish\n"
                     "📋 Roʻyxat — narx/miqdorni koʻrish, faollashtirish/oʻchirish\n"
                     "🧾 Yetkazilmagan buyurtmalarni koʻrish",
                     reply_markup=gifts_admin_menu())

@bot.message_handler(func=lambda m: m.text and "Sovgʻa qoʻshish" in m.text)
def add_gift_start(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id,
        "➕ <b>Yangi sovgʻa</b>\n\n"
        "Format (har biri alohida qatorda):\n"
        "<code>Nomi\nEmoji (masalan 🎁)\nNarxi\nMiqdori</code>\n\n"
        "Misol:\n<code>Yulduzcha\n⭐\n15000\n10</code>\n\n/cancel",
        reply_markup=back_only())
    bot.register_next_step_handler(msg, process_add_gift)

def process_add_gift(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "🎁", reply_markup=gifts_admin_menu())
        return
    lines = [l.strip() for l in message.text.split("\n") if l.strip()]
    if len(lines) < 4:
        bot.send_message(message.chat.id, "❌ 4 qator kerak: Nomi, Emoji, Narxi, Miqdori.", reply_markup=gifts_admin_menu())
        return
    name, emoji, price_s, stock_s = lines[0], lines[1], lines[2], lines[3]
    try:
        price = float(price_s.replace(" ", "").replace(",", ""))
        stock = int(stock_s)
    except Exception:
        bot.send_message(message.chat.id, "❌ Narx/miqdor notoʻgʻri.", reply_markup=gifts_admin_menu())
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO gifts (name, emoji, price, stock, is_active, created_at)
                     VALUES (%s, %s, %s, %s, 1, %s) RETURNING id""",
                  (name, emoji or "🎁", price, stock, now))
        gid = c.fetchone()["id"]
        conn.commit()
    finally:
        put_db(conn)
    bot.send_message(message.chat.id, f"✅ Qoʻshildi: {emoji} {name} — {format_money(price)} ({stock} dona) [#{gid}]",
                     reply_markup=gifts_admin_menu())

def _gifts_list_text():
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT id, name, emoji, price, stock, is_active FROM gifts ORDER BY id DESC")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        return "📋 Sovgʻalar yoʻq."
    text = "📋 <b>Barcha sovgʻalar</b>\n\n"
    for r in rows:
        st = "✅" if r["is_active"] else "🚫"
        text += f"{st} #{r['id']} {r['emoji'] or '🎁'} <b>{r['name']}</b>\n💵 {format_money(r['price'])} | 📦 {r['stock']} dona\n\n"
    return text[:4000]

@bot.message_handler(func=lambda m: m.text and "Sovgʻalar roʻyxati" in m.text)
def list_gifts_admin(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, _gifts_list_text(), reply_markup=gifts_admin_menu())

@bot.message_handler(func=lambda m: m.text and "Narxini oʻzgartirish" in m.text)
def set_gift_price_start(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Format: <code>ID YANGI_NARX</code>\nMasalan: <code>3 20000</code>\n/cancel",
                           reply_markup=back_only())
    bot.register_next_step_handler(msg, process_set_gift_price)

def process_set_gift_price(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "🎁", reply_markup=gifts_admin_menu())
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Notoʻgʻri format.", reply_markup=gifts_admin_menu())
        return
    try:
        gid = int(parts[0])
        price = float(parts[1])
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("UPDATE gifts SET price = %s WHERE id = %s", (price, gid))
            n = c.rowcount
            conn.commit()
        finally:
            put_db(conn)
        bot.send_message(message.chat.id, f"✅ Yangilandi." if n else "Topilmadi.", reply_markup=gifts_admin_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat son.", reply_markup=gifts_admin_menu())

@bot.message_handler(func=lambda m: m.text and "Miqdorini oʻzgartirish" in m.text)
def set_gift_stock_start(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Format: <code>ID YANGI_MIQDOR</code>\nMasalan: <code>3 25</code>\n/cancel",
                           reply_markup=back_only())
    bot.register_next_step_handler(msg, process_set_gift_stock)

def process_set_gift_stock(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "🎁", reply_markup=gifts_admin_menu())
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Notoʻgʻri format.", reply_markup=gifts_admin_menu())
        return
    try:
        gid = int(parts[0])
        stock = int(parts[1])
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("UPDATE gifts SET stock = %s WHERE id = %s", (stock, gid))
            n = c.rowcount
            conn.commit()
        finally:
            put_db(conn)
        bot.send_message(message.chat.id, "✅ Yangilandi." if n else "Topilmadi.", reply_markup=gifts_admin_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat son.", reply_markup=gifts_admin_menu())

@bot.message_handler(func=lambda m: m.text and "Sovgʻa oʻchirish" in m.text)
def delete_gift_start(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Oʻchiriladigan sovgʻa ID:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_delete_gift)

def process_delete_gift(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "🎁", reply_markup=gifts_admin_menu())
        return
    try:
        gid = int(message.text.strip())
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("UPDATE gifts SET is_active = 0 WHERE id = %s", (gid,))
            n = c.rowcount
            conn.commit()
        finally:
            put_db(conn)
        bot.send_message(message.chat.id, "✅ Oʻchirildi (nofaol qilindi)." if n else "Topilmadi.",
                         reply_markup=gifts_admin_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat ID.", reply_markup=gifts_admin_menu())

@bot.message_handler(func=lambda m: m.text and "Tekin sovgʻani belgilash" in m.text)
def set_free_gift_start(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id,
        "Referal orqali tekin beriladigan sovgʻa ID sini yozing.\n"
        "(«📋 Sovgʻalar roʻyxati» dan ID ni koʻring)\n/cancel",
        reply_markup=back_only())
    bot.register_next_step_handler(msg, process_set_free_gift)

def process_set_free_gift(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "🎁", reply_markup=gifts_admin_menu())
        return
    try:
        gid = int(message.text.strip())
        g = get_gift(gid)
        if not g:
            bot.send_message(message.chat.id, "Topilmadi.", reply_markup=gifts_admin_menu())
            return
        set_setting("free_gift_id", gid)
        bot.send_message(message.chat.id, f"✅ Tekin sovgʻa: {g['emoji'] or '🎁'} {g['name']}", reply_markup=gifts_admin_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat ID.", reply_markup=gifts_admin_menu())

@bot.message_handler(func=lambda m: m.text and "Yetkazilmagan buyurtmalar" in m.text)
def pending_gift_orders(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT id, user_id, gift_name, price, is_free, created_at FROM gift_orders
                     WHERE status = 'pending' ORDER BY id ASC LIMIT 30""")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "✅ Yetkazilmagan buyurtma yoʻq.", reply_markup=gifts_admin_menu())
        return
    for r in rows:
        tip = "🎁 Tekin" if r["is_free"] else format_money(r["price"])
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✅ Yuborildi deb belgilash", callback_data=f"deliver_g_{r['id']}"))
        bot.send_message(message.chat.id,
            f"🧾 №<code>{r['id']}</code>\n🎁 {r['gift_name']} | {tip}\n"
            f"👤 <code>{r['user_id']}</code>\n📅 {r['created_at']}",
            reply_markup=markup)
    bot.send_message(message.chat.id, "👆 Barcha kutilayotgan buyurtmalar.", reply_markup=gifts_admin_menu())

# --- Xabar yuborish ---
@bot.message_handler(func=lambda m: m.text == "📢 Xabar yuborish")
def broadcast(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "📢 Xabar yuboring (matn/rasm/video):\n/cancel", reply_markup=types.ReplyKeyboardRemove())
    bot.register_next_step_handler(msg, process_broadcast)

def process_broadcast(message):
    if not is_admin(message.from_user.id):
        return
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=admin_menu())
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE is_blocked = 0")
    users = c.fetchall()
    put_db(conn)
    ok = fail = 0
    for u in users:
        try:
            if message.content_type == "text":
                bot.send_message(u["user_id"], message.text)
            else:
                bot.copy_message(u["user_id"], message.chat.id, message.message_id)
            ok += 1
            time.sleep(0.04)
        except Exception:
            fail += 1
    bot.send_message(message.chat.id, f"✅ {ok} | ❌ {fail}", reply_markup=admin_menu())

# --- To'lov sozlamalari ---
@bot.message_handler(func=lambda m: m.text == "⏱ To'lov sozlamalari")
def payment_settings(message):
    if not is_admin(message.from_user.id):
        return
    text = (f"⏱ Vaqt: {get_setting('payment_time_minutes', 15)} daq\n"
            f"💵 Start: {format_money(get_setting('start_amount', 5000))}\n"
            f"💳 {get_setting('card_number')}\n👤 {get_setting('card_owner')}")
    bot.send_message(message.chat.id, text, reply_markup=payment_settings_menu())

@bot.message_handler(func=lambda m: m.text == "⏱ To'lov vaqtini o'zgartirish")
def change_payment_time(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Daqiqa (1-120):", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "payment_time_minutes", payment_settings_menu))

@bot.message_handler(func=lambda m: m.text == "💵 Boshlang'ich summa")
def change_start_amount(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Yangi summa:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "start_amount", payment_settings_menu))

@bot.message_handler(func=lambda m: m.text == "💳 Karta raqami")
def change_card(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Yangi karta:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_str_setting(m, "card_number", payment_settings_menu))

@bot.message_handler(func=lambda m: m.text == "👤 Karta egasi")
def change_owner(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Yangi ism:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_str_setting(m, "card_owner", payment_settings_menu))

def _set_int_setting(message, key, menu_func):
    if not is_admin(message.from_user.id):
        return
    if message.text == "🔙 Orqaga":
        bot.send_message(message.chat.id, "Orqaga", reply_markup=menu_func())
        return
    try:
        set_setting(key, int(message.text))
        bot.send_message(message.chat.id, f"✅ {message.text}", reply_markup=menu_func())
    except Exception:
        bot.send_message(message.chat.id, "Faqat son.", reply_markup=menu_func())

def _set_str_setting(message, key, menu_func):
    if not is_admin(message.from_user.id):
        return
    if message.text == "🔙 Orqaga":
        bot.send_message(message.chat.id, "Orqaga", reply_markup=menu_func())
        return
    set_setting(key, message.text.strip())
    bot.send_message(message.chat.id, f"✅ {message.text.strip()}", reply_markup=menu_func())

def _ref_status(key):
    return "✅ YOQIQ" if get_setting(key, True) else "❌ OʻCHIQ"

@bot.message_handler(func=lambda m: m.text == "🎁 Referal sozlamalari")
def ref_settings(message):
    if not is_admin(message.from_user.id):
        return
    text = (
        f"🎁 <b>Referal sozlamalari</b>\n\n"
        f"💰 Doʻst kelganda pul: <b>{format_money(get_setting('referral_bonus', 500))}</b> — {_ref_status('ref_bonus_on')}\n"
        f"📊 Foiz (sovgʻa olsa): <b>{get_setting('referral_percent', 10)}%</b> — {_ref_status('ref_percent_on')}\n"
        f"👥 N ta doʻst = 1 tekin: <b>{get_setting('free_gift_referrals', 20)}</b> — {_ref_status('ref_invites_on')}\n"
        f"🛒 Doʻstlar N ta sovgʻa = 1 tekin: <b>{get_setting('ref_orders_needed', 3)}</b> — "
        f"{'✅ YOQIQ' if get_setting('ref_orders_on', False) else '❌ OʻCHIQ'}\n\n"
        f"Har birini alohida yoqish/oʻchirish mumkin."
    )
    bot.send_message(message.chat.id, text, reply_markup=referral_settings_menu())

@bot.message_handler(func=lambda m: m.text == "📊 Referal foizi")
def change_ref_p(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Foiz (masalan 10):", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "referral_percent", referral_settings_menu))

@bot.message_handler(func=lambda m: m.text == "💰 1 referal uchun pul")
def change_ref_b(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Doʻst kelganda beriladigan summa:", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "referral_bonus", referral_settings_menu))

@bot.message_handler(func=lambda m: m.text and "Tekin sovgʻa uchun soni" in m.text)
def change_free_n(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Nechta doʻst = 1 tekin sovgʻa?", reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "free_gift_referrals", referral_settings_menu))

@bot.message_handler(func=lambda m: m.text and "Buyurtma soni" in m.text)
def change_ref_orders(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id,
                           "Doʻstlar jami nechta sovgʻa sotib olsa — 1 tekin sovgʻa?\nMasalan: <code>3</code>",
                           reply_markup=back_only())
    bot.register_next_step_handler(msg, lambda m: _set_int_setting(m, "ref_orders_needed", referral_settings_menu))

@bot.message_handler(func=lambda m: m.text and "Referal yoq" in m.text)
def ref_toggle_menu(message):
    if not is_admin(message.from_user.id):
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    items = [
        ("ref_bonus_on", "💰 Doʻst kelganda pul"),
        ("ref_percent_on", "📊 Sovgʻa foizi"),
        ("ref_invites_on", "👥 Doʻst soni → tekin"),
        ("ref_orders_on", "🛒 Buyurtma soni → tekin (yangi)"),
    ]
    for key, title in items:
        on = get_setting(key, key != "ref_orders_on")
        if key == "ref_orders_on":
            on = get_setting(key, False)
        st = "✅" if on else "❌"
        markup.add(types.InlineKeyboardButton(f"{st} {title}", callback_data=f"reftog_{key}"))
    bot.send_message(message.chat.id,
                     "⚙️ <b>Referal tizimlarini yoqish / oʻchirish</b>\n\nTugmani bosing — holat oʻzgaradi:",
                     reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("reftog_"))
def ref_toggle_cb(call):
    if not is_admin(call.from_user.id):
        return
    key = call.data.replace("reftog_", "")
    cur = get_setting(key, False)
    set_setting(key, not cur)
    bot.answer_callback_query(call.id, "Yoqildi ✅" if not cur else "Oʻchirildi ❌", show_alert=True)
    markup = types.InlineKeyboardMarkup(row_width=1)
    items = [
        ("ref_bonus_on", "💰 Doʻst kelganda pul"),
        ("ref_percent_on", "📊 Sovgʻa foizi"),
        ("ref_invites_on", "👥 Doʻst soni → tekin"),
        ("ref_orders_on", "🛒 Buyurtma soni → tekin (yangi)"),
    ]
    for k, title in items:
        on = get_setting(k, k != "ref_orders_on")
        if k == "ref_orders_on":
            on = get_setting(k, False)
        st = "✅" if on else "❌"
        markup.add(types.InlineKeyboardButton(f"{st} {title}", callback_data=f"reftog_{k}"))
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception:
        pass

@bot.message_handler(func=lambda m: m.text == "🟢 Botni yoqish")
def enable_bot(message):
    if is_admin(message.from_user.id):
        set_setting("bot_active", True)
        bot.send_message(message.chat.id, "✅ Yoqildi.", reply_markup=other_settings_menu())

@bot.message_handler(func=lambda m: m.text == "🔴 Botni o'chirish")
def disable_bot(message):
    if is_admin(message.from_user.id):
        set_setting("bot_active", False)
        bot.send_message(message.chat.id, "🔴 Oʻchirildi.", reply_markup=other_settings_menu())

@bot.message_handler(func=lambda m: m.text == "🛠 Texnik ishlar")
def toggle_maintenance(message):
    if not is_admin(message.from_user.id):
        return
    current = get_setting("maintenance", False)
    set_setting("maintenance", not current)
    bot.send_message(message.chat.id, f"🛠 {'Yoqildi' if not current else 'Oʻchirildi'}.",
                     reply_markup=other_settings_menu())

@bot.message_handler(func=lambda m: m.text == "⚙️ Boshqa sozlamalar")
def other_settings(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, "⚙️", reply_markup=other_settings_menu())

# --- Foydalanuvchilar ---
@bot.message_handler(func=lambda m: m.text == "👥 Foydalanuvchilar")
def users_admin(message):
    if is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "👥 <b>Foydalanuvchilar</b>", reply_markup=users_admin_menu())

def _users_list_page(page=0, per_page=12):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n FROM users")
        total = c.fetchone()["n"]
        c.execute("""SELECT user_id, full_name, username, balance, referrals_count, is_blocked, joined_date
                     FROM users ORDER BY joined_date DESC LIMIT %s OFFSET %s""",
                  (per_page, page * per_page))
        rows = c.fetchall()
    finally:
        put_db(conn)
    text = f"📋 <b>Barcha userlar</b> (jami {total})\n\n"
    markup = types.InlineKeyboardMarkup(row_width=1)
    if not rows:
        text += "Userlar yoʻq."
    else:
        for r in rows:
            st = "🚫" if r["is_blocked"] else "✅"
            un = f"@{r['username']}" if r["username"] else "—"
            text += f"{st} <code>{r['user_id']}</code> | {(r['full_name'] or '—')[:20]} ({un})\n💰 {format_money(r['balance'])}\n"
            markup.add(types.InlineKeyboardButton(
                f"{st} {r['user_id']} — {(r['full_name'] or '')[:16]}",
                callback_data=f"admin_user_{r['user_id']}"))
    pages = max(1, (total + per_page - 1) // per_page)
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("⬅️", callback_data=f"ulist_{page-1}"))
    nav.append(types.InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if (page + 1) * per_page < total:
        nav.append(types.InlineKeyboardButton("➡️", callback_data=f"ulist_{page+1}"))
    if nav:
        markup.row(*nav)
    return text[:3500], markup

@bot.message_handler(func=lambda m: m.text and ("Userlar ro" in m.text))
def list_users_admin(message):
    if not is_admin(message.from_user.id):
        return
    text, markup = _users_list_page(0)
    bot.send_message(message.chat.id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("ulist_"))
def users_list_page(call):
    if not is_admin(call.from_user.id):
        return
    page = int(call.data.replace("ulist_", ""))
    text, markup = _users_list_page(page)
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception:
        pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_user_"))
def admin_user_actions(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("admin_user_", ""))
    user = get_user(uid)
    if not user:
        bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
        return
    st = "🚫 Blok" if user["is_blocked"] else "✅ Faol"
    text = (f"👤 <b>User</b>\nID: <code>{user['user_id']}</code>\n"
            f"{user['full_name']} (@{user['username'] or 'yoq'})\n"
            f"💰 {format_money(user['balance'])}\n🎁 Ref: {format_money(user['referral_balance'])}\n"
            f"👥 {user['referrals_count']} | 🎁 {user['free_gifts']}\n{st}")
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("💬 Xabar", callback_data=f"adm_msg_{uid}"),
        types.InlineKeyboardButton("💵 +Balans", callback_data=f"adm_bal_{uid}"),
    )
    markup.add(
        types.InlineKeyboardButton("🚫 Blok/Ochish", callback_data=f"adm_block_{uid}"),
        types.InlineKeyboardButton("📋 Buyurtma", callback_data=f"adm_orders_{uid}"),
    )
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception:
        bot.send_message(call.from_user.id, text, reply_markup=markup)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_msg_"))
def adm_msg_start(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("adm_msg_", ""))
    msg = bot.send_message(call.from_user.id, f"💬 <code>{uid}</code> ga xabar:\n/cancel")
    bot.register_next_step_handler(msg, lambda m: process_admin_reply(m, uid))
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_bal_"))
def adm_bal_start(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("adm_bal_", ""))
    msg = bot.send_message(call.from_user.id, f"💵 <code>{uid}</code> uchun summa (+5000 yoki -2000):")
    bot.register_next_step_handler(msg, lambda m: process_adm_bal(m, uid))
    bot.answer_callback_query(call.id)

def process_adm_bal(message, uid):
    if not is_admin(message.from_user.id):
        return
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=users_admin_menu())
        return
    try:
        amount = float(message.text.strip().replace(" ", ""))
        update_balance(uid, amount)
        try:
            bot.send_message(uid, f"💵 Admin balans oʻzgartirdi: <b>{amount:+.0f}</b> soʻm")
        except Exception:
            pass
        bot.send_message(message.chat.id, f"✅ {uid} → {amount:+.0f}", reply_markup=users_admin_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat raqam.", reply_markup=users_admin_menu())

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_block_"))
def adm_block_toggle(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("adm_block_", ""))
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT is_blocked FROM users WHERE user_id = %s", (uid,))
        row = c.fetchone()
        if not row:
            bot.answer_callback_query(call.id, "Topilmadi", show_alert=True)
            return
        new_st = 0 if row["is_blocked"] else 1
        c.execute("UPDATE users SET is_blocked = %s WHERE user_id = %s", (new_st, uid))
        conn.commit()
        bot.answer_callback_query(call.id, "Bloklandi ✅" if new_st else "Ochildi ✅", show_alert=True)
    finally:
        put_db(conn)

@bot.callback_query_handler(func=lambda call: call.data.startswith("adm_orders_"))
def adm_user_orders(call):
    if not is_admin(call.from_user.id):
        return
    uid = int(call.data.replace("adm_orders_", ""))
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT gift_name, price, is_free, status, created_at FROM gift_orders
                     WHERE user_id = %s ORDER BY id DESC LIMIT 10""", (uid,))
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.answer_callback_query(call.id, "Buyurtma yoʻq", show_alert=True)
        return
    text = f"📋 <code>{uid}</code> buyurtmalari:\n\n"
    for r in rows:
        tip = "🎁" if r["is_free"] else format_money(r["price"])
        text += f"🎁 {r['gift_name']} | {tip} | {r['status']}\n"
    bot.send_message(call.from_user.id, text[:3000])
    bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda m: m.text and ("Balans o" in m.text and "zgartirish" in m.text))
def change_balance(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id,
                           "Format: <code>USER_ID +5000</code> yoki <code>USER_ID -2000</code>\n/cancel",
                           reply_markup=back_only())
    bot.register_next_step_handler(msg, process_change_balance)

def process_change_balance(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👥", reply_markup=users_admin_menu())
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Notoʻgʻri format.", reply_markup=users_admin_menu())
        return
    try:
        uid = int(parts[0])
        amount = float(parts[1])
        update_balance(uid, amount)
        try:
            bot.send_message(uid, f"💵 Balans oʻzgardi: <b>{amount:+.0f}</b> soʻm")
        except Exception:
            pass
        bot.send_message(message.chat.id, f"✅ {uid} → {amount:+.0f}", reply_markup=users_admin_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat: ID SUMMA.", reply_markup=users_admin_menu())

@bot.message_handler(func=lambda m: m.text and ("Bloklash" in m.text or "Ochish" in m.text))
def block_user(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "User ID:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_block_user)

def process_block_user(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👥", reply_markup=users_admin_menu())
        return
    try:
        uid = int(message.text.strip())
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("SELECT is_blocked FROM users WHERE user_id = %s", (uid,))
            row = c.fetchone()
            if not row:
                bot.send_message(message.chat.id, "Topilmadi.", reply_markup=users_admin_menu())
                return
            new_st = 0 if row["is_blocked"] else 1
            c.execute("UPDATE users SET is_blocked = %s WHERE user_id = %s", (new_st, uid))
            conn.commit()
            bot.send_message(message.chat.id, f"✅ {uid} {'bloklandi' if new_st else 'ochildi'}.",
                             reply_markup=users_admin_menu())
        finally:
            put_db(conn)
    except Exception:
        bot.send_message(message.chat.id, "Faqat ID.", reply_markup=users_admin_menu())

@bot.message_handler(func=lambda m: m.text and "qidirish" in m.text.lower() and is_admin(m.from_user.id))
def search_user(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "User ID yoki @username:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_search_user)

def process_search_user(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👥", reply_markup=users_admin_menu())
        return
    q = message.text.strip().replace("@", "")
    conn = get_db()
    try:
        c = conn.cursor()
        try:
            uid = int(q)
            c.execute("SELECT * FROM users WHERE user_id = %s", (uid,))
        except ValueError:
            c.execute("SELECT * FROM users WHERE username = %s", (q,))
        row = c.fetchone()
    finally:
        put_db(conn)
    if not row:
        bot.send_message(message.chat.id, "Topilmadi.", reply_markup=users_admin_menu())
        return
    text = (f"👤 <b>User</b>\nID: <code>{row['user_id']}</code>\n"
            f"{row['full_name']} (@{row['username'] or 'yoq'})\n"
            f"💰 {format_money(row['balance'])}\n🎁 {format_money(row['referral_balance'])}\n"
            f"👥 {row['referrals_count']} | 🎁 {row['free_gifts']}\n"
            f"Blok: {'Ha' if row['is_blocked'] else 'Yoʻq'}")
    bot.send_message(message.chat.id, text, reply_markup=users_admin_menu())

# --- To'lovlar (admin) ---
def _admin_payments_page(page=0, per_page=12):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='paid'")
        paid = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='pending'")
        pend = c.fetchone()
        c.execute("SELECT COUNT(*) as n, COALESCE(SUM(unique_amount),0) as s FROM payments WHERE status='expired'")
        exp = c.fetchone()
        c.execute("SELECT COUNT(*) as n FROM payments")
        total = c.fetchone()["n"]
        c.execute("""SELECT id, user_id, unique_amount, status, created_at, paid_at FROM payments
                     ORDER BY id DESC LIMIT %s OFFSET %s""", (per_page, page * per_page))
        rows = c.fetchall()
    finally:
        put_db(conn)
    text = (f"💰 <b>Barcha toʻlovlar</b>\n\n"
            f"✅ Tasdiqlangan: <b>{paid['n']}</b> — {format_money(paid['s'])}\n"
            f"⏳ Kutilayotgan: <b>{pend['n']}</b> — {format_money(pend['s'])}\n"
            f"⏰ Muddati oʻtgan: <b>{exp['n']}</b> — {format_money(exp['s'])}\n"
            f"📋 Jami: <b>{total}</b>\n\n")
    if not rows:
        text += "Hali yoʻq."
    else:
        for r in rows:
            st = {"paid": "✅", "pending": "⏳", "expired": "⏰"}.get(r["status"], "❓")
            text += f"{st} #{r['id']} | <code>{r['user_id']}</code> | {format_money(r['unique_amount'])}\n📅 {r['created_at']}\n"
    pages = max(1, (total + per_page - 1) // per_page)
    markup = types.InlineKeyboardMarkup(row_width=3)
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("⬅️", callback_data=f"apay_{page-1}"))
    nav.append(types.InlineKeyboardButton(f"{page+1}/{pages}", callback_data="noop"))
    if (page + 1) * per_page < total:
        nav.append(types.InlineKeyboardButton("➡️", callback_data=f"apay_{page+1}"))
    if nav:
        markup.row(*nav)
    text += "\n\nTasdiqlash: /confirm ID"
    return text[:4000], markup

@bot.message_handler(func=lambda m: m.text == "💰 To'lovlar" or m.text == "💰 Toʻlovlar")
def admin_payments(message):
    if not is_admin(message.from_user.id):
        return
    text, markup = _admin_payments_page(0)
    bot.send_message(message.chat.id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("apay_"))
def admin_pay_page(call):
    if not is_admin(call.from_user.id):
        return
    page = int(call.data.replace("apay_", ""))
    text, markup = _admin_payments_page(page)
    try:
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)
    except Exception:
        pass
    bot.answer_callback_query(call.id)

# --- Sharhlar ---
@bot.message_handler(func=lambda m: m.text == "💬 Sharhlar")
def reviews_menu(message):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""SELECT id, full_name, username, text, created_at FROM reviews
                     WHERE is_approved = 1 ORDER BY id DESC LIMIT 15""")
        rows = c.fetchall()
    finally:
        put_db(conn)
    text = "💬 <b>Sharhlar</b>\n\n"
    markup = types.InlineKeyboardMarkup(row_width=1)
    if not rows:
        text += "Hali sharh yoʻq."
    else:
        for r in rows:
            text += f"👤 {r['full_name']} (@{r['username'] or 'yoq'})\n{r['text']}\n\n"
            if is_admin(message.from_user.id):
                markup.add(types.InlineKeyboardButton(
                    f"🗑 Oʻchirish #{r['id']}", callback_data=f"delrev_{r['id']}"))
    markup.add(types.InlineKeyboardButton("✍️ Sharh qoldirish", callback_data="add_review"))
    bot.send_message(message.chat.id, text[:3500], reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("delrev_"))
def delete_review_cb(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Faqat admin", show_alert=True)
        return
    rid = int(call.data.replace("delrev_", ""))
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("DELETE FROM reviews WHERE id = %s", (rid,))
        conn.commit()
    finally:
        put_db(conn)
    bot.answer_callback_query(call.id, "✅ Sharh oʻchirildi", show_alert=True)
    try:
        bot.edit_message_text(f"✅ Sharh #{rid} oʻchirildi.", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda call: call.data == "add_review")
def add_review_start(call):
    msg = bot.send_message(call.from_user.id, "✍️ Sharhingizni yozing:\n/cancel")
    bot.register_next_step_handler(msg, process_add_review)
    bot.answer_callback_query(call.id)

def process_add_review(message):
    if message.text == "/cancel":
        bot.send_message(message.chat.id, "Bekor.", reply_markup=main_menu(message.from_user.id))
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO reviews (user_id, username, full_name, text, created_at, is_approved)
                     VALUES (%s, %s, %s, %s, %s, 1)""",
                  (message.from_user.id, message.from_user.username or "",
                   message.from_user.full_name or "", message.text[:500], now))
        conn.commit()
    finally:
        put_db(conn)
    bot.send_message(message.chat.id, "✅ Sharh qoʻshildi!", reply_markup=main_menu(message.from_user.id))

# --- Kanallar ---
@bot.message_handler(func=lambda m: m.text == "📢 Majburiy obuna")
def channels_admin(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, "📢 <b>Majburiy obuna</b>", reply_markup=channels_menu())

@bot.message_handler(func=lambda m: m.text and ("Kanal qo'shish" in m.text or "Kanal qoʻshish" in m.text))
def add_channel(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id,
        "Kanal @username, ID yoki forward qiling.\nPrivat kanal boʻlsa — invite link ham yuboring (yangi qatorda).\n/cancel",
        reply_markup=back_only())
    bot.register_next_step_handler(msg, process_add_channel)

def process_add_channel(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "📢", reply_markup=channels_menu())
        return
    text = (message.text or "").strip()
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    channel = lines[0] if lines else ""
    invite_link = lines[1] if len(lines) > 1 else ""
    chan_id = channel
    is_private = 0
    title = channel
    username = channel if channel.startswith("@") else (f"@{channel}" if not channel.lstrip("-").isdigit() else "")
    try:
        chat = bot.get_chat(channel)
        chan_id = str(chat.id)
        title = chat.title or channel
        username = f"@{chat.username}" if chat.username else ""
        is_private = 0 if chat.username else 1
    except Exception:
        pass
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""INSERT INTO channels (channel_id, channel_username, channel_title, is_private, invite_link)
                     VALUES (%s, %s, %s, %s, %s)""", (chan_id, username, title, is_private, invite_link))
        conn.commit()
    finally:
        put_db(conn)
    bot.send_message(message.chat.id, f"✅ Qoʻshildi: {title}", reply_markup=channels_menu())

@bot.message_handler(func=lambda m: m.text and "Kanallar ro" in m.text)
def list_channels(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT channel_id, channel_username, channel_title FROM channels")
        rows = c.fetchall()
    finally:
        put_db(conn)
    if not rows:
        bot.send_message(message.chat.id, "Kanal yoʻq.", reply_markup=channels_menu())
        return
    text = "📋 <b>Kanallar:</b>\n\n" + "\n".join(
        f"• {r['channel_title'] or r['channel_username'] or r['channel_id']}" for r in rows)
    bot.send_message(message.chat.id, text, reply_markup=channels_menu())

@bot.message_handler(func=lambda m: m.text and ("Kanal o'chirish" in m.text or "Kanal oʻchirish" in m.text))
def remove_channel(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Kanal @username yoki ID:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_remove_channel)

def process_remove_channel(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "📢", reply_markup=channels_menu())
        return
    channel = message.text.strip()
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("DELETE FROM channels WHERE channel_id = %s OR channel_username = %s", (channel, channel))
        n = c.rowcount
        conn.commit()
    finally:
        put_db(conn)
    bot.send_message(message.chat.id, "✅ Oʻchirildi." if n else "Topilmadi.", reply_markup=channels_menu())

# --- Adminlar ---
@bot.message_handler(func=lambda m: m.text == "👨‍💼 Adminlar")
def admins_admin(message):
    if is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "👨‍💼 <b>Adminlar</b>", reply_markup=admins_menu())

@bot.message_handler(func=lambda m: m.text and ("Admin qo'shish" in m.text or "Admin qoʻshish" in m.text))
def add_admin_h(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Yangi admin ID:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_add_admin)

def process_add_admin(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👨‍💼", reply_markup=admins_menu())
        return
    try:
        new_admin = int(message.text)
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (new_admin,))
            conn.commit()
            _admin_cache.add(new_admin)
        finally:
            put_db(conn)
        bot.send_message(message.chat.id, f"✅ {new_admin}", reply_markup=admins_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat raqam.", reply_markup=admins_menu())

@bot.message_handler(func=lambda m: m.text and ("Admin o'chirish" in m.text or "Admin oʻchirish" in m.text))
def remove_admin_h(message):
    if not is_admin(message.from_user.id):
        return
    msg = bot.send_message(message.chat.id, "Admin ID:\n/cancel", reply_markup=back_only())
    bot.register_next_step_handler(msg, process_remove_admin)

def process_remove_admin(message):
    if not is_admin(message.from_user.id):
        return
    if message.text in ["🔙 Orqaga", "/cancel"]:
        bot.send_message(message.chat.id, "👨‍💼", reply_markup=admins_menu())
        return
    try:
        admin_id = int(message.text)
        if admin_id in ADMIN_IDS:
            bot.send_message(message.chat.id, "Asosiy adminni oʻchirib boʻlmaydi.", reply_markup=admins_menu())
            return
        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("DELETE FROM admins WHERE user_id = %s", (admin_id,))
            conn.commit()
            _admin_cache.discard(admin_id)
        finally:
            put_db(conn)
        bot.send_message(message.chat.id, f"✅ {admin_id}", reply_markup=admins_menu())
    except Exception:
        bot.send_message(message.chat.id, "Faqat raqam.", reply_markup=admins_menu())

@bot.message_handler(func=lambda m: m.text and "Adminlar ro" in m.text)
def list_admins(message):
    if not is_admin(message.from_user.id):
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT user_id FROM admins")
        rows = c.fetchall()
    finally:
        put_db(conn)
    text = "📋 <b>Adminlar:</b>\n\n" + "\n".join(f"• <code>{r['user_id']}</code>" for r in rows)
    bot.send_message(message.chat.id, text, reply_markup=admins_menu())

@bot.message_handler(func=lambda m: is_admin(m.from_user.id) and (
    (m.forward_from and getattr(m.forward_from, "username", None) and
     "humo" in (m.forward_from.username or "").lower()) or
    (m.forward_from_chat and "humo" in (getattr(m.forward_from_chat, "username", None) or "").lower()) or
    (m.text and any(x in m.text.upper() for x in ["UZS", "HUMO", "+"])) and
    m.text and ("soʻm" in m.text.lower() or "сум" in m.text.lower() or "UZS" in m.text.upper() or "💰" in m.text)
))
def admin_humo_auto(message):
    """Admin HUMO chekini botga yuborsa / forward qilsa — avto tasdiqlash"""
    text = message.text or message.caption or ""
    amount = parse_humo_amount(text)
    if not amount:
        bot.send_message(message.chat.id, "⚠️ Summa aniqlanmadi. Matnni toʻliq yuboring.")
        return
    ok = process_humo_payment(amount)
    if ok:
        bot.send_message(message.chat.id, f"✅ Avto tasdiqlandi: <b>{format_money(amount)}</b>")
    else:
        bot.send_message(message.chat.id,
            f"⚠️ <b>{format_money(amount)}</b> ga mos pending toʻlov topilmadi.\n"
            f"User avval «Hisob toʻldirish» qilganiga ishonch hosil qiling.")

@bot.message_handler(commands=['confirm'])
def confirm_payment_cmd(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.send_message(message.chat.id, "Format: /confirm PAYMENT_ID")
        return
    try:
        pay_id = int(parts[1])
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_id, unique_amount, status FROM payments WHERE id = %s", (pay_id,))
        row = c.fetchone()
        if not row or row["status"] == "paid":
            bot.send_message(message.chat.id, "Topilmadi yoki allaqachon toʻlangan.")
            put_db(conn)
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE payments SET status = 'paid', paid_at = %s WHERE id = %s", (now, pay_id))
        c.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (row["unique_amount"], row["user_id"]))
        conn.commit()
        put_db(conn)
        try:
            bot.send_message(row["user_id"], f"✅ Toʻlov tasdiqlandi!\n💰 Hisobingiz <b>{format_money(row['unique_amount'])}</b> ga toʻldirildi.")
        except Exception:
            pass
        bot.send_message(message.chat.id, f"✅ #{pay_id} tasdiqlandi.")
    except Exception as e:
        bot.send_message(message.chat.id, f"Xato: {e}")

# ==================== FLASK WEBHOOK ====================
@app.route("/", methods=["GET"])
def health():
    return "SovgaMarket Bot ishlayapti ✅", 200

@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    if request.headers.get("content-type") == "application/json":
        json_string = request.get_data().decode("utf-8")
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "", 200
    return "Bad request", 403

def setup_webhook():
    if WEBHOOK_URL:
        url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
        bot.remove_webhook()
        time.sleep(0.5)
        bot.set_webhook(url=url)
        print(f"✅ Webhook: {url}")
    else:
        print("⚠️ WEBHOOK_URL yoʻq — polling rejimida ishlaydi")

# ==================== START ====================
if __name__ == "__main__":
    print("=" * 50)
    print("🎁 SovgaMarket Bot (Flask + PostgreSQL)")
    print("=" * 50)
    if not BOT_TOKEN or not DATABASE_URL:
        print("❌ BOT_TOKEN va DATABASE_URL majburiy!")
        exit(1)
    init_pool()
    init_db()
    start_expire_checker()
    _en_tel = os.getenv("ENABLE_TELETHON", "1" if API_ID else "0")
    if _en_tel == "1" and API_ID and API_HASH:
        start_telethon_listener()
    else:
        print("ℹ️ Telethon o'chiq — admin HUMO chekini botga forward qilsa ham avto ishlaydi")
    if WEBHOOK_URL:
        setup_webhook()
        app.run(host="0.0.0.0", port=PORT)
    else:
        print("📡 Polling rejimi...")
        bot.remove_webhook()
        bot.infinity_polling(none_stop=True, interval=1)
else:
    # Gunicorn uchun
    if BOT_TOKEN and DATABASE_URL:
        try:
            init_pool()
            init_db()
            start_expire_checker()
            _en_tel = os.getenv("ENABLE_TELETHON", "1" if API_ID else "0")
            if _en_tel == "1" and API_ID and API_HASH:
                start_telethon_listener()
            if WEBHOOK_URL:
                setup_webhook()
        except Exception as e:
            print("Startup error:", e)
