# -*- coding: utf-8 -*-
"""
SOVGAMARKET — Kitobxon majburiy ro'yxatdan o'tish moduli
==========================================================

Vazifalar:
1) @Kitobxonloyihasibot dan keladigan xabarlarni mavjud Telethon
   StringSession orqali kuzatadi.
2) Xabardan yangi referral foydalanuvchining ism/familiyasini ajratadi.
3) Avval ism/familiya bo'yicha "pending" yozuv yaratadi.
4) Gift bot foydalanuvchisi /start qilganda uning Telegram ID'sini
   Telegram profilidagi ism/familiya bilan pending yozuvga biriktiradi.
5) ID biriktirilgandan keyin keyingi tekshiruvlar faqat Telegram ID orqali
   bajariladi.
6) Bir xil ismli foydalanuvchilarni ustidan yozmaydi: har bir referral
   alohida yozuv.
7) Admin Kitobxon bot linkini yoqishi/o'chirishi/o'zgartirishi mumkin.
8) Mavjud PostgreSQL pool (app.py dagi get_db/put_db) bilan ishlaydi.
9) Mavjud Telethon client'ni qayta yaratmaydi.

MUHIM:
- Bu modul app.py ichidagi mavjud telethon_client bilan ishlashi uchun
  attach_telethon_client(client) chaqiriladi.
- app.py dagi start_telethon() ichida client tayyor bo'lgandan keyin:
      from kitob import attach_telethon_client
      attach_telethon_client(client)
  chaqirilishi kerak.
"""

import os
import re
import logging
from datetime import datetime

logger = logging.getLogger("kitob")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------
# APP BILAN ULASH
# ---------------------------------------------------------------------

_bot = None
_get_db = None
_put_db = None
_is_admin = None
_telethon_client = None
_listener_attached = False

DEFAULT_BOT_USERNAME = (
    os.getenv("KITOBXON_BOT_USERNAME", "Kitobxonloyihasibot")
    .strip()
    .lstrip("@")
)

DEFAULT_BOT_LINK = f"https://t.me/{DEFAULT_BOT_USERNAME}"


def configure(bot, get_db, put_db, is_admin=None):
    """
    app.py dan chaqiriladi.

    Misol:
        kitob.configure(
            bot=bot,
            get_db=get_db,
            put_db=put_db,
            is_admin=is_admin,
        )
        kitob.init_db()
    """
    global _bot, _get_db, _put_db, _is_admin
    _bot = bot
    _get_db = get_db
    _put_db = put_db
    _is_admin = is_admin


def _require_config():
    if _get_db is None or _put_db is None:
        raise RuntimeError(
            "kitob.py configure() chaqirilmagan. "
            "app.py dan configure(bot, get_db, put_db, is_admin) qiling."
        )


# ---------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------

def init_db():
    """
    Kitobxon jadvallarini avtomatik yaratadi.
    Mavjud PostgreSQL bazaga zarar bermaydi.
    """
    _require_config()

    conn = _get_db()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kitob_settings (
                id INTEGER PRIMARY KEY DEFAULT 1,
                enabled INTEGER DEFAULT 1,
                bot_username TEXT DEFAULT '',
                bot_link TEXT DEFAULT '',
                require_registration INTEGER DEFAULT 1,
                updated_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kitob_referrals (
                id BIGSERIAL PRIMARY KEY,
                full_name TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                first_name TEXT DEFAULT '',
                last_name TEXT DEFAULT '',
                telegram_id BIGINT,
                source_message_id BIGINT,
                reward INTEGER DEFAULT 0,
                raw_message TEXT DEFAULT '',
                matched INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                matched_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS kitob_users (
                telegram_id BIGINT PRIMARY KEY,
                full_name TEXT NOT NULL,
                first_name TEXT DEFAULT '',
                last_name TEXT DEFAULT '',
                first_registered_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
            """
        )

        # Tez qidirish uchun indexlar.
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_kitob_referrals_name
            ON kitob_referrals(normalized_name)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_kitob_referrals_telegram
            ON kitob_referrals(telegram_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_kitob_referrals_message
            ON kitob_referrals(source_message_id)
            """
        )

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cur.execute(
            """
            INSERT INTO kitob_settings
                (id, enabled, bot_username, bot_link,
                 require_registration, updated_at)
            VALUES (1, 1, %s, %s, 1, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (DEFAULT_BOT_USERNAME, DEFAULT_BOT_LINK, now),
        )

        conn.commit()
        logger.info("✅ Kitobxon DB jadvallari tayyor")
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_db(conn)


def _get_settings():
    _require_config()

    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM kitob_settings WHERE id = 1")
        row = cur.fetchone()

        if not row:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                """
                INSERT INTO kitob_settings
                    (id, enabled, bot_username, bot_link,
                     require_registration, updated_at)
                VALUES (1, 1, %s, %s, 1, %s)
                """,
                (DEFAULT_BOT_USERNAME, DEFAULT_BOT_LINK, now),
            )
            conn.commit()
            cur.execute("SELECT * FROM kitob_settings WHERE id = 1")
            row = cur.fetchone()

        return row
    finally:
        _put_db(conn)


def _setting_value(row, key, default=None):
    if not row:
        return default

    try:
        return row[key]
    except Exception:
        try:
            return row.get(key, default)
        except Exception:
            return default


def set_kitob_setting(key, value):
    allowed = {
        "enabled",
        "bot_username",
        "bot_link",
        "require_registration",
    }
    if key not in allowed:
        raise ValueError("Noto'g'ri Kitobxon setting")

    _require_config()

    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE kitob_settings
            SET {key} = %s,
                updated_at = %s
            WHERE id = 1
            """,
            (value, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
    finally:
        _put_db(conn)


def get_kitob_link():
    row = _get_settings()
    link = _setting_value(row, "bot_link", "") or ""
    return link.strip() or DEFAULT_BOT_LINK


def get_kitob_username():
    row = _get_settings()
    username = _setting_value(row, "bot_username", "") or ""
    return username.strip().lstrip("@") or DEFAULT_BOT_USERNAME


def is_kitob_enabled():
    row = _get_settings()
    return bool(_setting_value(row, "enabled", 1))


def is_registration_required():
    row = _get_settings()
    return bool(_setting_value(row, "require_registration", 1))


# ---------------------------------------------------------------------
# ISM/FAMILIYA NORMALIZATSIYASI
# ---------------------------------------------------------------------

def normalize_name(value):
    """
    Taqqoslash uchun ismni bir xil ko'rinishga keltiradi.

    Misol:
        "Abduvohidov Otabek" -> "abduvohidov otabek"
        "  Otabek   Abduvohidov " -> "otabek abduvohidov"
    """
    if not value:
        return ""

    value = str(value)
    value = value.replace("\u00a0", " ")
    value = value.replace("ʻ", "'").replace("’", "'").replace("`", "'")
    value = re.sub(r"\s+", " ", value).strip().casefold()

    # HTML/emoji/ortiqcha belgilarni taqqoslashdan chiqaramiz.
    value = re.sub(r"[^\w\s'\-]", " ", value, flags=re.UNICODE)
    value = re.sub(r"\s+", " ", value).strip()

    return value


def build_full_name(first_name="", last_name=""):
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()

    if last_name and first_name:
        # Sening talabing: "Familiya Ism"
        return f"{last_name} {first_name}".strip()

    return (last_name or first_name or "").strip()


def split_name(full_name):
    """
    Kitobxon xabaridagi ismni ajratish.

    Sening format bo'yicha:
        "Abduvohidov Otabek" -> last_name=Abduvohidov,
                                   first_name=Otabek

    Agar bitta so'z bo'lsa:
        "Samandar" -> first_name=Samandar
    """
    text = re.sub(r"\s+", " ", (full_name or "").strip())

    if not text:
        return "", ""

    parts = text.split(" ")

    if len(parts) == 1:
        return parts[0], ""

    return parts[-1], " ".join(parts[:-1])


# ---------------------------------------------------------------------
# KITOBXON XABARINI PARSE QILISH
# ---------------------------------------------------------------------

# Bir nechta ehtimoliy formatlar:
# "Sizning taklifingiz bilan Abduvohidov Otabek botga qo'shildi."
# "Sizning taklifingiz bilan Samandar botga qo'shildi."
REFERRAL_PATTERNS = [
    re.compile(
        r"Sizning\s+taklifingiz\s+bilan\s+(.+?)\s+botga\s+qo['’`ʻ]?shildi",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"taklifingiz\s+bilan\s+(.+?)\s+botga\s+qo['’`ʻ]?shildi",
        re.IGNORECASE | re.DOTALL,
    ),
]


def parse_kitobxon_message(text):
    """
    Natija:
    {
        "full_name": "...",
        "first_name": "...",
        "last_name": "...",
        "reward": 70
    }

    Agar xabar Kitobxon referral xabari bo'lmasa -> None.
    """
    text = (text or "").strip()
    if not text:
        return None

    full_name = None

    for pattern in REFERRAL_PATTERNS:
        match = pattern.search(text)
        if match:
            full_name = match.group(1).strip()
            break

    if not full_name:
        return None

    # Matn ichida tasodifiy HTML/emoji qolsa tozalash.
    full_name = re.sub(r"<[^>]+>", " ", full_name)
    full_name = re.sub(r"\s+", " ", full_name).strip(" .:-")

    if not full_name:
        return None

    # Rewardni xabardan olish.
    reward = 0
    reward_match = re.search(
        r"(?:\+|hisobingizga\s*\+?)\s*([0-9][0-9\s]*)\s*(?:🪙|tanga)",
        text,
        re.IGNORECASE,
    )
    if reward_match:
        try:
            reward = int(re.sub(r"\D", "", reward_match.group(1)))
        except Exception:
            reward = 0

    last_name, first_name = split_name(full_name)

    # Agar birinchi token familiya sifatida qaralgan bo'lsa,
    # build_full_name orqali qayta yig'amiz.
    normalized_full = build_full_name(first_name, last_name)

    return {
        "full_name": normalized_full or full_name,
        "first_name": first_name,
        "last_name": last_name,
        "reward": reward,
    }


# ---------------------------------------------------------------------
# REFERRAL SAQLASH
# ---------------------------------------------------------------------

def save_pending_referral(
    full_name,
    first_name,
    last_name,
    reward,
    source_message_id,
    raw_message,
):
    """
    Avval ism/familiyani saqlaydi.
    Telegram ID hali ma'lum bo'lmasa NULL qoladi.

    source_message_id unique mantiq bilan takroriy xabarni oldini oladi.
    """
    _require_config()

    normalized = normalize_name(full_name)
    if not normalized:
        return False

    conn = _get_db()
    try:
        cur = conn.cursor()

        # Bir xil Telegram message qayta kelib qolsa — qayta yozmaymiz.
        if source_message_id:
            cur.execute(
                """
                SELECT id
                FROM kitob_referrals
                WHERE source_message_id = %s
                LIMIT 1
                """,
                (int(source_message_id),),
            )
            if cur.fetchone():
                return False

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cur.execute(
            """
            INSERT INTO kitob_referrals
                (full_name, normalized_name, first_name, last_name,
                 telegram_id, source_message_id, reward,
                 raw_message, matched, created_at)
            VALUES (%s, %s, %s, %s, NULL, %s, %s, %s, 0, %s)
            """,
            (
                full_name,
                normalized,
                first_name or "",
                last_name or "",
                int(source_message_id) if source_message_id else None,
                int(reward or 0),
                (raw_message or "")[:4000],
                now,
            ),
        )

        conn.commit()

        logger.info(
            "📚 Kitobxon pending: %s | reward=%s | msg=%s",
            full_name,
            reward,
            source_message_id,
        )
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_db(conn)


# ---------------------------------------------------------------------
# TELEGRAM USER NOMINI KITOBXON PENDING RO'YXATIGA BOG'LASH
# ---------------------------------------------------------------------

def _candidate_names(first_name, last_name, full_name):
    """
    Bir nechta normalizatsiya varianti.
    """
    variants = []

    def add(v):
        n = normalize_name(v)
        if n and n not in variants:
            variants.append(n)

    add(full_name)

    if first_name and last_name:
        add(f"{last_name} {first_name}")
        add(f"{first_name} {last_name}")

    add(first_name)

    return variants


def find_and_bind_user(
    telegram_id,
    first_name,
    last_name="",
    username="",
):
    """
    Sening asosiy talabing:

    1) Birinchi marta ism/familiya bilan pending Kitobxon yozuvini topadi.
    2) Mos kelganda Telegram IDni o'sha yozuvga yozadi.
    3) IDni kitob_users jadvaliga saqlaydi.
    4) Keyingi safar tekshiruv faqat ID orqali bo'ladi.

    Qaytaradi:
        (True, row)  -> topildi/bog'landi
        (False, None) -> hali topilmadi
    """
    _require_config()

    telegram_id = int(telegram_id)
    full_name = build_full_name(first_name, last_name)

    # 1. Avval ID bo'yicha tekshirish.
    existing = get_kitob_user(telegram_id)
    if existing:
        update_kitob_user_seen(telegram_id)
        return True, existing

    candidates = _candidate_names(first_name, last_name, full_name)

    if not candidates:
        return False, None

    conn = _get_db()
    try:
        cur = conn.cursor()

        # 2. Exact normalized name bo'yicha pending yozuv.
        placeholders = ", ".join(["%s"] * len(candidates))
        cur.execute(
            f"""
            SELECT *
            FROM kitob_referrals
            WHERE matched = 0
              AND telegram_id IS NULL
              AND normalized_name IN ({placeholders})
            ORDER BY id ASC
            LIMIT 1
            """,
            tuple(candidates),
        )

        row = cur.fetchone()

        if not row:
            conn.rollback()
            return False, None

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 3. IDni aynan shu pending yozuvga biriktiramiz.
        cur.execute(
            """
            UPDATE kitob_referrals
            SET telegram_id = %s,
                matched = 1,
                matched_at = %s
            WHERE id = %s
              AND matched = 0
            """,
            (telegram_id, now, row["id"]),
        )

        # 4. ID + ism/familiyani alohida jadvalga saqlaymiz.
        cur.execute(
            """
            INSERT INTO kitob_users
                (telegram_id, full_name, first_name, last_name,
                 first_registered_at, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (telegram_id)
            DO UPDATE SET
                full_name = EXCLUDED.full_name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                last_seen_at = EXCLUDED.last_seen_at
            """,
            (
                telegram_id,
                full_name or row["full_name"],
                first_name or "",
                last_name or "",
                now,
                now,
            ),
        )

        conn.commit()

        logger.info(
            "✅ Kitobxon bog'landi: %s -> %s",
            telegram_id,
            full_name,
        )

        return True, row

    except Exception:
        conn.rollback()
        raise
    finally:
        _put_db(conn)


def get_kitob_user(telegram_id):
    _require_config()

    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM kitob_users
            WHERE telegram_id = %s
            LIMIT 1
            """,
            (int(telegram_id),),
        )
        return cur.fetchone()
    finally:
        _put_db(conn)


def update_kitob_user_seen(telegram_id):
    _require_config()

    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE kitob_users
            SET last_seen_at = %s
            WHERE telegram_id = %s
            """,
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                int(telegram_id),
            ),
        )
        conn.commit()
    finally:
        _put_db(conn)


def is_registered(telegram_id, first_name="", last_name=""):
    """
    Asosiy check.

    1) ID bor -> True.
    2) ID yo'q -> ism/familiya bilan birinchi marta bog'lashga urinadi.
    3) Mos kelmasa -> False.
    """
    if not is_kitob_enabled() or not is_registration_required():
        return True

    existing = get_kitob_user(telegram_id)
    if existing:
        update_kitob_user_seen(telegram_id)
        return True

    ok, _ = find_and_bind_user(
        telegram_id=telegram_id,
        first_name=first_name,
        last_name=last_name,
    )
    return bool(ok)


# ---------------------------------------------------------------------
# MAJBURIY OBUNA / TALABLAR
# ---------------------------------------------------------------------

def get_missing_requirement(telegram_id, first_name="", last_name=""):
    """
    Kitobxon talabi bajarilmagan bo'lsa:
        (display, url)
    qaytaradi.

    Bajarilgan bo'lsa None.
    """
    if not is_kitob_enabled() or not is_registration_required():
        return None

    if is_registered(
        telegram_id=telegram_id,
        first_name=first_name,
        last_name=last_name,
    ):
        return None

    return ("📚 Kitobxon loyihasi", get_kitob_link())


# ---------------------------------------------------------------------
# TELETHON LISTENER
# ---------------------------------------------------------------------

async def _get_kitob_entity(client):
    username = get_kitob_username()

    try:
        entity = await client.get_entity(username)
        logger.info(
            "✅ Kitobxon entity topildi: @%s id=%s",
            username,
            getattr(entity, "id", "?"),
        )
        return entity
    except Exception as exc:
        logger.error(
            "❌ Kitobxon bot topilmadi @%s: %s",
            username,
            exc,
        )
        return None


def attach_telethon_client(client):
    """
    app.py dagi mavjud Telethon clientga listener ulaydi.

    Yangi TelegramClient yaratmaydi.
    """
    global _telethon_client, _listener_attached

    if client is None:
        logger.warning("⚠️ Kitobxon: Telethon client None")
        return False

    if _listener_attached:
        logger.info("ℹ️ Kitobxon listener allaqachon ulangan")
        return True

    try:
        from telethon import events
    except ImportError:
        logger.error("❌ telethon o'rnatilmagan")
        return False

    _telethon_client = client

    async def handler(event):
        try:
            text = event.message.message or ""

            parsed = parse_kitobxon_message(text)
            if not parsed:
                return

            save_pending_referral(
                full_name=parsed["full_name"],
                first_name=parsed["first_name"],
                last_name=parsed["last_name"],
                reward=parsed["reward"],
                source_message_id=getattr(event.message, "id", None),
                raw_message=text,
            )

        except Exception as exc:
            logger.exception("❌ Kitobxon xabarini qayta ishlash xatosi: %s", exc)

    # get_entity orqali topilgan botga aniq listener.
    async def register():
        entity = await _get_kitob_entity(client)

        if entity is not None:
            client.add_event_handler(
                handler,
                events.NewMessage(incoming=True, from_users=entity),
            )
            logger.info("👁 Kitobxon listener faol: entity")
            return True

        # Entity olinmasa ham username bo'yicha fallback.
        username = get_kitob_username()
        client.add_event_handler(
            handler,
            events.NewMessage(
                incoming=True,
                from_users=username,
            ),
        )
        logger.info("👁 Kitobxon listener faol: username fallback")
        return True

    # app.py client allaqachon ishlayotgan event loopda.
    try:
        import asyncio

        loop = getattr(client, "loop", None)

        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(register(), loop)
            future.result(timeout=20)
        else:
            # Bu holat odatda testlarda uchraydi.
            asyncio.get_event_loop().run_until_complete(register())

        _listener_attached = True
        return True

    except Exception as exc:
        logger.exception("❌ Kitobxon listener ulanmadi: %s", exc)
        return False


async def _kitob_on_message(event):
    """Kitobxon botidan kelgan xabarni qayta ishlash (async listener uchun)."""
    try:
        text = event.message.message or ""
        parsed = parse_kitobxon_message(text)
        if not parsed:
            return
        save_pending_referral(
            full_name=parsed["full_name"],
            first_name=parsed["first_name"],
            last_name=parsed["last_name"],
            reward=parsed["reward"],
            source_message_id=getattr(event.message, "id", None),
            raw_message=text,
        )
    except Exception as exc:
        logger.exception("❌ Kitobxon xabarini qayta ishlash xatosi: %s", exc)


async def attach_telethon_client_async(client):
    """
    Telethon event loop ICHIDA (await bilan) listener ulaydi.
    app.py dagi run_listener() ichidan chaqiriladi:
        await kitob.attach_telethon_client_async(client)
    Har bir yangi client (qayta ulanishdan keyin) uchun alohida ulanadi.
    """
    global _telethon_client

    if client is None:
        logger.warning("⚠️ Kitobxon: Telethon client None")
        return False

    if getattr(client, "_kitob_attached", False):
        return True

    try:
        from telethon import events
    except ImportError:
        logger.error("❌ telethon o'rnatilmagan")
        return False

    _telethon_client = client

    entity = await _get_kitob_entity(client)

    if entity is not None:
        client.add_event_handler(
            _kitob_on_message,
            events.NewMessage(incoming=True, from_users=entity),
        )
        client._kitob_attached = True
        logger.info("👁 Kitobxon listener faol: entity")
        return True

    logger.error(
        "❌ Kitobxon listener ulanmadi: bot topilmadi (@%s). "
        "Telethon akkaunti u botga /start bosganmi va username to'g'rimi?",
        get_kitob_username(),
    )
    return False


# ---------------------------------------------------------------------
# ADMIN / USER UI YORDAMCHILARI
# ---------------------------------------------------------------------

def kitob_user_keyboard(types_module):
    """
    User uchun inline tugmalar.
    app.py dagi telebot.types ni parametr qilib berish mumkin.
    """
    mk = types_module.InlineKeyboardMarkup(row_width=1)

    link = get_kitob_link()

    if link:
        mk.add(
            types_module.InlineKeyboardButton(
                "📚 Kitobxon loyihasiga o'tish",
                url=link,
            )
        )

    mk.add(
        types_module.InlineKeyboardButton(
            "✅ Tekshirish",
            callback_data="kitob_check",
        )
    )
    return mk


def admin_status_text():
    row = _get_settings()

    enabled = bool(_setting_value(row, "enabled", 1))
    required = bool(_setting_value(row, "require_registration", 1))
    username = _setting_value(row, "bot_username", "") or "—"
    link = _setting_value(row, "bot_link", "") or "—"

    return (
        "📚 <b>Kitobxon majburiy ro'yxatdan o'tishi</b>\n\n"
        f"Holat: {'✅ Yoqilgan' if enabled else '❌ O‘chirilgan'}\n"
        f"Majburiy: {'✅ Ha' if required else '❌ Yo‘q'}\n"
        f"Bot: <code>@{username.lstrip('@')}</code>\n"
        f"Link: {link}\n\n"
        "Mantiq:\n"
        "1️⃣ Kitobxon xabari keladi.\n"
        "2️⃣ Ism/familiya pending bazaga yoziladi.\n"
        "3️⃣ Gift user shu ism bilan kirsa Telegram ID biriktiriladi.\n"
        "4️⃣ Keyingi tekshiruvlar ID orqali bajariladi."
    )


def register_handlers(bot, types_module, is_admin_func):
    """
    Ixtiyoriy: app.py ichida Kitobxon admin tugmalarini avtomatik ro'yxatdan
    o'tkazish uchun chaqiriladi.

    app.py:
        kitob.register_handlers(bot, types, is_admin)
    """

    @bot.message_handler(
        func=lambda m: m.text == "🤖 Kitobxon bot"
        and is_admin_func(m.from_user.id)
    )
    def kitob_admin_menu(m):
        mk = types_module.InlineKeyboardMarkup(row_width=1)

        mk.add(
            types_module.InlineKeyboardButton(
                "🟢/🔴 Yoqish/O'chirish",
                callback_data="kitob_toggle",
            )
        )
        mk.add(
            types_module.InlineKeyboardButton(
                "🔗 Linkni o'zgartirish",
                callback_data="kitob_set_link",
            )
        )
        mk.add(
            types_module.InlineKeyboardButton(
                "👥 Ro'yxatdan o'tganlar",
                callback_data="kitob_users_count",
            )
        )
        mk.add(
            types_module.InlineKeyboardButton(
                "⏳ Pending referral",
                callback_data="kitob_pending_count",
            )
        )

        bot.send_message(
            m.chat.id,
            admin_status_text(),
            reply_markup=mk,
        )

    @bot.callback_query_handler(func=lambda c: c.data == "kitob_toggle")
    def kitob_toggle(call):
        if not is_admin_func(call.from_user.id):
            return

        current = is_kitob_enabled()
        set_kitob_setting("enabled", 0 if current else 1)

        bot.answer_callback_query(
            call.id,
            "Yoqildi" if not current else "O'chirildi",
        )

        try:
            bot.edit_message_text(
                admin_status_text(),
                call.message.chat.id,
                call.message.message_id,
                reply_markup=_admin_inline(types_module),
            )
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data == "kitob_users_count")
    def kitob_users_count(call):
        if not is_admin_func(call.from_user.id):
            return

        _require_config()
        conn = _get_db()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS n FROM kitob_users")
            users = cur.fetchone()["n"]

            cur.execute(
                """
                SELECT COUNT(*) AS n
                FROM kitob_referrals
                WHERE matched = 1
                """
            )
            matched = cur.fetchone()["n"]
        finally:
            _put_db(conn)

        bot.answer_callback_query(call.id)
        bot.send_message(
            call.from_user.id,
            "📚 <b>Kitobxon statistikasi</b>\n\n"
            f"🆔 ID biriktirilgan: <b>{users}</b>\n"
            f"🔗 Referral moslangan: <b>{matched}</b>",
        )

    @bot.callback_query_handler(func=lambda c: c.data == "kitob_pending_count")
    def kitob_pending_count(call):
        if not is_admin_func(call.from_user.id):
            return

        _require_config()
        conn = _get_db()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) AS n
                FROM kitob_referrals
                WHERE matched = 0
                  AND telegram_id IS NULL
                """
            )
            n = cur.fetchone()["n"]
        finally:
            _put_db(conn)

        bot.answer_callback_query(call.id)
        bot.send_message(
            call.from_user.id,
            f"⏳ <b>Pending Kitobxon referral:</b> {n} ta",
        )

    @bot.callback_query_handler(func=lambda c: c.data == "kitob_set_link")
    def kitob_set_link(call):
        if not is_admin_func(call.from_user.id):
            return

        msg = bot.send_message(
            call.from_user.id,
            "🔗 <b>Kitobxon bot linkini yuboring:</b>\n\n"
            "Misol:\n"
            "<code>https://t.me/Kitobxonloyihasibot</code>\n\n"
            "❌ Bekor qilish: /cancel",
        )
        bot.register_next_step_handler(msg, _process_link)

        bot.answer_callback_query(call.id)

    def _process_link(message):
        if not is_admin_func(message.from_user.id):
            return

        text = (message.text or "").strip()

        if text in ("/cancel", "❌ Bekor"):
            bot.send_message(message.chat.id, "Bekor qilindi.")
            return

        username = extract_username(text)

        if not username:
            bot.send_message(
                message.chat.id,
                "❌ To'g'ri Telegram bot link yuboring.\n"
                "Misol: <code>https://t.me/Kitobxonloyihasibot</code>",
            )
            return

        link = f"https://t.me/{username}"

        set_kitob_setting("bot_username", username)
        set_kitob_setting("bot_link", link)

        bot.send_message(
            message.chat.id,
            f"✅ Kitobxon bot yangilandi:\n\n"
            f"🤖 @{username}\n"
            f"🔗 {link}",
        )

    @bot.callback_query_handler(func=lambda c: c.data == "kitob_check")
    def kitob_user_check(call):
        # Bu callbackni app.py ham ishlatishi mumkin.
        # Faqat Kitobxon holatini tekshiramiz.
        uid = call.from_user.id

        try:
            first = call.from_user.first_name or ""
            last = call.from_user.last_name or ""

            ok = is_registered(uid, first, last)

            if ok:
                bot.answer_callback_query(
                    call.id,
                    "✅ Kitobxon ro'yxatidan o'tgansiz!",
                )
            else:
                bot.answer_callback_query(
                    call.id,
                    "❌ Hali ro'yxatdan o'tmagansiz.",
                    show_alert=True,
                )
        except Exception as exc:
            logger.exception("Kitobxon check error: %s", exc)
            bot.answer_callback_query(
                call.id,
                "⚠️ Tekshirishda xato.",
                show_alert=True,
            )


def _admin_inline(types_module):
    mk = types_module.InlineKeyboardMarkup(row_width=1)
    mk.add(
        types_module.InlineKeyboardButton(
            "🟢/🔴 Yoqish/O'chirish",
            callback_data="kitob_toggle",
        )
    )
    mk.add(
        types_module.InlineKeyboardButton(
            "🔗 Linkni o'zgartirish",
            callback_data="kitob_set_link",
        )
    )
    return mk


def extract_username(text):
    """
    @username
    https://t.me/username
    t.me/username
    formatlarini qabul qiladi.
    """
    value = (text or "").strip()

    value = re.sub(
        r"^https?://t\.me/",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"^t\.me/", "", value, flags=re.IGNORECASE)
    value = value.split("?")[0].split("/")[0].strip().lstrip("@")

    if not re.fullmatch(r"[A-Za-z0-9_]{5,64}", value):
        return None

    return value


# ---------------------------------------------------------------------
# EXPORT / DEBUG
# ---------------------------------------------------------------------

def get_stats():
    _require_config()

    conn = _get_db()
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS n FROM kitob_users")
        users = cur.fetchone()["n"]

        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM kitob_referrals
            WHERE matched = 1
            """
        )
        matched = cur.fetchone()["n"]

        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM kitob_referrals
            WHERE matched = 0
              AND telegram_id IS NULL
            """
        )
        pending = cur.fetchone()["n"]

        return {
            "users": users,
            "matched": matched,
            "pending": pending,
        }
    finally:
        _put_db(conn)


__all__ = [
    "configure",
    "init_db",
    "attach_telethon_client",
    "register_handlers",
    "is_registered",
    "find_and_bind_user",
    "get_missing_requirement",
    "get_kitob_link",
    "get_kitob_username",
    "set_kitob_setting",
    "get_stats",
    "parse_kitobxon_message",
    "save_pending_referral",
]
