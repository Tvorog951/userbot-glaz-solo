# -*- coding: utf-8 -*-
"""
Бизнес-бот Telegram (Business API): зеркалирование сообщений бизнес-аккаунта,
уведомления о редактировании/удалении, сохранение в общую БД.
Запускается вместе с userbot, если задан BOT_TOKEN.
"""
import asyncio
import html
import json
import logging
from typing import Optional, Dict, Any, List

import aiohttp
from telegram import Update, Message, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    BaseHandler,
)
from telegram.constants import ChatAction

from config import BOT_TOKEN
from database import MessageDatabase

logger = logging.getLogger(__name__)

# Хранилище для callback "Fetch media" (token -> payload)
_action_store: Dict[str, Dict] = {}
_ACTION_TTL = 10 * 60  # 10 минут


def _put_action(connection_id: str, message_ids: List[int]) -> str:
    import time
    token = f"{int(time.time())}_{id(message_ids)}"
    _action_store[token] = {
        "connection_id": connection_id,
        "message_ids": message_ids,
    }
    return token


def _get_action(token: str) -> Optional[Dict]:
    return _action_store.pop(token, None)


def _msg_to_dict(msg: Message) -> Dict[str, Any]:
    """Сериализация сообщения для сохранения в БД."""
    if not msg:
        return {}
    try:
        return msg.to_dict()
    except Exception:
        d = {"message_id": msg.message_id, "chat": {"id": msg.chat.id} if msg.chat else {}, "date": msg.date}
        if msg.text:
            d["text"] = msg.text
        if msg.caption:
            d["caption"] = msg.caption
        if msg.photo:
            d["photo"] = [{"file_id": p.file_id} for p in msg.photo]
        if msg.video:
            d["video"] = {"file_id": msg.video.file_id}
        if msg.document:
            d["document"] = {"file_id": msg.document.file_id}
        if msg.voice:
            d["voice"] = {"file_id": msg.voice.file_id}
        if msg.audio:
            d["audio"] = {"file_id": msg.audio.file_id}
        if msg.sticker:
            d["sticker"] = {"file_id": msg.sticker.file_id}
        if msg.video_note:
            d["video_note"] = {"file_id": msg.video_note.file_id}
        if msg.from_user:
            d["from"] = msg.from_user.to_dict()
        return d


def _escape(s: str) -> str:
    return html.escape(s or "")


def _display_name(user: Optional[Dict]) -> str:
    if not user:
        return "Unknown"
    parts = [user.get("first_name"), user.get("last_name")]
    name = " ".join(p for p in parts if p).strip()
    return name or (f"@{user.get('username')}" if user.get("username") else "Unknown")


def _summarize_message(data: Dict) -> str:
    """Краткое описание сообщения для уведомлений."""
    if data.get("text"):
        t = data["text"][:220]
        return f"💬 {_escape(t)}{'…' if len(data.get('text', '')) > 220 else ''}"
    if data.get("caption"):
        t = data["caption"][:220]
        return f"📝 {_escape(t)}{'…' if len(data.get('caption', '')) > 220 else ''}"
    if data.get("photo"):
        return "🖼 Photo"
    if data.get("video"):
        return "🎞 Video"
    if data.get("video_note"):
        return "📹 Video Note"
    if data.get("document"):
        return "📄 Document"
    if data.get("voice"):
        return "🎙 Voice"
    if data.get("audio"):
        return "🎵 Audio"
    if data.get("sticker"):
        return "🔖 Sticker"
    if data.get("animation"):
        return "🖼 GIF"
    if data.get("location"):
        return "📍 Location"
    if data.get("venue"):
        return "📍 Venue"
    if data.get("contact"):
        return "👤 Contact"
    return "🗂 Сообщение"


async def _send_similar_message(bot: Bot, chat_id: int, data: Dict) -> bool:
    """Отправка сообщения, аналогичного сохранённому (по данным из БД)."""
    try:
        if data.get("text"):
            await bot.send_message(chat_id, data["text"], entities=data.get("entities"))
            return True
        if data.get("photo"):
            fid = data["photo"][-1]["file_id"] if isinstance(data["photo"][-1], dict) else data["photo"][-1].file_id
            await bot.send_photo(chat_id, fid, caption=data.get("caption"), caption_entities=data.get("caption_entities"))
            return True
        if data.get("document"):
            doc = data["document"]
            fid = doc["file_id"] if isinstance(doc, dict) else doc.file_id
            await bot.send_document(chat_id, fid, caption=data.get("caption"), caption_entities=data.get("caption_entities"))
            return True
        if data.get("video"):
            v = data["video"]
            fid = v["file_id"] if isinstance(v, dict) else v.file_id
            await bot.send_video(chat_id, fid, caption=data.get("caption"), caption_entities=data.get("caption_entities"))
            return True
        if data.get("video_note"):
            vn = data["video_note"]
            fid = vn["file_id"] if isinstance(vn, dict) else vn.file_id
            await bot.send_video_note(chat_id, fid)
            return True
        if data.get("sticker"):
            s = data["sticker"]
            fid = s["file_id"] if isinstance(s, dict) else s.file_id
            await bot.send_sticker(chat_id, fid)
            return True
        if data.get("voice"):
            v = data["voice"]
            fid = v["file_id"] if isinstance(v, dict) else v.file_id
            await bot.send_voice(chat_id, fid, caption=data.get("caption"), caption_entities=data.get("caption_entities"))
            return True
        if data.get("audio"):
            a = data["audio"]
            fid = a["file_id"] if isinstance(a, dict) else a.file_id
            await bot.send_audio(chat_id, fid, caption=data.get("caption"), caption_entities=data.get("caption_entities"))
            return True
        if data.get("animation"):
            a = data["animation"]
            fid = a["file_id"] if isinstance(a, dict) else a.file_id
            await bot.send_animation(chat_id, fid, caption=data.get("caption"), caption_entities=data.get("caption_entities"))
            return True
        await bot.send_message(chat_id, "⚠️ Тип сообщения не поддерживается для пересылки.")
    except Exception as e:
        logger.warning("Ошибка пересылки сообщения: %s", e)
    return False


async def _reupload_file(bot: Bot, file_id: str, chat_id: int, send_method: str, caption: Optional[str] = None) -> bool:
    """Скачать файл по file_id и отправить заново (для protected/view-once)."""
    try:
        f = await bot.get_file(file_id)
        path = f.file_path
        if not path:
            return False
        url = f"https://api.telegram.org/file/bot{bot.token}/{path}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return False
                body = await resp.read()
        if send_method == "photo":
            await bot.send_photo(chat_id, photo=body, caption=caption)
        elif send_method == "video":
            await bot.send_video(chat_id, video=body, caption=caption)
        elif send_method == "video_note":
            await bot.send_video_note(chat_id, video_note=body)
        elif send_method == "document":
            await bot.send_document(chat_id, document=body, caption=caption)
        else:
            return False
        return True
    except Exception as e:
        logger.warning("Ошибка reupload: %s", e)
    return False


def _get_file_id(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get("file_id")
    return getattr(obj, "file_id", None)


async def _try_resend_protected_reply(bot: Bot, reply_data: Dict, owner_chat_id: int) -> bool:
    """Если ответ на сообщение с protected content — попытаться переслать медиа через reupload."""
    if not reply_data.get("has_protected_content"):
        return False
    try:
        if reply_data.get("photo"):
            last = reply_data["photo"][-1] if reply_data["photo"] else None
            fid = _get_file_id(last) if isinstance(last, dict) else _get_file_id(last)
            if fid:
                return await _reupload_file(bot, fid, owner_chat_id, "photo", reply_data.get("caption"))
        if reply_data.get("video"):
            fid = _get_file_id(reply_data["video"])
            if fid:
                return await _reupload_file(bot, fid, owner_chat_id, "video", reply_data.get("caption"))
        if reply_data.get("video_note"):
            fid = _get_file_id(reply_data["video_note"])
            if fid:
                return await _reupload_file(bot, fid, owner_chat_id, "video_note")
    except Exception as e:
        logger.warning("Ошибка resend protected reply: %s", e)
    return False


def _get_db(context: ContextTypes.DEFAULT_TYPE) -> MessageDatabase:
    return context.application.bot_data["db"]


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    me = await context.bot.get_me()
    username = me.username or "bot"
    text = (
        "👋 Я могу зеркалировать сообщения вашего Telegram Business аккаунта сюда "
        "(новые, редактирования и удаления).\n\n"
        "<b>Как подключить</b>\n"
        "1) Откройте <b>Настройки</b> → <b>Бизнес</b> → <b>Чат-боты</b> → <b>Добавить бота</b>.\n"
        f"2) Выберите <b>@{username}</b> и разрешите доступ.\n"
        "3) Отправьте тестовое сообщение в бизнес-чате — я подтвержу здесь."
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    db = _get_db(context)
    total = await db.get_business_messages_count()
    text = f"📊 <b>Бизнес-бот</b>\n\nВсего сохранено бизнес-сообщений: <b>{total}</b>"
    await update.message.reply_text(text, parse_mode="HTML")


def _to_timestamp(d) -> int:
    if d is None:
        return 0
    if isinstance(d, int):
        return d
    if hasattr(d, "timestamp"):
        return int(d.timestamp())
    return 0


async def business_connection_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bc = update.business_connection
    if not bc:
        return
    db = _get_db(context)
    user_chat_id = bc.user_chat_id
    date_ts = _to_timestamp(getattr(bc, "date", None))
    await db.save_business_connection(
        bc.id,
        user_chat_id,
        bc.is_enabled,
        date_ts,
        bc.to_dict() if hasattr(bc, "to_dict") else {"id": bc.id, "user_chat_id": user_chat_id, "is_enabled": bc.is_enabled},
    )
    if not user_chat_id:
        return
    if bc.is_enabled:
        await context.bot.send_message(
            user_chat_id,
            f"✅ Бизнес-аккаунт подключён.\nConnection ID: <code>{_escape(bc.id)}</code>\n"
            "Отключить: Настройки → Бизнес → Чат-боты → Удалить.",
            parse_mode="HTML",
        )
    else:
        await context.bot.send_message(
            user_chat_id,
            f"❌ Бизнес-подключение отключено.\nConnection ID: <code>{_escape(bc.id)}</code>",
            parse_mode="HTML",
        )


async def business_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bm = update.business_message
    if not bm:
        return
    db = _get_db(context)
    conn = await db.get_business_connection(bm.business_connection_id)
    if not conn or not conn.get("user_chat_id"):
        return
    owner_chat_id = conn["user_chat_id"]
    data = _msg_to_dict(bm)
    await db.save_business_message(bm.business_connection_id, bm.message_id, bm.chat.id, data)
    # Ответ на сообщение с защищённым контентом — попытаться сохранить медиа
    if bm.reply_to_message and getattr(bm.reply_to_message, "has_protected_content", False):
        reply_data = _msg_to_dict(bm.reply_to_message)
        reply_data["has_protected_content"] = True
        await _try_resend_protected_reply(context.bot, reply_data, owner_chat_id)


async def edited_business_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    em = update.edited_business_message
    if not em:
        return
    db = _get_db(context)
    conn = await db.get_business_connection(em.business_connection_id)
    if not conn or not conn.get("user_chat_id"):
        return
    owner_chat_id = conn["user_chat_id"]
    old = await db.get_business_message(em.business_connection_id, em.message_id)
    old_data = (old.get("data") or {}) if old else {}
    old_text = old_data.get("text") or old_data.get("caption") or ""
    new_text = em.text or em.caption or ""
    sender = em.from_user or old_data.get("from", {})
    name = _display_name(sender if isinstance(sender, dict) else (sender.to_dict() if sender else {}))
    body = f"\n<b>Было:</b> {_escape(old_text[:300])}\n<b>Стало:</b> {_escape(new_text[:300])}" if (old_text or new_text) else "\n" + _summarize_message(_msg_to_dict(em))
    text = f"✏️ <b>Сообщение отредактировано</b>\nОт: {_escape(name)}{body}"
    await context.bot.send_message(owner_chat_id, text, parse_mode="HTML")
    await db.delete_business_message(em.business_connection_id, em.message_id)
    await db.save_business_message(em.business_connection_id, em.message_id, em.chat.id, _msg_to_dict(em))


async def deleted_business_messages_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del_update = update.deleted_business_messages
    if not del_update:
        return
    db = _get_db(context)
    conn = await db.get_business_connection(del_update.business_connection_id)
    if not conn or not conn.get("user_chat_id"):
        return
    owner_chat_id = conn["user_chat_id"]
    ids = del_update.message_ids
    previews = []
    for mid in ids[:3]:
        m = await db.get_business_message(del_update.business_connection_id, mid)
        if not m or not m.get("data"):
            continue
        from_user = (m.get("data") or {}).get("from", {})
        who = _display_name(from_user)
        previews.append(f"• {_escape(who)}: {_summarize_message(m['data'])}")
    extra = f"\n…и ещё {len(ids) - 3}" if len(ids) > 3 else ""
    text = f"🗑 <b>Сообщения удалены</b>\n" + "\n".join(previews) + extra
    token = _put_action(del_update.business_connection_id, ids)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Подробнее", callback_data=f"biz_details:{token}")],
        [InlineKeyboardButton("📎 Получить медиа", callback_data=f"biz_fetch:{token}")],
    ])
    await context.bot.send_message(owner_chat_id, text, parse_mode="HTML", reply_markup=kb)


async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return
    data = q.data
    try:
        if data == "biz_ok":
            await q.answer()
            return
        if data.startswith("biz_details:") or data.startswith("biz_fetch:"):
            token = data.split(":", 1)[1]
            payload = _get_action(token)
            if not payload:
                await q.answer("Срок действия кнопки истёк.")
                return
            db = _get_db(context)
            msgs = await db.get_business_messages_by_ids(payload["connection_id"], payload["message_ids"])
            if not msgs:
                await q.answer("Данные не найдены.")
                return
            chat_id = q.message.chat.id if q.message else 0
            if not chat_id:
                await q.answer()
                return
            if "biz_details" in data:
                for m in msgs[:25]:
                    d = m.get("data") or {}
                    who = _display_name(d.get("from"))
                    await context.bot.send_message(chat_id, f"• {_escape(who)}: {_summarize_message(d)}", parse_mode="HTML")
                    await asyncio.sleep(0.05)
                await q.answer("Готово.")
                return
            if "biz_fetch" in data:
                await context.bot.send_chat_action(chat_id, ChatAction.TYPING)
                for m in msgs:
                    d = m.get("data") or {}
                    await _send_similar_message(context.bot, chat_id, d)
                    await asyncio.sleep(0.15)
                await q.answer("Контент отправлен.")
                return
        await q.answer()
    except Exception as e:
        logger.exception("Ошибка в callback_query: %s", e)
        try:
            await q.answer("Ошибка.")
        except Exception:
            pass


class _BusinessMessageHandler(BaseHandler):
    """Обработчик входящих бизнес-сообщений (PTB не имеет встроенного)."""
    def check_update(self, update: object) -> bool:
        return getattr(update, "business_message", None) is not None


class _EditedBusinessMessageHandler(BaseHandler):
    """Обработчик отредактированных бизнес-сообщений."""
    def check_update(self, update: object) -> bool:
        return getattr(update, "edited_business_message", None) is not None


def build_application(db: MessageDatabase) -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    app.bot_data["db"] = db
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CallbackQueryHandler(callback_query_handler))
    # Business API: официальные обработчики (PTB 21+)
    try:
        from telegram.ext import BusinessConnectionHandler, BusinessMessagesDeletedHandler
        app.add_handler(BusinessConnectionHandler(business_connection_handler))
        app.add_handler(BusinessMessagesDeletedHandler(deleted_business_messages_handler))
    except ImportError:
        class _BusinessConnectionHandler(BaseHandler):
            def check_update(self, update: object) -> bool:
                return getattr(update, "business_connection", None) is not None
        class _DeletedBusinessHandler(BaseHandler):
            def check_update(self, update: object) -> bool:
                return getattr(update, "deleted_business_messages", None) is not None
        app.add_handler(_BusinessConnectionHandler(business_connection_handler))
        app.add_handler(_DeletedBusinessHandler(deleted_business_messages_handler))
    app.add_handler(_BusinessMessageHandler(business_message_handler))
    app.add_handler(_EditedBusinessMessageHandler(edited_business_message_handler))
    return app


async def run_business_bot(db: MessageDatabase) -> None:
    """Запуск поллинга бизнес-бота (вызывается из userbot)."""
    if not BOT_TOKEN:
        logger.info("BOT_TOKEN не задан — бизнес-бот отключён.")
        return
    app = build_application(db)
    allowed = [
        "message", "edited_message", "callback_query",
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages",
    ]
    await app.initialize()
    try:
        await app.start()
        logger.info("Бизнес-бот запущен (Business API).")
        await app.updater.start_polling(allowed_updates=allowed)
    except asyncio.CancelledError:
        pass
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
