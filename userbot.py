import asyncio
import logging
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User, Chat, Channel
from telethon.errors import FloodWaitError, ChatAdminRequiredError
from config import (
    API_ID,
    API_HASH,
    SESSION_NAME,
    STRING_SESSION,
    LOG_LEVEL,
    LOG_FILE,
    BOT_TOKEN,
    NOTIFY_CHAT_ID,
)
from database import MessageDatabase

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Инициализация базы данных
db = MessageDatabase()

# Инициализация клиента Telegram
session_arg = StringSession(STRING_SESSION) if STRING_SESSION else SESSION_NAME
client = TelegramClient(session_arg, API_ID, API_HASH)

# Флаг для отслеживания активного парсинга
parsing_active = {}

# Бот для уведомлений об удалённых/отредактированных сообщениях (не Business API — просто отправка в NOTIFY_CHAT_ID)
_notify_bot = None


def _init_notify_bot():
    """Инициализация бота для уведомлений (вызывается из main после start)."""
    global _notify_bot
    if BOT_TOKEN and NOTIFY_CHAT_ID is not None:
        try:
            from telegram import Bot
            _notify_bot = Bot(BOT_TOKEN)
            logger.info("Уведомления через бота включены (NOTIFY_CHAT_ID=%s)", NOTIFY_CHAT_ID)
        except Exception as e:
            logger.warning("Не удалось инициализировать бота уведомлений: %s", e)


async def _send_notification_via_bot(text: str):
    """Отправить уведомление в NOTIFY_CHAT_ID через бота."""
    if _notify_bot is None:
        return
    try:
        await _notify_bot.send_message(chat_id=NOTIFY_CHAT_ID, text=text)
    except Exception as e:
        logger.warning("Не удалось отправить уведомление через бота: %s", e)


def _format_sender(row):
    """Краткое имя отправителя из строки БД."""
    if not row:
        return "?"
    if row.get("username"):
        return f"@{row['username']}"
    name = (row.get("first_name") or "").strip()
    if row.get("last_name"):
        name = f"{name} {row['last_name']}".strip()
    return name or str(row.get("user_id") or "?")


def get_chat_info(chat):
    """Получение информации о чате"""
    if isinstance(chat, User):
        return {
            'chat_id': chat.id,
            'chat_title': f"{chat.first_name or ''} {chat.last_name or ''}".strip() or chat.username or f"User {chat.id}",
            'chat_type': 'private',
            'participants_count': 1
        }
    elif isinstance(chat, (Chat, Channel)):
        return {
            'chat_id': chat.id,
            'chat_title': getattr(chat, 'title', None) or f"Chat {chat.id}",
            'chat_type': 'channel' if isinstance(chat, Channel) else 'group',
            'participants_count': getattr(chat, 'participants_count', None)
        }
    return {
        'chat_id': chat.id if hasattr(chat, 'id') else 0,
        'chat_title': 'Unknown',
        'chat_type': 'unknown',
        'participants_count': None
    }


def get_user_info(sender):
    """Получение информации о пользователе"""
    if not sender:
        return {
            'user_id': None,
            'username': None,
            'first_name': None,
            'last_name': None
        }
    
    return {
        'user_id': sender.id,
        'username': getattr(sender, 'username', None),
        'first_name': getattr(sender, 'first_name', None),
        'last_name': getattr(sender, 'last_name', None)
    }


def get_media_info(message):
    """Получение информации о медиа в сообщении"""
    if not message.media:
        return {
            'has_media': False,
            'media_type': None
        }
    
    media_type = type(message.media).__name__
    return {
        'has_media': True,
        'media_type': media_type
    }


async def process_message(message, chat, sender=None):
    """Обработка и сохранение сообщения"""
    try:
        # Получение информации о чате
        chat_info = get_chat_info(chat)
        
        # Получение информации о пользователе
        if sender is None:
            try:
                sender = await message.get_sender()
            except Exception as e:
                logger.debug(f"Не удалось получить информацию об отправителе: {e}")
                sender = None
        user_info = get_user_info(sender)
        
        # Получение информации о медиа
        media_info = get_media_info(message)
        
        # Проверка, является ли сообщение ответом
        is_reply = message.reply_to is not None
        reply_to_message_id = None
        if is_reply and hasattr(message.reply_to, 'reply_to_msg_id'):
            reply_to_message_id = message.reply_to.reply_to_msg_id
        
        # Подготовка данных для сохранения
        message_data = {
            'message_id': message.id,
            'chat_id': chat_info['chat_id'],
            'chat_title': chat_info['chat_title'],
            'chat_type': chat_info['chat_type'],
            'user_id': user_info['user_id'],
            'username': user_info['username'],
            'first_name': user_info['first_name'],
            'last_name': user_info['last_name'],
            'message_text': message.text or message.raw_text or '',
            'date': message.date.isoformat() if message.date else datetime.now().isoformat(),
            'is_reply': 1 if is_reply else 0,
            'reply_to_message_id': reply_to_message_id,
            'has_media': 1 if media_info['has_media'] else 0,
            'media_type': media_info['media_type'],
            'raw_data': {
                'message_id': message.id,
                'date': message.date.isoformat() if message.date else None,
                'views': getattr(message, 'views', None),
                'forwards': getattr(message, 'forwards', None),
                'replies': getattr(message.replies, 'replies', None) if hasattr(message, 'replies') and message.replies else None,
            }
        }
        
        # Сохранение сообщения
        await db.save_message(message_data)
        
        # Сохранение информации о чате
        chat_data = {
            **chat_info,
            'metadata': {
                'access_hash': getattr(chat, 'access_hash', None),
                'username': getattr(chat, 'username', None)
            }
        }
        await db.save_chat(chat_data)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)
        return False


async def parse_chat_history(chat_entity, limit=None, offset_date=None):
    """
    Парсинг истории сообщений из чата
    
    Args:
        chat_entity: Объект чата (может быть username, ID или entity)
        limit: Максимальное количество сообщений для парсинга (None = все)
        offset_date: Дата, с которой начинать парсинг (None = с начала)
    """
    chat_id = None
    chat_title = "Unknown"
    
    try:
        # Получение информации о чате
        try:
            if isinstance(chat_entity, (int, str)):
                chat = await client.get_entity(chat_entity)
            else:
                chat = chat_entity
        except ValueError as e:
            logger.error(f"Группа не найдена: {chat_entity}. Ошибка: {e}")
            raise ValueError(f"Группа '{chat_entity}' не найдена. Проверьте username или ID, или убедитесь, что у вас есть доступ к группе.")
        except Exception as e:
            logger.error(f"Ошибка при получении информации о группе {chat_entity}: {e}")
            raise
        
        chat_info = get_chat_info(chat)
        chat_id = chat_info['chat_id']
        chat_title = chat_info['chat_title']
        
        # Проверка, не идет ли уже парсинг этого чата
        if chat_id in parsing_active and parsing_active[chat_id]:
            logger.warning(f"Парсинг чата {chat_title} уже выполняется")
            return False
        
        parsing_active[chat_id] = True
        logger.info(f"Начало парсинга истории чата: {chat_title} (ID: {chat_id})")
        
        total_parsed = 0
        errors_count = 0
        
        try:
            async for message in client.iter_messages(
                chat,
                limit=limit,
                offset_date=offset_date,
                reverse=False  # Сначала старые сообщения
            ):
                try:
                    # Пропускаем служебные сообщения
                    if message.action:
                        continue
                    
                    try:
                        sender = await message.get_sender()
                    except Exception as e:
                        logger.debug(f"Не удалось получить отправителя для сообщения {message.id}: {e}")
                        sender = None
                    
                    success = await process_message(message, chat, sender)
                    
                    if success:
                        total_parsed += 1
                        if total_parsed % 100 == 0:
                            logger.info(f"Обработано сообщений из {chat_title}: {total_parsed}")
                    else:
                        errors_count += 1
                    
                    # Небольшая задержка, чтобы не получить FloodWait
                    if total_parsed % 50 == 0:
                        await asyncio.sleep(1)
                        
                except FloodWaitError as e:
                    logger.warning(f"FloodWait: ожидание {e.seconds} секунд...")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    errors_count += 1
                    logger.error(f"Ошибка при обработке сообщения {message.id}: {e}")
                    continue
                    
        except ChatAdminRequiredError:
            logger.error(f"Нет доступа к истории чата {chat_title}. Убедитесь, что бот добавлен в группу и имеет права.")
            return False
        except Exception as e:
            logger.error(f"Ошибка при парсинге чата {chat_title}: {e}", exc_info=True)
            return False
        finally:
            parsing_active[chat_id] = False
        
        logger.info(f"Парсинг завершен: {chat_title}. Обработано: {total_parsed}, Ошибок: {errors_count}")
        return True
        
    except Exception as e:
        logger.error(f"Критическая ошибка при парсинге чата: {e}", exc_info=True)
        if chat_id:
            parsing_active[chat_id] = False
        return False


@client.on(events.NewMessage(incoming=True))
async def handler(event):
    """Обработчик новых сообщений"""
    try:
        message = event.message
        message_text = message.text or ""
        chat_id = event.chat_id
        
        # Логируем ВСЕ входящие сообщения для отладки
        logger.info(f"📨 ВХОДЯЩЕЕ сообщение: '{message_text[:100]}' | chat_id: {chat_id} | is_private: {event.is_private}")
        
        # Пропускаем служебные сообщения
        if message.action:
            return
        
        # Пропускаем команды (они обрабатываются отдельными обработчиками)
        # НО НЕ БЛОКИРУЕМ их - пусть специальные обработчики сработают
        if message_text.startswith('/'):
            logger.info(f"⚡ Обнаружена команда в общем обработчике: '{message_text}' - пропускаем для специальных обработчиков")
            return
        
        chat = await event.get_chat()
        sender = await event.get_sender()
        await process_message(message, chat, sender)
        
        chat_info = get_chat_info(chat)
        user_info = get_user_info(sender)
        logger.debug(f"Сохранено сообщение: {chat_info['chat_title']} - {user_info['username'] or user_info['first_name'] or 'Unknown'}")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)


@client.on(events.MessageEdited)
async def handler_edited(event):
    """Обработчик отредактированных сообщений: сохраняем новую версию и шлём уведомление через бота."""
    try:
        message = event.message
        chat = await event.get_chat()
        sender = await event.get_sender()
        chat_id = event.chat_id
        chat_info = get_chat_info(chat)
        chat_title = chat_info.get("chat_title") or f"Chat {chat_id}"
        # Старая версия из БД до сохранения новой
        old_row = await db.get_message_by_chat_and_id(chat_id, message.id)
        old_text = (old_row.get("message_text") or "").strip() if old_row else ""
        new_text = (message.text or message.raw_text or "").strip()
        # Сохраняем обновлённое сообщение
        await process_message(message, chat, sender)
        logger.debug(f"Отредактировано сообщение в чате {chat_id}")
        # Уведомление через бота
        if _notify_bot and old_text != new_text:
            who = _format_sender(old_row) if old_row else _format_sender(get_user_info(sender))
            preview = 400
            old_preview = (old_text[:preview] + "…") if len(old_text) > preview else (old_text or "—")
            new_preview = (new_text[:preview] + "…") if len(new_text) > preview else (new_text or "—")
            msg = (
                f"✏️ Редактирование в «{chat_title}»\n"
                f"От: {who}\n"
                f"Было: {old_preview}\n"
                f"Стало: {new_preview}"
            )
            if len(msg) > 4000:
                msg = msg[:3990] + "\n…"
            await _send_notification_via_bot(msg)
    except Exception as e:
        logger.error(f"Ошибка при обработке отредактированного сообщения: {e}", exc_info=True)


@client.on(events.MessageDeleted)
async def handler_deleted(event):
    """Обработчик удалённых сообщений: если сообщение было в БД — присылаем тебе через бота."""
    if _notify_bot is None:
        return
    try:
        chat_id = getattr(event, "chat_id", None)
        for msg_id in (event.deleted_ids or []):
            rows = []
            if chat_id is not None:
                row = await db.get_message_by_chat_and_id(chat_id, msg_id)
                if row:
                    rows.append(row)
            else:
                rows = await db.get_messages_by_message_id(msg_id)
            for row in rows:
                chat_title = row.get("chat_title") or f"Chat {row.get('chat_id')}"
                who = _format_sender(row)
                text = (row.get("message_text") or "").strip()
                has_media = row.get("has_media")
                media_note = " [медиа]" if has_media else ""
                preview = (text[:500] + "…") if len(text) > 500 else (text or "—")
                msg = (
                    f"🗑 Удалено в «{chat_title}»\n"
                    f"От: {who}{media_note}\n"
                    f"Текст: {preview}"
                )
                if len(msg) > 4000:
                    msg = msg[:3990] + "\n…"
                await _send_notification_via_bot(msg)
    except Exception as e:
        logger.error(f"Ошибка при обработке удалённого сообщения: {e}", exc_info=True)


# Обработчик команды /parse - используем более гибкий паттерн
@client.on(events.NewMessage(pattern=r'^/parse'))
async def parse_command_handler(event):
    """Обработчик команды /parse для парсинга истории чата"""
    try:
        # Логируем СРАЗУ при срабатывании обработчика
        message_text = event.message.text or ""
        chat_id = event.chat_id
        
        logger.info(f"🎯 ОБРАБОТЧИК /parse СРАБОТАЛ! Сообщение: '{message_text}' | chat_id: {chat_id}")
        
        # Получаем информацию о себе для проверки Saved Messages
        me = await client.get_me()
        is_saved_messages = (chat_id == me.id)
        
        logger.info(f"🔍 Детали: is_private: {event.is_private} | is_saved: {is_saved_messages} | my_id: {me.id}")
        
        # Команда работает в личных сообщениях (включая Saved Messages)
        # Saved Messages имеет chat_id равный вашему user_id
        if not event.is_private and not is_saved_messages:
            logger.warning(f"⚠️ Сообщение не из личного чата. Chat ID: {chat_id}, My ID: {me.id}")
            return
        
        # Получаем аргументы команды - парсим вручную из текста сообщения
        # Формат: /parse @username или /parse @username limit=1000
        parts = message_text.split(None, 1)  # Разделяем по пробелам, максимум 2 части
        if len(parts) < 2:
            await event.respond("❌ Неверный формат команды. Используйте: `/parse @username` или `/parse @username limit=1000`")
            return
        
        args = parts[1].strip()  # Все что после /parse
        
        # Парсим аргументы: /parse @username или /parse @username limit=1000
        args_parts = args.split()
        chat_identifier = args_parts[0]
        limit = None
        
        logger.info(f"📋 Парсинг аргументов: chat_identifier='{chat_identifier}', остальное='{args_parts[1:] if len(args_parts) > 1 else []}'")
        
        # Поиск параметра limit
        for part in args_parts[1:]:
            if part.startswith('limit='):
                try:
                    limit = int(part.split('=')[1])
                    logger.info(f"📊 Установлен лимит: {limit}")
                except ValueError:
                    logger.warning(f"⚠️ Неверный формат limit: {part}")
                    pass
        
        await event.respond(f"🔄 Начинаю парсинг чата: {chat_identifier}\n⏳ Это может занять некоторое время...")
        
        # Запускаем парсинг в фоне
        try:
            success = await parse_chat_history(chat_identifier, limit=limit)
            
            if success:
                count = await db.get_messages_count()  # Получаем общее количество
                await event.respond(
                    f"✅ Парсинг завершен!\n"
                    f"📊 Всего сообщений в базе: {count}\n"
                    f"💾 Используйте /stats для детальной статистики"
                )
            else:
                await event.respond(
                    "❌ Ошибка при парсинге.\n"
                    "Возможные причины:\n"
                    "• Группа приватная и вы не участник\n"
                    "• Неправильный username или ID\n"
                    "• Нет доступа к истории сообщений\n\n"
                    "Проверьте логи для подробностей."
                )
        except ValueError as e:
            # Ошибка при получении entity (группа не найдена)
            await event.respond(
                f"❌ Группа не найдена: {chat_identifier}\n\n"
                "Проверьте:\n"
                "• Правильность username (например: @groupname)\n"
                "• Правильность ID группы\n"
                "• Доступ к группе (для приватных групп нужно быть участником)"
            )
        except Exception as e:
            error_msg = str(e)
            if "username" in error_msg.lower() or "not found" in error_msg.lower():
                await event.respond(
                    f"❌ Группа не найдена или нет доступа.\n"
                    f"Ошибка: {error_msg}\n\n"
                    "Для приватных групп нужно быть участником."
                )
            else:
                await event.respond(f"❌ Ошибка: {error_msg}\nПроверьте логи для подробностей.")
            
    except Exception as e:
        logger.error(f"Ошибка в команде /parse: {e}", exc_info=True)
        await event.respond(f"❌ Критическая ошибка: {str(e)}")


@client.on(events.NewMessage(pattern=r'^/stats$', incoming=True, from_users=None))
async def stats_command_handler(event):
    """Обработчик команды /stats для получения статистики"""
    try:
        if not event.is_private:
            return
        
        total_messages = await db.get_messages_count()
        chats = await db.get_chats()
        
        stats_text = f"📊 **Статистика парсера**\n\n"
        stats_text += f"Всего сообщений: {total_messages}\n"
        stats_text += f"Всего чатов: {len(chats)}\n\n"
        stats_text += "**Топ чатов:**\n"
        
        # Получаем статистику по чатам
        for chat in chats[:10]:
            chat_messages = await db.get_messages_count(chat['chat_id'])
            stats_text += f"• {chat['chat_title']}: {chat_messages} сообщений\n"
        
        await event.respond(stats_text)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /stats: {e}", exc_info=True)
        await event.respond(f"❌ Ошибка: {str(e)}")


@client.on(events.NewMessage(pattern=r'^/help$'))
async def help_command_handler(event):
    """Обработчик команды /help"""
    try:
        chat_id = event.chat_id
        me = await client.get_me()
        is_saved_messages = (chat_id == me.id)
        
        logger.info(f"🔍 ОБРАБОТЧИК /help: chat_id: {chat_id} | is_private: {event.is_private} | is_saved: {is_saved_messages}")
        
        if not event.is_private and not is_saved_messages:
            return
        
        help_text = """
🤖 **Команды userbot:**

`/parse @username` — парсинг истории чата
`/parse @username limit=1000` — парсинг с лимитом
`/stats` — статистика
`/help` — эта справка

**Уведомления об удалениях/редактированиях:**  
В .env задайте BOT_TOKEN и NOTIFY_CHAT_ID (куда слать). Userbot отслеживает правки и удаления в чатах, где вы есть, и присылает тебе содержание через бота. Подключать бота в Настройках ТГ не нужно.

**Примеры:** `/parse @mygroup` • `/parse @support_group limit=5000`
        """
        
        await event.respond(help_text)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /help: {e}", exc_info=True)


async def main():
    """Основная функция запуска userbot"""
    logger.info("Запуск userbot...")
    
    # Подключение к базе данных
    await db.connect()
    logger.info("Подключено к базе данных")
    
    # Подключение к Telegram
    import os
    if STRING_SESSION:
        logger.info("Используется STRING_SESSION из переменных окружения")
        await client.start()
    else:
        # Проверяем наличие файла сессии
        session_file = f"{SESSION_NAME}.session"
        if not os.path.exists(session_file):
            logger.warning(f"Файл сессии {session_file} не найден!")
            logger.warning("Userbot требует авторизацию. Запустите локально один раз для создания сессии.")
            logger.warning("Или используйте переменные окружения PHONE и PHONE_CODE для авторизации.")
            
            # Попытка авторизации через переменные окружения
            phone = os.getenv('PHONE')
            phone_code = os.getenv('PHONE_CODE')
            
            if phone and phone_code:
                logger.info(f"Попытка авторизации через переменные окружения для {phone}")
                try:
                    await client.start(phone=phone, code_callback=lambda: phone_code)
                    logger.info("Авторизация успешна через переменные окружения!")
                except Exception as e:
                    logger.error(f"Ошибка авторизации через переменные окружения: {e}")
                    logger.error("Загрузите файл сессии или авторизуйтесь локально")
                    raise
            else:
                logger.error("Файл сессии не найден и переменные окружения PHONE/PHONE_CODE не указаны")
                logger.error("Запустите userbot локально один раз для создания сессии, затем загрузите файл на сервер или укажите STRING_SESSION")
                raise FileNotFoundError(f"Файл сессии {session_file} не найден. Загрузите его на сервер или авторизуйтесь локально.")
        else:
            await client.start()
    
    logger.info("Userbot запущен и готов к работе!")
    
    # Бот для уведомлений об удалённых/отредактированных (BOT_TOKEN + NOTIFY_CHAT_ID в .env)
    _init_notify_bot()
    
    # Получение информации о себе
    me = await client.get_me()
    logger.info(f"Вошли как: {me.first_name} {me.last_name or ''} (@{me.username or 'без username'})")
    logger.info(f"ID аккаунта: {me.id}")
    
    # Статистика
    messages_count = await db.get_messages_count()
    logger.info(f"Всего сообщений в базе: {messages_count}")
    
    # Информация о командах
    logger.info("Доступные команды (в личных сообщениях): /parse, /stats, /help")
    if _notify_bot:
        logger.info("Уведомления об удалениях/редактированиях включены (бот → NOTIFY_CHAT_ID)")
    
    # Запуск в режиме ожидания
    await client.run_until_disconnected()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановка userbot...")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        asyncio.run(db.close())

