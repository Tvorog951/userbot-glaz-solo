import os
from dotenv import load_dotenv

load_dotenv()

# Telegram API credentials
API_ID = int(os.getenv('API_ID', '0'))
API_HASH = os.getenv('API_HASH', '')
SESSION_NAME = os.getenv('SESSION_NAME', 'userbot_session')
# Optional: use STRING_SESSION instead of session file
STRING_SESSION = os.getenv('STRING_SESSION', '')

# Database settings
# Для Bothost.ru используйте /app/data/messages.db
DATABASE_PATH = os.getenv('DATABASE_PATH', 'messages.db')

# Уведомления через бота: userbot при редактировании/удалении сообщений в чатах
# присылает тебе уведомление в личку через этого бота (подключение в Настройках ТГ не нужно).
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
# Куда слать уведомления (обязательно если задан BOT_TOKEN): твой chat_id с ботом (напиши боту /start и укажи сюда свой user id или chat id).
_notify = (os.getenv('NOTIFY_CHAT_ID') or '').strip()
NOTIFY_CHAT_ID = int(_notify) if _notify.isdigit() else None

# Logging settings
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
# Для Bothost.ru используйте /app/data/userbot.log
LOG_FILE = os.getenv('LOG_FILE', 'userbot.log')

# Создаем директорию для данных, если путь содержит директорию
if '/' in DATABASE_PATH:
    db_dir = os.path.dirname(DATABASE_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

if '/' in LOG_FILE:
    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

