import aiosqlite
import json
from datetime import datetime
from typing import Optional, List, Dict
from config import DATABASE_PATH


class MessageDatabase:
    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self.connection: Optional[aiosqlite.Connection] = None

    async def connect(self):
        """Подключение к базе данных"""
        self.connection = await aiosqlite.connect(self.db_path)
        await self.create_tables()

    async def close(self):
        """Закрытие соединения с базой данных"""
        if self.connection:
            await self.connection.close()

    async def create_tables(self):
        """Создание таблиц в базе данных"""
        cursor = await self.connection.cursor()
        
        # Таблица для сообщений
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                chat_title TEXT,
                chat_type TEXT,
                user_id INTEGER,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                message_text TEXT,
                date TIMESTAMP,
                is_reply INTEGER DEFAULT 0,
                reply_to_message_id INTEGER,
                has_media INTEGER DEFAULT 0,
                media_type TEXT,
                raw_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица для чатов
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER UNIQUE NOT NULL,
                chat_title TEXT,
                chat_type TEXT,
                participants_count INTEGER,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP,
                metadata TEXT
            )
        ''')
        
        # Индексы для быстрого поиска
        await cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_messages_chat_id 
            ON messages(chat_id)
        ''')
        
        await cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_messages_date 
            ON messages(date)
        ''')
        
        await cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_messages_user_id 
            ON messages(user_id)
        ''')
        
        # Таблицы для Business Bot (Telegram Business API)
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS business_connections (
                id TEXT PRIMARY KEY,
                user_chat_id INTEGER,
                is_enabled INTEGER DEFAULT 1,
                date INTEGER,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS business_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_connection_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                chat_id INTEGER,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(business_connection_id, message_id)
            )
        ''')
        await cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_business_messages_conn 
            ON business_messages(business_connection_id)
        ''')
        
        await self.connection.commit()

    async def save_message(self, message_data: Dict):
        """Сохранение сообщения в базу данных"""
        cursor = await self.connection.cursor()
        
        try:
            await cursor.execute('''
                INSERT INTO messages (
                    message_id, chat_id, chat_title, chat_type,
                    user_id, username, first_name, last_name,
                    message_text, date, is_reply, reply_to_message_id,
                    has_media, media_type, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                message_data.get('message_id'),
                message_data.get('chat_id'),
                message_data.get('chat_title'),
                message_data.get('chat_type'),
                message_data.get('user_id'),
                message_data.get('username'),
                message_data.get('first_name'),
                message_data.get('last_name'),
                message_data.get('message_text'),
                message_data.get('date'),
                message_data.get('is_reply', 0),
                message_data.get('reply_to_message_id'),
                message_data.get('has_media', 0),
                message_data.get('media_type'),
                json.dumps(message_data.get('raw_data', {}))
            ))
            
            await self.connection.commit()
            return cursor.lastrowid
        except Exception as e:
            print(f"Ошибка при сохранении сообщения: {e}")
            await self.connection.rollback()
            return None

    async def save_chat(self, chat_data: Dict):
        """Сохранение информации о чате"""
        cursor = await self.connection.cursor()
        
        try:
            await cursor.execute('''
                INSERT OR REPLACE INTO chats (
                    chat_id, chat_title, chat_type, participants_count,
                    last_activity, metadata
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                chat_data.get('chat_id'),
                chat_data.get('chat_title'),
                chat_data.get('chat_type'),
                chat_data.get('participants_count'),
                datetime.now().isoformat(),
                json.dumps(chat_data.get('metadata', {}))
            ))
            
            await self.connection.commit()
        except Exception as e:
            print(f"Ошибка при сохранении чата: {e}")
            await self.connection.rollback()

    async def get_messages_count(self, chat_id: Optional[int] = None) -> int:
        """Получение количества сохраненных сообщений"""
        cursor = await self.connection.cursor()
        
        if chat_id:
            await cursor.execute('SELECT COUNT(*) FROM messages WHERE chat_id = ?', (chat_id,))
        else:
            await cursor.execute('SELECT COUNT(*) FROM messages')
        
        result = await cursor.fetchone()
        return result[0] if result else 0

    async def get_chats(self) -> List[Dict]:
        """Получение списка всех чатов"""
        cursor = await self.connection.cursor()
        await cursor.execute('SELECT * FROM chats ORDER BY last_activity DESC')
        
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        
        return [dict(zip(columns, row)) for row in rows]

    async def get_message_by_chat_and_id(self, chat_id: int, message_id: int) -> Optional[Dict]:
        """Получить одно сообщение по chat_id и message_id (для уведомлений об редактировании/удалении)."""
        cursor = await self.connection.cursor()
        await cursor.execute(
            'SELECT message_id, chat_id, chat_title, user_id, username, first_name, last_name, message_text, has_media, media_type FROM messages WHERE chat_id = ? AND message_id = ? ORDER BY id DESC LIMIT 1',
            (chat_id, message_id)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        cols = ['message_id', 'chat_id', 'chat_title', 'user_id', 'username', 'first_name', 'last_name', 'message_text', 'has_media', 'media_type']
        return dict(zip(cols, row))

    async def get_messages_by_message_id(self, message_id: int) -> List[Dict]:
        """Получить все сохранённые сообщения с данным message_id (когда чат при удалении неизвестен)."""
        cursor = await self.connection.cursor()
        await cursor.execute(
            'SELECT message_id, chat_id, chat_title, user_id, username, first_name, last_name, message_text, has_media, media_type FROM messages WHERE message_id = ?',
            (message_id,)
        )
        rows = await cursor.fetchall()
        cols = ['message_id', 'chat_id', 'chat_title', 'user_id', 'username', 'first_name', 'last_name', 'message_text', 'has_media', 'media_type']
        return [dict(zip(cols, row)) for row in rows]

    # --- Business Bot (Telegram Business API) ---

    async def save_business_connection(self, connection_id: str, user_chat_id: int, is_enabled: bool, date: int, data: Dict):
        """Сохранение/обновление бизнес-подключения"""
        cursor = await self.connection.cursor()
        try:
            await cursor.execute('''
                INSERT INTO business_connections (id, user_chat_id, is_enabled, date, data)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    user_chat_id=excluded.user_chat_id,
                    is_enabled=excluded.is_enabled,
                    date=excluded.date,
                    data=excluded.data
            ''', (connection_id, user_chat_id, 1 if is_enabled else 0, date, json.dumps(data)))
            await self.connection.commit()
        except Exception as e:
            print(f"Ошибка при сохранении business_connection: {e}")
            await self.connection.rollback()

    async def get_business_connection(self, connection_id: str) -> Optional[Dict]:
        """Получение бизнес-подключения по ID"""
        cursor = await self.connection.cursor()
        await cursor.execute(
            'SELECT id, user_chat_id, is_enabled, date, data FROM business_connections WHERE id = ?',
            (connection_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            'id': row[0],
            'user_chat_id': row[1],
            'is_enabled': bool(row[2]),
            'date': row[3],
            'data': json.loads(row[4]) if row[4] else {}
        }

    async def save_business_message(self, connection_id: str, message_id: int, chat_id: int, data: Dict):
        """Сохранение бизнес-сообщения"""
        cursor = await self.connection.cursor()
        try:
            await cursor.execute('''
                INSERT INTO business_messages (business_connection_id, message_id, chat_id, data)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(business_connection_id, message_id) DO UPDATE SET chat_id=excluded.chat_id, data=excluded.data
            ''', (connection_id, message_id, chat_id, json.dumps(data)))
            await self.connection.commit()
        except Exception as e:
            print(f"Ошибка при сохранении business_message: {e}")
            await self.connection.rollback()

    async def get_business_message(self, connection_id: str, message_id: int) -> Optional[Dict]:
        """Получение бизнес-сообщения по connection_id и message_id"""
        cursor = await self.connection.cursor()
        await cursor.execute(
            'SELECT business_connection_id, message_id, chat_id, data FROM business_messages WHERE business_connection_id = ? AND message_id = ?',
            (connection_id, message_id)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        out = {'business_connection_id': row[0], 'message_id': row[1], 'chat_id': row[2]}
        if row[3]:
            out['data'] = json.loads(row[3])
        return out

    async def delete_business_message(self, connection_id: str, message_id: int):
        """Удаление бизнес-сообщения из БД"""
        cursor = await self.connection.cursor()
        await cursor.execute(
            'DELETE FROM business_messages WHERE business_connection_id = ? AND message_id = ?',
            (connection_id, message_id)
        )
        await self.connection.commit()

    async def get_business_messages_by_ids(self, connection_id: str, message_ids: List[int]) -> List[Dict]:
        """Получение нескольких бизнес-сообщений по списку message_id"""
        if not message_ids:
            return []
        cursor = await self.connection.cursor()
        placeholders = ','.join('?' * len(message_ids))
        await cursor.execute(
            f'SELECT business_connection_id, message_id, chat_id, data FROM business_messages WHERE business_connection_id = ? AND message_id IN ({placeholders})',
            (connection_id, *message_ids)
        )
        rows = await cursor.fetchall()
        result = []
        for row in rows:
            item = {'business_connection_id': row[0], 'message_id': row[1], 'chat_id': row[2]}
            if row[3]:
                item['data'] = json.loads(row[3])
            result.append(item)
        return result

    async def get_business_messages_count(self, connection_id: Optional[str] = None) -> int:
        """Количество сохранённых бизнес-сообщений (всего или по connection_id)"""
        cursor = await self.connection.cursor()
        if connection_id:
            await cursor.execute('SELECT COUNT(*) FROM business_messages WHERE business_connection_id = ?', (connection_id,))
        else:
            await cursor.execute('SELECT COUNT(*) FROM business_messages')
        r = await cursor.fetchone()
        return r[0] if r else 0

