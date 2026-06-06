import asyncio
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import threading
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from contextlib import asynccontextmanager
import urllib.parse
import csv
import io

# ================= HEALTH SERVER FOR RENDER =================
from flask import Flask, render_template_string, jsonify, request
app = Flask(__name__)

# Global variables for web dashboard
start_time = time.time()
bot_username = os.environ.get("BOT_USERNAME", "xiomovies_bot")
# Global variable to store bot application instance for webhook
bot_app = None
bot_loop = None
bot_initialized = False

# ===========================================================
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    JobQueue
)
from telegram.request import HTTPXRequest

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

# Default channels (will be added to database on first run)
DEFAULT_CHANNELS = [
    os.environ.get("CHANNEL_1", "A_Knight_of_the_Seven_Kingdoms_t").replace("@", ""),
    os.environ.get("CHANNEL_2", "your_movies_web").replace("@", "")
]

# ============ RENDER POSTGRESQL WITH PSYCOPG2 ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL is not set!")
    print("💡 Add a PostgreSQL database in Render Dashboard and copy its Internal Database URL")
    raise ValueError("DATABASE_URL environment variable is required!")

DELETE_AFTER = 600  # 10 minutes
MAX_STORED_FILES = 10000
AUTO_CLEANUP_DAYS = 0  # DISABLED - No auto cleanup

# ============ AUTO BACKUP CONFIGURATION ============
AUTO_BACKUP_ENABLED = True  # Set to False to disable auto backup
AUTO_BACKUP_DAYS = 3  # Backup every 3 days

# Playable formats
PLAYABLE_EXTS = {"mp4", "mov", "m4v", "mpeg", "mpg"}

# All video extensions
ALL_VIDEO_EXTS = {
    "mp4", "mkv", "mov", "avi", "webm", "flv", "m4v",
    "3gp", "wmv", "mpg", "mpeg"
}

# Friendly channel names (for UI) - You can customize these!
CHANNEL_NAMES = {
    # Format: "channel_username": "Display Name"
    "A_Knight_of_the_Seven_Kingdoms_t": "Channel 1",
    "A_Knight_of_the_Seven_Kingdoms_r": "Main Channel",
    "A_Knight_of_the_Seven_Kingdoms_y": "Backup Channel",
    "your_movies_web": "Movies Channel",
}

# =========================================

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("psycopg2").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ================= DATABASE (Render PostgreSQL with psycopg2) =================

class Database:
    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        self.pool = None
        self._pool_initialized = False
        log.info(f"📀 Connecting to Render PostgreSQL with psycopg2...")

    def _get_pool_sync(self):
        """Synchronous pool initialization"""
        if self.pool is None:
            result = urllib.parse.urlparse(self.db_url)
            user = result.username
            password = urllib.parse.unquote(result.password) if result.password else ''
            database = result.path[1:]
            host = result.hostname
            port = result.port or 5432

            dsn = f"dbname='{database}' user='{user}' password='{password}' host='{host}' port='{port}'"
            log.info(f"🔌 Creating connection pool to Render PostgreSQL at {host}:{port}/{database}")

            try:
                self.pool = psycopg2.pool.SimpleConnectionPool(
                    1, 20, dsn=dsn, connect_timeout=30,
                    sslmode='require'
                )
                log.info("✅ Render PostgreSQL connection pool created (SSL enabled)")

                conn = self.pool.getconn()
                try:
                    with conn.cursor() as cur:
                        self._init_db(conn, cur)
                finally:
                    self.pool.putconn(conn)

                self._pool_initialized = True
                log.info("✅ Database tables initialized/verified.")

            except Exception as e:
                log.error(f"❌ Failed to create connection pool: {e}")
                raise
        return self.pool

    async def _get_pool_async(self):
        if self.pool is None:
            await asyncio.to_thread(self._get_pool_sync)
        return self.pool

    def _init_db(self, conn, cur):
        """Initialize database tables"""
        cur.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id SERIAL PRIMARY KEY,
                file_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                mime_type TEXT,
                is_video INTEGER DEFAULT 0,
                file_size BIGINT DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                access_count INTEGER DEFAULT 0
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS membership_cache (
                user_id BIGINT,
                channel TEXT,
                is_member INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, channel)
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_deletions (
                chat_id BIGINT NOT NULL,
                message_id INTEGER NOT NULL,
                scheduled_time TIMESTAMP NOT NULL,
                delete_after INTEGER DEFAULT 600,
                PRIMARY KEY (chat_id, message_id)
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_interactions INTEGER DEFAULT 1,
                total_files_accessed INTEGER DEFAULT 0,
                last_file_accessed TIMESTAMP
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS required_channels (
                id SERIAL PRIMARY KEY,
                channel_username TEXT UNIQUE NOT NULL,
                channel_name TEXT,
                added_by BIGINT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                position INTEGER DEFAULT 0
            )
        ''')
        
        cur.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_channels_active ON required_channels(is_active)')
        
        cur.execute("SELECT COUNT(*) FROM required_channels")
        count = cur.fetchone()[0]
        
        if count == 0 and DEFAULT_CHANNELS:
            for i, channel in enumerate(DEFAULT_CHANNELS):
                if channel:
                    friendly_name = CHANNEL_NAMES.get(channel, f"Channel {i+1}")
                    cur.execute('''
                        INSERT INTO required_channels (channel_username, channel_name, position, is_active)
                        VALUES (%s, %s, %s, 1)
                        ON CONFLICT (channel_username) DO NOTHING
                    ''', (channel, friendly_name, i))
                    log.info(f"Added default channel: {channel} as '{friendly_name}'")
        
        conn.commit()

    @asynccontextmanager
    async def get_db_connection(self):
        pool = await self._get_pool_async()
        conn = await asyncio.to_thread(pool.getconn)
        try:
            yield conn
        finally:
            await asyncio.to_thread(pool.putconn, conn)

    async def execute(self, query: str, params: tuple = None):
        async with self.get_db_connection() as conn:
            def _execute():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur
            return await asyncio.to_thread(_execute)

    async def fetchrow(self, query: str, params: tuple = None):
        async with self.get_db_connection() as conn:
            def _fetch():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchone()
            return await asyncio.to_thread(_fetch)

    async def fetchall(self, query: str, params: tuple = None):
        async with self.get_db_connection() as conn:
            def _fetch():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
            return await asyncio.to_thread(_fetch)

    async def execute_and_commit(self, query: str, params: tuple = None):
        async with self.get_db_connection() as conn:
            def _execute():
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    conn.commit()
                    return cur.rowcount
            return await asyncio.to_thread(_execute)

    async def get_db_storage_usage(self) -> Dict[str, Any]:
        try:
            result = await self.fetchrow('''
                SELECT 
                    pg_database_size(current_database()) as total_bytes,
                    (SELECT COALESCE(SUM(pg_total_relation_size(relid)), 0) 
                     FROM pg_stat_user_tables) as table_bytes,
                    (SELECT COALESCE(SUM(pg_indexes_size(relid)), 0) 
                     FROM pg_stat_user_tables) as index_bytes
            ''')
            
            if result:
                total_bytes = result['total_bytes'] or 0
                table_bytes = result['table_bytes'] or 0
                index_bytes = result['index_bytes'] or 0
                
                def format_bytes(bytes_val):
                    if bytes_val < 1024:
                        return f"{bytes_val} B"
                    elif bytes_val < 1024 * 1024:
                        return f"{bytes_val/1024:.2f} KB"
                    elif bytes_val < 1024 * 1024 * 1024:
                        return f"{bytes_val/(1024*1024):.2f} MB"
                    else:
                        return f"{bytes_val/(1024*1024*1024):.2f} GB"
                
                return {
                    'total': format_bytes(total_bytes),
                    'total_bytes': total_bytes,
                    'tables': format_bytes(table_bytes),
                    'indexes': format_bytes(index_bytes),
                    'tables_bytes': table_bytes,
                    'indexes_bytes': index_bytes
                }
        except Exception as e:
            log.error(f"Error getting DB storage: {e}")
        
        return {
            'total': 'Unknown',
            'total_bytes': 0,
            'tables': 'Unknown',
            'indexes': 'Unknown'
        }

    async def get_metadata_storage_info(self) -> Dict[str, Any]:
        try:
            files_count = await self.get_file_count()
            users_count = await self.get_user_count()
            cache_result = await self.fetchrow("SELECT COUNT(*) as count FROM membership_cache")
            cache_count = cache_result['count'] if cache_result else 0
            channels_count = await self.get_channel_count()
            
            estimated_metadata_bytes = (files_count * 200) + (users_count * 150) + (cache_count * 50) + (channels_count * 100)
            
            def format_bytes(bytes_val):
                if bytes_val < 1024:
                    return f"{bytes_val} B"
                elif bytes_val < 1024 * 1024:
                    return f"{bytes_val/1024:.2f} KB"
                else:
                    return f"{bytes_val/(1024*1024):.2f} MB"
            
            return {
                'files_count': files_count,
                'users_count': users_count,
                'cache_entries': cache_count,
                'channels_count': channels_count,
                'estimated_metadata': format_bytes(estimated_metadata_bytes),
                'estimated_bytes': estimated_metadata_bytes
            }
        except Exception as e:
            log.error(f"Error getting metadata info: {e}")
            return {
                'files_count': 0,
                'users_count': 0,
                'cache_entries': 0,
                'channels_count': 0,
                'estimated_metadata': 'Unknown',
                'estimated_bytes': 0
            }

    async def get_total_uploaded_size(self) -> int:
        result = await self.fetchrow("SELECT COALESCE(SUM(file_size), 0) as total FROM files")
        return result['total'] if result else 0

    async def get_required_channels(self, active_only: bool = True) -> List[str]:
        if active_only:
            rows = await self.fetchall("SELECT channel_username FROM required_channels WHERE is_active = 1 ORDER BY position, id")
        else:
            rows = await self.fetchall("SELECT channel_username FROM required_channels ORDER BY position, id")
        return [row['channel_username'] for row in rows]
    
    async def get_channels_with_details(self) -> List[Dict]:
        rows = await self.fetchall('''
            SELECT id, channel_username, channel_name, added_at, is_active, position
            FROM required_channels
            ORDER BY position, id
        ''')
        return [dict(row) for row in rows]
    
    async def add_channel(self, channel_username: str, added_by: int, channel_name: str = None) -> bool:
        clean_username = channel_username.replace("@", "").strip()
        if not clean_username:
            return False
        
        friendly_name = channel_name or CHANNEL_NAMES.get(clean_username, clean_username)
        result = await self.fetchrow("SELECT COALESCE(MAX(position), -1) + 1 as next_pos FROM required_channels")
        next_pos = result['next_pos'] if result else 0
        
        try:
            await self.execute_and_commit('''
                INSERT INTO required_channels (channel_username, channel_name, added_by, position, is_active)
                VALUES (%s, %s, %s, %s, 1)
                ON CONFLICT (channel_username) DO UPDATE
                SET is_active = 1,
                    added_by = EXCLUDED.added_by,
                    channel_name = COALESCE(EXCLUDED.channel_name, required_channels.channel_name)
            ''', (clean_username, friendly_name, added_by, next_pos))
            log.info(f"Channel added: @{clean_username} as '{friendly_name}' by user {added_by}")
            return True
        except Exception as e:
            log.error(f"Error adding channel: {e}")
            return False
    
    async def remove_channel(self, channel_username: str) -> bool:
        clean_username = channel_username.replace("@", "").strip()
        rowcount = await self.execute_and_commit('''
            UPDATE required_channels SET is_active = 0
            WHERE channel_username = %s
        ''', (clean_username,))
        if rowcount > 0:
            log.info(f"Channel removed: @{clean_username}")
            await self.execute_and_commit("DELETE FROM membership_cache WHERE channel = %s", (clean_username,))
            return True
        return False
    
    async def update_channel_name(self, channel_username: str, new_name: str) -> bool:
        clean_username = channel_username.replace("@", "").strip()
        rowcount = await self.execute_and_commit('''
            UPDATE required_channels SET channel_name = %s
            WHERE channel_username = %s
        ''', (new_name, clean_username))
        return rowcount > 0
    
    async def get_channel_count(self) -> int:
        result = await self.fetchrow("SELECT COUNT(*) as count FROM required_channels WHERE is_active = 1")
        return result['count'] if result else 0

    async def save_file(self, file_id: str, file_info: dict) -> str:
        async with self.get_db_connection() as conn:
            def _save():
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO files
                        (file_id, file_name, mime_type, is_video, file_size, access_count)
                        VALUES (%s, %s, %s, %s, %s, 0)
                        RETURNING id
                    ''', (
                        file_id,
                        file_info.get('file_name', ''),
                        file_info.get('mime_type', ''),
                        1 if file_info.get('is_video', False) else 0,
                        file_info.get('size', 0)
                    ))
                    new_id = cur.fetchone()[0]
                    conn.commit()
                    log.info(f"💾 Saved file {new_id}: {file_info.get('file_name', '')}")
                    return str(new_id)
            return await asyncio.to_thread(_save)

    async def get_file(self, file_id: str) -> Optional[dict]:
        try:
            file_id_int = int(file_id)
        except ValueError:
            return None

        async with self.get_db_connection() as conn:
            def _get():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute('''
                        UPDATE files
                        SET access_count = access_count + 1
                        WHERE id = %s
                        RETURNING file_id, file_name, mime_type, is_video, file_size,
                                  TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
                                  access_count
                    ''', (file_id_int,))
                    row = cur.fetchone()
                    if row:
                        conn.commit()
                        return dict(row)
                    return None
            return await asyncio.to_thread(_get)

    async def get_file_count(self) -> int:
        result = await self.fetchrow("SELECT COUNT(*) as count FROM files")
        return result['count'] if result else 0

    async def cache_membership(self, user_id: int, channel: str, is_member: bool):
        await self.execute_and_commit('''
            INSERT INTO membership_cache (user_id, channel, is_member, timestamp)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, channel) DO UPDATE
            SET is_member = EXCLUDED.is_member,
                timestamp = EXCLUDED.timestamp
        ''', (user_id, channel, 1 if is_member else 0))

    async def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        result = await self.fetchrow('''
            SELECT is_member FROM membership_cache
            WHERE user_id = %s AND channel = %s
            AND timestamp > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        ''', (user_id, channel))
        return bool(result['is_member']) if result else None

    async def clear_membership_cache(self, user_id: Optional[int] = None, channel: Optional[str] = None):
        if user_id and channel:
            await self.execute_and_commit(
                "DELETE FROM membership_cache WHERE user_id = %s AND channel = %s",
                (user_id, channel.replace("@", ""))
            )
        elif user_id:
            await self.execute_and_commit("DELETE FROM membership_cache WHERE user_id = %s", (user_id,))
        elif channel:
            await self.execute_and_commit(
                "DELETE FROM membership_cache WHERE channel = %s",
                (channel.replace("@", ""),)
            )
        else:
            await self.execute_and_commit("DELETE FROM membership_cache")
            log.info("Cleared all membership cache")

    async def delete_file(self, file_id: str) -> bool:
        try:
            file_id_int = int(file_id)
        except ValueError:
            return False
        rowcount = await self.execute_and_commit("DELETE FROM files WHERE id = %s", (file_id_int,))
        deleted = rowcount > 0
        if deleted:
            log.info(f"🗑️ Deleted file {file_id}")
        return deleted

    async def get_all_files(self) -> list:
        rows = await self.fetchall('''
            SELECT id, file_name, is_video, file_size,
                   TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
                   access_count
            FROM files
            ORDER BY timestamp DESC
        ''')
        return [(row['id'], row['file_name'], row['is_video'], row['file_size'], row['timestamp'], row['access_count']) for row in rows]

    async def schedule_message_deletion(self, chat_id: int, message_id: int):
        scheduled_time = datetime.now() + timedelta(seconds=DELETE_AFTER)
        await self.execute_and_commit('''
            INSERT INTO scheduled_deletions (chat_id, message_id, scheduled_time, delete_after)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chat_id, message_id) DO UPDATE
            SET scheduled_time = EXCLUDED.scheduled_time,
                delete_after = EXCLUDED.delete_after
        ''', (chat_id, message_id, scheduled_time, DELETE_AFTER))
        log.info(f"Scheduled deletion for message {message_id} in chat {chat_id}")

    async def get_due_messages(self):
        rows = await self.fetchall('''
            SELECT chat_id, message_id FROM scheduled_deletions
            WHERE scheduled_time <= CURRENT_TIMESTAMP
        ''')
        return [(row['chat_id'], row['message_id']) for row in rows]

    async def remove_scheduled_message(self, chat_id: int, message_id: int):
        await self.execute_and_commit(
            'DELETE FROM scheduled_deletions WHERE chat_id = %s AND message_id = %s',
            (chat_id, message_id)
        )
        log.info(f"Removed scheduled deletion for message {message_id}")

    async def update_user_interaction(self, user_id: int, username: str = None,
                                    first_name: str = None, last_name: str = None,
                                    file_accessed: bool = False):
        async with self.get_db_connection() as conn:
            def _update():
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
                    exists = cur.fetchone()
                    if exists:
                        cur.execute('''
                            UPDATE users
                            SET last_active = CURRENT_TIMESTAMP,
                                total_interactions = total_interactions + 1,
                                username = COALESCE(%s, username),
                                first_name = COALESCE(%s, first_name),
                                last_name = COALESCE(%s, last_name)
                            WHERE user_id = %s
                        ''', (username, first_name, last_name, user_id))
                        if file_accessed:
                            cur.execute('''
                                UPDATE users
                                SET total_files_accessed = total_files_accessed + 1,
                                    last_file_accessed = CURRENT_TIMESTAMP
                                WHERE user_id = %s
                            ''', (user_id,))
                    else:
                        cur.execute('''
                            INSERT INTO users
                            (user_id, username, first_name, last_name, first_seen, last_active, total_interactions)
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                        ''', (user_id, username, first_name, last_name))
                    conn.commit()
            await asyncio.to_thread(_update)

    async def get_user_stats(self) -> Dict[str, Any]:
        async def fetch_one(query, params=None):
            return await self.fetchrow(query, params)
        total_users_task = fetch_one("SELECT COUNT(*) as count FROM users")
        active_7d_task = fetch_one('''
            SELECT COUNT(*) as count FROM users
            WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '7 days'
        ''')
        active_30d_task = fetch_one('''
            SELECT COUNT(*) as count FROM users
            WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '30 days'
        ''')
        new_today_task = fetch_one('''
            SELECT COUNT(*) as count FROM users
            WHERE DATE(first_seen) = CURRENT_DATE
        ''')
        new_week_task = fetch_one('''
            SELECT COUNT(*) as count FROM users
            WHERE first_seen > CURRENT_TIMESTAMP - INTERVAL '7 days'
        ''')
        users_files_task = fetch_one('''
            SELECT COUNT(DISTINCT user_id) as count FROM users
            WHERE total_files_accessed > 0
        ''')
        top_users_task = self.fetchall('''
            SELECT user_id, username, first_name, last_name,
                   total_interactions, total_files_accessed,
                   TO_CHAR(last_active, 'YYYY-MM-DD HH24:MI:SS') as last_active,
                   TO_CHAR(first_seen, 'YYYY-MM-DD HH24:MI:SS') as first_seen
            FROM users
            ORDER BY total_interactions DESC
            LIMIT 10
        ''')
        growth_task = self.fetchall('''
            SELECT
                TO_CHAR(first_seen, 'YYYY-MM-DD') as date,
                COUNT(*) as new_users
            FROM users
            WHERE first_seen > CURRENT_TIMESTAMP - INTERVAL '30 days'
            GROUP BY date
            ORDER BY date DESC
            LIMIT 15
        ''')
        total_users, active_7d, active_30d, new_today, new_week, users_files, top_users, growth_data = await asyncio.gather(
            total_users_task, active_7d_task, active_30d_task, new_today_task, new_week_task, users_files_task, top_users_task, growth_task
        )
        return {
            'total_users': total_users['count'] if total_users else 0,
            'active_users_7d': active_7d['count'] if active_7d else 0,
            'active_users_30d': active_30d['count'] if active_30d else 0,
            'new_users_today': new_today['count'] if new_today else 0,
            'new_users_week': new_week['count'] if new_week else 0,
            'top_users': [(row['user_id'], row['username'], row['first_name'], row['last_name'], row['total_interactions'], row['total_files_accessed'], row['last_active'], row['first_seen']) for row in top_users],
            'users_with_files': users_files['count'] if users_files else 0,
            'growth_data': [(row['date'], row['new_users']) for row in growth_data]
        }

    async def get_all_user_ids(self, exclude_admin: bool = True) -> List[int]:
        if exclude_admin:
            rows = await self.fetchall("SELECT user_id FROM users WHERE user_id != %s", (ADMIN_ID,))
        else:
            rows = await self.fetchall("SELECT user_id FROM users")
        return [row['user_id'] for row in rows]

    async def get_user_count(self) -> int:
        result = await self.fetchrow("SELECT COUNT(*) as count FROM users")
        return result['count'] if result else 0

    async def close_pool(self):
        if self.pool:
            self.pool.closeall()
            log.info("Database connection pool closed")

db = Database()

# ============ MESSAGE DELETION SYSTEM ============
async def delete_message_job(context):
    try:
        job = context.job
        chat_id = job.chat_id
        message_id = job.data
        if not chat_id or not message_id:
            return
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            await db.remove_scheduled_message(chat_id, message_id)
        except Exception as e:
            error_msg = str(e).lower()
            if "message to delete not found" in error_msg:
                await db.remove_scheduled_message(chat_id, message_id)
    except Exception as e:
        log.error(f"Error in delete_message_job: {e}")

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await db.schedule_message_deletion(chat_id, message_id)
        if context.job_queue:
            context.job_queue.run_once(
                delete_message_job,
                DELETE_AFTER,
                data=message_id,
                chat_id=chat_id,
                name=f"delete_msg_{chat_id}_{message_id}_{int(time.time())}"
            )
    except Exception as e:
        log.error(f"Failed to schedule deletion: {e}")

async def cleanup_overdue_messages(context: ContextTypes.DEFAULT_TYPE):
    try:
        due_messages = await db.get_due_messages()
        if not due_messages:
            return
        for chat_id, message_id in due_messages:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                await db.remove_scheduled_message(chat_id, message_id)
            except Exception as e:
                error_msg = str(e).lower()
                if "message to delete not found" in error_msg:
                    await db.remove_scheduled_message(chat_id, message_id)
    except Exception as e:
        log.error(f"Error in cleanup_overdue_messages: {e}")

# ============ DYNAMIC MEMBERSHIP CHECK ============
async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> bool:
    clean_channel = channel.replace("@", "")
    if not force_check:
        cached = await db.get_cached_membership(user_id, clean_channel)
        if cached is not None:
            return cached
    try:
        if not channel.startswith("@"):
            channel_username = f"@{channel}"
        else:
            channel_username = channel
        member = await bot.get_chat_member(chat_id=channel_username, user_id=user_id)
        is_member = member.status in ["member", "administrator", "creator"]
        await db.cache_membership(user_id, clean_channel, is_member)
        return is_member
    except Exception as e:
        error_msg = str(e).lower()
        if "user not found" in error_msg or "user not participant" in error_msg:
            await db.cache_membership(user_id, clean_channel, False)
            return False
        elif "chat not found" in error_msg:
            return True
        elif "forbidden" in error_msg:
            return True
        else:
            return True

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, force_check: bool = False) -> Dict[str, Any]:
    bot = context.bot
    result = {"all_joined": False, "missing_channels": [], "missing_channel_names": [], "channel_status": {}}
    channels_data = await db.get_channels_with_details()
    active_channels = [c for c in channels_data if c['is_active'] == 1]
    if not active_channels:
        result["all_joined"] = True
        return result
    if force_check:
        await db.clear_membership_cache(user_id)
    for channel_data in active_channels:
        channel = channel_data['channel_username']
        channel_name = channel_data['channel_name'] or channel
        is_member = await check_user_in_channel(bot, channel, user_id, force_check)
        result["channel_status"][channel] = {'is_member': is_member, 'name': channel_name}
        if not is_member:
            result["missing_channels"].append(channel)
            result["missing_channel_names"].append(channel_name)
    result["all_joined"] = len(result["missing_channels"]) == 0
    return result

# ============ WEB ROUTES ============
@app.route('/')
def home():
    html_content = """
    <!DOCTYPE html>
<html>
<head>
    <title>🤖 Telegram File Bot</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            min-height: 100vh;
        }
        .container {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
        }
        h1 { color: white; margin-top: 0; font-size: 1.5rem; }
        .status {
            background: rgba(0, 255, 0, 0.2);
            padding: 10px;
            border-radius: 8px;
            margin: 10px 0;
            border-left: 4px solid #00ff00;
        }
        .info {
            background: rgba(255, 255, 255, 0.1);
            padding: 10px;
            border-radius: 8px;
            margin: 10px 0;
        }
        a {
            color: #FFD700;
            text-decoration: none;
        }
        .btn {
            display: inline-block;
            background: #4CAF50;
            color: white;
            padding: 8px 16px;
            border-radius: 6px;
            margin: 5px;
            font-size: 0.9rem;
        }
        code {
            background: rgba(0, 0, 0, 0.3);
            padding: 2px 4px;
            border-radius: 3px;
            font-family: monospace;
            font-size: 0.9rem;
        }
        ul { padding-left: 20px; }
        li { margin: 5px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Telegram File Bot</h1>
        <div class="status">
            <h3>✅ Status: <strong>ACTIVE</strong></h3>
            <p>Bot is running on Render with PostgreSQL</p>
            <p>Uptime: {{ uptime }}</p>
            <p>Files in DB: {{ file_count }}</p>
            <p>Users in DB: {{ user_count }}</p>
            <p>Required Channels: {{ channel_count }}</p>
            <p>📁 Storage: Metadata only (files stored on Telegram)</p>
            <p>📅 Auto Backup: Every {{ backup_days }} days</p>
        </div>
        <div class="info">
            <h3>📊 Bot Information</h3>
            <ul>
                <li>Bot: <strong>@{{ bot_username }}</strong></li>
                <li>Database: <strong>Render PostgreSQL</strong></li>
                <li>Message Auto-delete: <strong>{{ delete_minutes }} minutes</strong></li>
                <li>Auto Backup: <strong>Every {{ backup_days }} days</strong></li>
            </ul>
        </div>
        <div class="info">
            <h3>📞 Start Bot</h3>
            <p><a href="https://t.me/{{ bot_username }}" target="_blank" class="btn">Start @{{ bot_username }}</a></p>
        </div>
    </div>
</body>
</html>
    """
    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
        channel_count = loop.run_until_complete(db.get_channel_count())
        loop.close()
    except Exception as e:
        log.error(f"Error fetching counts: {e}")
        file_count = 0
        user_count = 0
        channel_count = 0
    return render_template_string(html_content,
                                  bot_username=bot_username,
                                  uptime=uptime_str,
                                  file_count=file_count,
                                  user_count=user_count,
                                  channel_count=channel_count,
                                  delete_minutes=DELETE_AFTER//60,
                                  backup_days=AUTO_BACKUP_DAYS)

@app.route('/health')
def health():
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
        channel_count = loop.run_until_complete(db.get_channel_count())
        loop.close()
    except Exception as e:
        file_count = 0
        user_count = 0
        channel_count = 0
    return jsonify({
        "status": "OK",
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time))),
        "database": "postgresql",
        "auto_backup_days": AUTO_BACKUP_DAYS,
        "file_count": file_count,
        "user_count": user_count,
        "channel_count": channel_count,
        "bot_initialized": bot_initialized
    }), 200

@app.route('/ping')
def ping():
    return "pong", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    global bot_app, bot_loop, bot_initialized
    if not bot_initialized or bot_app is None or bot_loop is None:
        return "Bot not ready", 503
    update_data = request.get_json()
    if not update_data:
        return "Invalid request", 400
    future = asyncio.run_coroutine_threadsafe(process_update(update_data, bot_app), bot_loop)
    try:
        future.result(timeout=1)
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        log.error(f"Error queueing update: {e}")
    return "OK", 200

async def process_update(update_data, application):
    try:
        update = Update.de_json(update_data, application.bot)
        await application.process_update(update)
    except Exception as e:
        log.error(f"Error processing update: {e}")

def run_flask_thread():
    port = int(os.environ.get('PORT', 10000))
    import warnings
    warnings.filterwarnings("ignore")
    import logging as flask_logging
    flask_logging.getLogger('werkzeug').setLevel(flask_logging.ERROR)
    flask_logging.getLogger('flask').setLevel(flask_logging.ERROR)
    os.environ['PYTHONASYNCIODEBUG'] = '0'
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

# ============ DATABASE BACKUP & EXPORT FEATURE ============

async def export_table_to_csv(table_name: str, columns: list) -> str:
    try:
        rows = await db.fetchall(f"SELECT * FROM {table_name}")
        if not rows:
            return None
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            row_data = [row.get(col, '') for col in columns]
            writer.writerow(row_data)
        return output.getvalue()
    except Exception as e:
        log.error(f"Error exporting {table_name}: {e}")
        return None

async def export_database_backup(update: Update = None, context: ContextTypes.DEFAULT_TYPE = None, send_to_admin: bool = True) -> Dict[str, Any]:
    backup_data = {}
    backup_info = {"export_time": datetime.now().isoformat(), "tables_exported": [], "row_counts": {}}
    tables_config = {
        "files": ["id", "file_id", "file_name", "mime_type", "is_video", "file_size", "timestamp", "access_count"],
        "users": ["user_id", "username", "first_name", "last_name", "first_seen", "last_active", "total_interactions", "total_files_accessed", "last_file_accessed"],
        "membership_cache": ["user_id", "channel", "is_member", "timestamp"],
        "required_channels": ["id", "channel_username", "channel_name", "added_by", "added_at", "is_active", "position"],
        "scheduled_deletions": ["chat_id", "message_id", "scheduled_time", "delete_after"]
    }
    for table_name, columns in tables_config.items():
        try:
            csv_content = await export_table_to_csv(table_name, columns)
            if csv_content:
                backup_data[f"{table_name}.csv"] = csv_content
                row_count = len(csv_content.splitlines()) - 1
                backup_info["tables_exported"].append(table_name)
                backup_info["row_counts"][table_name] = max(0, row_count)
                log.info(f"✅ Exported {table_name}: {row_count} rows")
            else:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(columns)
                backup_data[f"{table_name}.csv"] = output.getvalue()
                backup_info["tables_exported"].append(table_name)
                backup_info["row_counts"][table_name] = 0
                log.info(f"📭 Table {table_name} is empty")
        except Exception as e:
            log.error(f"❌ Failed to export {table_name}: {e}")
    metadata = {"export_info": backup_info, "bot_config": {"bot_username": bot_username, "delete_after_seconds": DELETE_AFTER, "auto_backup_days": AUTO_BACKUP_DAYS, "export_timestamp": datetime.now().isoformat()}}
    backup_data["metadata.json"] = json.dumps(metadata, indent=2)
    if send_to_admin and context:
        await send_backup_to_admin(context, backup_data, backup_info)
    return backup_data

async def send_backup_to_admin(context: ContextTypes.DEFAULT_TYPE, backup_data: Dict[str, str], backup_info: Dict[str, Any]):
    try:
        summary = f"📦 *Database Backup Created*\n\n⏰ Time: {backup_info['export_time']}\n📊 Tables exported: {len(backup_info['tables_exported'])}\n\n📈 *Row Counts:*\n"
        for table, count in backup_info['row_counts'].items():
            summary += f"   • {table}: {count} rows\n"
        summary += f"\n💾 *Total backup size:* {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n\n💡 *To restore:* Send all CSV files to bot and reply with `/import`"
        await context.bot.send_message(chat_id=ADMIN_ID, text=summary, parse_mode="Markdown")
        for filename, content in backup_data.items():
            if content and len(content) > 0:
                file_bytes = io.BytesIO(content.encode('utf-8'))
                file_bytes.seek(0)
                await context.bot.send_document(chat_id=ADMIN_ID, document=file_bytes, filename=f"db_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}", caption=f"📊 {filename}")
                await asyncio.sleep(0.5)
        log.info(f"✅ Database backup sent to admin")
    except Exception as e:
        log.error(f"❌ Failed to send backup to admin: {e}")

# ============ IMPORT/RESTORE FUNCTIONS ============

async def import_csv_to_table(table_name: str, csv_content: str, truncate_first: bool = True) -> Dict[str, Any]:
    result = {"success": False, "rows_imported": 0, "errors": [], "table": table_name}
    try:
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(csv_reader)
        if not rows:
            result["success"] = True
            result["rows_imported"] = 0
            return result
        async with db.get_db_connection() as conn:
            def _import():
                with conn.cursor() as cur:
                    if truncate_first:
                        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
                        log.info(f"🗑️ Truncated table {table_name}")
                    columns = list(rows[0].keys())
                    placeholders = ','.join(['%s'] * len(columns))
                    columns_str = ','.join(columns)
                    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                    imported = 0
                    for row in rows:
                        try:
                            values = []
                            for col in columns:
                                val = row[col]
                                if val == '' or val == 'NULL':
                                    values.append(None)
                                else:
                                    if col in ['id', 'is_video', 'access_count', 'total_interactions', 'total_files_accessed', 'is_active', 'position', 'delete_after', 'added_by']:
                                        try:
                                            values.append(int(val) if val else None)
                                        except:
                                            values.append(None)
                                    elif col in ['file_size']:
                                        try:
                                            values.append(int(val) if val else 0)
                                        except:
                                            values.append(0)
                                    else:
                                        values.append(val)
                            cur.execute(insert_query, values)
                            imported += 1
                            if imported % 1000 == 0:
                                conn.commit()
                        except Exception as e:
                            log.warning(f"Error importing row in {table_name}: {e}")
                            result["errors"].append(f"Row {imported+1}: {str(e)[:100]}")
                    conn.commit()
                    return imported
            result["rows_imported"] = await asyncio.to_thread(_import)
            result["success"] = True
            log.info(f"✅ Imported {result['rows_imported']} rows to {table_name}")
    except Exception as e:
        log.error(f"Failed to import {table_name}: {e}")
        result["errors"].append(str(e))
        result["success"] = False
    return result

async def reset_sequences():
    try:
        async with db.get_db_connection() as conn:
            def _reset():
                with conn.cursor() as cur:
                    cur.execute("SELECT setval('files_id_seq', COALESCE((SELECT MAX(id) FROM files), 1))")
                    cur.execute("SELECT setval('required_channels_id_seq', COALESCE((SELECT MAX(id) FROM required_channels), 1))")
                    conn.commit()
            await asyncio.to_thread(_reset)
    except Exception as e:
        log.error(f"Failed to reset sequences: {e}")

async def restore_from_backup(files_data: Dict[str, str]) -> Dict[str, Any]:
    restore_result = {"success": False, "tables_restored": [], "total_rows": 0, "errors": [], "timestamp": datetime.now().isoformat()}
    import_order = ["required_channels", "users", "files", "membership_cache", "scheduled_deletions"]
    for table_name in import_order:
        csv_filename = f"{table_name}.csv"
        if csv_filename in files_data and files_data[csv_filename]:
            log.info(f"📥 Importing {table_name}...")
            result = await import_csv_to_table(table_name, files_data[csv_filename], truncate_first=True)
            if result["success"]:
                restore_result["tables_restored"].append({"table": table_name, "rows": result["rows_imported"]})
                restore_result["total_rows"] += result["rows_imported"]
            else:
                restore_result["errors"].append(f"{table_name}: {', '.join(result['errors'])}")
        else:
            restore_result["errors"].append(f"Missing {csv_filename}")
    await reset_sequences()
    restore_result["success"] = len(restore_result["errors"]) == 0
    return restore_result

# ============ COMMAND HANDLERS ============

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error(f"Error: {context.error}", exc_info=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message:
            return
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args
        username = update.effective_user.username
        first_name = update.effective_user.first_name
        await db.update_user_interaction(user_id=user_id, username=username, first_name=first_name, last_name=update.effective_user.last_name)
        channels_data = await db.get_channels_with_details()
        active_channels = [c for c in channels_data if c['is_active'] == 1]
        if not args:
            keyboard = []
            for channel_data in active_channels:
                channel = channel_data['channel_username']
                channel_name = channel_data['channel_name'] or f"Channel"
                keyboard.append([InlineKeyboardButton(f"📢 Join {channel_name}", url=f"https://t.me/{channel}")])
            keyboard.append([InlineKeyboardButton("🔄 Check Membership", callback_data="check_membership")])
            channel_list = "\n".join([f"{i+1}. {c['channel_name'] or f'Channel {i+1}'}" for i, c in enumerate(active_channels)]) if active_channels else "No channels required!"
            sent_msg = await update.message.reply_text(f"🤖 *Welcome to File Sharing Bot*\n\n🔗 *How to use:*\n1️⃣ Use admin-provided links\n2️⃣ Join the required channels:\n{channel_list}\n3️⃣ Click 'Check Membership'\n\n⚠️ Messages auto-delete after {DELETE_AFTER//60} minutes\n💾 *Storage:* Metadata only (files stored on Telegram)\n📅 *Auto Backup:* Every {AUTO_BACKUP_DAYS} days", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
        key = args[0]
        file_info = await db.get_file(key)
        if not file_info:
            sent_msg = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
        result = await check_membership(user_id, context, force_check=True)
        if not result["all_joined"]:
            missing_channels = result["missing_channels"]
            missing_names = result["missing_channel_names"]
            keyboard = []
            for i, channel in enumerate(missing_channels):
                channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                keyboard.append([InlineKeyboardButton(f"📥 Join {channel_name}", url=f"https://t.me/{channel}")])
            keyboard.append([InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")])
            if len(missing_names) == 1:
                text = f"🔒 *Join {missing_names[0]} to access this file*"
            elif len(missing_names) == 2:
                text = f"🔒 *Join {missing_names[0]} and {missing_names[1]} to access this file*"
            else:
                channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                text = f"🔒 *Join {channels_text} to access this file*"
            sent_msg = await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
        await db.update_user_interaction(user_id=user_id, file_accessed=True)
        try:
            filename = file_info['file_name']
            ext = filename.lower().split('.')[-1] if '.' in filename else ""
            warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes"
            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(chat_id=chat_id, video=file_info["file_id"], caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}", parse_mode="Markdown", supports_streaming=True)
            else:
                sent = await context.bot.send_document(chat_id=chat_id, document=file_info["file_id"], caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}", parse_mode="Markdown")
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        except Exception as e:
            log.error(f"Error sending file: {e}")
            sent_msg = await update.message.reply_text("❌ Failed to send file")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    except Exception as e:
        log.error(f"Start error: {e}", exc_info=True)

async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        data = query.data
        user = query.from_user
        await db.update_user_interaction(user_id=user.id, username=user.username, first_name=user.first_name, last_name=user.last_name)
        if data == "check_membership":
            result = await check_membership(user_id, context, force_check=True)
            if result["all_joined"]:
                channels_data = await db.get_channels_with_details()
                active_channels = [c for c in channels_data if c['is_active'] == 1]
                channel_list = "\n".join([f"✅ {c['channel_name'] or f'Channel {i+1}'}" for i, c in enumerate(active_channels)])
                await query.edit_message_text(f"✅ *You've joined all required channels!*\n\n{channel_list}\n\nNow you can use file links from admin.", parse_mode="Markdown")
            else:
                missing_channels = result["missing_channels"]
                missing_names = result["missing_channel_names"]
                keyboard = []
                for i, channel in enumerate(missing_channels):
                    channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                    keyboard.append([InlineKeyboardButton(f"📥 Join {channel_name}", url=f"https://t.me/{channel}")])
                keyboard.append([InlineKeyboardButton("🔄 Check Again", callback_data="check_membership")])
                if len(missing_names) == 1:
                    text = f"❌ *Missing {missing_names[0]}*"
                elif len(missing_names) == 2:
                    text = f"❌ *Missing {missing_names[0]} and {missing_names[1]}*"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"❌ *Missing {channels_text}*"
                await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        if data.startswith("check|"):
            _, key = data.split("|")
            file_info = await db.get_file(key)
            if not file_info:
                await query.edit_message_text("❌ File not found")
                return
            result = await check_membership(user_id, context, force_check=True)
            if not result['all_joined']:
                missing_channels = result["missing_channels"]
                missing_names = result["missing_channel_names"]
                keyboard = []
                for i, channel in enumerate(missing_channels):
                    channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                    keyboard.append([InlineKeyboardButton(f"📥 Join {channel_name}", url=f"https://t.me/{channel}")])
                keyboard.append([InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")])
                if len(missing_names) == 1:
                    text = f"❌ *Join {missing_names[0]}*"
                elif len(missing_names) == 2:
                    text = f"❌ *Join {missing_names[0]} and {missing_names[1]}*"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"❌ *Join {channels_text}*"
                await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
                return
            await db.update_user_interaction(user_id=user_id, file_accessed=True)
            try:
                filename = file_info['file_name']
                ext = filename.lower().split('.')[-1] if '.' in filename else ""
                warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes"
                chat_id = query.message.chat_id
                if file_info['is_video'] and ext in PLAYABLE_EXTS:
                    sent = await context.bot.send_video(chat_id=chat_id, video=file_info["file_id"], caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}", parse_mode="Markdown", supports_streaming=True)
                else:
                    sent = await context.bot.send_document(chat_id=chat_id, document=file_info["file_id"], caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}", parse_mode="Markdown")
                await query.edit_message_text("✅ *File sent below!*", parse_mode="Markdown")
                await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            except Exception as e:
                log.error(f"Failed to send file: {e}")
                await query.edit_message_text("❌ Failed to send file")
    except Exception as e:
        log.error(f"Callback error: {e}", exc_info=True)

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        msg = update.message
        video = msg.video
        document = msg.document
        file_id = None
        filename = None
        mime_type = None
        file_size = 0
        is_video = False
        if video:
            file_id = video.file_id
            filename = video.file_name or f"video_{int(time.time())}.mp4"
            mime_type = video.mime_type or "video/mp4"
            file_size = video.file_size or 0
            is_video = True
        elif document:
            filename = document.file_name or f"document_{int(time.time())}"
            file_id = document.file_id
            mime_type = document.mime_type or ""
            file_size = document.file_size or 0
            ext = filename.lower().split('.')[-1] if '.' in filename else ""
            if ext in ALL_VIDEO_EXTS:
                is_video = True
        else:
            sent_msg = await msg.reply_text("❌ Send a video or document")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
        file_info = {"file_name": filename, "mime_type": mime_type, "is_video": is_video, "size": int(file_size) if file_size else 0}
        key = await db.save_file(file_id, file_info)
        link = f"https://t.me/{bot_username}?start={key}"
        sent_msg = await msg.reply_text(f"✅ *Upload Successful*\n\n📁 *Name:* `{filename}`\n🔑 *Key:* `{key}`\n💾 *Storage:* Metadata only\n\n🔗 *Link:*\n`{link}`", parse_mode="Markdown")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    except Exception as e:
        log.exception("Upload error")
        sent_msg = await update.message.reply_text(f"❌ Upload failed: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        message = update.message
        user_id = update.effective_user.id
        document = message.document
        if not document:
            return
        filename = document.file_name or ""
        if filename.endswith('.csv'):
            await handle_csv_upload(update, context, document, filename)
            return
        if user_id == ADMIN_ID:
            await upload(update, context)
        else:
            sent_msg = await message.reply_text("⛔ You are not authorized to upload files.")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    except Exception as e:
        log.error(f"Error in handle_document: {e}", exc_info=True)

async def handle_csv_upload(update: Update, context: ContextTypes.DEFAULT_TYPE, document, filename: str):
    message = update.message
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        sent_msg = await message.reply_text("⛔ Only admin can upload CSV backup files.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    if context.user_data.get('awaiting_csv_import'):
        csv_files = context.user_data.get('csv_files_collection', {})
        status_msg = await message.reply_text(f"📥 Downloading {filename}...")
        try:
            file = await context.bot.get_file(document.file_id)
            file_content = await file.download_as_bytearray()
            csv_content = file_content.decode('utf-8')
            csv_files[filename] = csv_content
            context.user_data['csv_files_collection'] = csv_files
            await status_msg.edit_text(f"✅ Received: {filename}\n📊 Size: {len(csv_content.splitlines())} lines\n\n📦 Total files collected: {len(csv_files)}\n\nSend more CSV files or type `/import_now` to restore database.")
        except Exception as e:
            await status_msg.edit_text(f"❌ Failed to download {filename}: {str(e)[:100]}")
        return
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Import This Backup", callback_data=f"start_import|{filename}"),
        InlineKeyboardButton("❌ Ignore", callback_data="ignore_csv")
    ]])
    context.user_data['pending_csv'] = {'file_id': document.file_id, 'filename': filename}
    sent_msg = await message.reply_text(f"📄 *CSV Backup File Detected*\n\nFile: `{filename}`\n\nThis appears to be a database backup file.\n\nDo you want to restore your database from this backup?\n\n⚠️ *Warning:* This will replace all existing data!", parse_mode="Markdown", reply_markup=keyboard)
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def start_import_collection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "ignore_csv":
        await query.edit_message_text("❌ Import cancelled.")
        if 'pending_csv' in context.user_data:
            del context.user_data['pending_csv']
        return
    if data.startswith("start_import"):
        filename = data.split("|")[1]
        pending = context.user_data.get('pending_csv', {})
        if not pending:
            await query.edit_message_text("❌ No CSV file found. Please send the CSV file again.")
            return
        await query.edit_message_text(f"📥 Downloading {filename}...")
        try:
            file = await context.bot.get_file(pending['file_id'])
            file_content = await file.download_as_bytearray()
            csv_content = file_content.decode('utf-8')
            context.user_data['awaiting_csv_import'] = True
            context.user_data['csv_files_collection'] = {filename: csv_content}
            await query.edit_message_text(f"✅ *Import Mode Activated*\n\nReceived: {filename}\n📊 Rows: {len(csv_content.splitlines()) - 1}\n\n📦 *Send all other CSV files* from your backup.\n\nType `/import_now` when you've sent all files.\nOr `/cancel_import` to abort.", parse_mode="Markdown")
            del context.user_data['pending_csv']
        except Exception as e:
            await query.edit_message_text(f"❌ Failed to download: {str(e)[:100]}")

async def import_now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    csv_files = context.user_data.get('csv_files_collection', {})
    if not csv_files:
        sent_msg = await update.message.reply_text("❌ No CSV files collected.\n\nFirst send CSV files, then use this command.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    summary = f"📊 *Files ready for import:*\n\n"
    for filename, content in csv_files.items():
        lines = len(content.splitlines())
        summary += f"• {filename}: {lines-1} records\n"
    summary += f"\n⚠️ *WARNING:* This will REPLACE all existing data!\n✅ Type `/confirm_import` to proceed\n❌ Type `/cancel_import` to abort"
    sent_msg = await update.message.reply_text(summary, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    context.user_data['ready_to_import'] = True

async def confirm_import_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.user_data.get('ready_to_import'):
        sent_msg = await update.message.reply_text("❌ No import prepared.\nUse `/import_now` after sending CSV files.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    csv_files = context.user_data.get('csv_files_collection', {})
    if not csv_files:
        sent_msg = await update.message.reply_text("❌ No CSV files found.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    status_msg = await update.message.reply_text("🔄 Restoring database from backup...")
    try:
        result = await restore_from_backup(csv_files)
        if result["success"]:
            success_msg = f"✅ *Database Import Successful!*\n\n📊 *Import Summary:*\n"
            user_count = 0
            for table in result["tables_restored"]:
                success_msg += f"• {table['table']}: {table['rows']} rows restored\n"
                if table['table'] == 'users':
                    user_count = table['rows']
            success_msg += f"\n📦 *Total rows restored:* {result['total_rows']}\n👥 *Users restored:* {user_count}\n✅ *Broadcasts will work with all restored users!*\n\n💡 Run `/stats` to verify data."
            await status_msg.edit_text(success_msg, parse_mode="Markdown")
            context.user_data.pop('csv_files_collection', None)
            context.user_data.pop('awaiting_csv_import', None)
            context.user_data.pop('ready_to_import', None)
        else:
            error_msg = f"❌ *Import failed*\n\nErrors: {', '.join(result['errors'][:5])}"
            await status_msg.edit_text(error_msg, parse_mode="Markdown")
    except Exception as e:
        log.error(f"Import error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Import failed: {str(e)[:200]}")

async def cancel_import_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    context.user_data.pop('csv_files_collection', None)
    context.user_data.pop('awaiting_csv_import', None)
    context.user_data.pop('ready_to_import', None)
    context.user_data.pop('pending_csv', None)
    sent_msg = await update.message.reply_text("❌ Import cancelled. All collected CSV files have been cleared.")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        sent_msg = await update.message.reply_text("⛔ Admin only command")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    status_msg = await update.message.reply_text("🔄 Creating database backup... This may take a moment...")
    try:
        backup_data = await export_database_backup(update=update, context=context, send_to_admin=False)
        await status_msg.edit_text(f"✅ Backup created!\n📦 Total size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n\nSending files now...")
        for filename, content in backup_data.items():
            if content:
                file_bytes = io.BytesIO(content.encode('utf-8'))
                file_bytes.seek(0)
                await context.bot.send_document(chat_id=ADMIN_ID, document=file_bytes, filename=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}", caption=f"📄 {filename}")
                await asyncio.sleep(0.5)
        await status_msg.delete()
        summary = f"✅ *Full Database Backup Complete*\n\n📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n💾 Total size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n\n💡 To restore: Send all CSV files and reply with `/import`"
        sent_msg = await update.message.reply_text(summary, parse_mode="Markdown")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    except Exception as e:
        log.error(f"Backup error: {e}")
        await status_msg.edit_text(f"❌ Backup failed: {str(e)[:200]}")

async def backup_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    channel_count = await db.get_channel_count()
    db_storage = await db.get_db_storage_usage()
    status_msg = f"📊 *Database Status*\n\n📈 *Data Summary:*\n• Files: {file_count}\n• Users: {user_count}\n• Channels: {channel_count}\n• DB Size: {db_storage.get('total', 'Unknown')}\n\n📅 *Auto Backup:* Every {AUTO_BACKUP_DAYS} days\n💾 *Manual Backup:* /backup\n📥 *Restore:* /import\n\n⚠️ Free tier PostgreSQL expires after 30 days! Run backups regularly."
    sent_msg = await update.message.reply_text(status_msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def auto_backup_job(context: ContextTypes.DEFAULT_TYPE):
    global AUTO_BACKUP_ENABLED
    if not AUTO_BACKUP_ENABLED:
        log.info("Auto backup is disabled, skipping...")
        return
    log.info(f"🔄 Running scheduled auto-backup (every {AUTO_BACKUP_DAYS} days)...")
    try:
        backup_data = await export_database_backup(update=None, context=context, send_to_admin=True)
        total_size_kb = sum(len(v) for v in backup_data.values()) / 1024
        await context.bot.send_message(chat_id=ADMIN_ID, text=f"📦 *Auto Backup Complete*\n\n📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n💾 Size: {total_size_kb:.2f} KB\n📊 Tables: {len(backup_data)-1}\n\n✅ Files sent above. Save them securely!\n⚠️ Your database expires soon, keep backups safe!", parse_mode="Markdown")
        log.info(f"✅ Auto-backup completed. Size: {total_size_kb:.2f} KB")
    except Exception as e:
        log.error(f"❌ Auto-backup failed: {e}")
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ *Auto Backup Failed*\n\nError: {str(e)[:200]}\n\nPlease run manual backup: /backup", parse_mode="Markdown")
        except:
            pass

async def auto_backup_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global AUTO_BACKUP_ENABLED, AUTO_BACKUP_DAYS
    if update.effective_user.id != ADMIN_ID:
        return
    if context.args:
        if context.args[0].lower() == "on":
            AUTO_BACKUP_ENABLED = True
            sent_msg = await update.message.reply_text(f"✅ Auto backup ENABLED (every {AUTO_BACKUP_DAYS} days)")
        elif context.args[0].lower() == "off":
            AUTO_BACKUP_ENABLED = False
            sent_msg = await update.message.reply_text("❌ Auto backup DISABLED")
        elif context.args[0].lower() == "days" and len(context.args) > 1:
            try:
                new_days = int(context.args[1])
                if 1 <= new_days <= 30:
                    AUTO_BACKUP_DAYS = new_days
                    sent_msg = await update.message.reply_text(f"✅ Auto backup interval set to {AUTO_BACKUP_DAYS} days")
                    if context.job_queue:
                        current_jobs = context.job_queue.jobs()
                        for job in current_jobs:
                            if job.name == "auto_backup":
                                job.schedule_removal()
                        context.job_queue.run_repeating(auto_backup_job, interval=AUTO_BACKUP_DAYS * 24 * 60 * 60, first=3600, name="auto_backup")
                else:
                    sent_msg = await update.message.reply_text("❌ Days must be between 1 and 30")
            except ValueError:
                sent_msg = await update.message.reply_text("❌ Invalid number")
        else:
            sent_msg = await update.message.reply_text(f"📅 *Auto Backup Settings*\n\nStatus: {'✅ ENABLED' if AUTO_BACKUP_ENABLED else '❌ DISABLED'}\nInterval: {AUTO_BACKUP_DAYS} days\n\n*Commands:*\n/auto_backup on - Enable\n/auto_backup off - Disable\n/auto_backup days <1-30> - Change interval\n/backup - Manual backup", parse_mode="Markdown")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    next_backup = "Not scheduled"
    if context.job_queue:
        for job in context.job_queue.jobs():
            if job.name == "auto_backup" and job.next_t:
                next_backup = str(job.next_t)[:19]
    sent_msg = await update.message.reply_text(f"📅 *Auto Backup Configuration*\n\nStatus: {'✅ ENABLED' if AUTO_BACKUP_ENABLED else '❌ DISABLED'}\nInterval: Every {AUTO_BACKUP_DAYS} days\nNext backup: {next_backup}\n\n💾 Manual: /backup\n⚙️ Settings: /auto_backup on/off/days <number>", parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    uptime = str(timedelta(seconds=int(time.time() - start_time)))
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    channel_count = await db.get_channel_count()
    db_storage = await db.get_db_storage_usage()
    files = await db.get_all_files()
    total_access = sum(f[5] for f in files) if files else 0
    escaped_bot_username = bot_username.replace("_", "\\_")
    sent_msg = await update.message.reply_text(f"📊 *Bot Statistics*\n\n🤖 Bot: @{escaped_bot_username}\n⏱ Uptime: {uptime}\n\n📁 Files: {file_count}\n👥 Users: {user_count}\n📢 Channels: {channel_count}\n👀 Accesses: {total_access}\n\n💾 PostgreSQL: {db_storage['total']}\n📅 Auto Backup: Every {AUTO_BACKUP_DAYS} days\n⏰ Auto-delete: {DELETE_AFTER//60} minutes", parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    files = await db.get_all_files()
    if not files:
        sent_msg = await update.message.reply_text("📁 No files stored")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    msg = f"📁 *Total Files: {len(files)}*\n\n"
    for file in files[:20]:
        file_id, name, is_video, size, ts, access = file
        size_mb = size / (1024*1024) if size else 0
        msg += f"🔑 `{file_id}` - {name[:30]}... ({size_mb:.1f}MB) - 👥 {access}\n"
    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        sent_msg = await update.message.reply_text("❌ Usage: /deletefile <key>")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    key = context.args[0]
    if await db.delete_file(key):
        sent_msg = await update.message.reply_text(f"✅ Deleted file {key}")
    else:
        sent_msg = await update.message.reply_text(f"❌ File {key} not found")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    stats_data = await db.get_user_stats()
    msg = f"📊 *User Statistics*\n\n👥 Total Users: {stats_data['total_users']}\n🟢 Active (7d): {stats_data['active_users_7d']}\n🟡 Active (30d): {stats_data['active_users_30d']}\n📈 New Today: {stats_data['new_users_today']}\n📁 File Accessors: {stats_data['users_with_files']}"
    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args and not update.message.reply_to_message:
        sent_msg = await update.message.reply_text("❌ Usage: /broadcast <message> or reply with /broadcast\nOptional: /broadcast --preview to see preview only")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    preview_mode = False
    args_list = context.args if context.args else []
    if args_list and args_list[0] == "--preview":
        preview_mode = True
        message_text = " ".join(args_list[1:]) if len(args_list) > 1 else ""
    else:
        if update.message.reply_to_message:
            message_text = update.message.reply_to_message.text or update.message.reply_to_message.caption
        else:
            message_text = " ".join(args_list) if args_list else ""
    if not message_text:
        sent_msg = await update.message.reply_text("❌ Message cannot be empty")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    status_msg = await update.message.reply_text("📊 Fetching user list...", parse_mode="Markdown")
    user_ids = await db.get_all_user_ids(exclude_admin=True)
    total_users = len(user_ids)
    if total_users == 0:
        await status_msg.edit_text("❌ No users found to broadcast")
        return
    if preview_mode:
        preview_text = f"🔍 *BROADCAST PREVIEW*\n\n📝 *Message:*\n{message_text[:200]}{'...' if len(message_text) > 200 else ''}\n\n👥 *Total users:* {total_users}\n📦 *Chunks:* {(total_users + 999) // 1000}\n\n*First 5 users:*\n"
        for i, uid in enumerate(user_ids[:5]):
            preview_text += f"{i+1}. `{uid}`\n"
        keyboard = [[InlineKeyboardButton("✅ Confirm Broadcast", callback_data=f"confirm_broadcast|{total_users}"), InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")]]
        await status_msg.edit_text(preview_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        context.chat_data['broadcast_message'] = message_text
        return
    await status_msg.edit_text(f"🔄 Starting broadcast to {total_users} users...\n📦 Processing in chunks of 1000 users")
    asyncio.create_task(process_broadcast_chunks(context, user_ids, message_text, status_msg))

async def process_broadcast_chunks(context: ContextTypes.DEFAULT_TYPE, user_ids: list, message_text: str, status_msg):
    CHUNK_SIZE = 1000
    total_users = len(user_ids)
    total_chunks = (total_users + CHUNK_SIZE - 1) // CHUNK_SIZE
    successful = 0
    failed = 0
    blocked = 0
    start_time = time.time()
    for chunk_num in range(total_chunks):
        chunk_start = chunk_num * CHUNK_SIZE
        chunk_end = min((chunk_num + 1) * CHUNK_SIZE, total_users)
        chunk_users = user_ids[chunk_start:chunk_end]
        chunk_success = 0
        for i, user_id in enumerate(chunk_users):
            try:
                await context.bot.send_message(chat_id=user_id, text=f"📢 *Broadcast Message*\n\n{message_text}", parse_mode="Markdown")
                chunk_success += 1
                successful += 1
                if (i + 1) % 100 == 0:
                    await status_msg.edit_text(f"📦 *Chunk {chunk_num + 1}/{total_chunks}* - {i + 1}/{len(chunk_users)} users\n✅ Sent: {successful}\n❌ Failed: {failed}\n🚫 Blocked: {blocked}", parse_mode="Markdown")
                await asyncio.sleep(0.05)
            except Exception as e:
                error_str = str(e).lower()
                if "blocked" in error_str or "forbidden" in error_str:
                    blocked += 1
                else:
                    failed += 1
        await status_msg.edit_text(f"✅ *Chunk {chunk_num + 1}/{total_chunks} Complete*\n📈 Overall: ✅{successful} ❌{failed} 🚫{blocked}", parse_mode="Markdown")
        if chunk_num < total_chunks - 1:
            await asyncio.sleep(2)
    elapsed_time = time.time() - start_time
    summary = f"✅ *Broadcast Complete!*\n\n👥 Total: {total_users}\n✅ Sent: {successful}\n❌ Failed: {failed}\n🚫 Blocked: {blocked}\n⏱️ Time: {elapsed_time:.1f}s"
    await status_msg.edit_text(summary, parse_mode="Markdown")

async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "cancel_broadcast":
        await query.edit_message_text("❌ Broadcast cancelled")
        return
    if data.startswith("confirm_broadcast"):
        try:
            total_users = int(data.split("|")[1])
            message_text = context.chat_data.get('broadcast_message', '')
            if not message_text:
                await query.edit_message_text("❌ Could not retrieve message.")
                return
            await query.edit_message_text(f"🔄 Starting broadcast to {total_users} users...\n📦 Processing in chunks of 1000 users")
            user_ids = await db.get_all_user_ids(exclude_admin=True)
            asyncio.create_task(process_broadcast_chunks(context, user_ids, message_text, query.message))
            context.chat_data.pop('broadcast_message', None)
        except Exception as e:
            log.error(f"Error in broadcast confirmation: {e}")
            await query.edit_message_text(f"❌ Error: {str(e)[:100]}")

async def addchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        sent_msg = await update.message.reply_text("❌ Usage: /addchannel <channel username> [friendly name]\nExample: /addchannel @my_channel \"My Channel\"")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    channel = context.args[0]
    friendly_name = None
    if len(context.args) > 1:
        friendly_name = " ".join(context.args[1:])
    user_id = update.effective_user.id
    try:
        clean_channel = channel.replace("@", "")
        bot_member = await context.bot.get_chat_member(f"@{clean_channel}", context.bot.id)
        if bot_member.status not in ["administrator", "creator"]:
            keyboard = [[InlineKeyboardButton("🤖 Add Bot to Channel", url=f"https://t.me/{clean_channel}?startchannel=bot")]]
            sent_msg = await update.message.reply_text(f"⚠️ *Bot is not an admin in @{clean_channel}*\n\nAdd the bot as admin to check memberships!", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
    except Exception as e:
        log.warning(f"Could not verify bot in channel {channel}: {e}")
    success = await db.add_channel(channel, user_id, friendly_name)
    if success:
        channels = await db.get_channels_with_details()
        active_channels = [c for c in channels if c['is_active'] == 1]
        channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']}" for i, c in enumerate(active_channels)])
        sent_msg = await update.message.reply_text(f"✅ *Channel added successfully!*\n\nAdded: {friendly_name or f'@{channel.replace('@', '')}'}\n\n📋 *Current required channels:*\n{channel_list}", parse_mode="Markdown")
    else:
        sent_msg = await update.message.reply_text("❌ Failed to add channel.")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def removechannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        sent_msg = await update.message.reply_text("❌ Usage: /removechannel <channel username>")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    channel = context.args[0]
    success = await db.remove_channel(channel)
    if success:
        channels = await db.get_channels_with_details()
        active_channels = [c for c in channels if c['is_active'] == 1]
        channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']}" for i, c in enumerate(active_channels)]) if active_channels else "No channels required"
        sent_msg = await update.message.reply_text(f"✅ *Channel removed successfully!*\n\nRemoved: @{channel.replace('@', '')}\n\n📋 *Current required channels:*\n{channel_list}", parse_mode="Markdown")
    else:
        sent_msg = await update.message.reply_text("❌ Channel not found.")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def listchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    channels = await db.get_channels_with_details()
    if not channels:
        sent_msg = await update.message.reply_text("📋 No channels configured.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    active_channels = [c for c in channels if c['is_active'] == 1]
    inactive_channels = [c for c in channels if c['is_active'] == 0]
    msg = f"📋 *Channel Management*\n\n📢 *Active Channels ({len(active_channels)}):*\n"
    for i, ch in enumerate(active_channels):
        added_date = ch['added_at'].strftime('%Y-%m-%d') if ch['added_at'] else 'Unknown'
        display_name = ch['channel_name'] or ch['channel_username']
        msg += f"{i+1}. {display_name}\n   └ @{ch['channel_username']} (added {added_date})\n"
    if inactive_channels:
        msg += f"\n⏸️ *Inactive Channels ({len(inactive_channels)}):*\n"
        for i, ch in enumerate(inactive_channels):
            msg += f"{i+1}. {ch['channel_name'] or ch['channel_username']} (@{ch['channel_username']})\n"
    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    channels_data = await db.get_channels_with_details()
    active_channels = [c for c in channels_data if c['is_active'] == 1]
    if not active_channels:
        sent_msg = await update.message.reply_text("📋 No channels configured.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    status_msg = await update.message.reply_text("🔍 Testing channel access...")
    results = []
    for ch in active_channels:
        channel = ch['channel_username']
        display_name = ch['channel_name'] or channel
        try:
            bot_member = await context.bot.get_chat_member(f"@{channel}", context.bot.id)
            if bot_member.status in ["administrator", "creator"]:
                results.append(f"✅ {display_name} - Bot is admin")
            else:
                results.append(f"⚠️ {display_name} - Bot is member")
        except Exception as e:
            error_msg = str(e)
            if "chat not found" in error_msg.lower():
                results.append(f"❌ {display_name} - Channel not found")
            else:
                results.append(f"❌ {display_name} - Error")
    await status_msg.edit_text("🔍 *Channel Access Test*\n\n" + "\n".join(results), parse_mode="Markdown")
    await schedule_message_deletion(context, status_msg.chat_id, status_msg.message_id)

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if context.args:
        channel = context.args[0]
        await db.clear_membership_cache(channel=channel)
        sent_msg = await update.message.reply_text(f"✅ Cache cleared for channel {channel}")
    else:
        await db.clear_membership_cache()
        sent_msg = await update.message.reply_text("✅ All cache cleared")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    user_id = update.effective_user.id
    channels_data = await db.get_channels_with_details()
    active_channels = [c for c in channels_data if c['is_active'] == 1]
    if not active_channels:
        sent_msg = await update.message.reply_text("📋 No channels configured.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    results = []
    for ch in active_channels:
        channel = ch['channel_username']
        display_name = ch['channel_name'] or channel
        try:
            member = await context.bot.get_chat_member(f"@{channel}", user_id)
            results.append(f"{display_name}: ✅ {member.status}")
        except Exception as e:
            results.append(f"{display_name}: ❌ Not joined")
    await update.message.reply_text("🔍 *Channel Access Test*\n\n" + "\n".join(results), parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

# ============ MAIN ============
async def initialize_bot():
    global bot_app, bot_loop, bot_initialized
    if not BOT_TOKEN or not ADMIN_ID:
        log.error("Missing BOT_TOKEN or ADMIN_ID")
        return None
    log.info("Initializing database connection pool...")
    try:
        db._get_pool_sync()
        log.info("Database pool initialized.")
    except Exception as e:
        log.error(f"Failed to initialize database: {e}", exc_info=True)
        return None
    request = HTTPXRequest(connection_pool_size=40)
    application = Application.builder().token(BOT_TOKEN).request(request).build()
    await application.initialize()
    bot_loop = asyncio.get_running_loop()
    bot_app = application
    if application.job_queue:
        application.job_queue.run_repeating(cleanup_overdue_messages, interval=300, first=10)
        if AUTO_BACKUP_ENABLED:
            application.job_queue.run_repeating(auto_backup_job, interval=AUTO_BACKUP_DAYS * 24 * 60 * 60, first=3600, name="auto_backup")
            log.info(f"📅 Auto-backup scheduled (every {AUTO_BACKUP_DAYS} days)")
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("listfiles", listfiles))
    application.add_handler(CommandHandler("deletefile", deletefile))
    application.add_handler(CommandHandler("users", users))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("clearcache", clearcache))
    application.add_handler(CommandHandler("testchannel", testchannel))
    application.add_handler(CommandHandler("addchannel", addchannel))
    application.add_handler(CommandHandler("removechannel", removechannel))
    application.add_handler(CommandHandler("listchannels", listchannels))
    application.add_handler(CommandHandler("testchannels", testchannels))
    application.add_handler(CommandHandler("backup", backup_command))
    application.add_handler(CommandHandler("backup_status", backup_status))
    application.add_handler(CommandHandler("auto_backup", auto_backup_settings))
    application.add_handler(CommandHandler("import", import_command))
    application.add_handler(CommandHandler("import_now", import_now_command))
    application.add_handler(CommandHandler("confirm_import", confirm_import_command))
    application.add_handler(CommandHandler("cancel_import", cancel_import_command))
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check_membership$"))
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check\\|"))
    application.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^(confirm_broadcast|cancel_broadcast)$"))
    application.add_handler(CallbackQueryHandler(start_import_collection, pattern="^(start_import|ignore_csv)$"))
    application.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.PRIVATE, handle_document))
    render_url = os.environ.get('RENDER_EXTERNAL_URL')
    if not render_url:
        render_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'localhost')}"
    webhook_url = f"{render_url}/webhook"
    log.info(f"Setting webhook to: {webhook_url}")
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        await application.bot.set_webhook(url=webhook_url, allowed_updates=Update.ALL_TYPES, max_connections=40)
        log.info("✅ Webhook set successfully")
    except Exception as e:
        log.error(f"Failed to set webhook: {e}", exc_info=True)
        return None
    bot_initialized = True
    log.info("🤖 Bot initialized and ready via webhook")
    log.info(f"📁 Files: {await db.get_file_count()}")
    log.info(f"👥 Users: {await db.get_user_count()}")
    log.info(f"📢 Channels: {await db.get_channel_count()}")
    log.info(f"📅 Auto backup: Every {AUTO_BACKUP_DAYS} days")
    return application

async def main_async():
    global bot_app
    bot_app = await initialize_bot()
    if bot_app is None:
        log.error("Failed to initialize bot. Exiting.")
        return
    log.info("Bot is running. Waiting for webhook events...")
    while True:
        await asyncio.sleep(3600)

def main():
    print("\n" + "=" * 60)
    print("🤖 TELEGRAM FILE BOT - COMPLETE VERSION")
    print("=" * 60)
    print(f"✅ Bot: @{bot_username}")
    print(f"✅ Admin: {ADMIN_ID}")
    print(f"✅ Database: Render PostgreSQL")
    print(f"✅ Auto Backup: Every {AUTO_BACKUP_DAYS} days")
    print(f"✅ Auto Cleanup: DISABLED")
    print(f"✅ Python Version: {sys.version}")
    print("=" * 60 + "\n")
    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    log.info("Flask thread started")
    time.sleep(2)
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
    finally:
        log.info("Shutting down...")
        if bot_app:
            asyncio.run(bot_app.shutdown())
        asyncio.run(db.close_pool())
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
