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
bot_username = "xoticcroissant_bot"
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

# Playable formats
PLAYABLE_EXTS = {"mp4", "mov", "m4v", "mpeg", "mpg"}

# All video extensions
ALL_VIDEO_EXTS = {
    "mp4", "mkv", "mov", "avi", "webm", "flv", "m4v",
    "3gp", "wmv", "mpg", "mpeg"
}

# Friendly channel names (for UI)
CHANNEL_NAMES = {
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
        """Synchronous pool initialization - called only once"""
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
                log.error(f"❌ Failed to create connection pool to Render PostgreSQL: {e}")
                raise
        return self.pool

    async def _get_pool_async(self):
        """Async wrapper for pool initialization"""
        if self.pool is None:
            await asyncio.to_thread(self._get_pool_sync)
        return self.pool

    def _init_db(self, conn, cur):
        """Initialize database tables (synchronous)"""
        # Files table
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
        
        # Membership cache table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS membership_cache (
                user_id BIGINT,
                channel TEXT,
                is_member INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, channel)
            )
        ''')
        
        # Scheduled deletions table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_deletions (
                chat_id BIGINT NOT NULL,
                message_id INTEGER NOT NULL,
                scheduled_time TIMESTAMP NOT NULL,
                delete_after INTEGER DEFAULT 600,
                PRIMARY KEY (chat_id, message_id)
            )
        ''')
        
        # Users table
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
        
        # Required channels table
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
        
        # Create indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_channels_active ON required_channels(is_active)')
        
        # Insert default channels if table is empty
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
        """Asynchronous context manager to get and return a connection from the pool."""
        pool = await self._get_pool_async()
        conn = await asyncio.to_thread(pool.getconn)
        try:
            yield conn
        finally:
            await asyncio.to_thread(pool.putconn, conn)

    async def execute(self, query: str, params: tuple = None):
        """Execute a query and return cursor"""
        async with self.get_db_connection() as conn:
            def _execute():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur
            return await asyncio.to_thread(_execute)

    async def fetchrow(self, query: str, params: tuple = None):
        """Fetch one row as a dictionary."""
        async with self.get_db_connection() as conn:
            def _fetch():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchone()
            return await asyncio.to_thread(_fetch)

    async def fetchall(self, query: str, params: tuple = None):
        """Fetch all rows as a list of dictionaries."""
        async with self.get_db_connection() as conn:
            def _fetch():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
            return await asyncio.to_thread(_fetch)

    async def execute_and_commit(self, query: str, params: tuple = None):
        """Execute query and commit."""
        async with self.get_db_connection() as conn:
            def _execute():
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    conn.commit()
                    return cur.rowcount
            return await asyncio.to_thread(_execute)

    async def save_file(self, file_id: str, file_info: dict) -> str:
        """Save file info and return generated ID."""
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
        """Get file info by ID."""
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
        """Get total number of files."""
        result = await self.fetchrow("SELECT COUNT(*) as count FROM files")
        return result['count'] if result else 0

    async def cache_membership(self, user_id: int, channel: str, is_member: bool):
        """Cache membership check result."""
        await self.execute_and_commit('''
            INSERT INTO membership_cache (user_id, channel, is_member, timestamp)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, channel) DO UPDATE
            SET is_member = EXCLUDED.is_member,
                timestamp = EXCLUDED.timestamp
        ''', (user_id, channel, 1 if is_member else 0))

    async def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        """Get cached membership result (valid for 5 minutes)."""
        result = await self.fetchrow('''
            SELECT is_member FROM membership_cache
            WHERE user_id = %s AND channel = %s
            AND timestamp > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        ''', (user_id, channel))
        return bool(result['is_member']) if result else None

    async def clear_membership_cache(self, user_id: Optional[int] = None, channel: Optional[str] = None):
        """Clear membership cache for a user, channel, or all."""
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
        """Manually delete a file from database."""
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
        """Get all files for admin view."""
        rows = await self.fetchall('''
            SELECT id, file_name, is_video, file_size,
                   TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
                   access_count
            FROM files
            ORDER BY timestamp DESC
        ''')
        return [(row['id'], row['file_name'], row['is_video'], row['file_size'], row['timestamp'], row['access_count']) for row in rows]

    async def schedule_message_deletion(self, chat_id: int, message_id: int):
        """Schedule a message for deletion."""
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
        """Get messages that are due for deletion."""
        rows = await self.fetchall('''
            SELECT chat_id, message_id FROM scheduled_deletions
            WHERE scheduled_time <= CURRENT_TIMESTAMP
        ''')
        return [(row['chat_id'], row['message_id']) for row in rows]

    async def remove_scheduled_message(self, chat_id: int, message_id: int):
        """Remove message from scheduled deletions."""
        await self.execute_and_commit(
            'DELETE FROM scheduled_deletions WHERE chat_id = %s AND message_id = %s',
            (chat_id, message_id)
        )
        log.info(f"Removed scheduled deletion for message {message_id}")

    async def update_user_interaction(self, user_id: int, username: str = None,
                                    first_name: str = None, last_name: str = None,
                                    file_accessed: bool = False):
        """Update user interaction timestamp and count."""
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
        """Get comprehensive user statistics."""
        total_users = await self.fetchrow("SELECT COUNT(*) as count FROM users")
        active_7d = await self.fetchrow('''
            SELECT COUNT(*) as count FROM users
            WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '7 days'
        ''')
        active_30d = await self.fetchrow('''
            SELECT COUNT(*) as count FROM users
            WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '30 days'
        ''')
        new_today = await self.fetchrow('''
            SELECT COUNT(*) as count FROM users
            WHERE DATE(first_seen) = CURRENT_DATE
        ''')
        new_week = await self.fetchrow('''
            SELECT COUNT(*) as count FROM users
            WHERE first_seen > CURRENT_TIMESTAMP - INTERVAL '7 days'
        ''')
        users_files = await self.fetchrow('''
            SELECT COUNT(DISTINCT user_id) as count FROM users
            WHERE total_files_accessed > 0
        ''')
        top_users = await self.fetchall('''
            SELECT user_id, username, first_name, last_name,
                   total_interactions, total_files_accessed,
                   TO_CHAR(last_active, 'YYYY-MM-DD HH24:MI:SS') as last_active,
                   TO_CHAR(first_seen, 'YYYY-MM-DD HH24:MI:SS') as first_seen
            FROM users
            ORDER BY total_interactions DESC
            LIMIT 10
        ''')
        growth_data = await self.fetchall('''
            SELECT
                TO_CHAR(first_seen, 'YYYY-MM-DD') as date,
                COUNT(*) as new_users
            FROM users
            WHERE first_seen > CURRENT_TIMESTAMP - INTERVAL '30 days'
            GROUP BY date
            ORDER BY date DESC
            LIMIT 15
        ''')

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
        """Get all user IDs for broadcasting."""
        if exclude_admin:
            rows = await self.fetchall("SELECT user_id FROM users WHERE user_id != %s", (ADMIN_ID,))
        else:
            rows = await self.fetchall("SELECT user_id FROM users")
        return [row['user_id'] for row in rows]

    async def get_user_count(self) -> int:
        """Get total number of users."""
        result = await self.fetchrow("SELECT COUNT(*) as count FROM users")
        return result['count'] if result else 0

    async def get_required_channels(self, active_only: bool = True) -> List[str]:
        """Get list of all required channels"""
        if active_only:
            rows = await self.fetchall("SELECT channel_username FROM required_channels WHERE is_active = 1 ORDER BY position, id")
        else:
            rows = await self.fetchall("SELECT channel_username FROM required_channels ORDER BY position, id")
        return [row['channel_username'] for row in rows]
    
    async def get_channels_with_details(self) -> List[Dict]:
        """Get channels with all details for listing"""
        rows = await self.fetchall('''
            SELECT id, channel_username, channel_name, added_at, is_active, position
            FROM required_channels
            ORDER BY position, id
        ''')
        return [dict(row) for row in rows]
    
    async def add_channel(self, channel_username: str, added_by: int, channel_name: str = None) -> bool:
        """Add a new required channel"""
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
        """Remove a required channel (soft delete by setting inactive)"""
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
    
    async def get_channel_count(self) -> int:
        """Get number of active required channels"""
        result = await self.fetchrow("SELECT COUNT(*) as count FROM required_channels WHERE is_active = 1")
        return result['count'] if result else 0

    async def get_db_storage_usage(self) -> Dict[str, Any]:
        """Get PostgreSQL database storage usage"""
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
                    'indexes': format_bytes(index_bytes)
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
        """Get detailed metadata storage info"""
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
        """Get total size of all files uploaded"""
        result = await self.fetchrow("SELECT COALESCE(SUM(file_size), 0) as total FROM files")
        return result['total'] if result else 0

    async def close_pool(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            log.info("Database connection pool closed")

# Initialize database
db = Database()

# ============ MESSAGE DELETION SYSTEM ============
async def delete_message_job(context):
    """Delete message after timer"""
    try:
        job = context.job
        chat_id = job.chat_id
        message_id = job.data

        if not chat_id or not message_id:
            return

        log.info(f"🗑️ Attempting to delete message {message_id} from chat {chat_id}")

        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            log.info(f"✅ Successfully deleted message {message_id}")
            await db.remove_scheduled_message(chat_id, message_id)
        except Exception as e:
            error_msg = str(e).lower()
            if "message to delete not found" in error_msg:
                await db.remove_scheduled_message(chat_id, message_id)
            elif "message can't be deleted" in error_msg:
                log.warning(f"Can't delete message {message_id}")
            else:
                log.error(f"Failed to delete message {message_id}: {e}")

    except Exception as e:
        log.error(f"Error in delete_message_job: {e}", exc_info=True)

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    """Schedule a message for deletion"""
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
            log.info(f"Scheduled deletion of message {message_id} in {DELETE_AFTER} seconds")
    except Exception as e:
        log.error(f"Failed to schedule deletion: {e}")

async def cleanup_overdue_messages(context: ContextTypes.DEFAULT_TYPE):
    """Clean up overdue messages"""
    try:
        due_messages = await db.get_due_messages()
        if not due_messages:
            return

        log.info(f"Found {len(due_messages)} overdue messages")

        for chat_id, message_id in due_messages:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                log.info(f"✅ Cleanup: Deleted overdue message {message_id}")
                await db.remove_scheduled_message(chat_id, message_id)
            except Exception as e:
                error_msg = str(e).lower()
                if "message to delete not found" in error_msg:
                    await db.remove_scheduled_message(chat_id, message_id)
                else:
                    log.error(f"Cleanup failed for {message_id}: {e}")

    except Exception as e:
        log.error(f"Error in cleanup_overdue_messages: {e}")

# ============ DYNAMIC MEMBERSHIP CHECK ============
async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> bool:
    """Check if user is in channel"""
    clean_channel = channel.replace("@", "")
    
    if not force_check:
        cached = await db.get_cached_membership(user_id, clean_channel)
        if cached is not None:
            log.info(f"✅ CACHE HIT: User {user_id} in {clean_channel}: {cached}")
            return cached

    try:
        if not channel.startswith("@"):
            channel_username = f"@{channel}"
        else:
            channel_username = channel

        log.info(f"🔍 Checking user {user_id} in channel {channel_username}")
        member = await bot.get_chat_member(chat_id=channel_username, user_id=user_id)
        is_member = member.status in ["member", "administrator", "creator"]
        log.info(f"✅ User {user_id} in {clean_channel}: {is_member} (status: {member.status})")

        await db.cache_membership(user_id, clean_channel, is_member)
        return is_member

    except Exception as e:
        error_msg = str(e).lower()
        log.error(f"❌ Error checking user {user_id} in {clean_channel}: {error_msg}")
        
        if "user not found" in error_msg or "user not participant" in error_msg:
            await db.cache_membership(user_id, clean_channel, False)
            return False
        elif "chat not found" in error_msg:
            log.error(f"Channel @{clean_channel} not found!")
            return True
        elif "forbidden" in error_msg:
            log.error(f"Bot can't access @{clean_channel}")
            return True
        else:
            return True

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, force_check: bool = False) -> Dict[str, Any]:
    """Check if user is member of all required channels"""
    bot = context.bot

    result = {
        "all_joined": False,
        "missing_channels": [],
        "missing_channel_names": [],
        "channel_status": {}
    }

    channels_data = await db.get_channels_with_details()
    active_channels = [c for c in channels_data if c['is_active'] == 1]
    
    log.info(f"📋 Found {len(active_channels)} active channels: {[c['channel_username'] for c in active_channels]}")
    
    if not active_channels:
        log.info(f"✅ No channels required for user {user_id} - auto approving")
        result["all_joined"] = True
        return result

    if force_check:
        log.info(f"🔄 Force check - clearing cache for user {user_id}")
        await db.clear_membership_cache(user_id)

    for channel_data in active_channels:
        channel = channel_data['channel_username']
        channel_name = channel_data['channel_name'] or channel
        
        log.info(f"🔍 Checking user {user_id} in channel @{channel} ({channel_name})")
        is_member = await check_user_in_channel(bot, channel, user_id, force_check)
        
        result["channel_status"][channel] = {
            'is_member': is_member,
            'name': channel_name
        }
        
        if not is_member:
            log.info(f"❌ User {user_id} NOT in channel @{channel}")
            result["missing_channels"].append(channel)
            result["missing_channel_names"].append(channel_name)

    result["all_joined"] = len(result["missing_channels"]) == 0
    log.info(f"📊 Final result for user {user_id}: all_joined={result['all_joined']}, missing={result['missing_channel_names']}")
    
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
        a { color: #FFD700; text-decoration: none; }
        .btn {
            display: inline-block;
            background: #4CAF50;
            color: white;
            padding: 8px 16px;
            border-radius: 6px;
            margin: 5px;
            font-size: 0.9rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Telegram File Bot</h1>
        <div class="status">
            <h3>✅ Status: <strong>ACTIVE</strong></h3>
            <p>Bot is running on Render with PostgreSQL (psycopg2)</p>
            <p>Uptime: {{ uptime }}</p>
            <p>Files in DB: {{ file_count }}</p>
            <p>Users in DB: {{ user_count }}</p>
            <p>Required Channels: {{ channel_count }}</p>
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
        log.error(f"Error fetching counts for home route: {e}")
        file_count = 0
        user_count = 0
        channel_count = 0

    return render_template_string(html_content,
                                  bot_username=bot_username,
                                  uptime=uptime_str,
                                  file_count=file_count,
                                  user_count=user_count,
                                  channel_count=channel_count)

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
        log.error(f"Error in health check: {e}")
        file_count = 0
        user_count = 0
        channel_count = 0

    return jsonify({
        "status": "OK",
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time))),
        "database": "postgresql",
        "driver": "psycopg2-binary",
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
    """Handle Telegram webhook updates"""
    global bot_app, bot_loop, bot_initialized
    
    if not bot_initialized or bot_app is None or bot_loop is None:
        log.error("Bot application not fully initialized for webhook")
        return "Bot not ready", 503

    update_data = request.get_json()
    if not update_data:
        return "Invalid request", 400

    future = asyncio.run_coroutine_threadsafe(
        process_update(update_data, bot_app),
        bot_loop
    )
    
    try:
        future.result(timeout=1)
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        log.error(f"Error queueing update: {e}")

    return "OK", 200

async def process_update(update_data, application):
    """Process Telegram update"""
    try:
        update = Update.de_json(update_data, application.bot)
        await application.process_update(update)
    except Exception as e:
        log.error(f"Error processing update: {e}", exc_info=True)

def run_flask_thread():
    """Run Flask server in a thread"""
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
    """Export a table to CSV format and return CSV content"""
    try:
        rows = await db.fetchall(f"SELECT * FROM {table_name}")
        
        if not rows:
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            return output.getvalue()
        
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
    """Export entire database to CSV files"""
    
    backup_data = {}
    backup_info = {
        "export_time": datetime.now().isoformat(),
        "tables_exported": [],
        "row_counts": {}
    }
    
    tables_config = {
        "files": ["id", "file_id", "file_name", "mime_type", "is_video", 
                  "file_size", "timestamp", "access_count"],
        "users": ["user_id", "username", "first_name", "last_name", 
                  "first_seen", "last_active", "total_interactions", 
                  "total_files_accessed", "last_file_accessed"],
        "membership_cache": ["user_id", "channel", "is_member", "timestamp"],
        "required_channels": ["id", "channel_username", "channel_name", "added_by", 
                              "added_at", "is_active", "position"],
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
                
        except Exception as e:
            log.error(f"❌ Failed to export {table_name}: {e}")
    
    metadata = {
        "export_info": backup_info,
        "bot_config": {
            "bot_username": bot_username,
            "delete_after_seconds": DELETE_AFTER,
            "auto_cleanup_days": AUTO_CLEANUP_DAYS,
            "export_timestamp": datetime.now().isoformat()
        }
    }
    
    backup_data["metadata.json"] = json.dumps(metadata, indent=2)
    backup_info["metadata_created"] = True
    
    if send_to_admin and context:
        await send_backup_to_admin(context, backup_data, backup_info)
    
    return backup_data

async def send_backup_to_admin(context: ContextTypes.DEFAULT_TYPE, backup_data: Dict[str, str], backup_info: Dict[str, Any]):
    """Send backup files to admin"""
    try:
        summary = f"📦 *Database Backup Created*\n\n"
        summary += f"⏰ Time: {backup_info['export_time']}\n"
        summary += f"📊 Tables exported: {len(backup_info['tables_exported'])}\n\n"
        summary += f"📈 *Row Counts:*\n"
        
        for table, count in backup_info['row_counts'].items():
            summary += f"   • {table}: {count} rows\n"
        
        summary += f"\n💾 *Total backup size:* {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n"
        summary += f"\n⚠️ *Important:* Save these files immediately!\n"
        summary += f"Your Render PostgreSQL data will be lost after 1 month.\n\n"
        summary += f"💡 *To restore:* Forward ALL files back to bot and use `/import`\n"
        summary += f"📌 The bot now accepts both exact filenames (files.csv) and timestamped filenames (backup_*_files.csv)"
        
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=summary,
            parse_mode="Markdown"
        )
        
        for filename, content in backup_data.items():
            if content and len(content) > 0:
                file_bytes = io.BytesIO(content.encode('utf-8'))
                file_bytes.seek(0)
                
                file_emoji = "📋" if filename.endswith('.json') else "📄"
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                send_filename = f"backup_{timestamp}_{filename}"
                
                await context.bot.send_document(
                    chat_id=ADMIN_ID,
                    document=file_bytes,
                    filename=send_filename,
                    caption=f"{file_emoji} {filename} - {len(content.splitlines())} lines"
                )
                
                await asyncio.sleep(0.5)
        
        instructions = f"""
✅ *Backup Complete!*

📋 *To Restore on New Database:*

1. Create new PostgreSQL database on Render
2. Update DATABASE_URL environment variable
3. Restart bot
4. Forward ALL backup files (CSV + JSON) to bot
5. Use `/import` to restore
6. Confirm import
7. All users and files restored! ✅

🔧 *Commands:*
• `/backup` - Create new backup
• `/backup_status` - Check database health
• `/import` - Restore from backup files
• `/import_status` - Check collected files

⚠️ *Your users and broadcasts will work after restore!*
📌 The bot now supports both exact and timestamped filenames
        """
        
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=instructions,
            parse_mode="Markdown"
        )
        
        log.info(f"✅ Database backup sent to admin (ID: {ADMIN_ID})")
        
    except Exception as e:
        log.error(f"❌ Failed to send backup to admin: {e}")

# ============ IMPORT/RESTORE FUNCTIONS ============

async def import_csv_to_table(table_name: str, csv_content: str, truncate_first: bool = True) -> Dict[str, Any]:
    """Import CSV data to a specific table"""
    result = {
        "success": False,
        "rows_imported": 0,
        "errors": [],
        "table": table_name
    }
    
    try:
        # Parse CSV content
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(csv_reader)
        
        if not rows:
            result["success"] = True
            result["rows_imported"] = 0
            log.warning(f"No rows found in CSV content for table {table_name}")
            return result
        
        log.info(f"Importing {len(rows)} rows to table {table_name}")
        
        async with db.get_db_connection() as conn:
            def _import():
                imported = 0
                with conn.cursor() as cur:
                    # Optionally truncate table first
                    if truncate_first:
                        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
                        log.info(f"🗑️ Truncated table {table_name}")
                    
                    # Get column names from CSV header
                    columns = list(rows[0].keys())
                    # Clean column names (remove any whitespace/BOM)
                    columns = [col.strip().lstrip('\ufeff') for col in columns]
                    placeholders = ','.join(['%s'] * len(columns))
                    columns_str = ','.join(columns)
                    
                    # Prepare INSERT statement
                    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                    log.info(f"Insert query: {insert_query}")
                    
                    # Insert each row
                    for i, row in enumerate(rows):
                        try:
                            # Clean row keys
                            cleaned_row = {}
                            for k, v in row.items():
                                cleaned_row[k.strip().lstrip('\ufeff')] = v
                            
                            # Convert values to appropriate types
                            values = []
                            for col in columns:
                                val = cleaned_row.get(col, '').strip() if cleaned_row.get(col) else ''
                                
                                # Handle NULL values
                                if val == '' or val.upper() == 'NULL':
                                    values.append(None)
                                else:
                                    # Try to convert numeric values
                                    if col in ['id', 'is_video', 'access_count', 'total_interactions', 
                                              'total_files_accessed', 'is_active', 'position', 
                                              'delete_after', 'added_by', 'is_member', 'user_id',
                                              'chat_id', 'message_id']:
                                        try:
                                            values.append(int(float(val)) if val else None)
                                        except (ValueError, TypeError):
                                            values.append(None)
                                    elif col in ['file_size']:
                                        try:
                                            values.append(int(float(val)) if val else 0)
                                        except (ValueError, TypeError):
                                            values.append(0)
                                    elif col in ['timestamp', 'scheduled_time', 'first_seen', 
                                               'last_active', 'last_file_accessed', 'added_at']:
                                        # Handle date fields
                                        try:
                                            if val:
                                                # Try various date formats
                                                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', 
                                                          '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                                                    try:
                                                        datetime.strptime(val[:19], '%Y-%m-%d %H:%M:%S')
                                                        break
                                                    except:
                                                        continue
                                            values.append(val if val else None)
                                        except:
                                            values.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                                    else:
                                        values.append(val)
                            
                            cur.execute(insert_query, tuple(values))
                            imported += 1
                            
                            # Commit every 500 rows
                            if imported % 500 == 0:
                                conn.commit()
                                log.info(f"Imported {imported} rows to {table_name}...")
                                
                        except Exception as e:
                            error_msg = f"Row {i+1}: {str(e)[:100]}"
                            log.warning(f"Error importing row in {table_name}: {error_msg}")
                            result["errors"].append(error_msg)
                            # Continue with next row
                            continue
                    
                    conn.commit()
                    log.info(f"Committed {imported} rows to {table_name}")
                    return imported
            
            result["rows_imported"] = await asyncio.to_thread(_import)
            result["success"] = True
            log.info(f"✅ Imported {result['rows_imported']} rows to {table_name}")
            
    except Exception as e:
        log.error(f"Failed to import {table_name}: {e}", exc_info=True)
        result["errors"].append(str(e))
        result["success"] = False
    
    return result

async def reset_sequences():
    """Reset PostgreSQL sequences after import"""
    try:
        async with db.get_db_connection() as conn:
            def _reset():
                with conn.cursor() as cur:
                    cur.execute("SELECT setval('files_id_seq', COALESCE((SELECT MAX(id) FROM files), 1))")
                    cur.execute("SELECT setval('required_channels_id_seq', COALESCE((SELECT MAX(id) FROM required_channels), 1))")
                    conn.commit()
                    log.info("✅ Sequences reset successfully")
            await asyncio.to_thread(_reset)
    except Exception as e:
        log.error(f"Failed to reset sequences: {e}")

async def restore_from_backup(files_data: Dict[str, str]) -> Dict[str, Any]:
    """Restore entire database from backup files"""
    
    restore_result = {
        "success": False,
        "tables_restored": [],
        "total_rows": 0,
        "errors": [],
        "timestamp": datetime.now().isoformat()
    }
    
    # Define import order (important for foreign keys)
    import_order = [
        "required_channels",
        "users",
        "files",
        "membership_cache",
        "scheduled_deletions"
    ]
    
    # Import tables in correct order
    for table_name in import_order:
        csv_filename = f"{table_name}.csv"
        
        if csv_filename in files_data and files_data[csv_filename]:
            log.info(f"📥 Importing {table_name}...")
            
            result = await import_csv_to_table(table_name, files_data[csv_filename], truncate_first=True)
            
            if result["success"]:
                restore_result["tables_restored"].append({
                    "table": table_name,
                    "rows": result["rows_imported"]
                })
                restore_result["total_rows"] += result["rows_imported"]
                log.info(f"✅ Successfully imported {result['rows_imported']} rows to {table_name}")
            else:
                error_msg = f"{table_name}: {', '.join(result['errors'][:5])}" if result['errors'] else f"{table_name}: Unknown error"
                restore_result["errors"].append(error_msg)
                log.error(f"❌ Failed to import {table_name}: {error_msg}")
        else:
            log.warning(f"⚠️ No CSV file found for {table_name}")
            if table_name in ['files', 'users', 'required_channels']:
                restore_result["errors"].append(f"Missing required table: {csv_filename}")
    
    # Reset sequences
    await reset_sequences()
    
    # Check if we imported at least the required tables
    imported_tables = [t['table'] for t in restore_result['tables_restored']]
    required_tables = ['required_channels', 'users', 'files']
    
    if all(t in imported_tables for t in required_tables):
        restore_result["success"] = True
    else:
        restore_result["success"] = False
        missing = [t for t in required_tables if t not in imported_tables]
        restore_result["errors"].append(f"Missing required tables after import: {', '.join(missing)}")
    
    return restore_result

# ============ COMMAND HANDLERS ============

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    log.error(f"Error: {context.error}", exc_info=True)

# ============ FORWARDED FILE HANDLER (CSV + JSON) ============
async def handle_forwarded_backup_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detect and process forwarded CSV and JSON backup files"""
    try:
        msg = update.message
        
        if not msg.document:
            return
        
        doc = msg.document
        
        is_csv = doc.file_name and doc.file_name.endswith('.csv')
        is_json = doc.file_name and doc.file_name.endswith('.json')
        
        if not (is_csv or is_json):
            if update.effective_user.id == ADMIN_ID:
                return
            return
        
        if update.effective_user.id == ADMIN_ID:
            file_type = "CSV" if is_csv else "JSON"
            log.info(f"📥 Admin sent {file_type} file: {doc.file_name}")
            
            if 'pending_backup_files' not in context.user_data:
                context.user_data['pending_backup_files'] = {}
            
            try:
                file = await context.bot.get_file(doc.file_id)
                file_content = await file.download_as_bytearray()
                file_text = file_content.decode('utf-8')
                
                context.user_data['pending_backup_files'][doc.file_name] = file_text
                
                if is_csv:
                    lines = len(file_text.splitlines()) - 1
                    record_info = f"📊 Records: {lines}"
                else:
                    record_info = f"📋 JSON metadata file"
                
                collected_files = list(context.user_data['pending_backup_files'].keys())
                log.info(f"📦 Collected backup files: {collected_files}")
                
                file_emoji = "📄" if is_csv else "📋"
                sent_msg = await msg.reply_text(
                    f"✅ *{file_type} File Received*\n\n"
                    f"{file_emoji} File: `{doc.file_name}`\n"
                    f"{record_info}\n"
                    f"💾 Size: {doc.file_size / 1024:.1f} KB\n\n"
                    f"📦 Files collected: {len(context.user_data['pending_backup_files'])}\n\n"
                    f"💡 When ready, use `/import` to restore all collected files.\n"
                    f"🔍 Use `/import_status` to check collected files.\n\n"
                    f"⚠️ *Note:* Forwarded backup files are automatically collected.",
                    parse_mode="Markdown"
                )
                
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                
            except Exception as e:
                log.error(f"Error downloading {file_type} file: {e}")
                sent_msg = await msg.reply_text(f"❌ Error downloading {file_type} file: {str(e)[:200]}")
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            
        else:
            file_type = "CSV" if is_csv else "JSON"
            log.info(f"ℹ️ Non-admin user {update.effective_user.id} sent {file_type} file (ignored)")
            
    except Exception as e:
        log.error(f"Error handling backup file: {e}", exc_info=True)

# ============ START COMMAND ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    try:
        if not update.message:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args
        username = update.effective_user.username
        first_name = update.effective_user.first_name

        log.info(f"🚀 /start command from user {user_id} (@{username}) with args: {args}")

        await db.update_user_interaction(
            user_id=user_id,
            username=username,
            first_name=first_name,
            last_name=update.effective_user.last_name
        )

        channels_data = await db.get_channels_with_details()
        active_channels = [c for c in channels_data if c['is_active'] == 1]
        
        if not args:
            log.info(f"👋 Showing welcome menu to user {user_id}")
            keyboard = []
            
            for channel_data in active_channels:
                channel = channel_data['channel_username']
                channel_name = channel_data['channel_name'] or f"Channel"
                keyboard.append([InlineKeyboardButton(
                    f"📢 Join {channel_name}", 
                    url=f"https://t.me/{channel}"
                )])
            
            keyboard.append([InlineKeyboardButton(
                "🔄 Check Membership", 
                callback_data="check_membership"
            )])

            if active_channels:
                channel_list = "\n".join([f"{i+1}. {c['channel_name'] or f'Channel {i+1}'}" for i, c in enumerate(active_channels)])
            else:
                channel_list = "No channels required!"

            sent_msg = await update.message.reply_text(
                "🤖 *Welcome to File Sharing Bot*\n\n"
                "🔗 *How to use:*\n"
                "1️⃣ Use admin-provided links\n"
                "2️⃣ Join the required channels:\n"
                f"{channel_list}\n"
                "3️⃣ Click 'Check Membership'\n\n"
                f"⚠️ Messages auto-delete after {DELETE_AFTER//60} minutes",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        key = args[0]
        log.info(f"🔑 User {user_id} accessing file key: {key}")
        
        file_info = await db.get_file(key)

        if not file_info:
            log.warning(f"❌ File key {key} not found for user {user_id}")
            sent_msg = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        log.info(f"📁 File found: {file_info['file_name']}")

        result = await check_membership(user_id, context, force_check=True)

        if not result["all_joined"]:
            missing_channels = result["missing_channels"]
            missing_names = result["missing_channel_names"]
            
            keyboard = []
            
            for i, channel in enumerate(missing_channels):
                channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                keyboard.append([InlineKeyboardButton(
                    f"📥 Join {channel_name}", 
                    url=f"https://t.me/{channel}"
                )])
            
            keyboard.append([InlineKeyboardButton(
                "✅ Check Again", 
                callback_data=f"check|{key}"
            )])
            
            if len(missing_names) == 1:
                text = f"🔒 *Join {missing_names[0]} to access this file*"
            elif len(missing_names) == 2:
                text = f"🔒 *Join {missing_names[0]} and {missing_names[1]} to access this file*"
            else:
                channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                text = f"🔒 *Join {channels_text} to access this file*"
            
            sent_msg = await update.message.reply_text(
                text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        log.info(f"✅ User {user_id} has joined all channels. Sending file...")
        await db.update_user_interaction(user_id=user_id, file_accessed=True)

        try:
            filename = file_info['file_name']
            ext = filename.lower().split('.')[-1] if '.' in filename else ""

            warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes"

            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(
                    chat_id=chat_id,
                    video=file_info["file_id"],
                    caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                    parse_mode="Markdown",
                    supports_streaming=True
                )
            else:
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_info["file_id"],
                    caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                    parse_mode="Markdown"
                )

            await schedule_message_deletion(context, sent.chat_id, sent.message_id)

        except Exception as e:
            log.error(f"❌ Error sending file to user {user_id}: {e}", exc_info=True)
            sent_msg = await update.message.reply_text("❌ Failed to send file")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.error(f"❌ Start error: {e}", exc_info=True)

# ============ CHECK JOIN CALLBACK ============
async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries"""
    try:
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        data = query.data
        username = query.from_user.username

        user = query.from_user
        await db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )

        if data == "check_membership":
            result = await check_membership(user_id, context, force_check=True)

            if result["all_joined"]:
                channels_data = await db.get_channels_with_details()
                active_channels = [c for c in channels_data if c['is_active'] == 1]
                channel_list = "\n".join([f"✅ {c['channel_name'] or f'Channel {i+1}'}" for i, c in enumerate(active_channels)])
                
                await query.edit_message_text(
                    f"✅ *You've joined all required channels!*\n\n"
                    f"{channel_list}\n\n"
                    f"Now you can use file links from admin.",
                    parse_mode="Markdown"
                )
            else:
                missing_channels = result["missing_channels"]
                missing_names = result["missing_channel_names"]
                
                keyboard = []
                
                for i, channel in enumerate(missing_channels):
                    channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                    keyboard.append([InlineKeyboardButton(
                        f"📥 Join {channel_name}", 
                        url=f"https://t.me/{channel}"
                    )])
                
                keyboard.append([InlineKeyboardButton(
                    "🔄 Check Again", 
                    callback_data="check_membership"
                )])
                
                if len(missing_names) == 1:
                    text = f"❌ *Missing {missing_names[0]}*"
                elif len(missing_names) == 2:
                    text = f"❌ *Missing {missing_names[0]} and {missing_names[1]}*"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"❌ *Missing {channels_text}*"

                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
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
                    keyboard.append([InlineKeyboardButton(
                        f"📥 Join {channel_name}", 
                        url=f"https://t.me/{channel}"
                    )])
                
                keyboard.append([InlineKeyboardButton(
                    "✅ Check Again", 
                    callback_data=f"check|{key}"
                )])
                
                if len(missing_names) == 1:
                    text = f"❌ *Join {missing_names[0]}*"
                elif len(missing_names) == 2:
                    text = f"❌ *Join {missing_names[0]} and {missing_names[1]}*"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"❌ *Join {channels_text}*"

                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            await db.update_user_interaction(user_id=user_id, file_accessed=True)

            try:
                filename = file_info['file_name']
                ext = filename.lower().split('.')[-1] if '.' in filename else ""

                warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes"
                chat_id = query.message.chat_id

                if file_info['is_video'] and ext in PLAYABLE_EXTS:
                    sent = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_info["file_id"],
                        caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                        parse_mode="Markdown",
                        supports_streaming=True
                    )
                else:
                    sent = await context.bot.send_document(
                        chat_id=chat_id,
                        document=file_info["file_id"],
                        caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                        parse_mode="Markdown"
                    )

                await query.edit_message_text("✅ *File sent below!*", parse_mode="Markdown")
                await schedule_message_deletion(context, sent.chat_id, sent.message_id)

            except Exception as e:
                log.error(f"❌ Failed to send file to user {user_id}: {e}", exc_info=True)
                await query.edit_message_text("❌ Failed to send file")

    except Exception as e:
        log.error(f"❌ Callback error: {e}", exc_info=True)

# ============ CHANNEL MANAGEMENT COMMANDS ============
async def addchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new required channel (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        sent_msg = await update.message.reply_text(
            "❌ Usage: /addchannel <channel username> [friendly name]\n"
            "Example: /addchannel @my_channel \"My Channel\""
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    channel = context.args[0]
    friendly_name = None
    
    if len(context.args) > 1:
        friendly_name = " ".join(context.args[1:])
    
    user_id = update.effective_user.id
    
    success = await db.add_channel(channel, user_id, friendly_name)
    
    if success:
        channels = await db.get_channels_with_details()
        active_channels = [c for c in channels if c['is_active'] == 1]
        channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']}" for i, c in enumerate(active_channels)])
        
        sent_msg = await update.message.reply_text(
            f"✅ *Channel added successfully!*\n\n"
            f"Added: {friendly_name or f'@{channel.replace("@", "")}'}\n\n"
            f"📋 *Current required channels:*\n{channel_list}",
            parse_mode="Markdown"
        )
    else:
        sent_msg = await update.message.reply_text("❌ Failed to add channel. It might already exist.")
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def removechannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a required channel (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        sent_msg = await update.message.reply_text(
            "❌ Usage: /removechannel <channel username>\n"
            "Example: /removechannel @my_channel"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    channel = context.args[0]
    success = await db.remove_channel(channel)
    
    if success:
        channels = await db.get_channels_with_details()
        active_channels = [c for c in channels if c['is_active'] == 1]
        
        if active_channels:
            channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']}" for i, c in enumerate(active_channels)])
        else:
            channel_list = "No channels required (all access granted)"
        
        sent_msg = await update.message.reply_text(
            f"✅ *Channel removed successfully!*\n\n"
            f"Removed: @{channel.replace('@', '')}\n\n"
            f"📋 *Current required channels:*\n{channel_list}",
            parse_mode="Markdown"
        )
    else:
        sent_msg = await update.message.reply_text("❌ Channel not found or already removed.")
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def listchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all required channels (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    channels = await db.get_channels_with_details()
    
    if not channels:
        sent_msg = await update.message.reply_text(
            "📋 *No channels configured*\n\n"
            "Use /addchannel to add required channels.",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    active_channels = [c for c in channels if c['is_active'] == 1]
    inactive_channels = [c for c in channels if c['is_active'] == 0]
    
    msg = f"📋 *Channel Management*\n\n"
    msg += f"📢 *Active Channels ({len(active_channels)}):*\n"
    
    for i, ch in enumerate(active_channels):
        added_date = ch['added_at'].strftime('%Y-%m-%d') if ch['added_at'] else 'Unknown'
        display_name = ch['channel_name'] or ch['channel_username']
        msg += f"{i+1}. {display_name}\n"
        msg += f"   └ Username: @{ch['channel_username']}\n"
        msg += f"   └ Added: {added_date}\n"
    
    if inactive_channels:
        msg += f"\n⏸️ *Inactive Channels ({len(inactive_channels)}):*\n"
        for i, ch in enumerate(inactive_channels):
            display_name = ch['channel_name'] or ch['channel_username']
            msg += f"{i+1}. {display_name} (@{ch['channel_username']})\n"
    
    msg += f"\n💡 *Commands:*\n"
    msg += f"/addchannel @channel [name] - Add channel\n"
    msg += f"/removechannel @channel - Remove channel\n"
    msg += f"/testchannels - Test bot access to all channels"
    
    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test bot access to all required channels (admin only)"""
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
            chat = await context.bot.get_chat(f"@{channel}")
            bot_member = await context.bot.get_chat_member(f"@{channel}", context.bot.id)
            
            if bot_member.status in ["administrator", "creator"]:
                results.append(f"✅ {display_name} - Bot is admin")
            else:
                results.append(f"⚠️ {display_name} - Bot is member (not admin)")
                
        except Exception as e:
            error_msg = str(e)
            if "chat not found" in error_msg.lower():
                results.append(f"❌ {display_name} - Channel not found")
            elif "forbidden" in error_msg.lower():
                results.append(f"❌ {display_name} - Bot not in channel")
            else:
                results.append(f"❌ {display_name} - Error: {error_msg[:50]}")
    
    result_text = "🔍 *Channel Access Test*\n\n" + "\n".join(results)
    
    await status_msg.edit_text(result_text, parse_mode="Markdown")
    await schedule_message_deletion(context, status_msg.chat_id, status_msg.message_id)

# ============ EXISTING COMMAND HANDLERS ============
async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Upload file handler (admin only)"""
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

        file_info = {
            "file_name": filename,
            "mime_type": mime_type,
            "is_video": is_video,
            "size": int(file_size) if file_size else 0
        }

        key = await db.save_file(file_id, file_info)
        link = f"https://t.me/{bot_username}?start={key}"

        sent_msg = await msg.reply_text(
            f"✅ *Upload Successful*\n\n"
            f"📁 *Name:* `{filename}`\n"
            f"🔑 *Key:* `{key}`\n\n"
            f"🔗 *Link:*\n`{link}`",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.exception("Upload error")
        sent_msg = await update.message.reply_text(f"❌ Upload failed: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stats command (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    uptime = str(timedelta(seconds=int(time.time() - start_time)))
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    channel_count = await db.get_channel_count()

    db_storage = await db.get_db_storage_usage()
    metadata_info = await db.get_metadata_storage_info()
    total_uploaded_bytes = await db.get_total_uploaded_size()
    
    def format_bytes(bytes_val):
        if bytes_val < 1024:
            return f"{bytes_val} B"
        elif bytes_val < 1024 * 1024:
            return f"{bytes_val/1024:.2f} KB"
        elif bytes_val < 1024 * 1024 * 1024:
            return f"{bytes_val/(1024*1024):.2f} MB"
        else:
            return f"{bytes_val/(1024*1024*1024):.2f} GB"
    
    total_uploaded = format_bytes(total_uploaded_bytes)

    files = await db.get_all_files()
    total_access = sum(f[5] for f in files) if files else 0

    try:
        sent_msg = await update.message.reply_text(
            f"📊 *Bot Statistics*\n\n"
            f"🤖 Bot: @{bot_username}\n"
            f"⏱ Uptime: {uptime}\n\n"
            f"📁 *Files:* {file_count}\n"
            f"📦 *Total Uploaded:* {total_uploaded} (on Telegram)\n"
            f"👥 *Users:* {user_count}\n"
            f"📢 *Required Channels:* {channel_count}\n"
            f"👀 *Total Accesses:* {total_access}\n\n"
            f"💾 *PostgreSQL Storage (REAL):*\n"
            f"   ├─ Total DB: {db_storage['total']}\n"
            f"   ├─ Tables: {db_storage['tables']}\n"
            f"   └─ Indexes: {db_storage['indexes']}\n\n"
            f"📊 *Metadata Stats:*\n"
            f"   ├─ Cache Entries: {metadata_info['cache_entries']}\n"
            f"   └─ Est. Metadata: {metadata_info['estimated_metadata']}\n\n"
            f"⏰ Auto-delete: {DELETE_AFTER//60} minutes\n"
            f"🧹 Auto Cleanup: DISABLED (Permanent storage)\n"
            f"📅 Auto Backup: Every 3 days",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    except Exception as e:
        log.error(f"Error in stats command: {e}", exc_info=True)

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List files (admin only)"""
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
    """Delete file (admin only)"""
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
    """User stats (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    stats_data = await db.get_user_stats()

    msg = (
        f"📊 *User Statistics*\n\n"
        f"👥 Total Users: {stats_data['total_users']}\n"
        f"🟢 Active (7d): {stats_data['active_users_7d']}\n"
        f"🟡 Active (30d): {stats_data['active_users_30d']}\n"
        f"📈 New Today: {stats_data['new_users_today']}\n"
        f"📁 File Accessors: {stats_data['users_with_files']}\n"
    )

    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

# ============ ENHANCED BROADCAST FEATURE ============
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast to users (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args and not update.message.reply_to_message:
        sent_msg = await update.message.reply_text(
            "❌ Usage: /broadcast <message> or reply with /broadcast\n"
            "Optional: /broadcast --preview to see preview only"
        )
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
        preview_text = f"🔍 *BROADCAST PREVIEW*\n\n"
        preview_text += f"📝 *Message:*\n{message_text[:200]}{'...' if len(message_text) > 200 else ''}\n\n"
        preview_text += f"👥 *Total users:* {total_users}\n"
        preview_text += f"📦 *Chunks:* {(total_users + 999) // 1000} chunks of 1000\n\n"
        preview_text += f"*First 5 users:*\n"
        
        for i, uid in enumerate(user_ids[:5]):
            preview_text += f"{i+1}. `{uid}`\n"
        
        keyboard = [[
            InlineKeyboardButton("✅ Confirm Broadcast", callback_data=f"confirm_broadcast|{total_users}"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")
        ]]
        
        await status_msg.edit_text(
            preview_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        context.chat_data['broadcast_message'] = message_text
        return

    await status_msg.edit_text(f"🔄 Starting broadcast to {total_users} users...\n📦 Processing in chunks of 1000 users")
    asyncio.create_task(process_broadcast_chunks(context, user_ids, message_text, status_msg))

async def process_broadcast_chunks(context: ContextTypes.DEFAULT_TYPE, user_ids: list, message_text: str, status_msg):
    """Process broadcast in chunks of 1000 users"""
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
        chunk_failed = 0
        chunk_blocked = 0
        
        await status_msg.edit_text(
            f"📦 *Processing Chunk {chunk_num + 1}/{total_chunks}*\n"
            f"👥 Users: {len(chunk_users)}\n"
            f"✅ Sent: {successful}\n"
            f"❌ Failed: {failed}\n"
            f"🚫 Blocked: {blocked}",
            parse_mode="Markdown"
        )
        
        for i, user_id in enumerate(chunk_users):
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"📢 *Broadcast Message*\n\n{message_text}",
                    parse_mode="Markdown"
                )
                chunk_success += 1
                successful += 1
                
                if (i + 1) % 100 == 0:
                    await status_msg.edit_text(
                        f"📦 *Chunk {chunk_num + 1}/{total_chunks}* - {i + 1}/{len(chunk_users)} users\n"
                        f"✅ Sent: {successful}\n"
                        f"❌ Failed: {failed}\n"
                        f"🚫 Blocked: {blocked}",
                        parse_mode="Markdown"
                    )
                
                await asyncio.sleep(0.05)
                
            except Exception as e:
                error_str = str(e).lower()
                if "blocked" in error_str or "forbidden" in error_str or "deactivated" in error_str:
                    chunk_blocked += 1
                    blocked += 1
                else:
                    chunk_failed += 1
                    failed += 1
        
        if chunk_num < total_chunks - 1:
            await asyncio.sleep(2)
    
    elapsed_time = time.time() - start_time
    avg_speed = successful / elapsed_time if elapsed_time > 0 else 0
    
    summary = f"✅ *Broadcast Complete!*\n\n"
    summary += f"📊 *Final Statistics:*\n"
    summary += f"👥 Total Users: {total_users}\n"
    summary += f"✅ Successfully Sent: {successful}\n"
    summary += f"❌ Failed: {failed}\n"
    summary += f"🚫 Blocked/Deactivated: {blocked}\n"
    summary += f"⏱️ Time Taken: {elapsed_time:.1f} seconds\n"
    summary += f"⚡ Avg Speed: {avg_speed:.1f} users/sec\n\n"
    summary += f"📈 Success Rate: {(successful / total_users * 100):.1f}%"
    
    await status_msg.edit_text(summary, parse_mode="Markdown")
    log.info(f"Broadcast completed: {successful}/{total_users} successful, {failed} failed, {blocked} blocked")

async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle broadcast confirmation callbacks"""
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
                await query.edit_message_text("❌ Could not retrieve message. Please try again.")
                return
            
            await query.edit_message_text(
                f"🔄 Starting broadcast to {total_users} users...\n"
                f"📦 Processing in chunks of 1000 users"
            )
            
            user_ids = await db.get_all_user_ids(exclude_admin=True)
            
            asyncio.create_task(process_broadcast_chunks(
                context, user_ids, message_text, query.message
            ))
            
            context.chat_data.pop('broadcast_message', None)
            
        except Exception as e:
            log.error(f"Error in broadcast confirmation: {e}")
            await query.edit_message_text(f"❌ Error starting broadcast: {str(e)[:100]}")

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache (admin only)"""
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
    """Test channel access (admin only)"""
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
            status = f"✅ {member.status}"
        except Exception as e:
            status = f"❌ {str(e)[:50]}"
        
        results.append(f"{display_name}: {status}")

    result_text = "🔍 *Channel Access Test*\n\n" + "\n".join(results)
    
    sent_msg = await update.message.reply_text(result_text, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

# ============ BACKUP AND IMPORT COMMANDS ============

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual backup command"""
    if update.effective_user.id != ADMIN_ID:
        sent_msg = await update.message.reply_text("⛔ Admin only command")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    status_msg = await update.message.reply_text("🔄 Creating database backup...")
    
    try:
        backup_data = await export_database_backup(update=update, context=context, send_to_admin=True)
        
        await status_msg.edit_text(
            f"✅ Backup created!\n📦 Total size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB"
        )
        
        summary = f"✅ *Full Database Backup Complete*\n\n"
        summary += f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        summary += f"💾 Total size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n\n"
        summary += f"💡 To restore: Send all backup files (CSV + JSON) and use `/import`\n"
        summary += f"📌 Bot accepts both exact filenames and timestamped filenames"
        
        sent_msg = await update.message.reply_text(summary, parse_mode="Markdown")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        
    except Exception as e:
        log.error(f"Backup error: {e}")
        await status_msg.edit_text(f"❌ Backup failed: {str(e)[:200]}")

async def backup_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check backup status"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    channel_count = await db.get_channel_count()
    db_storage = await db.get_db_storage_usage()
    
    status_msg = f"""
📊 *Database Status*

📈 *Data Summary:*
• Files: {file_count}
• Users: {user_count}
• Channels: {channel_count}
• DB Size: {db_storage.get('total', 'Unknown')}

💾 *Backup Ready:* Yes
• Use `/backup` to create backup
• Use `/import` to restore from backup
• Auto-backup: Every 3 days

⚠️ *Remember:* Free tier PostgreSQL expires after 30 days!
• Run `/backup` regularly
• Save backup files to cloud storage
• Bot accepts both exact and timestamped filenames
"""
    
    sent_msg = await update.message.reply_text(status_msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def import_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check status of collected backup files for import"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    pending_files = context.user_data.get('pending_backup_files', {})
    if not pending_files:
        pending_files = context.user_data.get('pending_csv_files', {})
    
    if not pending_files:
        sent_msg = await update.message.reply_text(
            "📋 *No backup files collected*\n\n"
            "Send backup files (CSV + JSON) to start the import process.\n"
            "Required files: files.csv, users.csv, required_channels.csv\n"
            "Optional: metadata.json, membership_cache.csv, scheduled_deletions.csv\n\n"
            "💡 *Tip:* Forward backup files directly to bot and they'll be auto-collected\n"
            "📌 Bot accepts both exact filenames and timestamped filenames",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    status = f"📋 *Collected Backup Files* ({len(pending_files)})\n\n"
    
    csv_files = {k: v for k, v in pending_files.items() if k.endswith('.csv')}
    json_files = {k: v for k, v in pending_files.items() if k.endswith('.json')}
    
    if csv_files:
        status += f"📄 *CSV Files:*\n"
        for filename, content in csv_files.items():
            lines = len(content.splitlines()) - 1
            status += f"✅ {filename}: {lines} records\n"
    
    if json_files:
        status += f"\n📋 *JSON Files:*\n"
        for filename in json_files:
            status += f"✅ {filename}\n"
    
    # Check for required files
    required_patterns = ['files.csv', 'users.csv', 'required_channels.csv']
    found_required = []
    missing_required = []
    
    for required in required_patterns:
        found = False
        for filename in pending_files.keys():
            if required.replace('.csv', '') in filename.replace('.csv', '').lower():
                found_required.append(required)
                found = True
                break
        if not found:
            missing_required.append(required)
    
    if missing_required:
        status += f"\n⚠️ *Missing required files:* {', '.join(missing_required)}\n"
    else:
        status += f"\n✅ All required files collected!\n"
    
    status += f"\n💡 Use `/import` to restore all collected files"
    
    sent_msg = await update.message.reply_text(status, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def import_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Import database from backup files - FIXED VERSION"""
    if update.effective_user.id != ADMIN_ID:
        sent_msg = await update.message.reply_text("⛔ Admin only command")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    # Check for collected backup files
    collected_files = context.user_data.get('pending_backup_files', {})
    if not collected_files:
        collected_files = context.user_data.get('pending_csv_files', {})
    
    log.info(f"Import command - collected files: {list(collected_files.keys()) if collected_files else 'None'}")
    
    # Check if replying to a message
    if not update.message.reply_to_message and not collected_files:
        sent_msg = await update.message.reply_text(
            "📥 *Import Database from Backup*\n\n"
            "**Two ways to import:**\n\n"
            "1️⃣ *Forward backup files* directly to bot\n"
            "   • Bot will automatically collect them\n"
            "   • Supports CSV and JSON files\n"
            "   • Then use `/import` to restore\n\n"
            "2️⃣ *Reply to backup files* with `/import`\n"
            "   • Send all backup files (CSV + JSON)\n"
            "   • Reply to that message with `/import`\n\n"
            "**Required files:**\n"
            "• files.csv (or backup_*_files.csv)\n"
            "• users.csv (or backup_*_users.csv)\n"
            "• required_channels.csv (or backup_*_required_channels.csv)\n"
            "• metadata.json (recommended)\n"
            "• membership_cache.csv (optional)\n"
            "• scheduled_deletions.csv (optional)\n\n"
            "📌 Bot accepts both exact and timestamped filenames\n"
            "⚠️ **Warning:** This will replace ALL existing data!",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    # Collect backup files
    backup_files = {}
    
    if collected_files:
        backup_files = collected_files.copy()
        log.info(f"✅ Using {len(backup_files)} collected backup files: {list(backup_files.keys())}")
    
    if not backup_files and update.message.reply_to_message:
        replied_msg = update.message.reply_to_message
        
        if replied_msg.document:
            doc = replied_msg.document
            if doc.file_name and (doc.file_name.endswith('.csv') or doc.file_name.endswith('.json')):
                try:
                    file = await context.bot.get_file(doc.file_id)
                    content = await file.download_as_bytearray()
                    file_text = content.decode('utf-8')
                    backup_files[doc.file_name] = file_text
                    log.info(f"Found backup file in replied message: {doc.file_name}")
                except Exception as e:
                    log.error(f"Error downloading backup file: {e}")
    
    if not backup_files:
        sent_msg = await update.message.reply_text(
            "❌ No backup files found.\n\n"
            "Please send backup files first (CSV + JSON), then use `/import`\n\n"
            f"📋 Currently collected files: {list(collected_files.keys()) if collected_files else 'None'}\n"
            f"Required: files.csv (or backup_*_files.csv), users.csv, required_channels.csv\n\n"
            f"💡 Tip: Forward backup files directly to bot\n"
            f"📌 Bot accepts both exact and timestamped filenames"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    # Map timestamped filenames to required table names
    required_table_mapping = {
        'files.csv': ['files'],
        'users.csv': ['users'],
        'required_channels.csv': ['required_channels', 'required_channel'],
        'membership_cache.csv': ['membership_cache', 'membership'],
        'scheduled_deletions.csv': ['scheduled_deletions', 'scheduled_deletion'],
        'metadata.json': ['metadata']
    }
    
    normalized_files = {}
    missing_files = []
    
    for required_file, patterns in required_table_mapping.items():
        found = False
        for filename, content in backup_files.items():
            for pattern in patterns:
                # Match if pattern is in filename (case insensitive, ignoring special chars)
                clean_filename = filename.lower().replace('_', '').replace('-', '').replace(' ', '')
                if pattern.lower() in clean_filename:
                    normalized_files[required_file] = content
                    found = True
                    log.info(f"✅ Matched {filename} -> {required_file} (pattern: {pattern})")
                    break
            if found:
                break
        
        if not found:
            if required_file in ['files.csv', 'users.csv', 'required_channels.csv']:
                missing_files.append(required_file)
    
    if missing_files:
        found_files = list(backup_files.keys())
        sent_msg = await update.message.reply_text(
            f"❌ Missing required files: {', '.join(missing_files)}\n\n"
            f"📁 Files found: {', '.join(found_files) if found_files else 'None'}\n"
            f"📋 Required: files.csv, users.csv, required_channels.csv\n\n"
            f"💡 *Tip:* Make sure your backup includes all required CSV files.\n"
            f"📌 The bot supports both exact filenames (files.csv) and timestamped filenames (backup_*_files.csv)\n\n"
            f"🔍 *Debug:* Send files again or check filenames",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    backup_files = normalized_files
    log.info(f"✅ Normalized backup files: {list(backup_files.keys())}")
    
    # Confirm before proceeding
    confirm_keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ YES, Import Now", callback_data="confirm_import"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_import")
    ]])
    
    csv_files = {k: v for k, v in backup_files.items() if k.endswith('.csv')}
    json_files = {k: v for k, v in backup_files.items() if k.endswith('.json')}
    
    summary = f"📊 *Backup Files Ready for Import*\n\n"
    
    if csv_files:
        summary += f"📄 *CSV Files ({len(csv_files)}):*\n"
        for filename, content in csv_files.items():
            lines = len(content.splitlines()) - 1
            summary += f"• {filename}: {lines} records\n"
    
    if json_files:
        summary += f"\n📋 *JSON Files ({len(json_files)}):*\n"
        for filename in json_files:
            try:
                json_data = json.loads(backup_files[filename])
                if 'export_info' in json_data:
                    export_info = json_data['export_info']
                    tables = export_info.get('tables_exported', [])
                    summary += f"• {filename}: Metadata with {len(tables)} tables\n"
                else:
                    summary += f"• {filename}: JSON file\n"
            except:
                summary += f"• {filename}: JSON file\n"
    
    summary += f"\n⚠️ *WARNING:* This will REPLACE all existing data in your database!\n"
    summary += f"✅ Make sure this is the correct backup before proceeding.\n\n"
    summary += f"📋 *Import Order:*\n"
    summary += f"1. required_channels.csv\n"
    summary += f"2. users.csv\n"
    summary += f"3. files.csv\n"
    summary += f"4. membership_cache.csv\n"
    summary += f"5. scheduled_deletions.csv"
    
    sent_msg = await update.message.reply_text(
        summary,
        parse_mode="Markdown",
        reply_markup=confirm_keyboard
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    
    # Store normalized backup files for the callback
    context.chat_data['import_backup_files'] = backup_files
    log.info(f"Stored {len(backup_files)} normalized backup files in chat_data for import confirmation")

async def import_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle import confirmation - FIXED VERSION"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "cancel_import":
        await query.edit_message_text("❌ Import cancelled. No changes were made.")
        return
    
    if data == "confirm_import":
        # Get stored backup files
        backup_files = context.chat_data.get('import_backup_files', {})
        
        if not backup_files:
            backup_files = context.user_data.get('pending_backup_files', {})
        
        if not backup_files:
            log.error("No backup files found in chat_data or user_data")
            await query.edit_message_text("❌ No backup files found. Please try again by sending backup files first.")
            return
        
        log.info(f"Starting import with files: {list(backup_files.keys())}")
        
        await query.edit_message_text("🔄 Importing data... This may take a few moments...")
        
        try:
            # Filter only CSV files for database import
            csv_files = {k: v for k, v in backup_files.items() if k.endswith('.csv')}
            
            if not csv_files:
                await query.edit_message_text("❌ No CSV files found for import. Please check your backup files.")
                return
            
            log.info(f"Importing {len(csv_files)} CSV files: {list(csv_files.keys())}")
            
            # Perform the restore
            result = await restore_from_backup(csv_files)
            
            if result["success"]:
                # Clear collected files after successful import
                context.user_data.pop('pending_backup_files', None)
                context.user_data.pop('pending_csv_files', None)
                context.chat_data.pop('import_backup_files', None)
                context.chat_data.pop('import_csv_files', None)
                
                success_msg = f"✅ *Database Import Successful!*\n\n"
                success_msg += f"📊 *Import Summary:*\n"
                
                for table in result["tables_restored"]:
                    success_msg += f"• {table['table']}: {table['rows']} rows restored\n"
                
                success_msg += f"\n📦 *Total rows restored:* {result['total_rows']}\n"
                success_msg += f"🕐 *Completed at:* {result['timestamp']}\n\n"
                
                if 'metadata.json' in backup_files:
                    success_msg += f"📋 *Metadata:* JSON file was included in backup\n"
                
                success_msg += f"\n💡 *Next steps:*\n"
                success_msg += f"• Run `/stats` to verify data\n"
                success_msg += f"• Run `/listchannels` to check channels\n"
                success_msg += f"• Broadcast will work with all restored users! ✅\n\n"
                success_msg += f"⚠️ *Remember:* Your database will still expire. Run `/backup` regularly!\n"
                success_msg += f"📅 Auto-backup runs every 3 days"
                
                await query.edit_message_text(success_msg, parse_mode="Markdown")
                
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"🎉 Database restored from backup! {result['total_rows']} rows imported. All users restored for broadcasts!"
                )
                
            else:
                error_msg = f"❌ *Import Completed with Errors*\n\n"
                
                if result.get("errors"):
                    error_msg += f"⚠️ {len(result['errors'])} errors occurred:\n"
                    for error in result['errors'][:10]:
                        error_msg += f"• {error}\n"
                
                if result.get("tables_restored"):
                    error_msg += f"\n✅ Successfully restored tables:\n"
                    for table in result["tables_restored"]:
                        error_msg += f"• {table['table']}: {table['rows']} rows\n"
                
                await query.edit_message_text(error_msg, parse_mode="Markdown")
                
        except Exception as e:
            log.error(f"Import callback error: {e}", exc_info=True)
            await query.edit_message_text(f"❌ Import failed: {str(e)[:200]}\n\nPlease check logs for details.")

async def auto_backup_job(context: ContextTypes.DEFAULT_TYPE):
    """Automated backup job - runs every 3 days"""
    log.info("🔄 Running scheduled auto-backup (every 3 days)...")
    
    try:
        backup_data = await export_database_backup(update=None, context=context, send_to_admin=True)
        log.info(f"✅ Auto-backup completed. Size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB")
        
    except Exception as e:
        log.error(f"❌ Auto-backup failed: {e}")
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"⚠️ Auto-backup failed: {str(e)[:200]}\n\nPlease run manual backup with /backup"
            )
        except:
            pass

# ============ MAIN ============
async def initialize_bot():
    """Initialize bot application"""
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
        application.job_queue.run_repeating(
            cleanup_overdue_messages,
            interval=300,
            first=10
        )
        
        application.job_queue.run_repeating(
            auto_backup_job,
            interval=259200,  # 3 days
            first=3600  # Start after 1 hour
        )
        log.info("📅 Auto-backup scheduled (every 3 days)")

    application.add_error_handler(error_handler)
    
    # Add backup file handler (CSV + JSON) - MUST be before upload handler
    application.add_handler(
        MessageHandler(
            (filters.Document.FileExtension("csv") | filters.Document.FileExtension("json")) & filters.ChatType.PRIVATE,
            handle_forwarded_backup_files
        )
    )
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("listfiles", listfiles))
    application.add_handler(CommandHandler("deletefile", deletefile))
    application.add_handler(CommandHandler("users", users))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("clearcache", clearcache))
    application.add_handler(CommandHandler("testchannel", testchannel))
    
    # Channel management commands
    application.add_handler(CommandHandler("addchannel", addchannel))
    application.add_handler(CommandHandler("removechannel", removechannel))
    application.add_handler(CommandHandler("listchannels", listchannels))
    application.add_handler(CommandHandler("testchannels", testchannels))
    
    # Backup and import commands
    application.add_handler(CommandHandler("backup", backup_command))
    application.add_handler(CommandHandler("backup_status", backup_status))
    application.add_handler(CommandHandler("import", import_command))
    application.add_handler(CommandHandler("import_status", import_status))

    # Add callback handlers
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check_membership$"))
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check\\|"))
    application.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^(confirm_broadcast|cancel_broadcast)$"))
    application.add_handler(CallbackQueryHandler(import_callback, pattern="^(confirm_import|cancel_import)$"))

    # Add upload handler (admin only) - Make sure CSV and JSON files are excluded
    upload_filter = (filters.VIDEO | (filters.Document.ALL & ~filters.Document.FileExtension("csv") & ~filters.Document.FileExtension("json")))
    application.add_handler(
        MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE, upload)
    )

    # Set webhook
    render_url = os.environ.get('RENDER_EXTERNAL_URL')
    if not render_url:
        render_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'localhost')}"

    webhook_url = f"{render_url}/webhook"
    log.info(f"Setting webhook to: {webhook_url}")

    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        await application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
            max_connections=40
        )
        log.info("✅ Webhook set successfully")
    except Exception as e:
        log.error(f"Failed to set webhook: {e}", exc_info=True)
        return None

    bot_initialized = True
    
    log.info("🤖 Bot initialized and ready via webhook")
    log.info(f"📁 Files in database: {await db.get_file_count()}")
    log.info(f"👥 Users in database: {await db.get_user_count()}")
    log.info(f"📢 Required channels: {await db.get_channel_count()}")
    log.info(f"🧹 Auto cleanup: DISABLED (Permanent storage)")
    log.info(f"📅 Auto backup: Enabled (every 3 days)")
    log.info(f"📋 Backup file support: CSV + JSON (exact and timestamped filenames)")
    log.info(f"✅ Python Version: {sys.version}")

    return application

async def main_async():
    """Async main function"""
    global bot_app
    
    bot_app = await initialize_bot()
    
    if bot_app is None:
        log.error("Failed to initialize bot. Exiting.")
        return

    log.info("Bot is running. Waiting for webhook events...")
    
    while True:
        await asyncio.sleep(3600)

def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("🤖 TELEGRAM FILE BOT - COMPLETE VERSION")
    print("=" * 60)
    print(f"✅ Bot: @{bot_username}")
    print(f"✅ Admin: {ADMIN_ID}")
    print(f"✅ Database: Render PostgreSQL")
    print(f"✅ Auto Cleanup: DISABLED (Permanent storage)")
    print(f"✅ Storage: Metadata only (Files on Telegram)")
    print(f"✅ Backup: Enabled (manual + auto every 3 days)")
    print(f"✅ Import: Enabled (restore from CSV + JSON)")
    print(f"✅ File Handler: Auto-detect CSV and JSON backup files")
    print(f"✅ Filename Support: Exact and timestamped filenames")
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
        log.error(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        log.info("Shutting down...")
        if bot_app:
            asyncio.run(bot_app.shutdown())
        asyncio.run(db.close_pool())
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
