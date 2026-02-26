#!/usr/bin/env python3
# stats.py
"""
Отдельная служба статистики для Telegram бота.
Запускается как самостоятельный процесс.
"""

import asyncio
import logging
import os
import json
import signal
import sys
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from collections import Counter
from pathlib import Path

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Импортируем существующие модули БД
from database.database import Database
from database.database_config import DatabaseConfig

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_env_file():
    """Загружает переменные из .env файла."""
    env_file = Path('.env')
    if not env_file.exists():
        return
    
    logger.info("Загрузка переменных из .env файла")
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            if '=' in line:
                key, value = line.split('=', 1)
                # Убираем кавычки если есть
                value = value.strip('"\'')
                os.environ[key.strip()] = value
                logger.debug(f"Загружена переменная: {key.strip()}")

# Загружаем .env при старте
load_env_file()


class Config:
    """Конфигурация для службы статистики."""
    # Настройки Telegram Bot API
    STATS_BOT_API_KEY = os.getenv('STATS_BOT_API_KEY')
    
    @classmethod
    def validate(cls):
        """Проверяет обязательные параметры конфигурации."""
        if not cls.STATS_BOT_API_KEY:
            raise ValueError(
                "STATS_BOT_API_KEY не установлен. "
                "Проверьте наличие переменной в .env файле"
            )


class StatsBot:
    """Telegram бот для статистики."""
    
    def __init__(self):
        self.application: Optional[Application] = None
        self.is_running = False
        self.db_pool = None
    
    async def initialize(self):
        """Инициализирует бота и подключается к БД."""
        logger.info("Инициализация бота статистики...")
        
        # Подключаемся к БД через существующий класс Database
        try:
            self.db_pool = await Database.get_pool()
            logger.info("Подключение к БД успешно установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise
        
        # Создаем приложение бота
        self.application = Application.builder().token(Config.STATS_BOT_API_KEY).build()
        
        # Регистрация обработчиков команд
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_help))
        self.application.add_handler(CommandHandler("input", self.cmd_input))
        self.application.add_handler(CommandHandler("editor", self.cmd_editor))
        self.application.add_handler(CommandHandler("longterm", self.cmd_longterm))
        self.application.add_handler(CommandHandler("authors", self.cmd_authors))
        
        logger.info("Бот статистики инициализирован")
    
    def _get_commands_text(self) -> str:
        """Возвращает текст со списком команд в виде гиперссылок."""
        return (
            "\n\n---\n"
            "/input \n"
            "/editor \n"
            "/longterm \n"
            "/authors \n"
            "/help"
        )
    
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start."""
        welcome_text = (
            "Бот статистики системы мониторинга\n"
            "\n"
            "/input - статистика поступления сообщений\n"
            "/editor - количество записей в редакторе\n"
            "/longterm - долгосрочная статистика тем и настроений\n"
            "/authors - статистика авторов\n"
            "/help - справка"
        ) + self._get_commands_text()
        
        await update.message.reply_text(welcome_text)
    
    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help."""
        help_text = (
            "/input - статистика поступления сообщений за 24 часа\n"
            "  Показывает количество записей в таблицах:\n"
            "  telegram_posts, telegram_posts_top, telegram_posts_top_top\n"
            "\n"
            "/editor - количество записей в таблице editor\n"
            "\n"
            "/longterm - долгосрочная статистика тем и настроений\n"
            "  Данные из таблицы state (lt-topic, lt-mood) в формате JSON[]\n"
            "\n"
            "/authors - статистика авторов\n"
            "  Анализ последних 50 записей таблицы published"
        ) + self._get_commands_text()
        
        await update.message.reply_text(help_text)
    
    async def cmd_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /input - статистика поступления сообщений."""
        try:
            # Текущее время UTC
            now_utc = datetime.utcnow()
            day_ago_utc = now_utc - timedelta(days=1)
            
            async with self.db_pool.acquire() as conn:
                # telegram_posts за последние 24 часа
                posts_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE post_time >= $1",
                    day_ago_utc
                ) or 0
                
                # telegram_posts_top за последние 24 часа
                posts_top_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts_top WHERE post_time >= $1",
                    day_ago_utc
                ) or 0
                
                # telegram_posts_top_top за последние 24 часа
                posts_top_top_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts_top_top WHERE post_time >= $1",
                    day_ago_utc
                ) or 0
            
            response = (
                f"/input\n"
                f"{posts_count} > {posts_top_count} > {posts_top_top_count}\n"
                f"\n"
                f"telegram_posts: {posts_count}\n"
                f"telegram_posts_top: {posts_top_count}\n"
                f"telegram_posts_top_top: {posts_top_top_count}"
            ) + self._get_commands_text()
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке /input: {e}")
            await update.message.reply_text(f"Ошибка при получении статистики: {e}")
    
    async def cmd_editor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /editor - количество записей в таблице editor."""
        try:
            async with self.db_pool.acquire() as conn:
                # Проверяем существование таблицы editor
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'editor'
                    )
                """)
                
                if not table_exists:
                    await update.message.reply_text("Таблица editor не существует")
                    return
                
                count = await conn.fetchval("SELECT COUNT(*) FROM editor") or 0
            
            response = f"/editor\n{count}\n\nЗаписей в редакторе: {count}" + self._get_commands_text()
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке /editor: {e}")
            await update.message.reply_text(f"Ошибка при получении статистики: {e}")
    
    async def cmd_longterm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /longterm - статистика тем и настроений."""
        try:
            async with self.db_pool.acquire() as conn:
                # Проверяем существование таблицы state
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'state'
                    )
                """)
                
                if not table_exists:
                    await update.message.reply_text("Таблица state не существует")
                    return
                
                # Получаем данные из таблицы state
                row = await conn.fetchrow('SELECT "lt-topic", "lt-mood" FROM state LIMIT 1')
            
            if not row:
                await update.message.reply_text("Нет данных в таблице state")
                return
            
            # Парсим темы и настроения
            topics_text = self._parse_json_strings_array(row['lt-topic'], "ТЕМЫ")
            moods_text = self._parse_json_strings_array(row['lt-mood'], "НАСТРОЕНИЕ", shorten=True)
            
            response = f"/longterm\n\n{topics_text}\n\n{moods_text}" + self._get_commands_text()
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке /longterm: {e}")
            await update.message.reply_text(f"Ошибка при получении статистики: {e}")
    
    def _parse_json_strings_array(self, data: Any, title: str, shorten: bool = False) -> str:
        """
        Парсит массив строк JSON.
        Каждый элемент - строка вида '{"topic": "...", "weight": 0.xx}'
        Если shorten=True - обрезает текст в скобках для настроений
        """
        try:
            items = []
            
            if isinstance(data, list):
                logger.info(f"Получен список из {len(data)} строк JSON")
                
                for i, json_str in enumerate(data):
                    try:
                        # Парсим каждую строку как JSON
                        if isinstance(json_str, str):
                            obj = json.loads(json_str)
                            logger.debug(f"Элемент {i} распарсен: {obj}")
                            
                            if isinstance(obj, dict):
                                if 'topic' in obj:
                                    items.append((obj['topic'], obj['weight'] * 100))
                                elif 'mood' in obj:
                                    # Для настроений убираем текст в скобках если нужно
                                    mood_text = obj['mood']
                                    if shorten:
                                        # Убираем все что в скобках вместе со скобками
                                        mood_text = re.sub(r'\s*\([^)]*\)', '', mood_text)
                                    items.append((mood_text, obj['weight'] * 100))
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка парсинга JSON строки {i}: {e}")
                        continue
            
            if items:
                # Сортируем по убыванию веса
                items.sort(key=lambda x: x[1], reverse=True)
                
                result = [f"{title}"]
                for name, weight in items:
                    result.append(f"{name} = {weight:.0f}%")
                return "\n".join(result)
            else:
                return f"{title}\nНет данных"
                
        except Exception as e:
            logger.error(f"Ошибка в _parse_json_strings_array для {title}: {e}")
            return f"{title}\nОшибка парсинга данных"
        
    async def cmd_authors(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /authors - статистика авторов из таблицы published."""
        try:
            async with self.db_pool.acquire() as conn:
                # Проверяем существование таблицы published
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'published'
                    )
                """)
                
                if not table_exists:
                    await update.message.reply_text("Таблица published не существует")
                    return
                
                # Получаем последние 50 записей с авторами из таблицы published
                rows = await conn.fetch("""
                    SELECT author FROM published 
                    WHERE author IS NOT NULL AND author != '' 
                    ORDER BY id DESC LIMIT 50
                """)
            
            if not rows:
                await update.message.reply_text("Нет данных об авторах в таблице published")
                return
            
            # Считаем частоту авторов
            authors = [row['author'] for row in rows if row['author']]
            author_counts = Counter(authors)
            total = len(authors)
            unique_authors = len(author_counts)
            
            if total == 0:
                await update.message.reply_text("Нет данных об авторах")
                return
            
            # Формируем статистику
            sorted_authors = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)
            
            response_lines = ["/authors"]
            response_lines.append(f"Всего авторов: {unique_authors} (из {total} записей)")
            response_lines.append("")  # Пустая строка для разделения
            
            for author, count in sorted_authors:
                percentage = (count / total) * 100
                response_lines.append(f"{author.upper()} = {percentage:.0f}%")
            
            response = "\n".join(response_lines) + self._get_commands_text()
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке /authors: {e}")
            await update.message.reply_text(f"Ошибка при получении статистики: {e}")
    
    async def start(self):
        """Запускает бота."""
        if not self.application:
            await self.initialize()
        
        try:
            logger.info("Запуск бота статистики...")
            self.is_running = True
            
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            logger.info("Бот статистики запущен и готов к работе")
            
        except Exception as e:
            logger.error(f"Ошибка при запуске бота: {e}")
            raise
    
    async def stop(self):
        """Останавливает бота."""
        self.is_running = False
        if self.application:
            try:
                if self.application.updater:
                    await self.application.updater.stop()
                await self.application.stop()
                await self.application.shutdown()
                logger.info("Бот статистики остановлен")
            except Exception as e:
                logger.error(f"Ошибка при остановке бота: {e}")
    
    async def run_forever(self):
        """Держит бота в рабочем состоянии."""
        try:
            while self.is_running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Получен сигнал остановки")
        finally:
            await self.stop()


class StatsService:
    """Основной сервис статистики."""
    
    def __init__(self):
        self.config = Config
        self.bot: Optional[StatsBot] = None
        self.is_running = False
        self.tasks = []
        
        # Проверяем конфигурацию
        try:
            self.config.validate()
            logger.info("Конфигурация загружена успешно")
        except ValueError as e:
            logger.error(f"Ошибка конфигурации: {e}")
            sys.exit(1)
        
        # Настройка обработчиков сигналов
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов остановки."""
        logger.info(f"Получен сигнал {signum}")
        self.is_running = False
    
    async def initialize(self):
        """Инициализирует сервис."""
        logger.info("Инициализация сервиса статистики...")
        
        # Создаем и инициализируем бота
        self.bot = StatsBot()
        await self.bot.initialize()
        
        logger.info("Сервис статистики инициализирован")
    
    async def start(self):
        """Запускает сервис."""
        logger.info("Запуск сервиса статистики...")
        
        # Инициализация
        await self.initialize()
        
        # Запуск бота
        await self.bot.start()
        self.is_running = True
        
        # Запускаем задачу для поддержания работы
        task = asyncio.create_task(self.bot.run_forever())
        self.tasks.append(task)
        
        logger.info("Сервис статистики запущен")
    
    async def stop(self):
        """Останавливает сервис."""
        logger.info("Остановка сервиса статистики...")
        self.is_running = False
        
        # Останавливаем бота
        if self.bot:
            await self.bot.stop()
        
        # Отменяем все задачи
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Закрываем соединения с БД
        await Database.close()
        
        logger.info("Сервис статистики остановлен")
    
    async def run(self):
        """Основной метод запуска."""
        try:
            await self.start()
            
            # Держим сервис активным
            while self.is_running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Ошибка в работе сервиса: {e}")
        finally:
            await self.stop()


async def main():
    """Точка входа для службы статистики."""
    service = StatsService()
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Сервис остановлен пользователем")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")
        sys.exit(1)