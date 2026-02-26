# database/database.py
import asyncpg
import logging
from typing import Optional
from database.database_config import DatabaseConfig

class Database:
    """
    Единый менеджер подключений к БД для всех служб.
    С отдельным пулом для embedder для избежания блокировок.
    """
    _pool: Optional[asyncpg.Pool] = None
    _embedder_pool: Optional[asyncpg.Pool] = None  # <-- ОТДЕЛЬНЫЙ ПУЛ ДЛЯ EMBEDDER
    _initialized = False
    
    @classmethod
    async def get_pool(cls) -> asyncpg.Pool:
        """
        Возвращает общий пул подключений для большинства служб.
        """
        if cls._pool is None:
            logging.info("Создание общего пула подключений к БД...")
            try:
                # Логируем параметры подключения (без пароля)
                logging.info(f"Параметры подключения к БД:")
                logging.info(f"  Хост: {DatabaseConfig.DB_HOST}")
                logging.info(f"  Порт: {DatabaseConfig.DB_PORT}")
                logging.info(f"  База данных: {DatabaseConfig.DB_NAME}")
                logging.info(f"  Пользователь: {DatabaseConfig.DB_USER}")
                logging.info(f"  SSL: require")
                logging.info(f"  Размер пула: min=2, max=8")
                
                cls._pool = await asyncpg.create_pool(
                    user=DatabaseConfig.DB_USER,
                    password=DatabaseConfig.DB_PASS,
                    database=DatabaseConfig.DB_NAME,
                    host=DatabaseConfig.DB_HOST,
                    port=DatabaseConfig.DB_PORT,
                    ssl='require',
                    min_size=2,
                    max_size=8,
                    max_inactive_connection_lifetime=60
                )
                
                # Проверяем подключение
                async with cls._pool.acquire() as test_conn:
                    db_version = await test_conn.fetchval("SELECT version();")
                    logging.info(f"✅ Общий пул подключений к БД создан успешно")
                    logging.info(f"   Версия БД: {db_version.split(',')[0]}")
                    
            except Exception as e:
                logging.critical(f"❌ Критическая ошибка создания пула БД: {e}")
                logging.critical(f"   Проверьте параметры подключения в database_config.py")
                logging.critical(f"   Хост: {DatabaseConfig.DB_HOST}:{DatabaseConfig.DB_PORT}")
                logging.critical(f"   База: {DatabaseConfig.DB_NAME}, Пользователь: {DatabaseConfig.DB_USER}")
                raise
        
        return cls._pool
    
    @classmethod
    async def get_embedder_pool(cls) -> asyncpg.Pool:
        """
        Возвращает отдельный пул подключений для службы embedder.
        Это предотвращает блокировки при длительных операциях с векторами.
        """
        if cls._embedder_pool is None:
            logging.info("Создание отдельного пула подключений для embedder...")
            try:
                # Логируем параметры подключения для embedder
                logging.info(f"Параметры подключения embedder:")
                logging.info(f"  Хост: {DatabaseConfig.DB_HOST}")
                logging.info(f"  Порт: {DatabaseConfig.DB_PORT}")
                logging.info(f"  База данных: {DatabaseConfig.DB_NAME}")
                logging.info(f"  Пользователь: {DatabaseConfig.DB_USER}")
                logging.info(f"  Размер пула: min=2, max=4")
                logging.info(f"  Таймаут команды: 300s")
                
                cls._embedder_pool = await asyncpg.create_pool(
                    user=DatabaseConfig.DB_USER,
                    password=DatabaseConfig.DB_PASS,
                    database=DatabaseConfig.DB_NAME,
                    host=DatabaseConfig.DB_HOST,
                    port=DatabaseConfig.DB_PORT,
                    ssl='require',
                    min_size=2,           # Минимальное количество соединений
                    max_size=4,           # Максимальное - меньше чем у основного пула
                    max_inactive_connection_lifetime=120,  # Больше время жизни
                    command_timeout=300,  # Увеличиваем таймаут для долгих операций
                    max_queries=50000     # Больше запросов в соединении
                )
                
                # Проверяем подключение для embedder
                async with cls._embedder_pool.acquire() as test_conn:
                    db_name = await test_conn.fetchval("SELECT current_database();")
                    logging.info(f"✅ Отдельный пул подключений для embedder создан успешно")
                    logging.info(f"   Подключено к БД: {db_name}")
                    
            except Exception as e:
                logging.critical(f"❌ Ошибка создания пула embedder БД: {e}")
                logging.warning("⚠️  Используем основной пул для embedder")
                # В случае ошибки возвращаем основной пул
                return await cls.get_pool()
        
        return cls._embedder_pool
    
    @classmethod
    async def initialize_database(cls):
        """
        Инициализация БД: создание таблиц и проверка структуры.
        Должна вызываться ОДИН раз при запуске приложения.
        """
        if cls._initialized:
            logging.debug("БД уже инициализирована, пропускаем инициализацию")
            return
            
        logging.info("🚀 Начинаем инициализацию базы данных...")
        
        try:
            pool = await cls.get_pool()
            logging.info("✅ Получен пул подключений для инициализации БД")
        except Exception as e:
            logging.critical(f"❌ Не удалось получить пул подключений для инициализации: {e}")
            raise

        async with pool.acquire() as conn:
            logging.info("🔍 Проверяем существование таблиц...")
            
            try:
                # Создаем основную таблицу, если не существует
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS telegram_posts (
                        id BIGSERIAL PRIMARY KEY,
                        post_time TIMESTAMP WITH TIME ZONE NOT NULL, 
                        text_content TEXT NOT NULL,
                        message_link TEXT,
                        finished BOOLEAN DEFAULT FALSE,
                        analyzed BOOLEAN DEFAULT FALSE,
                        filter_initial BOOLEAN,
                        filter_initial_explain TEXT,
                        context BOOLEAN,
                        context_score REAL,
                        context_explain TEXT,
                        essence BOOLEAN,
                        essence_score REAL,
                        essence_explain TEXT       
                    );
                """)
                logging.info("✅ Таблица 'telegram_posts' создана/проверена")
                
                # Проверяем наличие всех столбцов в основной таблице
                await cls._check_table_columns(conn, 'telegram_posts', cls._get_main_table_columns())
                
            except Exception as e:
                logging.error(f"❌ Ошибка при работе с таблицей 'telegram_posts': {e}")
                raise
                
            try:
                # Создаем таблицу для топовых сообщений с новой структурой
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS telegram_posts_top (
                        id BIGSERIAL PRIMARY KEY,
                        post_time TIMESTAMP WITH TIME ZONE NOT NULL, 
                        text_content TEXT NOT NULL,
                        message_link TEXT,
                        
                        tag1 TEXT,
                        tag2 TEXT,
                        tag3 TEXT,
                        tag4 TEXT,
                        tag5 TEXT,
                        
                        vector1 vector(768),
                        vector2 vector(768),
                        vector3 vector(768),
                        vector4 vector(768),
                        vector5 vector(768),
                        
                        taged BOOLEAN DEFAULT FALSE,
                        analyzed BOOLEAN DEFAULT FALSE,
                        coincide_24hr REAL,
                        essence REAL,
                        final_score REAL,
                        final BOOLEAN DEFAULT FALSE,
                        finished BOOLEAN DEFAULT FALSE,

                        tag1_score REAL,
                        tag2_score REAL,
                        tag3_score REAL,
                        tag4_score REAL,
                        tag5_score REAL,
                                   
                        text_short TEXT,
                        myth BOOLEAN DEFAULT FALSE,
                        myth_score REAL
                                   
                    );
                """)
                logging.info("✅ Таблица 'telegram_posts_top' создана/проверена")
                
                # Проверяем наличие всех столбцов в таблице топовых сообщений
                await cls._check_table_columns(conn, 'telegram_posts_top', cls._get_top_table_columns())
                
            except Exception as e:
                logging.error(f"❌ Ошибка при работе с таблицей 'telegram_posts_top': {e}")
                raise

            try:
                # Создаем таблицу для самых топовых сообщений (топ из топа)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS telegram_posts_top_top (
                        id BIGSERIAL PRIMARY KEY,
                        post_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        text_content TEXT NOT NULL,
                        text_short TEXT NOT NULL,
                        message_link TEXT,
                        finished BOOLEAN DEFAULT FALSE,
                        analyzed BOOLEAN DEFAULT FALSE,

                        total_score REAL,
                        news_final_score REAL,

                        comment_best TEXT,
                        author_best TEXT,
                        comment_score_best REAL,

                        comment_1 TEXT,
                        author_1 TEXT,
                        comment_score_1 REAL,
                                   
                        comment_2 TEXT,
                        author_2 TEXT,
                        comment_score_2 REAL,
                                   
                        comment_3 TEXT,
                        author_3 TEXT,
                        comment_score_3 REAL
                    );
                """)
                logging.info("✅ Таблица 'telegram_posts_top_top' создана/проверена")
                
                # Проверяем наличие всех столбцов в таблице самых топовых сообщений
                await cls._check_table_columns(conn, 'telegram_posts_top_top', cls._get_top_top_table_columns())
                
            except Exception as e:
                logging.error(f"❌ Ошибка при работе с таблицей 'telegram_posts_top_top': {e}")
                raise
            
        cls._initialized = True
        logging.info("🎉 Инициализация БД завершена успешно")
    
    @classmethod
    def _get_main_table_columns(cls):
        """Возвращает структуру столбцов для основной таблицы."""
        return {
            'id': 'BIGSERIAL PRIMARY KEY',
            'post_time': 'TIMESTAMP WITH TIME ZONE NOT NULL',
            'text_content': 'TEXT NOT NULL',
            'message_link': 'TEXT',
            'finished': 'BOOLEAN DEFAULT FALSE',
            'analyzed': 'BOOLEAN DEFAULT FALSE',
            'filter_initial': 'BOOLEAN',
            'filter_initial_explain': 'TEXT',
            'context': 'BOOLEAN',
            'context_score': 'REAL',
            'context_explain': 'TEXT',
            'essence': 'BOOLEAN',
            'essence_score': 'REAL',
            'essence_explain': 'TEXT'
        }
    
    @classmethod
    def _get_top_table_columns(cls):
        """Возвращает структуру столбцов для таблицы топовых сообщений."""
        return {
            'id': 'BIGSERIAL PRIMARY KEY',
            'post_time': 'TIMESTAMP WITH TIME ZONE NOT NULL',
            'text_content': 'TEXT NOT NULL',
            'message_link': 'TEXT',
            
            'tag1': 'TEXT',
            'tag2': 'TEXT',
            'tag3': 'TEXT',
            'tag4': 'TEXT',
            'tag5': 'TEXT',
            
            'vector1': 'vector(768)',
            'vector2': 'vector(768)',
            'vector3': 'vector(768)',
            'vector4': 'vector(768)',
            'vector5': 'vector(768)',
            
            'taged': 'BOOLEAN DEFAULT FALSE',
            'analyzed': 'BOOLEAN DEFAULT FALSE',
            'coincide_24hr': 'REAL',
            'essence': 'REAL',
            'final_score': 'REAL',
            'final': 'BOOLEAN DEFAULT FALSE',
            'finished': 'BOOLEAN DEFAULT FALSE',

            'tag1_score': 'REAL',
            'tag2_score': 'REAL',
            'tag3_score': 'REAL',
            'tag4_score': 'REAL',
            'tag5_score': 'REAL',

            'text_short': 'TEXT',
            'myth': 'BOOLEAN DEFAULT FALSE',
            'myth_score': 'REAL'
        }

    @classmethod
    def _get_top_top_table_columns(cls):
        """Возвращает структуру столбцов для таблицы самых топовых сообщений."""
        return {
            'id': 'BIGSERIAL PRIMARY KEY',
            'post_time': 'TIMESTAMP WITH TIME ZONE NOT NULL',
            'text_content': 'TEXT NOT NULL',
            'text_short': 'TEXT NOT NULL',
            'message_link': 'TEXT',
            'finished': 'BOOLEAN DEFAULT FALSE',
            'analyzed': 'BOOLEAN DEFAULT FALSE',

            'total_score': 'REAL',
            'news_final_score': 'REAL',

            'author_best': 'TEXT',
            'comment_best': 'TEXT',
            'comment_score_best': 'REAL',

            'comment_1': 'TEXT',
            'author_1': 'TEXT',
            'comment_score_1': 'REAL',
            'author_2': 'TEXT',
            'comment_2': 'TEXT',
            'comment_score_2': 'REAL',
            'comment_3': 'TEXT',
            'author_3': 'TEXT',
            'comment_score_3': 'REAL'
        }
    
    @classmethod
    async def _check_table_columns(cls, conn, table_name: str, required_columns: dict):
        """
        Проверяет наличие всех необходимых столбцов в указанной таблице.
        """
        logging.info(f"🔍 Проверяем структуру таблицы '{table_name}'...")
        
        try:
            # Получаем информацию о столбцах таблицы
            columns = await conn.fetch("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = $1
            """, table_name)
            
            existing_columns = {row['column_name'] for row in columns}
            missing_columns = set(required_columns.keys()) - existing_columns
            
            if missing_columns:
                logging.warning(f"⚠️  В таблице '{table_name}' отсутствуют столбцы: {missing_columns}")
                # АВТОМАТИЧЕСКИ ДОБАВЛЯЕМ отсутствующие столбцы
                await cls._add_missing_columns(conn, table_name, missing_columns, required_columns)
            else:
                logging.info(f"✅ Все необходимые столбцы присутствуют в таблице '{table_name}'")
                logging.debug(f"   Столбцы таблицы '{table_name}': {existing_columns}")
                
        except Exception as e:
            logging.error(f"❌ Ошибка при проверке структуры таблицы '{table_name}': {e}")
            raise
    
    @classmethod
    async def _add_missing_columns(cls, conn, table_name: str, missing_columns: set, column_definitions: dict):
        """
        Добавляет отсутствующие столбцы в указанную таблицу.
        """
        logging.info(f"🔧 Добавляем отсутствующие столбцы в таблицу '{table_name}'...")
        added_columns = []
        
        for column in missing_columns:
            if column in column_definitions:
                try:
                    # Для столбца 'id' не добавляем, так как он PRIMARY KEY
                    if column == 'id':
                        continue
                        
                    await conn.execute(f"""
                        ALTER TABLE {table_name} 
                        ADD COLUMN {column} {column_definitions[column]}
                    """)
                    added_columns.append(column)
                    logging.info(f"✅ Добавлен столбец '{column}' в таблицу '{table_name}'")
                except Exception as e:
                    logging.error(f"❌ Ошибка добавления столбца '{column}' в таблицу '{table_name}': {e}")
        
        if added_columns:
            logging.info(f"🎉 Успешно добавлены столбцы в таблицу '{table_name}': {added_columns}")
        else:
            logging.info(f"ℹ️  Не было добавлено новых столбцов в таблицу '{table_name}'")
    
    @classmethod
    async def test_connection(cls):
        """
        Тестирует подключение к БД и возвращает статус.
        """
        try:
            pool = await cls.get_pool()
            async with pool.acquire() as conn:
                # Выполняем простой запрос для проверки подключения
                result = await conn.fetchval("SELECT 1")
                if result == 1:
                    logging.info("✅ Тест подключения к БД: УСПЕХ")
                    return True
                else:
                    logging.error("❌ Тест подключения к БД: НЕИЗВЕСТНАЯ ОШИБКА")
                    return False
        except Exception as e:
            logging.critical(f"❌ Тест подключения к БД: ОШИБКА - {e}")
            return False
    
    @classmethod
    async def close(cls):
        """
        Закрывает все пулы подключений.
        Вызывается при завершении приложения.
        """
        logging.info("Завершение работы с БД...")
        
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            logging.info("✅ Общий пул подключений к БД закрыт")
            
        if cls._embedder_pool:
            await cls._embedder_pool.close()
            cls._embedder_pool = None
            logging.info("✅ Пул подключений embedder закрыт")
            
        cls._initialized = False
        logging.info("✅ Все подключения к БД закрыты")