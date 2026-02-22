"""
PostgreSQL Database - conversations table
Структура: id, username, role, content, created_at, display_name, answer_id, message_timestamp
Автоматично створює базу даних та таблиці при першому запуску.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)


def _ensure_database_exists():
    """Перевірити та створити базу даних якщо не існує."""
    db_name = os.getenv('DB_NAME', 'inst_ai_manager')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'danil')
    db_password = os.getenv('DB_PASSWORD', 'danilus15')

    try:
        # Підключаємось до системної бази postgres
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname='postgres',
            user=db_user,
            password=db_password
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            # Перевіряємо чи існує наша база
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cur.fetchone()

            if not exists:
                cur.execute(f'CREATE DATABASE "{db_name}"')
                logger.info(f"Базу даних '{db_name}' створено")
            else:
                logger.debug(f"База даних '{db_name}' вже існує")

        conn.close()

    except Exception as e:
        logger.error(f"Помилка перевірки/створення бази даних: {e}")
        raise


class Database:
    def __init__(self):
        self.conn = None
        _ensure_database_exists()
        self.connect()
        self.create_tables()

    def connect(self):
        """Підключення до PostgreSQL."""
        try:
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                dbname=os.getenv('DB_NAME', 'inst_ai_manager'),
                user=os.getenv('DB_USER', 'danil'),
                password=os.getenv('DB_PASSWORD', 'danilus15')
            )
            self.conn.autocommit = True
            logger.info("Підключено до PostgreSQL")
        except Exception as e:
            logger.error(f"Помилка підключення до DB: {e}")
            raise

    def create_tables(self):
        """Створення таблиць."""
        with self.conn.cursor() as cur:
            # Conversations - одна таблиця для всіх повідомлень (user + assistant)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) NOT NULL,
                    role VARCHAR(50) NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    display_name VARCHAR(255),
                    answer_id INTEGER REFERENCES conversations(id),
                    message_timestamp TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_conversations_username ON conversations(username);
                CREATE INDEX IF NOT EXISTS idx_conversations_created_at ON conversations(created_at);
            """)

            # Products - товари з бази знань
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    category VARCHAR(100),
                    description TEXT,
                    price DECIMAL(10,2),
                    sizes TEXT,
                    colors TEXT,
                    material TEXT,
                    in_stock BOOLEAN DEFAULT TRUE,
                    image_url TEXT,
                    related_products TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Orders - замовлення
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) NOT NULL,
                    display_name VARCHAR(255),
                    full_name VARCHAR(255),
                    phone VARCHAR(50),
                    city VARCHAR(100),
                    nova_poshta VARCHAR(255),
                    products TEXT,
                    total_price DECIMAL(10,2),
                    status VARCHAR(50) DEFAULT 'new',
                    ttn VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Leads - потенційні клієнти
            cur.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) NOT NULL UNIQUE,
                    display_name VARCHAR(255),
                    phone VARCHAR(50),
                    email VARCHAR(255),
                    city VARCHAR(100),
                    delivery_address TEXT,
                    interested_products TEXT,
                    source VARCHAR(100) DEFAULT 'instagram_dm',
                    status VARCHAR(50) DEFAULT 'new',
                    notes TEXT,
                    first_contact TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_contact TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    messages_count INTEGER DEFAULT 1
                );

                CREATE INDEX IF NOT EXISTS idx_leads_username ON leads(username);
                CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
            """)

            # Міграція: додаємо delivery_address якщо не існує
            cur.execute("""
                ALTER TABLE leads ADD COLUMN IF NOT EXISTS delivery_address TEXT;
            """)

            logger.info("Таблиці створено/перевірено")

    # ==================== CONVERSATIONS ====================

    def add_message(self, username: str, role: str, content: str,
                    display_name: str = None, answer_id: int = None,
                    message_timestamp: datetime = None) -> int:
        """
        Додати повідомлення в conversations.
        role: 'user' або 'assistant'
        Повертає ID створеного запису.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO conversations (username, role, content, display_name, answer_id, message_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (username, role, content, display_name, answer_id, message_timestamp))
            msg_id = cur.fetchone()[0]
            return msg_id

    def add_user_message(self, username: str, content: str,
                         display_name: str = None,
                         message_timestamp: datetime = None) -> int:
        """Додати повідомлення від користувача."""
        return self.add_message(
            username=username,
            role='user',
            content=content,
            display_name=display_name,
            message_timestamp=message_timestamp
        )

    def add_assistant_message(self, username: str, content: str,
                              display_name: str = None,
                              answer_id: int = None) -> int:
        """Додати відповідь асистента."""
        return self.add_message(
            username=username,
            role='assistant',
            content=content,
            display_name=display_name,
            answer_id=answer_id
        )

    def get_conversation_history(self, username: str, limit: int = 20) -> list:
        """Отримати історію розмови з користувачем (останні N повідомлень)."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, username, role, content, created_at, display_name, answer_id, message_timestamp
                FROM conversations
                WHERE username = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (username, limit))
            messages = cur.fetchall()
            # Повертаємо в хронологічному порядку
            return list(reversed(messages))

    def get_user_display_name(self, username: str) -> str:
        """Отримати збережений display_name для username з БД (останній непорожній)."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT display_name FROM conversations
                    WHERE username = %s AND display_name IS NOT NULL AND display_name != ''
                    ORDER BY created_at DESC LIMIT 1
                """, (username,))
                row = cur.fetchone()
                return row[0] if row else None
        except Exception:
            return None

    def is_bot_message_in_db(self, username: str, text: str) -> bool:
        """Перевірити чи є таке повідомлення бота (assistant) в БД для даного username.
        Перевіряємо substring-пошуком: Instagram може відображати один DB-запис як декілька bubble,
        тому текст з екрану може бути лише частиною повного збереженого тексту."""
        # Ескейпимо спецсимволи LIKE: % і _
        escaped = text.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM conversations
                WHERE username = %s AND role = 'assistant' AND content LIKE %s ESCAPE '\\'
                LIMIT 1
            """, (username, f'%{escaped}%'))
            return cur.fetchone() is not None

    def get_last_user_message_id(self, username: str) -> int:
        """Отримати ID останнього повідомлення користувача."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM conversations
                WHERE username = %s AND role = 'user'
                ORDER BY created_at DESC
                LIMIT 1
            """, (username,))
            result = cur.fetchone()
            return result[0] if result else None

    def update_answer_id(self, user_message_id: int, assistant_message_id: int):
        """Оновити answer_id для повідомлення користувача (зв'язок з відповіддю)."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE conversations
                SET answer_id = %s
                WHERE id = %s
            """, (assistant_message_id, user_message_id))

    def is_message_processed(self, username: str, message_timestamp: datetime) -> bool:
        """Перевірка чи повідомлення вже оброблено (за timestamp)."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM conversations
                WHERE username = %s AND message_timestamp = %s AND role = 'user'
                LIMIT 1
            """, (username, message_timestamp))
            return cur.fetchone() is not None

    # ==================== PRODUCTS ====================

    def get_product_by_name(self, name: str) -> dict:
        """Пошук товару за назвою (часткове співпадіння)."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM products
                WHERE LOWER(name) LIKE LOWER(%s) AND in_stock = TRUE
                LIMIT 1
            """, (f'%{name}%',))
            return cur.fetchone()

    def get_products_by_category(self, category: str) -> list:
        """Отримати товари за категорією."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM products
                WHERE LOWER(category) LIKE LOWER(%s) AND in_stock = TRUE
                ORDER BY name
            """, (f'%{category}%',))
            return cur.fetchall()

    def search_products(self, query: str) -> list:
        """Пошук товарів за запитом (назва, опис, категорія)."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM products
                WHERE (LOWER(name) LIKE LOWER(%s)
                    OR LOWER(description) LIKE LOWER(%s)
                    OR LOWER(category) LIKE LOWER(%s))
                AND in_stock = TRUE
                ORDER BY name
                LIMIT 10
            """, (f'%{query}%', f'%{query}%', f'%{query}%'))
            return cur.fetchall()

    def add_product(self, name: str, category: str, price: float,
                    description: str = None, sizes: str = None,
                    colors: str = None, material: str = None,
                    image_url: str = None, related_products: str = None) -> int:
        """Додати новий товар."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO products (name, category, description, price, sizes, colors, material, image_url, related_products)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (name, category, description, price, sizes, colors, material, image_url, related_products))
            return cur.fetchone()[0]

    # ==================== ORDERS ====================

    def create_order(self, username: str, display_name: str = None,
                     full_name: str = None, phone: str = None,
                     city: str = None, nova_poshta: str = None,
                     products: str = None, total_price: float = None) -> int:
        """Створити нове замовлення."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (username, display_name, full_name, phone, city, nova_poshta, products, total_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (username, display_name, full_name, phone, city, nova_poshta, products, total_price))
            return cur.fetchone()[0]

    def update_order_status(self, order_id: int, status: str, ttn: str = None):
        """Оновити статус замовлення."""
        with self.conn.cursor() as cur:
            if ttn:
                cur.execute("""
                    UPDATE orders SET status = %s, ttn = %s WHERE id = %s
                """, (status, ttn, order_id))
            else:
                cur.execute("""
                    UPDATE orders SET status = %s WHERE id = %s
                """, (status, order_id))

    def get_user_orders(self, username: str) -> list:
        """Отримати замовлення користувача."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM orders
                WHERE username = %s
                ORDER BY created_at DESC
            """, (username,))
            return cur.fetchall()

    # ==================== LEADS ====================

    def create_or_update_lead(self, username: str, display_name: str = None,
                               phone: str = None, email: str = None,
                               city: str = None, delivery_address: str = None,
                               interested_products: str = None,
                               notes: str = None) -> int:
        """
        Створити або оновити ліда.
        Викликається ТІЛЬКИ при підтвердженні замовлення ([ORDER] блок).
        delivery_address: "ПІБ, місто, відділення НП"
        interested_products: підтверджені замовлені товари
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO leads (username, display_name, phone, email, city,
                                   delivery_address, interested_products, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (username) DO UPDATE SET
                    display_name = COALESCE(EXCLUDED.display_name, leads.display_name),
                    phone = COALESCE(EXCLUDED.phone, leads.phone),
                    email = COALESCE(EXCLUDED.email, leads.email),
                    city = COALESCE(EXCLUDED.city, leads.city),
                    delivery_address = COALESCE(EXCLUDED.delivery_address, leads.delivery_address),
                    interested_products = COALESCE(EXCLUDED.interested_products, leads.interested_products),
                    notes = COALESCE(EXCLUDED.notes, leads.notes),
                    last_contact = CURRENT_TIMESTAMP,
                    messages_count = leads.messages_count + 1
                RETURNING id
            """, (username, display_name, phone, email, city,
                  delivery_address, interested_products, notes))
            return cur.fetchone()[0]

    def get_lead(self, username: str) -> dict:
        """Отримати ліда за username."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM leads WHERE username = %s", (username,))
            return cur.fetchone()

    def update_lead_status(self, username: str, status: str):
        """Оновити статус ліда (new, contacted, qualified, converted, lost)."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE leads SET status = %s, last_contact = CURRENT_TIMESTAMP
                WHERE username = %s
            """, (status, username))

    def update_lead_phone(self, username: str, phone: str):
        """Оновити телефон ліда."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE leads SET phone = %s, last_contact = CURRENT_TIMESTAMP
                WHERE username = %s
            """, (phone, username))

    def get_all_leads(self, status: str = None, limit: int = 100) -> list:
        """Отримати всіх лідів (з фільтром по статусу)."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            if status:
                cur.execute("""
                    SELECT * FROM leads WHERE status = %s
                    ORDER BY last_contact DESC LIMIT %s
                """, (status, limit))
            else:
                cur.execute("""
                    SELECT * FROM leads ORDER BY last_contact DESC LIMIT %s
                """, (limit,))
            return cur.fetchall()

    def close(self):
        """Закрити з'єднання."""
        if self.conn:
            self.conn.close()
            logger.info("З'єднання з DB закрито")


def main():
    """Запуск створення/оновлення таблиць."""
    print("=" * 60)
    print("  НАЛАШТУВАННЯ БАЗИ ДАНИХ - inst_ai_manager")
    print("=" * 60)

    try:
        db = Database()
        print("\n[OK] Підключено до PostgreSQL")
        print("[OK] Таблиці створено/оновлено:")
        print("     - conversations (повідомлення user/assistant)")
        print("     - products (товари)")
        print("     - orders (замовлення)")
        print("     - leads (потенційні клієнти)")

        # Показуємо структуру таблиць
        with db.conn.cursor() as cur:
            # Conversations
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'conversations'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()

            print("\n" + "-" * 60)
            print("  Структура таблиці CONVERSATIONS:")
            print("-" * 60)
            for col in columns:
                nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                print(f"  {col[0]:<20} {col[1]:<20} {nullable}")

            # Products
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'products'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()

            print("\n" + "-" * 60)
            print("  Структура таблиці PRODUCTS:")
            print("-" * 60)
            for col in columns:
                nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                print(f"  {col[0]:<20} {col[1]:<20} {nullable}")

            # Orders
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'orders'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()

            print("\n" + "-" * 60)
            print("  Структура таблиці ORDERS:")
            print("-" * 60)
            for col in columns:
                nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                print(f"  {col[0]:<20} {col[1]:<20} {nullable}")

            # Leads
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'leads'
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()

            print("\n" + "-" * 60)
            print("  Структура таблиці LEADS:")
            print("-" * 60)
            for col in columns:
                nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                print(f"  {col[0]:<20} {col[1]:<20} {nullable}")

            # Кількість записів
            print("\n" + "-" * 60)
            print("  Статистика:")
            print("-" * 60)

            cur.execute("SELECT COUNT(*) FROM conversations")
            count = cur.fetchone()[0]
            print(f"  conversations: {count} записів")

            cur.execute("SELECT COUNT(*) FROM products")
            count = cur.fetchone()[0]
            print(f"  products: {count} записів")

            cur.execute("SELECT COUNT(*) FROM orders")
            count = cur.fetchone()[0]
            print(f"  orders: {count} записів")

            cur.execute("SELECT COUNT(*) FROM leads")
            count = cur.fetchone()[0]
            print(f"  leads: {count} записів")

        db.close()

        print("\n" + "=" * 60)
        print("  БАЗА ДАНИХ ГОТОВА!")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ПОМИЛКА] {e}")
        import traceback
        traceback.print_exc()
        print("\nПеревірте:")
        print("  1. PostgreSQL запущено")
        print("  2. .env файл з правильними credentials")
        print("  3. Користувач має права на створення баз даних")


if __name__ == '__main__':
    main()
