"""
PostgreSQL Database - conversations table
Struktura jak na skrinshoti: id, username, role, content, created_at, display_name, answer_id, message_timestamp
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.conn = None
        self.connect()
        self.create_tables()

    def connect(self):
        """Pidkliuchennia do PostgreSQL."""
        try:
            self.conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                dbname=os.getenv('DB_NAME', 'inst_ai_manager'),
                user=os.getenv('DB_USER', 'danil'),
                password=os.getenv('DB_PASSWORD', 'danilus15')
            )
            self.conn.autocommit = True
            logger.info("Pidkliucheno do PostgreSQL")
        except Exception as e:
            logger.error(f"Pomylka pidkliuchennia do DB: {e}")
            raise

    def create_tables(self):
        """Stvorennia tablyts (conversations - jak na skrinshoti)."""
        with self.conn.cursor() as cur:
            # Conversations - odna tablytsia dlia vsikh povidomlen (user + assistant)
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

            # Products - tovary z bazy znan
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

            # Orders - zamovlennia
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

            # Leads - potentsiini klienty
            cur.execute("""
                CREATE TABLE IF NOT EXISTS leads (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) NOT NULL UNIQUE,
                    display_name VARCHAR(255),
                    phone VARCHAR(50),
                    email VARCHAR(255),
                    city VARCHAR(100),
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

            logger.info("Tablytsi stvoreno/perevireno")

    # ==================== CONVERSATIONS ====================

    def add_message(self, username: str, role: str, content: str,
                    display_name: str = None, answer_id: int = None,
                    message_timestamp: datetime = None) -> int:
        """
        Dodaty povidomlennia v conversations.
        role: 'user' abo 'assistant'
        Povertaie ID stvorenoho zapysu.
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
        """Dodaty povidomlennia vid korystuvacha."""
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
        """Dodaty vidpovid assistenta."""
        return self.add_message(
            username=username,
            role='assistant',
            content=content,
            display_name=display_name,
            answer_id=answer_id
        )

    def get_conversation_history(self, username: str, limit: int = 20) -> list:
        """Otrymaty istoriiu rozmovy z korystuvachem (ostanni N povidomlen)."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, username, role, content, created_at, display_name, answer_id, message_timestamp
                FROM conversations
                WHERE username = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (username, limit))
            messages = cur.fetchall()
            # Povertajemo v khronolohichnomu poriadku
            return list(reversed(messages))

    def get_last_user_message_id(self, username: str) -> int:
        """Otrymaty ID ostannoho povidomlennia korystuvacha."""
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
        """Onovyty answer_id dlia povidomlennia korystuvacha (zviazok z vidpoviddiu)."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE conversations
                SET answer_id = %s
                WHERE id = %s
            """, (assistant_message_id, user_message_id))

    def is_message_processed(self, username: str, message_timestamp: datetime) -> bool:
        """Perevirka chy povidomlennia vzhe obrobleno (za timestamp)."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM conversations
                WHERE username = %s AND message_timestamp = %s AND role = 'user'
                LIMIT 1
            """, (username, message_timestamp))
            return cur.fetchone() is not None

    # ==================== PRODUCTS ====================

    def get_product_by_name(self, name: str) -> dict:
        """Poshuk tovaru za nazvoiu (chastkove spivpadinnia)."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM products
                WHERE LOWER(name) LIKE LOWER(%s) AND in_stock = TRUE
                LIMIT 1
            """, (f'%{name}%',))
            return cur.fetchone()

    def get_products_by_category(self, category: str) -> list:
        """Otrymaty tovary za katehoriieiu."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM products
                WHERE LOWER(category) LIKE LOWER(%s) AND in_stock = TRUE
                ORDER BY name
            """, (f'%{category}%',))
            return cur.fetchall()

    def search_products(self, query: str) -> list:
        """Poshuk tovariv za zapytom (nazva, opys, katehoriia)."""
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
        """Dodaty novyj tovar."""
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
        """Stvoryty nove zamovlennia."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO orders (username, display_name, full_name, phone, city, nova_poshta, products, total_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (username, display_name, full_name, phone, city, nova_poshta, products, total_price))
            return cur.fetchone()[0]

    def update_order_status(self, order_id: int, status: str, ttn: str = None):
        """Onovyty status zamovlennia."""
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
        """Otrymaty zamovlennia korystuvacha."""
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
                               city: str = None, interested_products: str = None,
                               notes: str = None) -> int:
        """
        Stvoryty abo onovyty lida.
        Jakshcho lid vzhe isnuje - onovyty dani ta zbilshyty lichylnyk povidomlen.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO leads (username, display_name, phone, email, city, interested_products, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (username) DO UPDATE SET
                    display_name = COALESCE(EXCLUDED.display_name, leads.display_name),
                    phone = COALESCE(EXCLUDED.phone, leads.phone),
                    email = COALESCE(EXCLUDED.email, leads.email),
                    city = COALESCE(EXCLUDED.city, leads.city),
                    interested_products = COALESCE(EXCLUDED.interested_products, leads.interested_products),
                    notes = COALESCE(EXCLUDED.notes, leads.notes),
                    last_contact = CURRENT_TIMESTAMP,
                    messages_count = leads.messages_count + 1
                RETURNING id
            """, (username, display_name, phone, email, city, interested_products, notes))
            return cur.fetchone()[0]

    def get_lead(self, username: str) -> dict:
        """Otrymaty lida za username."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM leads WHERE username = %s", (username,))
            return cur.fetchone()

    def update_lead_status(self, username: str, status: str):
        """Onovyty status lida (new, contacted, qualified, converted, lost)."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE leads SET status = %s, last_contact = CURRENT_TIMESTAMP
                WHERE username = %s
            """, (status, username))

    def update_lead_phone(self, username: str, phone: str):
        """Onovyty telefon lida."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE leads SET phone = %s, last_contact = CURRENT_TIMESTAMP
                WHERE username = %s
            """, (phone, username))

    def get_all_leads(self, status: str = None, limit: int = 100) -> list:
        """Otrymaty vsikh lidiv (z filtrom po statusu)."""
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
        """Zakryty ziednannia."""
        if self.conn:
            self.conn.close()
            logger.info("Ziednannia z DB zakryto")


def main():
    """Zapusk stvorennia/onovlennia tablyt's."""
    print("=" * 60)
    print("  DATABASE SETUP - inst_ai_manager")
    print("=" * 60)

    try:
        db = Database()
        print("\n[OK] Pidkliucheno do PostgreSQL")
        print("[OK] Tablytsi stvoreno/onovleno:")
        print("     - conversations (povidomlennia user/assistant)")
        print("     - products (tovary)")
        print("     - orders (zamovlennia)")
        print("     - leads (potentsiini klienty)")

        # Pokazujemo strukturu tablyt's
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
            print("  CONVERSATIONS table structure:")
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
            print("  PRODUCTS table structure:")
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
            print("  ORDERS table structure:")
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
            print("  LEADS table structure:")
            print("-" * 60)
            for col in columns:
                nullable = "NULL" if col[2] == 'YES' else "NOT NULL"
                print(f"  {col[0]:<20} {col[1]:<20} {nullable}")

            # Kilkist zapysiv
            print("\n" + "-" * 60)
            print("  Statistics:")
            print("-" * 60)

            cur.execute("SELECT COUNT(*) FROM conversations")
            count = cur.fetchone()[0]
            print(f"  conversations: {count} records")

            cur.execute("SELECT COUNT(*) FROM products")
            count = cur.fetchone()[0]
            print(f"  products: {count} records")

            cur.execute("SELECT COUNT(*) FROM orders")
            count = cur.fetchone()[0]
            print(f"  orders: {count} records")

            cur.execute("SELECT COUNT(*) FROM leads")
            count = cur.fetchone()[0]
            print(f"  leads: {count} records")

        db.close()

        print("\n" + "=" * 60)
        print("  DATABASE READY!")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        print("\nPerevirte:")
        print("  1. PostgreSQL zapuscheno")
        print("  2. Baza 'inst_ai_manager' isnuje")
        print("  3. .env fajl z pravilnymy credentials")


if __name__ == '__main__':
    main()
