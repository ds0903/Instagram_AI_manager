"""
Google Sheets Manager - База знань для Instagram AI Agent
Аркуші (опціональні, крім Каталогу):
- Каталог (товари: назва, матеріал, ціна, кольори, розміри, опис)
- Шаблони (скелети відповідей у фірмовому стилі)
- Логіка (поведінка у стандартних ситуаціях)
- Складні_питання (обробка складних запитань)
Якщо аркуш не існує — пропускається, AI працює самостійно.
"""
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class GoogleSheetsManager:
    """Менеджер для роботи з Google Таблицями - база знань магазину"""

    def __init__(self, credentials_file: str = None, spreadsheet_url: str = None):
        """
        Ініціалізація менеджера Google Sheets

        Args:
            credentials_file: Шлях до JSON файлу з credentials
            spreadsheet_url: URL Google таблиці
        """
        self.credentials_file = credentials_file or os.getenv('GOOGLE_SHEETS_CREDENTIALS', 'credentials.json')
        self.spreadsheet_url = spreadsheet_url or self._build_url()
        self.client = None
        self.spreadsheet = None

    def _build_url(self) -> str:
        """Побудувати URL з ID таблиці"""
        sheet_id = os.getenv('GOOGLE_SHEET_ID', '')
        if sheet_id:
            return f'https://docs.google.com/spreadsheets/d/{sheet_id}/edit'
        return os.getenv('GOOGLE_SHEET_URL', '')

    def connect(self) -> bool:
        """Підключення до Google Sheets"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]

            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.credentials_file,
                scope
            )
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_url(self.spreadsheet_url)

            logger.info("Підключено до Google Sheets")
            return True

        except Exception as e:
            logger.error(f"Помилка підключення до Google Sheets: {e}")
            return False

    # ==================== КАТАЛОГ ТОВАРІВ ====================

    def get_products(self) -> list:
        """
        Отримати всі товари з аркуша "Каталог"

        Очікувані колонки (з Excel):
        - Назва
        - Куди носити
        - Матеріал
        - Опис товару
        - Кольори
        - Доступні розміри
        - Ціна
        - Акція - 15%
        - Супутні товари
        - Примітка
        - Фото URL (опціонально — посилання на фото товару)

        Returns:
            list: [{назва, матеріал, ціна, ...}, ...]
        """
        try:
            worksheet = self.spreadsheet.worksheet("Каталог")
            data = worksheet.get_all_values()

            if len(data) < 2:
                logger.warning("Каталог порожній")
                return []

            # Знаходимо рядок з заголовками (може бути не перший)
            headers = None
            header_row_idx = 0
            for i, row in enumerate(data):
                if 'Назва' in row or 'Назва ' in row:
                    headers = [h.strip() for h in row]
                    header_row_idx = i
                    break

            if not headers:
                logger.warning("Заголовки не знайдено")
                return []

            products = []
            current_product = None

            for row in data[header_row_idx + 1:]:
                if not row or not any(row):
                    continue

                # Якщо є назва - це новий товар
                name_idx = headers.index('Назва') if 'Назва' in headers else headers.index('Назва ')
                if row[name_idx] and row[name_idx].strip():
                    # Зберігаємо попередній товар
                    if current_product:
                        products.append(current_product)

                    current_product = {}
                    for i, header in enumerate(headers):
                        if i < len(row) and row[i]:
                            current_product[header] = row[i]

                    # Ініціалізуємо список цін по розмірам
                    current_product['prices_by_size'] = []
                    size_col = None
                    price_col = None
                    for i, h in enumerate(headers):
                        if 'розмір' in h.lower():
                            size_col = i
                        if 'ціна' in h.lower() and 'акція' not in h.lower():
                            price_col = i

                    if size_col is not None and price_col is not None:
                        if row[size_col] and row[price_col]:
                            current_product['prices_by_size'].append({
                                'sizes': row[size_col],
                                'price': row[price_col]
                            })

                elif current_product:
                    # Додатковий рядок з розмірами/цінами для поточного товару
                    size_col = None
                    price_col = None
                    for i, h in enumerate(headers):
                        if 'розмір' in h.lower():
                            size_col = i
                        if 'ціна' in h.lower() and 'акція' not in h.lower():
                            price_col = i

                    if size_col is not None and price_col is not None:
                        if len(row) > size_col and len(row) > price_col:
                            if row[size_col] and row[price_col]:
                                current_product['prices_by_size'].append({
                                    'sizes': row[size_col],
                                    'price': row[price_col]
                                })

            # Додаємо останній товар
            if current_product:
                products.append(current_product)

            logger.info(f"Завантажено {len(products)} товарів")
            return products

        except Exception as e:
            logger.error(f"Помилка читання каталогу: {e}")
            return []

    def find_product_by_name(self, query: str) -> dict:
        """
        Пошук товару за назвою (часткове співпадіння)

        Args:
            query: Пошуковий запит

        Returns:
            dict: Дані товару або None
        """
        products = self.get_products()
        query_lower = query.lower().strip()

        for product in products:
            name = product.get('Назва', '').lower()
            artikul = product.get('Артикул', '').lower()
            category = product.get('Категорія', '').lower()

            if query_lower in name or query_lower in artikul or query_lower in category:
                logger.info(f"Знайдено товар: {product.get('Назва')}")
                return product

        logger.info(f"Товар не знайдено: {query}")
        return None

    def find_products_by_category(self, category: str) -> list:
        """
        Пошук товарів за категорією

        Args:
            category: Назва категорії

        Returns:
            list: Список товарів
        """
        products = self.get_products()
        category_lower = category.lower().strip()

        result = []
        for product in products:
            prod_category = product.get('Категорія', '').lower()
            if category_lower in prod_category:
                result.append(product)

        logger.info(f"Знайдено {len(result)} товарів в категорії '{category}'")
        return result

    def find_products_by_size(self, size: str) -> list:
        """
        Пошук товарів за розміром

        Args:
            size: Розмір (наприклад "110", "S", "M")

        Returns:
            list: Список товарів з таким розміром
        """
        products = self.get_products()
        size_lower = size.lower().strip()

        result = []
        for product in products:
            sizes = product.get('Розміри', '').lower()
            if size_lower in sizes:
                result.append(product)

        logger.info(f"Знайдено {len(result)} товарів з розміром '{size}'")
        return result

    def get_price_for_size(self, product: dict, size_query: str) -> dict:
        """
        Отримати ціну для конкретного розміру

        Args:
            product: Товар з get_products()
            size_query: Запит розміру (напр. "158", "M", "XL")

        Returns:
            dict: {'sizes': '152-158, 158-164', 'price': '1700 грн', 'discount_price': '1445 грн'}
        """
        prices_by_size = product.get('prices_by_size', [])
        size_query_lower = size_query.lower().strip()

        for price_info in prices_by_size:
            sizes = price_info.get('sizes', '').lower()
            if size_query_lower in sizes:
                result = {
                    'sizes': price_info.get('sizes'),
                    'price': price_info.get('price')
                }
                # Розраховуємо знижку 15%
                try:
                    price_num = int(''.join(filter(str.isdigit, price_info.get('price', '0'))))
                    discount_price = int(price_num * 0.85)
                    result['discount_price'] = f"{discount_price} грн"
                except:
                    pass
                return result

        return None

    def get_related_products(self, product: dict) -> list:
        """
        Отримати супутні товари (для Upsell)

        Args:
            product: Товар з get_products()

        Returns:
            list: Список супутніх товарів
        """
        related_str = product.get('Супутні товари', '')
        if not related_str:
            return []

        # Припускаємо що супутні товари вказані через кому (артикули або назви)
        related_items = [item.strip() for item in related_str.split(',') if item.strip()]

        result = []
        for item in related_items:
            found = self.find_product_by_name(item)
            if found:
                result.append(found)

        return result

    # ==================== ШАБЛОНИ ====================

    def get_templates(self) -> dict:
        """Отримати шаблони відповідей з аркуша 'Шаблони'. Якщо немає — повертає {}."""
        try:
            worksheet = self.spreadsheet.worksheet("Шаблони")
            data = worksheet.get_all_values()
            templates = {}
            for row in data[1:]:
                if len(row) >= 2 and row[0] and row[1]:
                    templates[row[0].strip()] = row[1]
            logger.info(f"Завантажено {len(templates)} шаблонів")
            return templates
        except gspread.exceptions.WorksheetNotFound:
            logger.info("Аркуш 'Шаблони' не знайдено — пропускаємо")
            return {}
        except Exception as e:
            logger.info(f"Аркуш 'Шаблони' недоступний: {e}")
            return {}

    # ==================== ЛОГІКА ПОВЕДІНКИ ====================

    def get_behavior_rules(self) -> list:
        """Отримати правила поведінки з аркуша 'Логіка'. Якщо немає — повертає []."""
        try:
            worksheet = self.spreadsheet.worksheet("Логіка")
            data = worksheet.get_all_values()
            if len(data) < 2:
                return []
            headers = data[0]
            rules = []
            for row in data[1:]:
                if not row or not any(row):
                    continue
                rule = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        rule[header] = row[i]
                triggers_str = rule.get('Тригери', '')
                rule['triggers_list'] = [t.strip().lower() for t in triggers_str.split(',') if t.strip()]
                rules.append(rule)
            logger.info(f"Завантажено {len(rules)} правил поведінки")
            return rules
        except gspread.exceptions.WorksheetNotFound:
            logger.info("Аркуш 'Логіка' не знайдено — пропускаємо")
            return []
        except Exception as e:
            logger.info(f"Аркуш 'Логіка' недоступний: {e}")
            return []

    def check_triggers(self, message: str) -> dict:
        """Перевірити тригерні слова. Якщо аркуша немає — повертає None (AI відповість сама)."""
        rules = self.get_behavior_rules()
        if not rules:
            return None
        message_lower = message.lower().strip()
        for rule in rules:
            for trigger in rule.get('triggers_list', []):
                if trigger in message_lower:
                    logger.info(f"Спрацював тригер '{trigger}' → '{rule.get('Ситуація')}'")
                    return rule
        return None

    # ==================== СКЛАДНІ ПИТАННЯ ====================

    def get_complex_questions(self) -> dict:
        """Отримати відповіді на складні питання з аркуша 'Складні_питання'. Якщо немає — повертає {}."""
        try:
            worksheet = self.spreadsheet.worksheet("Складні_питання")
            data = worksheet.get_all_values()
            questions = {}
            for row in data[1:]:
                if len(row) >= 2 and row[0] and row[1]:
                    questions[row[0].strip().lower()] = row[1]
            logger.info(f"Завантажено {len(questions)} складних питань")
            return questions
        except gspread.exceptions.WorksheetNotFound:
            logger.info("Аркуш 'Складні_питання' не знайдено — пропускаємо")
            return {}
        except Exception as e:
            logger.info(f"Аркуш 'Складні_питання' недоступний: {e}")
            return {}

    def find_answer_for_question(self, question: str) -> str:
        """Пошук відповіді на складне питання. Якщо немає — повертає None (AI відповість сама)."""
        questions = self.get_complex_questions()
        if not questions:
            return None
        question_lower = question.lower().strip()
        for stored_q, answer in questions.items():
            if question_lower in stored_q or stored_q in question_lower:
                return answer
        return None

    # ==================== ФОТО ТОВАРІВ ====================

    def get_product_photo_url(self, product_name: str) -> str:
        """
        Знайти URL фото товару за назвою.
        Шукає в колонці 'Фото URL' або 'Фото' аркуша 'Каталог'.

        Args:
            product_name: Назва товару (напр. 'Костюм "Харпер"')

        Returns:
            str: URL фото або None
        """
        products = self.get_products()
        if not products:
            return None

        query_lower = product_name.lower().strip().strip('"\'«»')

        for product in products:
            name = product.get('Назва', product.get('Назва ', '')).lower().strip()
            # Точне або часткове співпадіння
            if query_lower in name or name in query_lower:
                # Шукаємо URL фото в різних можливих колонках
                photo_url = (
                    product.get('Фото URL') or
                    product.get('Фото') or
                    product.get('Фото URL ') or
                    product.get('Photo URL') or
                    product.get('Зображення') or
                    ''
                ).strip()
                if photo_url:
                    logger.info(f"Знайдено фото для '{product_name}': {photo_url[:80]}")
                    return photo_url
                else:
                    logger.info(f"Товар '{product_name}' знайдено, але фото URL відсутній")
                    return None

        logger.info(f"Товар '{product_name}' не знайдено для фото")
        return None

    # ==================== КОНТЕКСТ ДЛЯ AI ====================

    def get_products_context_for_ai(self, query: str = None) -> str:
        """
        Отримати контекст про товари для AI промпту

        Args:
            query: Пошуковий запит (опціонально)

        Returns:
            str: Текстовий опис товарів для промпту
        """
        if query:
            # Шукаємо конкретні товари
            product = self.find_product_by_name(query)
            if product:
                result = f"Знайдено товар: {product.get('Назва', product.get('Назва '))}\n"
                result += f"Матеріал: {product.get('Матеріал', 'N/A')}\n"
                result += f"Опис: {product.get('Опис товару', 'N/A')}\n"
                result += f"Кольори: {product.get('Кольри', product.get('Кольори', 'N/A'))}\n"
                result += f"Супутні товари: {product.get('Супутні товари', 'N/A')}\n"

                # Ціни по розмірам
                prices = product.get('prices_by_size', [])
                if prices:
                    result += "Ціни по розмірам:\n"
                    for p in prices:
                        price_str = p.get('price', '')
                        try:
                            price_num = int(''.join(filter(str.isdigit, price_str)))
                            discount = int(price_num * 0.85)
                            result += f"  - {p.get('sizes')}: {price_str} (зі знижкою 15%: {discount} грн)\n"
                        except:
                            result += f"  - {p.get('sizes')}: {price_str}\n"

                return result

            return "Товар не знайдено в каталозі."
        else:
            # Загальний список товарів
            products = self.get_products()
            if not products:
                return "Каталог товарів порожній."

            result = "Доступні товари:\n"
            for p in products:
                name = p.get('Назва', p.get('Назва ', 'N/A'))
                result += f"- {name}\n"
                prices = p.get('prices_by_size', [])
                if prices:
                    for price_info in prices[:2]:  # Показуємо перші 2 ціни
                        result += f"  {price_info.get('sizes')}: {price_info.get('price')}\n"

            return result


def main():
    """Тест підключення та читання даних"""
    print("=" * 60)
    print("  ТЕСТ GOOGLE SHEETS MANAGER")
    print("=" * 60)

    gs = GoogleSheetsManager()

    print(f"\nCredentials: {gs.credentials_file}")
    print(f"Spreadsheet URL: {gs.spreadsheet_url}")

    if not gs.spreadsheet_url:
        print("\n[ПОМИЛКА] GOOGLE_SHEET_ID або GOOGLE_SHEET_URL не вказано в .env")
        print("\nДодай в .env:")
        print("  GOOGLE_SHEET_ID=your_spreadsheet_id")
        print("  GOOGLE_SHEETS_CREDENTIALS=credentials.json")
        return

    if not Path(gs.credentials_file).exists():
        print(f"\n[ПОМИЛКА] Файл credentials не знайдено: {gs.credentials_file}")
        print("\nСтвори service account в Google Cloud Console та завантаж JSON")
        return

    print("\nПідключаємось...")
    if not gs.connect():
        print("[ПОМИЛКА] Не вдалося підключитися")
        return

    print("[OK] Підключено до Google Sheets!")

    # Тестуємо читання
    print("\n" + "-" * 60)
    print("  КАТАЛОГ ТОВАРІВ")
    print("-" * 60)

    products = gs.get_products()
    print(f"Знайдено {len(products)} товарів")

    if products:
        print("\nТовари:")
        for p in products[:5]:
            name = p.get('Назва', p.get('Назва ', 'N/A'))
            print(f"\n  {name}")
            print(f"    Матеріал: {p.get('Матеріал', 'N/A')[:50]}...")
            print(f"    Кольори: {p.get('Кольри', p.get('Кольори', 'N/A'))[:50]}...")
            print(f"    Супутні: {p.get('Супутні товари', 'N/A')}")
            prices = p.get('prices_by_size', [])
            if prices:
                print("    Ціни по розмірам:")
                for price in prices:
                    print(f"      - {price.get('sizes')}: {price.get('price')}")

    print("\n" + "=" * 60)
    print("  ТЕСТ ЗАВЕРШЕНО!")
    print("=" * 60)


if __name__ == '__main__':
    main()
