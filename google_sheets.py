"""
Google Sheets Manager - База знань для Instagram AI Agent
Аркуші:
- Каталог (товари: назва, артикул, ціна, склад, опис, фото)
- Шаблони (скелети відповідей у фірмовому стилі)
- Логіка (поведінка у стандартних ситуаціях)
- Складні_питання (обробка складних запитань)
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

            logger.info("Pidkliucheno do Google Sheets")
            return True

        except Exception as e:
            logger.error(f"Pomylka pidkliuchennia do Google Sheets: {e}")
            return False

    # ==================== КАТАЛОГ ТОВАРІВ ====================

    def get_products(self) -> list:
        """
        Отримати всі товари з аркуша "Каталог"

        Очікувані колонки:
        - Назва
        - Артикул
        - Категорія
        - Ціна
        - Розміри
        - Кольори
        - Склад (матеріал)
        - Опис
        - Фото URL
        - Супутні товари
        - В наявності (Так/Ні)

        Returns:
            list: [{назва, артикул, ціна, ...}, ...]
        """
        try:
            worksheet = self.spreadsheet.worksheet("Каталог")
            data = worksheet.get_all_values()

            if len(data) < 2:
                logger.warning("Kataloh porozhnij")
                return []

            headers = data[0]
            products = []

            for row in data[1:]:
                if not row or not any(row):
                    continue

                product = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        product[header] = row[i]

                # Фільтруємо по наявності
                in_stock = product.get('В наявності', '').strip().lower()
                if in_stock == 'ні' or in_stock == 'no':
                    continue

                products.append(product)

            logger.info(f"Zavantazheno {len(products)} tovariv")
            return products

        except Exception as e:
            logger.error(f"Pomylka chytannia katalohu: {e}")
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
                logger.info(f"Znajdeno tovar: {product.get('Назва')}")
                return product

        logger.info(f"Tovar ne znajdeno: {query}")
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

        logger.info(f"Znajdeno {len(result)} tovariv v kategorii '{category}'")
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

        logger.info(f"Znajdeno {len(result)} tovariv z rozmirom '{size}'")
        return result

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
        """
        Отримати шаблони відповідей з аркуша "Шаблони"

        Колонки: Назва шаблону | Текст

        Returns:
            dict: {назва_шаблону: текст}
        """
        try:
            worksheet = self.spreadsheet.worksheet("Шаблони")
            data = worksheet.get_all_values()

            templates = {}
            for row in data[1:]:
                if len(row) >= 2 and row[0] and row[1]:
                    template_name = row[0].strip()
                    template_text = row[1]
                    templates[template_name] = template_text

            logger.info(f"Zavantazheno {len(templates)} shabloniv")
            return templates

        except Exception as e:
            logger.error(f"Pomylka chytannia shabloniv: {e}")
            return {}

    # ==================== ЛОГІКА ПОВЕДІНКИ ====================

    def get_behavior_rules(self) -> list:
        """
        Отримати правила поведінки з аркуша "Логіка"

        Колонки: Ситуація | Тригери | Відповідь | Дія

        Returns:
            list: [{ситуація, тригери, відповідь, дія}, ...]
        """
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

                # Парсимо тригери (через кому)
                triggers_str = rule.get('Тригери', '')
                rule['triggers_list'] = [t.strip().lower() for t in triggers_str.split(',') if t.strip()]

                rules.append(rule)

            logger.info(f"Zavantazheno {len(rules)} pravyl povedinky")
            return rules

        except Exception as e:
            logger.error(f"Pomylka chytannia lohiky: {e}")
            return []

    def check_triggers(self, message: str) -> dict:
        """
        Перевірити чи є тригерні слова в повідомленні

        Args:
            message: Текст повідомлення

        Returns:
            dict: Правило яке спрацювало, або None
        """
        rules = self.get_behavior_rules()
        message_lower = message.lower().strip()

        for rule in rules:
            triggers = rule.get('triggers_list', [])
            for trigger in triggers:
                if trigger in message_lower:
                    logger.info(f"Spratsiuvav tryher '{trigger}' dlia sytuatsii '{rule.get('Ситуація')}'")
                    return rule

        return None

    # ==================== СКЛАДНІ ПИТАННЯ ====================

    def get_complex_questions(self) -> dict:
        """
        Отримати відповіді на складні питання з аркуша "Складні_питання"

        Колонки: Питання | Відповідь

        Returns:
            dict: {питання: відповідь}
        """
        try:
            worksheet = self.spreadsheet.worksheet("Складні_питання")
            data = worksheet.get_all_values()

            questions = {}
            for row in data[1:]:
                if len(row) >= 2 and row[0] and row[1]:
                    question = row[0].strip().lower()
                    answer = row[1]
                    questions[question] = answer

            logger.info(f"Zavantazheno {len(questions)} skladnykh pytan")
            return questions

        except Exception as e:
            logger.error(f"Pomylka chytannia skladnykh pytan: {e}")
            return {}

    def find_answer_for_question(self, question: str) -> str:
        """
        Пошук відповіді на складне питання

        Args:
            question: Текст питання

        Returns:
            str: Відповідь або None
        """
        questions = self.get_complex_questions()
        question_lower = question.lower().strip()

        for stored_q, answer in questions.items():
            if question_lower in stored_q or stored_q in question_lower:
                logger.info(f"Znajdeno vidpovid na pytannia")
                return answer

        return None

    # ==================== ЛОГУВАННЯ ====================

    def log_unusual_question(self, username: str, question: str, context: str = "") -> bool:
        """
        Записати нестандартне питання в аркуш "Нестандартні_питання"

        Args:
            username: Instagram username
            question: Текст питання
            context: Контекст діалогу

        Returns:
            bool: True якщо успішно
        """
        try:
            from datetime import datetime

            worksheet = self.spreadsheet.worksheet("Нестандартні_питання")
            now = datetime.now().strftime("%d.%m.%Y %H:%M")

            worksheet.append_row([now, f"@{username}", question, context])

            logger.info(f"Nestandartne pytannia zapysano: '{question[:50]}...'")
            return True

        except Exception as e:
            logger.error(f"Pomylka zapysu pytannia: {e}")
            return False

    # ==================== ФОРМУВАННЯ ПОВІДОМЛЕНЬ ====================

    def format_product_message(self, product: dict) -> str:
        """
        Форматувати повідомлення про товар

        Args:
            product: Дані товару

        Returns:
            str: Готове повідомлення
        """
        templates = self.get_templates()
        template = templates.get('Товар', None)

        if template:
            try:
                return template.format(**product)
            except KeyError:
                pass

        # Дефолтний формат
        name = product.get('Назва', '')
        price = product.get('Ціна', '')
        material = product.get('Склад', '')
        sizes = product.get('Розміри', '')
        description = product.get('Опис', '')

        message = f"*{name}*\n"
        if price:
            message += f"Ціна: {price} грн\n"
        if material:
            message += f"Склад: {material}\n"
        if sizes:
            message += f"Розміри: {sizes}\n"
        if description:
            message += f"\n{description}"

        return message

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
                return f"Знайдено товар:\n- {product.get('Назва')}: {product.get('Ціна')} грн, розміри: {product.get('Розміри')}, склад: {product.get('Склад')}"

            # Пробуємо за категорією
            products = self.find_products_by_category(query)
            if products:
                result = f"Товари в категорії '{query}':\n"
                for p in products[:5]:
                    result += f"- {p.get('Назва')}: {p.get('Ціна')} грн\n"
                return result

            return "Товар не знайдено в каталозі."
        else:
            # Загальний список категорій
            products = self.get_products()
            categories = set()
            for p in products:
                cat = p.get('Категорія', '')
                if cat:
                    categories.add(cat)

            if categories:
                return f"Доступні категорії товарів: {', '.join(categories)}"
            return "Каталог товарів порожній."


def main():
    """Тест підключення та читання даних"""
    print("=" * 60)
    print("  GOOGLE SHEETS MANAGER TEST")
    print("=" * 60)

    gs = GoogleSheetsManager()

    print(f"\nCredentials: {gs.credentials_file}")
    print(f"Spreadsheet URL: {gs.spreadsheet_url}")

    if not gs.spreadsheet_url:
        print("\n[ERROR] GOOGLE_SHEET_ID abo GOOGLE_SHEET_URL ne vkazano v .env")
        print("\nDodaj v .env:")
        print("  GOOGLE_SHEET_ID=your_spreadsheet_id")
        print("  GOOGLE_SHEETS_CREDENTIALS=credentials.json")
        return

    if not Path(gs.credentials_file).exists():
        print(f"\n[ERROR] Fajl credentials ne znajdeno: {gs.credentials_file}")
        print("\nStvor service account v Google Cloud Console ta zavantazh JSON")
        return

    print("\nPidkliuchajemosj...")
    if not gs.connect():
        print("[ERROR] Ne vdalosja pidkliuchytysja")
        return

    print("[OK] Pidkliucheno do Google Sheets!")

    # Тестуємо читання
    print("\n" + "-" * 60)
    print("  KATALOH TOVARIV")
    print("-" * 60)

    products = gs.get_products()
    print(f"Znajdeno {len(products)} tovariv")

    if products:
        print("\nPershi 3 tovary:")
        for p in products[:3]:
            print(f"  - {p.get('Назва', 'N/A')}: {p.get('Ціна', 'N/A')} grn")

    print("\n" + "-" * 60)
    print("  SHABLONY")
    print("-" * 60)

    templates = gs.get_templates()
    print(f"Znajdeno {len(templates)} shabloniv")

    if templates:
        print("\nNazvy shabloniv:")
        for name in list(templates.keys())[:5]:
            print(f"  - {name}")

    print("\n" + "-" * 60)
    print("  LOHIKA POVEDINKY")
    print("-" * 60)

    rules = gs.get_behavior_rules()
    print(f"Znajdeno {len(rules)} pravyl")

    if rules:
        print("\nPershi 3 sytuatsii:")
        for r in rules[:3]:
            print(f"  - {r.get('Ситуація', 'N/A')}: tryhery={r.get('triggers_list', [])}")

    print("\n" + "=" * 60)
    print("  TEST COMPLETE!")
    print("=" * 60)


if __name__ == '__main__':
    main()
