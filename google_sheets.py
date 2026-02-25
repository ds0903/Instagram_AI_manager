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
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import re
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
        self.drive_service = None

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

            # Drive API — ті самі credentials
            self.drive_service = build('drive', 'v3', credentials=creds)

            logger.info("Підключено до Google Sheets")
            return True

        except Exception as e:
            logger.error(f"Помилка підключення до Google Sheets: {e}")
            return False

    # ==================== GOOGLE DRIVE ====================

    @staticmethod
    def extract_drive_file_id(url: str) -> str:
        """Витягти File ID з будь-якого формату Google Drive посилання (файл)."""
        patterns = [
            r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)',
            r'drive\.google\.com/(?:open|uc)\?(?:.*&)?id=([a-zA-Z0-9_-]+)',
            r'id=([a-zA-Z0-9_-]+)',
        ]
        for pattern in patterns:
            m = re.search(pattern, url)
            if m:
                return m.group(1)
        return None

    @staticmethod
    def extract_drive_folder_id(url: str) -> str:
        """Витягти Folder ID з Google Drive посилання на папку."""
        m = re.search(r'drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)', url)
        if m:
            return m.group(1)
        return None

    @staticmethod
    def is_drive_folder_url(url: str) -> bool:
        """Перевірити чи є URL посиланням на папку Google Drive."""
        return 'drive.google.com/drive/folders/' in url

    def list_folder_files(self, folder_id: str, path_prefix: str = '') -> list:
        """
        Рекурсивно отримати всі файли з папки Google Drive.

        Returns:
            list: [{name, id, path, url}]
                  path — шлях з урахуванням підпапок (напр. "Розмірна сітка/Дівчинка.jpg")
                  url  — https://drive.google.com/uc?id=FILE_ID (для використання в [PHOTO:])
        """
        if not self.drive_service:
            return []
        try:
            results = self.drive_service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id, name, mimeType)",
                orderBy="name"
            ).execute()
            files = results.get('files', [])
            output = []
            for f in files:
                full_path = f"{path_prefix}/{f['name']}" if path_prefix else f['name']
                if f['mimeType'] == 'application/vnd.google-apps.folder':
                    # Рекурсія в підпапку
                    sub = self.list_folder_files(f['id'], path_prefix=full_path)
                    output.extend(sub)
                else:
                    output.append({
                        'name': f['name'],
                        'id': f['id'],
                        'path': full_path,
                        'url': f"https://drive.google.com/uc?id={f['id']}"
                    })
            return output
        except Exception as e:
            logger.error(f"Помилка listування папки {folder_id}: {e}")
            return []

    def download_drive_file(self, url: str) -> bytes:
        """
        Завантажити файл з Google Drive через API (не публічне посилання).
        Працює для будь-якого файлу до якого є доступ у сервісного акаунту.

        Args:
            url: Будь-яке Google Drive посилання (sharing link, uc?id=, тощо)

        Returns:
            bytes: Вміст файлу або None при помилці
        """
        if not self.drive_service:
            logger.warning("Drive API не підключено")
            return None

        file_id = self.extract_drive_file_id(url)
        if not file_id:
            logger.warning(f"Не вдалося витягти File ID з: {url}")
            return None

        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            data = buffer.getvalue()
            logger.info(f"Завантажено з Drive: file_id={file_id}, розмір={len(data)} байт")
            return data
        except Exception as e:
            logger.error(f"Помилка завантаження з Drive (file_id={file_id}): {e}")
            return None

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
                    discount_col = None
                    for i, h in enumerate(headers):
                        if 'розмір' in h.lower():
                            size_col = i
                        if 'ціна' in h.lower() and 'акція' not in h.lower():
                            price_col = i
                        if 'акція' in h.lower():
                            discount_col = i

                    if size_col is not None and price_col is not None:
                        if row[size_col] and row[price_col]:
                            discount_val = ''
                            if discount_col is not None and discount_col < len(row):
                                discount_val = row[discount_col].strip()
                            current_product['prices_by_size'].append({
                                'sizes': row[size_col],
                                'price': row[price_col],
                                'discount': discount_val
                            })

                elif current_product:
                    # Додатковий рядок з розмірами/цінами для поточного товару
                    size_col = None
                    price_col = None
                    discount_col = None
                    for i, h in enumerate(headers):
                        if 'розмір' in h.lower():
                            size_col = i
                        if 'ціна' in h.lower() and 'акція' not in h.lower():
                            price_col = i
                        if 'акція' in h.lower():
                            discount_col = i

                    if size_col is not None and price_col is not None:
                        if len(row) > size_col and len(row) > price_col:
                            if row[size_col] and row[price_col]:
                                discount_val = ''
                                if discount_col is not None and discount_col < len(row):
                                    discount_val = row[discount_col].strip()
                                current_product['prices_by_size'].append({
                                    'sizes': row[size_col],
                                    'price': row[price_col],
                                    'discount': discount_val
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
        Пошук товару за назвою (часткове співпадіння в обидва боки).
        Шукає і query в назві, і назву/ключові слова в query.

        Args:
            query: Пошуковий запит (може бути повне повідомлення користувача)

        Returns:
            dict: Дані товару або None
        """
        products = self.get_products()
        query_lower = query.lower().strip()

        # 1. Прямий пошук: query в назві/артикулі/категорії
        for product in products:
            name = product.get('Назва', product.get('Назва ', '')).lower()
            artikul = product.get('Артикул', '').lower()
            category = product.get('Категорія', '').lower()

            if query_lower in name or query_lower in artikul or query_lower in category:
                logger.info(f"Знайдено товар (прямий): {product.get('Назва', product.get('Назва '))}")
                return product

        # 2. Зворотний пошук: назва товару або її ключові слова в query
        for product in products:
            name = product.get('Назва', product.get('Назва ', '')).lower().strip()
            if not name:
                continue

            # Повна назва в query
            if name in query_lower:
                logger.info(f"Знайдено товар (назва в запиті): {product.get('Назва', product.get('Назва '))}")
                return product

            # Ключові слова з назви (без лапок, мін 3 символи)
            name_words = [w.strip('"\'«»()') for w in name.split() if len(w.strip('"\'«»()')) >= 3]
            for word in name_words:
                if word in query_lower:
                    logger.info(f"Знайдено товар (слово '{word}' в запиті): {product.get('Назва', product.get('Назва '))}")
                    return product

        logger.info(f"Товар не знайдено: {query[:80]}")
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

    def save_unanswered_question(self, question: str, username: str = "") -> bool:
        """
        Зберегти нове питання в аркуш 'Складні_питання' (колонка A).
        Колонка B (відповідь) залишається порожньою — менеджер заповнить пізніше.
        Дедуплікація: не додає якщо таке питання вже є.
        """
        try:
            worksheet = self.spreadsheet.worksheet("Складні_питання")

            # Перевіряємо чи таке питання вже є (дедуплікація)
            existing = worksheet.col_values(1)  # Колонка A
            question_stripped = question.strip()
            for existing_q in existing:
                if existing_q.strip().lower() == question_stripped.lower():
                    logger.info(f"Питання вже є в Складні_питання: '{question_stripped[:60]}'")
                    return False

            # Додаємо нове питання (A=питання, B=порожньо, C=username)
            worksheet.append_row([question_stripped, "", username])
            logger.info(f"Збережено нове питання в Складні_питання: '{question_stripped[:60]}' (від {username})")
            return True

        except gspread.exceptions.WorksheetNotFound:
            logger.warning("Аркуш 'Складні_питання' не знайдено — не можу зберегти питання")
            return False
        except Exception as e:
            logger.warning(f"Помилка збереження питання: {e}")
            return False

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
        Отримати ПОВНИЙ каталог товарів з усіма деталями для AI.
        AI сама визначає який товар підходить під запит клієнта.

        Returns:
            str: Повний каталог товарів для system prompt
        """
        products = self.get_products()
        if not products:
            return "Каталог товарів порожній."

        logger.info(f"Завантажено {len(products)} товарів")

        result = "== ПОВНИЙ КАТАЛОГ ТОВАРІВ (шукай товар ТІЛЬКИ тут) ==\n\n"
        for i, p in enumerate(products, 1):
            name = p.get('Назва', p.get('Назва ', 'N/A'))
            hp_id = p.get('ID Товара', '').strip()
            result += f"📦 {i}. {name}"
            if hp_id:
                result += f" [HugeProfit ID: {hp_id}]"
            result += "\n"

            where_to_wear = p.get('Куди носити', p.get('Куди носити ', ''))
            if where_to_wear:
                result += f"   Куди носити: {where_to_wear}\n"

            material = p.get('Матеріал', '')
            if material:
                result += f"   Матеріал: {material}\n"

            description = p.get('Опис товару', '')
            if description:
                result += f"   Опис: {description}\n"

            colors = p.get('Кольори', p.get('Кольри', ''))
            if colors:
                result += f"   Кольори: {colors}\n"

            sizes = p.get('Доступні розміри', '')
            if sizes:
                result += f"   Розміри: {sizes}\n"

            related = p.get('Супутні товари', '')
            if related:
                result += f"   Супутні товари: {related}\n"

            note = p.get('Примітка', '')
            if note:
                result += f"   Примітка: {note}\n"

            # Фото — або папка Drive (listуємо файли) або звичайні URL
            photo_raw = (
                p.get('Фото URL') or p.get('Фото') or
                p.get('Фото URL ') or p.get('Photo URL') or ''
            ).strip()
            if photo_raw:
                if self.is_drive_folder_url(photo_raw):
                    folder_id = self.extract_drive_folder_id(photo_raw)
                    if folder_id:
                        files = self.list_folder_files(folder_id)
                        if files:
                            result += "   Фото (використовуй URL з цього списку в маркерах [PHOTO:url] або [ALBUM:url1 url2 ...]):\n"
                            for f in files:
                                result += f"     {f['path']} → {f['url']}\n"
                        else:
                            result += "   Фото: папка порожня або недоступна\n"
                    else:
                        result += f"   Фото: {photo_raw}\n"
                else:
                    result += f"   Фото: {photo_raw}\n"

            # Ціни по розмірам
            prices = p.get('prices_by_size', [])
            if prices:
                result += "   Ціни:\n"
                for pr in prices:
                    price_str = pr.get('price', '')
                    discount_str = pr.get('discount', '').replace('%', '').strip()
                    try:
                        price_num = int(''.join(filter(str.isdigit, price_str)))
                        # Знижка з колонки "Акція" (0 або порожньо = без знижки)
                        discount_pct = 0
                        if discount_str:
                            try:
                                discount_pct = int(discount_str)
                            except ValueError:
                                pass
                        if discount_pct > 0:
                            discounted = int(price_num * (1 - discount_pct / 100))
                            result += f"     {pr.get('sizes')}: {price_str} (акція -{discount_pct}%: {discounted} грн)\n"
                        else:
                            result += f"     {pr.get('sizes')}: {price_str}\n"
                    except Exception:
                        result += f"     {pr.get('sizes')}: {price_str}\n"

            result += "\n"

        result += "== КІНЕЦЬ КАТАЛОГУ. Називай ТІЛЬКИ товари з цього списку! ==\n"
        return result

    def get_product_id_map(self) -> dict:
        """
        Повертає словник {назва_товару_lowercase: hugeprofit_pid} для всіх товарів
        що мають заповнений "ID Товара" в Google Sheets.

        Використовується для пошуку pid при передачі замовлення в HugeProfit.
        """
        products = self.get_products()
        id_map = {}
        for p in products:
            hp_id = p.get('ID Товара', '').strip()
            name = (p.get('Назва') or p.get('Назва ', '')).strip()
            if hp_id and name:
                try:
                    id_map[name.lower()] = int(hp_id)
                except ValueError:
                    pass
        logger.info(f"HugeProfit ID map: {len(id_map)} товарів")
        return id_map


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
