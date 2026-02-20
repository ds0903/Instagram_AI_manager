"""
AI Agent - Gemini API інтеграція
Читає prompts.yml, формує контекст, відправляє до Gemini
Інтеграція з Google Sheets (база знань) та Telegram (ескалація)
"""
import os
import re
import time
import yaml
import base64
from google import genai
from google.genai import types
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

# Завантажуємо промпти з YAML
PROMPTS_FILE = Path(__file__).parent / 'prompts.yml'

# Тригери для ескалації (передача оператору)
ESCALATION_TRIGGERS = [
    'менеджер', 'manager', 'оператор', 'людина', 'человек',
    'покликати', 'покличте', 'позовіть', 'хочу з людиною',
    'жива людина', 'real person', 'human',
    'скарга', 'complaint', 'повернення', 'return', 'refund',
    'скандал', 'обман', 'шахрай', 'fraud'
]


class AIAgent:
    def __init__(self, db):
        self.db = db
        self.client = genai.Client(
            api_key=os.getenv('GEMINI_API_KEY')
        )
        self.model = os.getenv('GEMINI_MODEL', 'gemini-3-flash-preview')
        self.prompts = self._load_prompts()

        # Google Sheets Manager (база знань)
        self.sheets_manager = None
        self._init_google_sheets()

        # Telegram Notifier (ескалація)
        self.telegram = None
        self._init_telegram()

        logger.info(f"AI Agent ініціалізовано, модель: {self.model}")

    def _init_google_sheets(self):
        """Ініціалізація Google Sheets Manager."""
        try:
            from google_sheets import GoogleSheetsManager
            self.sheets_manager = GoogleSheetsManager()
            if self.sheets_manager.connect():
                logger.info("Google Sheets підключено")
            else:
                logger.warning("Google Sheets не підключено - буде використано локальні дані")
                self.sheets_manager = None
        except Exception as e:
            logger.warning(f"Google Sheets недоступний: {e}")
            self.sheets_manager = None

    def _init_telegram(self):
        """Ініціалізація Telegram Notifier."""
        try:
            from telegram_notifier import TelegramNotifier
            self.telegram = TelegramNotifier()
            if not self.telegram.enabled:
                logger.warning("Telegram не налаштовано")
                self.telegram = None
        except Exception as e:
            logger.warning(f"Telegram недоступний: {e}")
            self.telegram = None

    def _load_prompts(self) -> dict:
        """Завантаження промптів з YAML файлу."""
        try:
            with open(PROMPTS_FILE, 'r', encoding='utf-8') as f:
                prompts = yaml.safe_load(f)
            logger.info("Промпти завантажено з prompts.yml")
            return prompts
        except Exception as e:
            logger.error(f"Помилка завантаження промптів: {e}")
            return {}

    def reload_prompts(self):
        """Перезавантаження промптів (без рестарту)."""
        self.prompts = self._load_prompts()

    def _build_conversation_context(self, username: str) -> list:
        """
        Формування контексту розмови для Gemini.
        Повертає list types.Content у форматі Gemini API.
        """
        # Отримуємо історію розмови з DB
        history = self.db.get_conversation_history(username, limit=20)

        messages = []
        for msg in history:
            # Gemini використовує 'model' замість 'assistant'
            role = 'model' if msg['role'] == 'assistant' else msg['role']
            messages.append(
                types.Content(
                    role=role,
                    parts=[types.Part(text=msg['content'])]
                )
            )

        return messages

    def _get_products_context(self) -> str:
        """Отримати ПОВНИЙ каталог товарів для промпту. AI сама шукає потрібний товар."""
        if self.sheets_manager:
            try:
                return self.sheets_manager.get_products_context_for_ai()
            except Exception as e:
                logger.warning(f"Помилка Google Sheets: {e}")

        return "Каталог товарів недоступний."

    def _check_escalation(self, message: str) -> bool:
        """Перевірити чи потрібна ескалація (передача оператору)."""
        message_lower = message.lower()
        for trigger in ESCALATION_TRIGGERS:
            if trigger in message_lower:
                logger.info(f"Знайдено тригер ескалації: '{trigger}'")
                return True
        return False

    def _check_behavior_rules(self, message: str) -> dict:
        """Перевірити правила поведінки з Google Sheets. Якщо аркуша немає — повертає None."""
        if self.sheets_manager:
            try:
                return self.sheets_manager.check_triggers(message)
            except Exception:
                pass
        return None

    def _get_sheets_context(self, message: str, username: str = "") -> str:
        """Отримати додатковий контекст з Google Sheets (шаблони, складні питання, логіка)."""
        parts = []
        if not self.sheets_manager:
            return ""

        # Шаблони відповідей
        try:
            templates = self.sheets_manager.get_templates()
            if templates:
                parts.append("Шаблони відповідей (використовуй якщо підходить):")
                for name, text in templates.items():
                    parts.append(f"  [{name}]: {text}")
        except Exception:
            pass

        # Логіка поведінки (ситуації + тригери + дії)
        try:
            rules = self.sheets_manager.get_behavior_rules()
            if rules:
                parts.append("\nПравила поведінки (Логіка):")
                for rule in rules:
                    situation = rule.get('Ситуація', '')
                    triggers = rule.get('Тригери', '')
                    response = rule.get('Відповідь', '')
                    action = rule.get('Дія', '')
                    parts.append(f"  [{situation}] тригери: {triggers} → {response} (дія: {action})")
        except Exception:
            pass

        # Складні питання (готові відповіді)
        try:
            answer = self.sheets_manager.find_answer_for_question(message)
            if answer:
                parts.append(f"\nГотова відповідь на це питання: {answer}")
        except Exception:
            pass

        return "\n".join(parts)

    def _extract_phone(self, message: str) -> str:
        """Витягнути телефон з повідомлення."""
        # Шукаємо українські та міжнародні номери
        patterns = [
            r'\+380\d{9}',           # +380XXXXXXXXX
            r'380\d{9}',             # 380XXXXXXXXX
            r'0\d{9}',               # 0XXXXXXXXX
            r'\d{3}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}',  # XXX XXX XX XX
        ]
        for pattern in patterns:
            match = re.search(pattern, message)
            if match:
                return match.group()
        return None

    def _parse_order(self, response: str) -> dict:
        """
        Парсинг блоку [ORDER]...[/ORDER] з відповіді AI.
        Повертає dict з даними замовлення або None.
        """
        match = re.search(r'\[ORDER\](.*?)\[/ORDER\]', response, re.DOTALL)
        if not match:
            return None

        block = match.group(1).strip()
        order = {}
        for line in block.split('\n'):
            line = line.strip()
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip().lower()
                value = value.strip()
                if key in ('піб', 'пiб', "ім'я", 'імя', 'name'):
                    order['full_name'] = value
                elif key in ('телефон', 'phone', 'тел'):
                    order['phone'] = value
                elif key in ('місто', 'city'):
                    order['city'] = value
                elif key in ('нп', 'нова пошта', 'відділення', 'nova_poshta'):
                    order['nova_poshta'] = value
                elif key in ('товари', 'товар', 'products'):
                    order['products'] = value
                elif key in ('сума', 'total', 'ціна'):
                    order['total_price'] = value

        if order.get('full_name') or order.get('phone'):
            logger.info(f"Розпізнано замовлення: {order}")
            return order
        return None

    def _strip_order_block(self, response: str) -> str:
        """Видалити блок [ORDER]...[/ORDER] з тексту відповіді (клієнт не бачить)."""
        return re.sub(r'\s*\[ORDER\].*?\[/ORDER\]\s*', '', response, flags=re.DOTALL).strip()

    def _parse_photo_markers(self, response: str) -> list:
        """
        Парсинг маркерів [PHOTO:https://...] з відповіді AI.
        AI сама обирає конкретний URL з каталогу (опис кольору → URL).
        Повертає список URL для відправки.
        """
        markers = re.findall(r'\[PHOTO:(https?://[^\]]+)\]', response)
        if markers:
            logger.info(f"Знайдено {len(markers)} фото URL: {[m[:60] for m in markers]}")
        return markers

    def _strip_photo_markers(self, response: str) -> str:
        """Видалити маркери [PHOTO:...] з тексту відповіді (клієнт не бачить)."""
        return re.sub(r'\s*\[PHOTO:.+?\]', '', response).strip()

    def get_product_photo_url(self, product_name: str) -> str:
        """Знайти URL фото товару через Google Sheets."""
        if self.sheets_manager:
            try:
                return self.sheets_manager.get_product_photo_url(product_name)
            except Exception as e:
                logger.warning(f"Помилка пошуку фото: {e}")
        return None

    def _process_order(self, username: str, display_name: str, order_data: dict) -> int:
        """
        Зберегти замовлення в БД та відправити сповіщення в Telegram.
        Повертає order_id.
        """
        # Парсимо суму (число з рядка)
        total_price = None
        if order_data.get('total_price'):
            digits = ''.join(filter(str.isdigit, order_data['total_price']))
            if digits:
                total_price = float(digits)

        # Зберігаємо замовлення в БД
        order_id = self.db.create_order(
            username=username,
            display_name=display_name,
            full_name=order_data.get('full_name'),
            phone=order_data.get('phone'),
            city=order_data.get('city'),
            nova_poshta=order_data.get('nova_poshta'),
            products=order_data.get('products'),
            total_price=total_price
        )
        logger.info(f"Замовлення #{order_id} створено для {username}")

        # Оновлюємо ліда з новими даними
        self.db.create_or_update_lead(
            username=username,
            display_name=display_name,
            phone=order_data.get('phone'),
            city=order_data.get('city')
        )

        # Сповіщення в Telegram
        if self.telegram:
            self.telegram.notify_new_order(
                username=username,
                order_data=order_data
            )
            logger.info(f"Telegram сповіщення про замовлення #{order_id} відправлено")

        return order_id

    def escalate_to_human(self, username: str, display_name: str,
                          reason: str, last_message: str) -> bool:
        """Відправити повідомлення про ескалацію в Telegram."""
        if self.telegram:
            return self.telegram.notify_escalation(
                username=username,
                display_name=display_name,
                reason=reason,
                last_message=last_message
            )
        logger.warning("Telegram не налаштовано, ескалація не відправлена")
        return False

    def generate_response(self, username: str, user_message: str,
                          display_name: str = None,
                          message_type: str = 'text',
                          image_data=None,
                          audio_data=None) -> str:
        """
        Генерація відповіді від AI.

        Args:
            username: Instagram username
            user_message: текст повідомлення
            display_name: ім'я користувача (якщо відомо)
            message_type: 'text', 'image', 'voice', 'story_media', 'story_reply', 'post_share'
            image_data: bytes (одне фото) або list[bytes] (скріншоти сторіз)
            audio_data: bytes (одне аудіо) або list[bytes] (кілька голосових)

        Returns:
            Текст відповіді
        """
        try:
            # Системний промпт
            system_prompt = self.prompts.get('system_prompt', '')

            # Додаємо ПОВНИЙ каталог товарів (AI сама шукає потрібний товар)
            products_context = self._get_products_context()
            system_prompt += f"\n\n{products_context}"

            # Додаємо контекст з Google Sheets (шаблони, складні питання)
            sheets_context = self._get_sheets_context(user_message, username=username)
            if sheets_context:
                system_prompt += f"\n\n{sheets_context}"

            # Додаємо ім'я користувача в контекст
            if display_name:
                system_prompt += f"\n\nІм'я клієнта: {display_name}"

            # Формуємо історію розмови
            messages = self._build_conversation_context(username)

            # Нормалізуємо audio_data до списку
            audio_list = []
            if audio_data:
                if isinstance(audio_data, list):
                    audio_list = audio_data
                else:
                    audio_list = [audio_data]

            # Додаємо поточне повідомлення
            if message_type == 'image' and image_data:
                # Vision API - аналіз зображення
                text_prompt = user_message or (
                    "Клієнт надіслав фото — розпізнай весь текст на зображенні"
                    " (моделі, розміри, ціни), визнач товар і запропонуй з асортименту."
                )
                # Auto-detect mime type (screenshot = PNG, download = JPEG)
                mime = "image/png" if image_data[:4] == b'\x89PNG' else "image/jpeg"
                logger.info(f"📷 Відправляємо зображення в Gemini Vision: {len(image_data)} байт, mime={mime}")
                logger.info(f"📷 Текстовий промпт до фото: '{text_prompt[:100]}'")
                messages.append(
                    types.Content(
                        role="user",
                        parts=[
                            types.Part(text=text_prompt),
                            types.Part(
                                inline_data=types.Blob(
                                    mime_type=mime,
                                    data=image_data
                                )
                            )
                        ]
                    )
                )
            elif message_type == 'voice' and audio_list:
                # Audio API - аналіз голосових повідомлень (одне або кілька)
                text_prompt = user_message or "Клієнт надіслав голосове повідомлення. Прослухай і відповідай."
                parts = [types.Part(text=text_prompt)]
                for i, audio_bytes in enumerate(audio_list):
                    mime = self._detect_audio_mime(audio_bytes)
                    logger.info(f"🎤 Аудіо #{i+1}: {len(audio_bytes)} байт, mime={mime}")
                    parts.append(
                        types.Part(
                            inline_data=types.Blob(
                                mime_type=mime,
                                data=audio_bytes
                            )
                        )
                    )
                logger.info(f"🎤 Відправляємо {len(audio_list)} голосових в Gemini")
                logger.info(f"🎤 Текстовий промпт: '{text_prompt[:100]}'")
                messages.append(
                    types.Content(role="user", parts=parts)
                )
            elif message_type == 'story_media' and image_data and isinstance(image_data, list):
                # Story screenshots - кілька зображень сторіз (фото або кадри відео)
                text_prompt = user_message or (
                    "Клієнт відповів на сторіз. Розпізнай весь текст на скріншотах "
                    "(моделі, розміри, ціни), визнач товар і запропонуй з асортименту."
                )
                parts = [types.Part(text=text_prompt)]
                for i, screenshot in enumerate(image_data):
                    mime = "image/png"
                    logger.info(f"📖 Скріншот сторіз #{i+1}: {len(screenshot)} байт")
                    parts.append(
                        types.Part(
                            inline_data=types.Blob(
                                mime_type=mime,
                                data=screenshot
                            )
                        )
                    )
                logger.info(f"📖 Відправляємо {len(image_data)} скріншотів сторіз в Gemini Vision")
                logger.info(f"📖 Текстовий промпт: '{text_prompt[:100]}'")
                messages.append(
                    types.Content(role="user", parts=parts)
                )
            else:
                # Звичайне текстове повідомлення
                messages.append(
                    types.Content(
                        role="user",
                        parts=[types.Part(text=user_message)]
                    )
                )

            # Викликаємо Gemini API з retry (до 3 спроб при тимчасових помилках)
            max_retries = 3
            last_error = None
            for attempt in range(1, max_retries + 1):
                try:
                    response = self.client.models.generate_content(
                        model=self.model,
                        contents=messages,
                        config=types.GenerateContentConfig(
                            system_instruction=system_prompt,
                            max_output_tokens=3072
                        )
                    )

                    # Отримуємо текст відповіді
                    assistant_message = response.text

                    if message_type == 'image':
                        logger.info(f"📷 AI Vision відповідь для {username}: {assistant_message[:200]}")
                    elif message_type == 'voice':
                        logger.info(f"🎤 AI Audio відповідь ({len(audio_list)} голосових) для {username}: {assistant_message[:200]}")
                    elif message_type == 'story_media':
                        count = len(image_data) if isinstance(image_data, list) else 1
                        logger.info(f"📖 AI Story відповідь ({count} скріншотів) для {username}: {assistant_message[:200]}")
                    else:
                        logger.info(f"Відповідь згенеровано для {username}: {assistant_message[:100]}...")

                    return assistant_message

                except Exception as api_err:
                    last_error = api_err
                    error_str = str(api_err).lower()
                    # Retry тільки при тимчасових помилках (429, 500, 503)
                    is_retryable = any(code in error_str for code in ['429', '500', '503', 'rate limit', 'unavailable', 'overloaded'])
                    if is_retryable and attempt < max_retries:
                        wait_sec = attempt * 5  # 5с, 10с
                        logger.warning(f"⚠️ Gemini API помилка (спроба {attempt}/{max_retries}): {api_err}. Retry через {wait_sec}с...")
                        time.sleep(wait_sec)
                        continue
                    else:
                        break

            # Всі спроби вичерпані або не-retryable помилка
            e = last_error
            error_str = str(e).lower()
            if 'rate limit' in error_str or '429' in error_str:
                logger.error(f"AI Rate Limit (після {max_retries} спроб): {e}")
                self._notify_ai_error(
                    f"🚨 AI FALLBACK для @{username}\n"
                    f"Помилка: Rate Limit\n"
                    f"Спроб: {attempt}/{max_retries}\n"
                    f"Тип: {message_type}\n"
                    f"Клієнт отримав fallback-відповідь!\n"
                    f"Деталі: {e}"
                )
            elif 'authentication' in error_str or 'api key' in error_str or '401' in error_str:
                logger.error(f"AI Auth Error: {e}")
                self._notify_ai_error(
                    f"🚨 AI FALLBACK для @{username}\n"
                    f"Помилка: Authentication Error (API key)\n"
                    f"Клієнт отримав fallback-відповідь!\n"
                    f"Деталі: {e}"
                )
            elif '400' in error_str or '500' in error_str or '503' in error_str:
                logger.error(f"AI API Error (після {max_retries} спроб): {e}")
                self._notify_ai_error(
                    f"🚨 AI FALLBACK для @{username}\n"
                    f"Помилка: API Error ({attempt} спроб)\n"
                    f"Тип: {message_type}\n"
                    f"Клієнт отримав fallback-відповідь!\n"
                    f"Деталі: {e}"
                )
            else:
                logger.error(f"Помилка генерації відповіді: {e}")
                self._notify_ai_error(
                    f"🚨 AI FALLBACK для @{username}\n"
                    f"Невідома помилка\n"
                    f"Тип: {message_type}\n"
                    f"Клієнт отримав fallback-відповідь!\n"
                    f"Деталі: {e}"
                )
            return self.prompts.get('fallback', 'Вибачте, сталася помилка. Спробуйте ще раз.')

        except Exception as e:
            logger.error(f"Критична помилка в generate_response: {e}")
            self._notify_ai_error(
                f"🚨 AI КРИТИЧНА ПОМИЛКА для @{username}\n"
                f"Деталі: {e}"
            )
            return self.prompts.get('fallback', 'Вибачте, сталася помилка. Спробуйте ще раз.')

    @staticmethod
    def _detect_audio_mime(data: bytes) -> str:
        """Визначити MIME-тип аудіо за magic bytes."""
        if len(data) < 12:
            return 'audio/mp4'
        if data[:4] == b'OggS':
            return 'audio/ogg'
        if data[:3] == b'ID3' or data[:2] in (b'\xff\xfb', b'\xff\xf3', b'\xff\xf2'):
            return 'audio/mpeg'
        if data[:4] == b'RIFF':
            return 'audio/wav'
        if data[4:8] == b'ftyp':
            return 'audio/mp4'
        return 'audio/mp4'

    def _notify_ai_error(self, error_msg: str):
        """Відправити сповіщення про помилку AI в Telegram"""
        try:
            if self.telegram:
                self.telegram.notify_error(f"Помилка AI Agent:\n{error_msg}")
        except Exception as e:
            logger.warning(f"Не вдалося відправити сповіщення: {e}")

    def process_message(self, username: str, content: str,
                        display_name: str = None,
                        message_type: str = 'text',
                        message_timestamp=None,
                        image_data: bytes = None,
                        audio_data: bytes = None) -> str:
        """
        Повний цикл обробки повідомлення:
        1. Збереження user message в DB
        2. Перевірка ескалації
        3. Створення/оновлення ліда
        4. Генерація відповіді
        5. Збереження assistant message в DB

        Returns:
            Текст відповіді для відправки
        """
        # 1. Перевіряємо чи не оброблено вже
        if message_timestamp:
            if self.db.is_message_processed(username, message_timestamp):
                logger.info(f"Повідомлення від {username} вже оброблено, пропускаємо")
                return None

        # 2. Зберігаємо повідомлення користувача
        user_msg_id = self.db.add_user_message(
            username=username,
            content=content,
            display_name=display_name,
            message_timestamp=message_timestamp
        )
        logger.info(f"Збережено user message id={user_msg_id} від {username}")

        # 3. Створюємо/оновлюємо ліда
        phone = self._extract_phone(content)
        self.db.create_or_update_lead(
            username=username,
            display_name=display_name,
            phone=phone
        )
        logger.info(f"Лід оновлено: {username}")

        # 4. Перевіряємо ескалацію
        if self._check_escalation(content):
            logger.info(f"Ескалація для {username}")
            self.escalate_to_human(
                username=username,
                display_name=display_name,
                reason="Клієнт просить зв'язку з оператором",
                last_message=content
            )
            # Все одно генеруємо відповідь, але з попередженням
            escalation_note = self.prompts.get('escalation_response',
                'Зрозуміло! Передаю ваше запитання нашому менеджеру. Він зв\'яжеться з вами найближчим часом.')
            response_text = escalation_note
        else:
            # 5. Перевіряємо правила поведінки (Google Sheets, якщо є)
            behavior_rule = self._check_behavior_rules(content)
            if behavior_rule and behavior_rule.get('Відповідь'):
                response_text = behavior_rule.get('Відповідь')
                logger.info(f"Застосовано правило: {behavior_rule.get('Ситуація')}")
            else:
                # 6. Генеруємо відповідь через AI (fallback)
                response_text = self.generate_response(
                    username=username,
                    user_message=content,
                    display_name=display_name,
                    message_type=message_type,
                    image_data=image_data,
                    audio_data=audio_data
                )

        # 7. Зберігаємо відповідь асистента
        assistant_msg_id = self.db.add_assistant_message(
            username=username,
            content=response_text,
            display_name=display_name,
            answer_id=user_msg_id
        )
        logger.info(f"Збережено assistant message id={assistant_msg_id}")

        # 8. Оновлюємо answer_id в user message
        self.db.update_answer_id(user_msg_id, assistant_msg_id)

        # 9. Сповіщення про нового ліда (якщо це перший контакт)
        lead = self.db.get_lead(username)
        if lead and lead.get('messages_count') == 1 and self.telegram:
            self.telegram.notify_new_lead(
                username=username,
                display_name=display_name,
                phone=phone,
                products=content[:100] if content else None
            )

        return response_text

    def get_greeting(self) -> str:
        """Отримати привітання."""
        return self.prompts.get('greeting', 'Вітаю! Чим можу допомогти?')

    def get_prompt(self, key: str) -> str:
        """Отримати промпт за ключем."""
        return self.prompts.get(key, '')
