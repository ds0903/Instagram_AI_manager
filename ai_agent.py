"""
AI Agent - Gemini API інтеграція
Читає prompts.yml, формує контекст, відправляє до Gemini
Інтеграція з Google Sheets (база знань) та Telegram (ескалація)
"""
import os
import re
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

    def _get_products_context(self, query: str = None) -> str:
        """Отримати контекст про товари для промпту (Google Sheets або DB)."""
        # Спроба Google Sheets
        if self.sheets_manager:
            try:
                return self.sheets_manager.get_products_context_for_ai(query)
            except Exception as e:
                logger.warning(f"Помилка Google Sheets: {e}")

        # Fallback на DB
        if query:
            products = self.db.search_products(query)
        else:
            products = []

        if not products:
            return "База товарів: (товари не знайдено за запитом)"

        products_text = "Доступні товари:\n"
        for p in products:
            products_text += f"- {p['name']}: {p['price']} грн, розміри: {p.get('sizes', 'N/A')}, матеріал: {p.get('material', 'N/A')}\n"

        return products_text

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

    def _get_sheets_context(self, message: str) -> str:
        """Отримати додатковий контекст з Google Sheets (шаблони, складні питання)."""
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

        # Складні питання
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
                          image_data: bytes = None,
                          audio_data=None) -> str:
        """
        Генерація відповіді від AI.

        Args:
            username: Instagram username
            user_message: текст повідомлення
            display_name: ім'я користувача (якщо відомо)
            message_type: 'text', 'image', 'voice', 'story_reply', 'post_share'
            image_data: дані зображення (для Vision API)
            audio_data: bytes (одне аудіо) або list[bytes] (кілька голосових)

        Returns:
            Текст відповіді
        """
        try:
            # Системний промпт
            system_prompt = self.prompts.get('system_prompt', '')

            # Додаємо контекст про товари (якщо є запит)
            products_context = self._get_products_context(user_message)
            system_prompt += f"\n\n{products_context}"

            # Додаємо контекст з Google Sheets (шаблони, складні питання)
            sheets_context = self._get_sheets_context(user_message)
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
                text_prompt = user_message or "Що це за товар? Допоможіть з вибором."
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
            else:
                # Звичайне текстове повідомлення
                messages.append(
                    types.Content(
                        role="user",
                        parts=[types.Part(text=user_message)]
                    )
                )

            # Викликаємо Gemini API
            response = self.client.models.generate_content(
                model=self.model,
                contents=messages,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    max_output_tokens=1024
                )
            )

            # Отримуємо текст відповіді
            assistant_message = response.text

            if message_type == 'image':
                logger.info(f"📷 AI Vision відповідь для {username}: {assistant_message[:200]}")
            elif message_type == 'voice':
                logger.info(f"🎤 AI Audio відповідь ({len(audio_list)} голосових) для {username}: {assistant_message[:200]}")
            else:
                logger.info(f"Відповідь згенеровано для {username}: {assistant_message[:100]}...")

            return assistant_message

        except Exception as e:
            error_str = str(e).lower()
            if 'rate limit' in error_str or '429' in error_str:
                logger.error(f"AI Rate Limit: {e}")
                self._notify_ai_error(f"Rate Limit (токени/запити): {e}")
            elif 'authentication' in error_str or 'api key' in error_str or '401' in error_str:
                logger.error(f"AI Auth Error: {e}")
                self._notify_ai_error(f"Authentication Error (API key): {e}")
            elif '400' in error_str or '500' in error_str or '503' in error_str:
                logger.error(f"AI API Error: {e}")
                self._notify_ai_error(f"API Error: {e}")
            else:
                logger.error(f"Помилка генерації відповіді: {e}")
                self._notify_ai_error(f"Невідома помилка AI: {e}")
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
