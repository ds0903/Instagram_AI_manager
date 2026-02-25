"""
Telegram Notifier - сповіщення менеджерам
Реєстрація адміна: /admin PASSWORD в Telegram боті
Всі зареєстровані адміни отримують сповіщення.
"""
import os
import json
import threading
import requests
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

ADMINS_FILE = Path(__file__).parent / 'telegram_admins.json'


def _load_admins() -> list:
    if ADMINS_FILE.exists():
        try:
            return json.loads(ADMINS_FILE.read_text(encoding='utf-8'))
        except Exception:
            return []
    return []


def _save_admins(admins: list):
    ADMINS_FILE.write_text(json.dumps(admins), encoding='utf-8')


class TelegramAdminListener:
    """
    Слухає команди від Telegram і реєструє адмінів.
    Запускати в окремому потоці: TelegramAdminListener().start()
    """

    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.password = os.getenv('TELEGRAM_ADMIN_PASSWORD', '')
        self._offset = 0
        self._running = False

    def start(self):
        if not self.bot_token or not self.password:
            logger.warning("TelegramAdminListener: немає TOKEN або ADMIN_PASSWORD — не запущено")
            return
        self._running = True
        t = threading.Thread(target=self._poll_loop, daemon=True, name='TelegramAdminListener')
        t.start()
        logger.info("TelegramAdminListener запущено")

    def stop(self):
        self._running = False

    def _poll_loop(self):
        while self._running:
            try:
                updates = self._get_updates()
                for upd in updates:
                    self._handle(upd)
            except Exception as e:
                logger.error(f"TelegramAdminListener poll error: {e}")
            threading.Event().wait(3)

    def _get_updates(self) -> list:
        url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        resp = requests.get(url, params={'offset': self._offset, 'timeout': 10}, timeout=15)
        data = resp.json()
        updates = data.get('result', [])
        if updates:
            self._offset = updates[-1]['update_id'] + 1
        return updates

    def _handle(self, upd: dict):
        msg = upd.get('message') or upd.get('channel_post')
        if not msg:
            return
        chat_id = str(msg['chat']['id'])
        text = (msg.get('text') or '').strip()

        if not text.startswith('/admin'):
            return

        parts = text.split()
        if len(parts) < 2:
            self._send(chat_id, "❌ Використання: /admin PASSWORD")
            return

        if parts[1] == self.password:
            admins = _load_admins()
            if chat_id not in admins:
                admins.append(chat_id)
                _save_admins(admins)
                logger.info(f"TelegramAdmin: новий адмін зареєстровано chat_id={chat_id}")
                self._send(chat_id, "✅ Ви зареєстровані як адмін! Будете отримувати всі сповіщення.")
            else:
                self._send(chat_id, "ℹ️ Ви вже зареєстровані як адмін.")
        else:
            self._send(chat_id, "❌ Невірний пароль.")

    def _send(self, chat_id: str, text: str):
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            requests.post(url, json={'chat_id': chat_id, 'text': text}, timeout=10)
        except Exception as e:
            logger.error(f"TelegramAdminListener send error: {e}")


class TelegramNotifier:
    """Відправка сповіщень всім зареєстрованим адмінам"""

    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.enabled = bool(self.bot_token)

        if self.enabled:
            logger.info("Telegram Notifier увімкнено")
        else:
            logger.warning("Telegram Notifier вимкнено (немає TELEGRAM_BOT_TOKEN)")

    def send_message(self, text: str, parse_mode: str = 'HTML') -> bool:
        if not self.enabled:
            return False

        admins = _load_admins()
        if not admins:
            logger.warning("Telegram: немає зареєстрованих адмінів (відправте /admin PASSWORD боту)")
            return False

        success = False
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        for chat_id in admins:
            try:
                resp = requests.post(url, json={
                    'chat_id': chat_id,
                    'text': text,
                    'parse_mode': parse_mode
                }, timeout=10)
                if resp.status_code == 200:
                    success = True
                else:
                    logger.error(f"Telegram [{chat_id}] помилка: {resp.status_code} {resp.text[:200]}")
            except Exception as e:
                logger.error(f"Telegram [{chat_id}] виняток: {e}")

        return success

    def notify_escalation(self, username: str, display_name: str,
                          reason: str, last_message: str,
                          dialog_link: str = None) -> bool:
        text = f"""🚨 <b>ЕСКАЛАЦІЯ - Потрібен оператор!</b>

👤 <b>Клієнт:</b> @{username}
📛 <b>Ім'я:</b> {display_name or 'Невідомо'}

⚠️ <b>Причина:</b> {reason}

💬 <b>Останнє повідомлення:</b>
<i>{last_message[:500]}</i>
"""
        if dialog_link:
            text += f"\n🔗 <a href='{dialog_link}'>Перейти до діалогу</a>"
        return self.send_message(text)

    def notify_new_lead(self, username: str, display_name: str,
                        phone: str = None, city: str = None,
                        delivery_address: str = None,
                        products: str = None, is_upsell: bool = False) -> bool:
        title = "➕ <b>ДОПРОДАЖ — НОВИЙ ЛІД!</b>" if is_upsell else "🎯 <b>НОВИЙ ЛІД!</b>"
        text = f"""{title}

👤 <b>Клієнт:</b> @{username}
📛 <b>ПІБ:</b> {display_name or 'Невідомо'}
"""
        if phone:
            text += f"📱 <b>Телефон:</b> {phone}\n"
        if city:
            text += f"🏙️ <b>Місто:</b> {city}\n"
        if delivery_address:
            text += f"📦 <b>Адреса:</b> {delivery_address}\n"
        if products:
            text += f"🛒 <b>Товар:</b> {products}\n"
        return self.send_message(text)

    def notify_contact_change(self, username: str, display_name: str,
                               change_description: str) -> bool:
        """Сповіщення коли клієнт хоче змінити контактні дані."""
        text = f"""✏️ <b>ЗАПИТ НА ЗМІНУ ДАНИХ</b>

👤 <b>Клієнт:</b> @{username}
📛 <b>Ім'я:</b> {display_name or 'Невідомо'}

📝 <b>Що хоче змінити:</b>
<i>{change_description[:500]}</i>

⚠️ Потрібна ручна зміна даних менеджером.
"""
        return self.send_message(text)

    def notify_new_order(self, username: str, order_data: dict) -> bool:
        text = f"""✅ <b>НОВЕ ЗАМОВЛЕННЯ!</b>

👤 <b>Клієнт:</b> @{username}
📛 <b>ПІБ:</b> {order_data.get('full_name', 'N/A')}
📱 <b>Телефон:</b> {order_data.get('phone', 'N/A')}
🏙️ <b>Місто:</b> {order_data.get('city', 'N/A')}
📦 <b>НП:</b> {order_data.get('nova_poshta', 'N/A')}

🛒 <b>Товари:</b>
{order_data.get('products', 'N/A')}

💰 <b>Сума:</b> {order_data.get('total_price', 'N/A')} грн
"""
        return self.send_message(text)

    def notify_error(self, error_message: str) -> bool:
        text = f"""❌ <b>ПОМИЛКА БОТА</b>

{error_message[:1000]}
"""
        return self.send_message(text)

    def notify_manager_chat_new_message(self, username: str, display_name: str,
                                        last_message: str, count: int = 1) -> bool:
        text = f"""👤 <b>Нове повідомлення від клієнта</b>

⚠️ <b>Увага:</b> У цьому чаті раніше писав менеджер вручну — бот не відповідає автоматично.

👤 <b>Клієнт:</b> @{username}
📛 <b>Ім'я:</b> {display_name or 'Невідомо'}
📩 <b>Нових повідомлень:</b> {count}

💬 <b>Останнє повідомлення:</b>
<i>{last_message[:500]}</i>
"""
        return self.send_message(text)

    def notify_unusual_question(self, username: str, question: str) -> bool:
        text = f"""❓ <b>Нестандартне питання</b>

👤 <b>Від:</b> @{username}

💬 <b>Питання:</b>
<i>{question[:500]}</i>

⚠️ AI не зміг відповісти автоматично.
"""
        return self.send_message(text)


if __name__ == '__main__':
    print("Зареєстровані адміни:", _load_admins())
