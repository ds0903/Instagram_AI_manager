"""
Telegram Notifier - сповіщення менеджеру
Ескалація при:
- Прямому запиті ("покликати людину", "менеджер")
- Виявленні конфлікту
- Нестандартних питаннях
"""
import os
import requests
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Відправка сповіщень в Telegram"""

    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
        self.enabled = bool(self.bot_token and self.chat_id)

        if self.enabled:
            logger.info("Telegram Notifier uvimkneno")
        else:
            logger.warning("Telegram Notifier vymkneno (nemaje TOKEN abo CHAT_ID)")

    def send_message(self, text: str, parse_mode: str = 'HTML') -> bool:
        """
        Відправити повідомлення в Telegram

        Args:
            text: Текст повідомлення
            parse_mode: HTML або Markdown

        Returns:
            bool: True якщо успішно
        """
        if not self.enabled:
            logger.warning("Telegram ne nalashtvano, povidomlennia ne vidpravleno")
            return False

        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': text,
                'parse_mode': parse_mode
            }

            response = requests.post(url, json=payload, timeout=10)

            if response.status_code == 200:
                logger.info("Telegram povidomlennia vidpravleno")
                return True
            else:
                logger.error(f"Telegram error: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Pomylka vidpravky v Telegram: {e}")
            return False

    def notify_escalation(self, username: str, display_name: str,
                          reason: str, last_message: str,
                          dialog_link: str = None) -> bool:
        """
        Сповіщення про ескалацію (передача оператору)

        Args:
            username: Instagram username
            display_name: Ім'я клієнта
            reason: Причина ескалації
            last_message: Останнє повідомлення клієнта
            dialog_link: Посилання на діалог (якщо є)

        Returns:
            bool: True якщо успішно
        """
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
                        phone: str = None, products: str = None) -> bool:
        """
        Сповіщення про нового ліда

        Args:
            username: Instagram username
            display_name: Ім'я клієнта
            phone: Телефон (якщо є)
            products: Товари які цікавлять

        Returns:
            bool: True якщо успішно
        """
        text = f"""🎯 <b>НОВИЙ ЛІД!</b>

👤 <b>Клієнт:</b> @{username}
📛 <b>Ім'я:</b> {display_name or 'Невідомо'}
"""

        if phone:
            text += f"📱 <b>Телефон:</b> {phone}\n"

        if products:
            text += f"🛒 <b>Цікавлять:</b> {products}\n"

        return self.send_message(text)

    def notify_new_order(self, username: str, order_data: dict) -> bool:
        """
        Сповіщення про нове замовлення

        Args:
            username: Instagram username
            order_data: Дані замовлення

        Returns:
            bool: True якщо успішно
        """
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
        """
        Сповіщення про помилку

        Args:
            error_message: Текст помилки

        Returns:
            bool: True якщо успішно
        """
        text = f"""❌ <b>ПОМИЛКА БОТА</b>

{error_message[:1000]}
"""
        return self.send_message(text)

    def notify_unusual_question(self, username: str, question: str) -> bool:
        """
        Сповіщення про нестандартне питання

        Args:
            username: Instagram username
            question: Текст питання

        Returns:
            bool: True якщо успішно
        """
        text = f"""❓ <b>Нестандартне питання</b>

👤 <b>Від:</b> @{username}

💬 <b>Питання:</b>
<i>{question[:500]}</i>

⚠️ AI не зміг відповісти автоматично.
"""
        return self.send_message(text)


def main():
    """Тест відправки повідомлення"""
    print("=" * 60)
    print("  TELEGRAM NOTIFIER TEST")
    print("=" * 60)

    notifier = TelegramNotifier()

    if not notifier.enabled:
        print("\n[WARNING] Telegram ne nalashtvano!")
        print("Dodaj v .env:")
        print("  TELEGRAM_BOT_TOKEN=your_bot_token")
        print("  TELEGRAM_CHAT_ID=your_chat_id")
        return

    print("\nVidpravliaju testove povidomlennia...")

    success = notifier.send_message(
        "🤖 <b>Test</b>\n\nInstagram AI Agent працює!",
        parse_mode='HTML'
    )

    if success:
        print("[OK] Povidomlennia vidpravleno!")
    else:
        print("[ERROR] Ne vdalosja vidpravyty")


if __name__ == '__main__':
    main()
