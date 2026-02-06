"""
Instagram Direct Handler
Читання та відправка повідомлень в Direct через Selenium
"""
import time
import random
import logging
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)


class DirectHandler:
    def __init__(self, driver, ai_agent):
        self.driver = driver
        self.ai_agent = ai_agent
        self.processed_messages = set()  # Вже оброблені повідомлення

    def go_to_inbox(self) -> bool:
        """Перехід в Direct inbox."""
        try:
            self.driver.get('https://www.instagram.com/direct/inbox/')
            time.sleep(3)

            # Чекаємо завантаження
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'x9f619')]"))
            )

            logger.info("Direct inbox відкрито")
            return True
        except Exception as e:
            logger.error(f"Помилка відкриття inbox: {e}")
            return False

    def get_unread_chats(self) -> list:
        """
        Отримати список чатів з непрочитаними повідомленнями.
        Повертає list словників: [{'username': str, 'element': WebElement, 'unread': bool}]
        """
        chats = []
        try:
            # Шукаємо всі чати в списку
            chat_elements = self.driver.find_elements(
                By.XPATH, "//div[@role='listitem']//a[contains(@href, '/direct/t/')]"
            )

            for chat_elem in chat_elements:
                try:
                    href = chat_elem.get_attribute('href')
                    # Отримуємо username з чату (якщо є)
                    username_elem = chat_elem.find_element(By.XPATH, ".//span")
                    username = username_elem.text if username_elem else "unknown"

                    # Перевіряємо чи є непрочитані (блакитна крапка)
                    unread = False
                    try:
                        # Шукаємо індикатор непрочитаного
                        chat_elem.find_element(By.XPATH, ".//div[contains(@class, 'unread')]")
                        unread = True
                    except Exception:
                        pass

                    chats.append({
                        'username': username,
                        'href': href,
                        'element': chat_elem,
                        'unread': unread
                    })
                except Exception:
                    continue

            logger.info(f"Знайдено {len(chats)} чатів, з них непрочитаних: {sum(1 for c in chats if c['unread'])}")
            return chats

        except Exception as e:
            logger.error(f"Помилка отримання чатів: {e}")
            return []

    def open_chat(self, chat_href: str) -> bool:
        """Відкрити конкретний чат."""
        try:
            self.driver.get(chat_href)
            time.sleep(2)

            # Чекаємо завантаження чату
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
            )

            logger.info(f"Чат відкрито: {chat_href}")
            return True
        except Exception as e:
            logger.error(f"Помилка відкриття чату: {e}")
            return False

    def get_chat_messages(self) -> list:
        """
        Отримати повідомлення з відкритого чату.
        Повертає list: [{'role': 'user'/'assistant', 'content': str, 'timestamp': datetime}]
        """
        messages = []
        try:
            # Шукаємо всі повідомлення в чаті
            message_elements = self.driver.find_elements(
                By.XPATH, "//div[contains(@class, 'x1lliihq')]//span"
            )

            for msg_elem in message_elements:
                try:
                    content = msg_elem.text
                    if not content or len(content) < 1:
                        continue

                    # Визначаємо чи це наше повідомлення чи клієнта
                    parent = msg_elem.find_element(By.XPATH, "./ancestor::div[contains(@class, 'message')]")
                    is_own = 'own' in parent.get_attribute('class').lower() if parent else False

                    messages.append({
                        'role': 'assistant' if is_own else 'user',
                        'content': content,
                        'timestamp': datetime.now()
                    })
                except Exception:
                    continue

            return messages

        except Exception as e:
            logger.error(f"Помилка читання повідомлень: {e}")
            return []

    def get_last_message(self) -> dict:
        """Отримати останнє повідомлення в чаті."""
        try:
            # Шукаємо останнє повідомлення
            message_divs = self.driver.find_elements(
                By.XPATH, "//div[@role='row']//div[contains(@class, 'x1lliihq')]"
            )

            if not message_divs:
                return None

            last_msg_div = message_divs[-1]

            # Отримуємо текст
            try:
                content_span = last_msg_div.find_element(By.XPATH, ".//span")
                content = content_span.text
            except Exception:
                content = last_msg_div.text

            if not content:
                return None

            # Визначаємо від кого повідомлення
            parent_classes = last_msg_div.get_attribute('class') or ''

            # Спрощена логіка - за замовчуванням вважаємо що від користувача
            is_from_user = True

            return {
                'content': content,
                'is_from_user': is_from_user,
                'timestamp': datetime.now()
            }

        except Exception as e:
            logger.error(f"Помилка отримання останнього повідомлення: {e}")
            return None

    def send_message(self, text: str) -> bool:
        """Відправити повідомлення в поточний чат."""
        try:
            # Шукаємо поле вводу
            textbox = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
            )

            # Клікаємо на поле
            textbox.click()
            time.sleep(0.5)

            # Вводимо текст посимвольно (імітація людини)
            for char in text:
                textbox.send_keys(char)
                time.sleep(random.uniform(0.02, 0.08))

            time.sleep(0.5)

            # Відправляємо (Enter)
            textbox.send_keys(Keys.RETURN)
            time.sleep(1)

            logger.info(f"Повідомлення відправлено: {text[:50]}...")
            return True

        except Exception as e:
            logger.error(f"Помилка відправки повідомлення: {e}")
            return False

    def get_chat_username(self) -> str:
        """Отримати username співрозмовника з відкритого чату."""
        try:
            # Шукаємо username в хедері чату
            header = self.driver.find_element(
                By.XPATH, "//header//a[contains(@href, '/')]//span"
            )
            username = header.text
            return username
        except Exception:
            try:
                # Альтернативний спосіб
                header = self.driver.find_element(
                    By.XPATH, "//div[contains(@class, 'x1n2onr6')]//span[contains(@class, 'x1lliihq')]"
                )
                return header.text
            except Exception:
                return "unknown_user"

    def get_display_name(self) -> str:
        """Отримати display name (ім'я) співрозмовника."""
        try:
            # Шукаємо display name в хедері
            name_elem = self.driver.find_element(
                By.XPATH, "//header//div[contains(@class, 'x1lliihq')]//span"
            )
            return name_elem.text
        except Exception:
            return None

    def process_chat(self, chat_href: str) -> bool:
        """
        Обробка одного чату:
        1. Відкрити чат
        2. Прочитати останнє повідомлення
        3. Згенерувати відповідь через AI
        4. Відправити відповідь
        """
        try:
            # 1. Відкриваємо чат
            if not self.open_chat(chat_href):
                return False

            time.sleep(1)

            # 2. Отримуємо username та display_name
            username = self.get_chat_username()
            display_name = self.get_display_name()

            logger.info(f"Обробка чату: {username} ({display_name})")

            # 3. Отримуємо останнє повідомлення
            last_message = self.get_last_message()

            if not last_message or not last_message.get('is_from_user'):
                logger.info(f"Немає нових повідомлень від користувача в {username}")
                return False

            content = last_message['content']
            timestamp = last_message.get('timestamp')

            # 4. Перевіряємо чи не оброблено вже
            msg_key = f"{username}:{content[:50]}"
            if msg_key in self.processed_messages:
                logger.info(f"Повідомлення вже оброблено: {msg_key}")
                return False

            # 5. Обробка через AI Agent
            response = self.ai_agent.process_message(
                username=username,
                content=content,
                display_name=display_name,
                message_type='text',
                message_timestamp=timestamp
            )

            if not response:
                return False

            # 6. Відправляємо відповідь
            success = self.send_message(response)

            if success:
                self.processed_messages.add(msg_key)
                logger.info(f"Успішно відповіли {username}")

            return success

        except Exception as e:
            logger.error(f"Помилка обробки чату: {e}")
            return False

    def run_inbox_loop(self, check_interval: int = 30, heartbeat_callback=None):
        """
        Головний цикл: перевіряє inbox, обробляє нові повідомлення.

        Args:
            check_interval: інтервал перевірки в секундах
            heartbeat_callback: функція для оновлення heartbeat (watchdog)
        """
        logger.info(f"Запуск inbox loop, інтервал: {check_interval}с")

        def heartbeat(msg: str = None):
            if heartbeat_callback:
                heartbeat_callback(msg)

        while True:
            try:
                heartbeat("Ітерація inbox loop")

                # Переходимо в inbox
                if not self.go_to_inbox():
                    time.sleep(check_interval)
                    continue

                heartbeat("Отримання непрочитаних чатів")

                # Отримуємо непрочитані чати
                chats = self.get_unread_chats()
                unread_chats = [c for c in chats if c['unread']]

                if unread_chats:
                    logger.info(f"Знайдено {len(unread_chats)} непрочитаних чатів")

                    for chat in unread_chats:
                        heartbeat(f"Обробка чату: {chat.get('username', 'unknown')}")
                        self.process_chat(chat['href'])
                        time.sleep(random.uniform(2, 5))  # Пауза між чатами

                # Чекаємо перед наступною перевіркою
                logger.info(f"Чекаємо {check_interval}с...")
                heartbeat("Очікування наступної перевірки")
                time.sleep(check_interval)

            except KeyboardInterrupt:
                logger.info("Зупинка за запитом користувача")
                raise  # Передаємо вверх для коректної обробки
            except Exception as e:
                logger.error(f"Помилка в inbox loop: {e}")
                heartbeat("Помилка в циклі, повтор")
                time.sleep(check_interval)
