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
    # Всі 3 локації для перевірки непрочитаних чатів
    DM_LOCATIONS = [
        {'url': 'https://www.instagram.com/direct/inbox/', 'name': 'Директ'},
        {'url': 'https://www.instagram.com/direct/requests/', 'name': 'Запити'},
        {'url': 'https://www.instagram.com/direct/requests/hidden/', 'name': 'Приховані запити'},
    ]

    def __init__(self, driver, ai_agent):
        self.driver = driver
        self.ai_agent = ai_agent
        self.processed_messages = set()  # Вже оброблені повідомлення

    def go_to_location(self, url: str) -> bool:
        """Перехід на конкретну сторінку Direct (inbox/requests/hidden)."""
        try:
            self.driver.get(url)
            time.sleep(3)

            # Чекаємо завантаження саме списку чатів (div[@role='listitem'])
            # або хоча б контейнера сторінки (якщо чатів немає взагалі)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@role='listitem']"))
                )
            except Exception:
                # Можливо чатів немає на цій сторінці — це нормально
                logger.info(f"Чатів не знайдено на {url} (сторінка порожня)")

            logger.info(f"Відкрито: {url}")
            return True
        except Exception as e:
            logger.error(f"Помилка відкриття {url}: {e}")
            return False

    def go_to_inbox(self) -> bool:
        """Перехід в Direct inbox (зворотна сумісність)."""
        return self.go_to_location('https://www.instagram.com/direct/inbox/')

    def get_unread_chats(self) -> list:
        """
        Отримати список чатів з непрочитаними повідомленнями на поточній сторінці.
        Шукаємо по div[@role='listitem'] — це контейнер кожного чату.
        Непрочитані визначаються по span[data-visualcompletion="ignore"] з текстом "Unread"
        всередині цього контейнера (НЕ всередині <a>, а в батьківському listitem).
        """
        chats = []
        try:
            # Шукаємо ВСІ listitem — це батьківський контейнер кожного чату
            listitem_elements = self.driver.find_elements(
                By.XPATH, "//div[@role='listitem']"
            )

            logger.info(f"Знайдено {len(listitem_elements)} listitem елементів")

            for listitem in listitem_elements:
                try:
                    # Шукаємо посилання на чат всередині listitem
                    try:
                        link = listitem.find_element(By.XPATH, ".//a[contains(@href, '/direct/t/')]")
                        href = link.get_attribute('href')
                    except Exception:
                        # На сторінках requests може не бути прямого посилання —
                        # тоді сам listitem є клікабельним
                        href = None

                    # Отримуємо username/ім'я з чату
                    username = "unknown"
                    try:
                        spans = listitem.find_elements(By.XPATH, ".//span")
                        for span in spans:
                            text = span.text.strip()
                            # Пропускаємо службові тексти та порожні
                            if text and text.lower() != 'unread' and len(text) > 1:
                                username = text
                                break
                    except Exception:
                        pass

                    # Перевіряємо чи є непрочитані — шукаємо в БАТЬКІВСЬКОМУ listitem
                    # span[data-visualcompletion="ignore"] з div що містить "Unread"
                    unread = False
                    try:
                        unread_span = listitem.find_element(
                            By.XPATH, ".//span[@data-visualcompletion='ignore']"
                        )
                        inner_text = unread_span.text.strip()
                        if 'unread' in inner_text.lower():
                            unread = True
                    except Exception:
                        pass

                    chats.append({
                        'username': username,
                        'href': href,
                        'element': listitem,
                        'unread': unread
                    })
                except Exception:
                    continue

            unread_count = sum(1 for c in chats if c['unread'])
            logger.info(f"Знайдено {len(chats)} чатів, непрочитаних: {unread_count}")
            return chats

        except Exception as e:
            logger.error(f"Помилка отримання чатів: {e}")
            return []

    def get_all_unread_chats(self) -> list:
        """
        Обійти всі 3 локації (inbox, requests, hidden requests)
        і зібрати всі непрочитані чати.
        Повертає: [{'username': str, 'href': str, 'element': WebElement, 'location': str, 'location_url': str}]
        """
        all_unread = []

        for location in self.DM_LOCATIONS:
            url = location['url']
            name = location['name']

            logger.info(f"Перевіряю: {name} ({url})")

            if not self.go_to_location(url):
                logger.warning(f"Не вдалося відкрити {name}, пропускаю")
                continue

            chats = self.get_unread_chats()
            unread_in_location = [c for c in chats if c['unread']]

            if unread_in_location:
                logger.info(f"  {name}: знайдено {len(unread_in_location)} непрочитаних")
                for chat in unread_in_location:
                    all_unread.append({
                        'username': chat['username'],
                        'href': chat['href'],
                        'element': chat['element'],
                        'location': name,
                        'location_url': url,
                    })
            else:
                logger.info(f"  {name}: немає непрочитаних")

            time.sleep(random.uniform(1, 2))

        logger.info(f"Всього непрочитаних чатів у всіх локаціях: {len(all_unread)}")
        return all_unread

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

    def process_chat_by_click(self, chat_info: dict) -> bool:
        """
        Обробка чату через клік по елементу (для requests/hidden де немає прямого href).
        Спочатку повертаємось на сторінку локації, знаходимо елемент заново і клікаємо.
        """
        try:
            location_url = chat_info.get('location_url')
            username = chat_info.get('username', 'unknown')

            # Повертаємось на сторінку де був чат
            if location_url:
                self.go_to_location(location_url)

            # Знаходимо непрочитані елементи заново (після навігації елементи стають stale)
            listitem_elements = self.driver.find_elements(By.XPATH, "//div[@role='listitem']")

            for listitem in listitem_elements:
                try:
                    # Перевіряємо чи це потрібний чат по username
                    spans = listitem.find_elements(By.XPATH, ".//span")
                    found_username = False
                    for span in spans:
                        if span.text.strip() == username:
                            found_username = True
                            break

                    if not found_username:
                        continue

                    # Клікаємо на елемент щоб відкрити чат
                    listitem.click()
                    time.sleep(2)

                    # Далі стандартна обробка
                    chat_username = self.get_chat_username()
                    display_name = self.get_display_name()

                    logger.info(f"Обробка чату (клік): {chat_username} ({display_name})")

                    last_message = self.get_last_message()
                    if not last_message or not last_message.get('is_from_user'):
                        logger.info(f"Немає нових повідомлень від користувача в {chat_username}")
                        return False

                    content = last_message['content']
                    timestamp = last_message.get('timestamp')

                    msg_key = f"{chat_username}:{content[:50]}"
                    if msg_key in self.processed_messages:
                        logger.info(f"Повідомлення вже оброблено: {msg_key}")
                        return False

                    response = self.ai_agent.process_message(
                        username=chat_username,
                        content=content,
                        display_name=display_name,
                        message_type='text',
                        message_timestamp=timestamp
                    )

                    if not response:
                        return False

                    success = self.send_message(response)
                    if success:
                        self.processed_messages.add(msg_key)
                        logger.info(f"Успішно відповіли {chat_username}")

                    return success

                except Exception as e:
                    logger.error(f"Помилка кліку по чату: {e}")
                    continue

            logger.warning(f"Не знайдено чат {username} для кліку")
            return False

        except Exception as e:
            logger.error(f"Помилка process_chat_by_click: {e}")
            return False

    def run_inbox_loop(self, check_interval: int = 30, heartbeat_callback=None):
        """
        Головний цикл: перевіряє ВСІ 3 локації (inbox, requests, hidden),
        обробляє нові повідомлення.

        Args:
            check_interval: інтервал перевірки в секундах
            heartbeat_callback: функція для оновлення heartbeat (watchdog)
        """
        logger.info(f"Запуск inbox loop, інтервал: {check_interval}с")
        logger.info(f"Локації для перевірки: {[loc['name'] for loc in self.DM_LOCATIONS]}")

        def heartbeat(msg: str = None):
            if heartbeat_callback:
                heartbeat_callback(msg)

        while True:
            try:
                heartbeat("Ітерація inbox loop — обхід всіх локацій")

                # Збираємо непрочитані чати з усіх 3 локацій
                all_unread = self.get_all_unread_chats()
                heartbeat(f"Знайдено {len(all_unread)} непрочитаних")

                if all_unread:
                    logger.info(f"Знайдено {len(all_unread)} непрочитаних чатів у всіх локаціях")

                    for chat in all_unread:
                        heartbeat(f"Обробка чату: {chat.get('username', 'unknown')} [{chat.get('location')}]")
                        logger.info(f"Обробка: {chat['username']} з {chat['location']}")

                        if chat.get('href'):
                            # Є пряме посилання — переходимо по href
                            self.process_chat(chat['href'])
                        else:
                            # Немає href (requests/hidden) — клікаємо по елементу
                            self.process_chat_by_click(chat)
                        time.sleep(random.uniform(2, 5))  # Пауза між чатами
                else:
                    logger.info("Немає непрочитаних чатів у жодній локації")

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
