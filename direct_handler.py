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
from selenium.webdriver.common.action_chains import ActionChains
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

    # [DEBUG] Фільтр — відповідаємо тільки цьому username (None = всім)
    DEBUG_ONLY_USERNAME = "Danyl"  # TODO: прибрати після дебагу (поставити None)

    def __init__(self, driver, ai_agent):
        self.driver = driver
        self.ai_agent = ai_agent
        self.processed_messages = set()  # Вже оброблені повідомлення
        self._last_user_message_element = None  # Елемент останнього повідомлення користувача (для hover+reply)

    def go_to_location(self, url: str) -> bool:
        """Перехід на конкретну сторінку Direct (inbox/requests/hidden)."""
        try:
            self.driver.get(url)
            time.sleep(3)

            # Чекаємо завантаження чатів — на inbox це role="listitem",
            # на requests/hidden це role="button" всередині списку
            try:
                WebDriverWait(self.driver, 10).until(
                    lambda d: d.find_elements(By.XPATH, "//div[@role='listitem']")
                    or d.find_elements(By.XPATH, "//div[@role='button'][@tabindex='0']")
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

    # def get_unread_chats(self) -> list:
    #     """
    #     Отримати непрочитані чати на поточній сторінці.
    #     Стратегія: шукаємо ЗНИЗУ ВГОРУ — спочатку знаходимо span[data-visualcompletion="ignore"]
    #     з текстом "Unread", потім піднімаємось до батьківського клікабельного елемента.
    #
    #     На inbox: контейнер = div[@role='listitem']
    #     На requests/hidden: контейнер = div[@role='button']
    #     """
    #     chats = []
    #     try:
    #         unread_indicators = self.driver.find_elements(
    #             By.XPATH, "//span[@data-visualcompletion='ignore']"
    #         )
    #         logger.info(f"Знайдено {len(unread_indicators)} span[data-visualcompletion='ignore']")
    #
    #         for indicator in unread_indicators:
    #             try:
    #                 inner_text = indicator.text.strip()
    #                 if 'unread' not in inner_text.lower():
    #                     continue
    #
    #                 clickable = None
    #                 try:
    #                     clickable = indicator.find_element(
    #                         By.XPATH, "./ancestor::div[@role='button']"
    #                     )
    #                 except Exception:
    #                     pass
    #                 if clickable is None:
    #                     try:
    #                         clickable = indicator.find_element(
    #                             By.XPATH, "./ancestor::div[@role='listitem']"
    #                         )
    #                     except Exception:
    #                         pass
    #                 if clickable is None:
    #                     continue
    #
    #                 username = "unknown"
    #                 try:
    #                     title_span = clickable.find_element(By.XPATH, ".//span[@title]")
    #                     username = title_span.get_attribute('title')
    #                 except Exception:
    #                     try:
    #                         spans = clickable.find_elements(By.XPATH, ".//span")
    #                         for span in spans:
    #                             text = span.text.strip()
    #                             if text and text.lower() != 'unread' and len(text) > 1:
    #                                 username = text
    #                                 break
    #                     except Exception:
    #                         pass
    #
    #                 href = None
    #                 try:
    #                     link = clickable.find_element(By.XPATH, ".//a[contains(@href, '/direct/')]")
    #                     href = link.get_attribute('href')
    #                 except Exception:
    #                     pass
    #
    #                 chats.append({
    #                     'username': username,
    #                     'href': href,
    #                     'element': clickable,
    #                     'unread': True
    #                 })
    #                 logger.info(f"  Непрочитаний чат: {username}")
    #
    #             except Exception:
    #                 continue
    #
    #         logger.info(f"Знайдено {len(chats)} непрочитаних чатів")
    #         return chats
    #     except Exception as e:
    #         logger.error(f"Помилка отримання чатів: {e}")
    #         return []

    def get_all_chats(self) -> list:
        """
        [DEBUG] Отримати ВСІ чати на поточній сторінці (не тільки непрочитані).
        Шукаємо всі span[@title] (ім'я користувача) і піднімаємось до клікабельного контейнера.
        """
        chats = []
        try:
            # Шукаємо всі span з title — це імена користувачів у списку чатів
            title_spans = self.driver.find_elements(By.XPATH, "//span[@title]")

            logger.info(f"[DEBUG] Знайдено {len(title_spans)} span[@title] на сторінці")

            for title_span in title_spans:
                try:
                    username = title_span.get_attribute('title')
                    if not username or len(username) < 1:
                        continue

                    # Піднімаємось до клікабельного контейнера
                    clickable = None
                    try:
                        clickable = title_span.find_element(
                            By.XPATH, "./ancestor::div[@role='button']"
                        )
                    except Exception:
                        pass

                    if clickable is None:
                        try:
                            clickable = title_span.find_element(
                                By.XPATH, "./ancestor::div[@role='listitem']"
                            )
                        except Exception:
                            pass

                    if clickable is None:
                        continue

                    # Шукаємо href якщо є
                    href = None
                    try:
                        link = clickable.find_element(By.XPATH, ".//a[contains(@href, '/direct/')]")
                        href = link.get_attribute('href')
                    except Exception:
                        pass

                    chats.append({
                        'username': username,
                        'href': href,
                        'element': clickable,
                        'unread': True  # В debug режимі вважаємо всі як "нові"
                    })

                    logger.info(f"  [DEBUG] Чат: {username} (href={href is not None})")

                except Exception:
                    continue

            logger.info(f"[DEBUG] Знайдено {len(chats)} чатів всього")
            return chats

        except Exception as e:
            logger.error(f"Помилка отримання чатів: {e}")
            return []

    def try_accept_request(self) -> bool:
        """
        Перевірити чи є кнопка Accept (прийняти запит на переписку).
        Кнопка Accept — це div[@role='button'] з текстом "Accept" прямо всередині (без span).
        Якщо є — натиснути і дочекатись завантаження.
        """
        try:
            # Кнопка Accept — div[@role='button'] з прямим текстом "Accept"
            accept_buttons = self.driver.find_elements(
                By.XPATH, "//div[@role='button'][text()='Accept']"
            )

            if not accept_buttons:
                logger.info("Кнопка Accept не знайдена (це звичайний чат)")
                return False

            logger.info(f"Знайдено кнопку Accept!")
            accept_buttons[0].click()
            logger.info("Натиснуто Accept — запит на переписку прийнято!")

            # Чекаємо поки чат повністю завантажиться (textbox з'явиться)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
                )
                logger.info("Чат завантажено після Accept (textbox знайдено)")
            except Exception:
                logger.warning("Textbox не з'явився після Accept, чекаємо ще...")
                time.sleep(5)

            return True

        except Exception as e:
            logger.error(f"Помилка пошуку/кліку Accept: {e}")
            return False

    def get_all_unread_chats(self) -> list:
        """
        Обійти всі 3 локації (inbox, requests, hidden requests)
        і зібрати чати.
        Повертає: [{'username': str, 'href': str, 'element': WebElement, 'location': str, 'location_url': str}]
        """
        all_chats = []

        for location in self.DM_LOCATIONS:
            url = location['url']
            name = location['name']

            logger.info(f"Перевіряю: {name} ({url})")

            if not self.go_to_location(url):
                logger.warning(f"Не вдалося відкрити {name}, пропускаю")
                continue

            # [DEBUG] Використовуємо get_all_chats() — всі чати, не тільки непрочитані
            # Коли дебаг закінчиться — замінити на get_unread_chats()
            found_chats = self.get_all_chats()
            # found_chats = self.get_unread_chats()  # TODO: розкоментувати після дебагу

            if found_chats:
                logger.info(f"  {name}: знайдено {len(found_chats)} чатів")
                for chat in found_chats:
                    all_chats.append({
                        'username': chat['username'],
                        'href': chat['href'],
                        'element': chat['element'],
                        'location': name,
                        'location_url': url,
                    })
            else:
                logger.info(f"  {name}: чатів не знайдено")

            time.sleep(random.uniform(1, 2))

        logger.info(f"[DEBUG] Всього чатів у всіх локаціях: {len(all_chats)}")
        return all_chats

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

    def _is_message_from_user(self, msg_element, chat_username: str) -> bool:
        """
        Визначити чи повідомлення від користувача через <a href="/username">.
        Піднімаємось по DOM від елемента повідомлення і шукаємо profile link.
        Якщо знайшли <a href="/username"> — це повідомлення користувача.
        Якщо не знайшли — це наше повідомлення (assistant).
        """
        try:
            return self.driver.execute_script("""
                var msg = arguments[0];
                var username = arguments[1].toLowerCase();

                var current = msg;
                for (var i = 0; i < 12; i++) {
                    current = current.parentElement;
                    if (!current || current === document.body) break;

                    // Зупиняємось на великих контейнерах
                    var role = current.getAttribute('role');
                    if (role === 'grid' || role === 'main' ||
                        current.tagName === 'MAIN' || current.tagName === 'SECTION') {
                        break;
                    }

                    // Шукаємо profile link
                    var link = current.querySelector('a[aria-label^="Open the profile page"]');
                    if (link) {
                        var href = (link.getAttribute('href') || '').toLowerCase();
                        return href.includes('/' + username);
                    }
                }
                return false;
            """, msg_element, chat_username)
        except Exception as e:
            logger.error(f"Помилка визначення відправника: {e}")
            return False

    def get_last_message(self, chat_username: str = None) -> dict:
        """
        Отримати останнє повідомлення в чаті та визначити відправника.
        Використовує <a href="/username"> для визначення повідомлень користувача.
        Повертає dict з 'content', 'is_from_user', 'element', 'timestamp'.
        """
        if not chat_username:
            chat_username = self.get_chat_username()

        # Шукаємо всі повідомлення
        msg_divs = self.driver.find_elements(
            By.XPATH, "//div[@role='presentation']//div[@dir='auto']"
        )

        # Fallback
        if not msg_divs:
            msg_divs = self.driver.find_elements(
                By.XPATH, "//span[@dir='auto']//div[@dir='auto']"
            )

        if not msg_divs:
            logger.warning("Не вдалося знайти повідомлення в чаті")
            return None

        # Збираємо всі повідомлення з визначенням ролей
        all_messages = []
        for msg_div in msg_divs:
            text = msg_div.text.strip()
            if not text:
                continue

            is_from_user = self._is_message_from_user(msg_div, chat_username)

            all_messages.append({
                'content': text,
                'is_from_user': is_from_user,
                'element': msg_div,
                'timestamp': datetime.now()
            })

        if not all_messages:
            logger.warning("Не знайдено повідомлень з текстом")
            return None

        # Логуємо всі знайдені повідомлення для дебагу
        for i, msg in enumerate(all_messages):
            role_str = 'USER' if msg['is_from_user'] else 'ASSISTANT'
            logger.info(f"  [{i+1}] {role_str}: '{msg['content'][:60]}'")

        # Зберігаємо останнє повідомлення КОРИСТУВАЧА для hover+reply
        last_user_msgs = [m for m in all_messages if m['is_from_user']]
        self._last_user_message_element = last_user_msgs[-1]['element'] if last_user_msgs else None

        # Повертаємо ОСТАННЄ повідомлення (щоб перевірити чи треба відповідати)
        last = all_messages[-1]
        logger.info(f"Останнє повідомлення: '{last['content'][:50]}' "
                     f"(від {'користувача' if last['is_from_user'] else 'нас'})")

        return last

    def hover_and_click_reply(self, message_element, chat_username: str = None) -> bool:
        """
        Навести мишку на повідомлення користувача і натиснути кнопку Reply.
        Кнопки (реакція, відповісти, поділитися) з'являються при hover
        в контейнері div[style*='--x-width: 96px'] поруч з повідомленням.
        Reply — це 2-га кнопка (span з svg).
        """
        try:
            # Піднімаємось вище — до контейнера всього повідомлення (з аватаром і toolbar)
            hover_target = message_element
            try:
                # Від div[@dir='auto'] піднімаємось до великого контейнера повідомлення
                # Шукаємо предка, який містить toolbar div[style*='--x-width: 96px']
                hover_target = self.driver.execute_script("""
                    var el = arguments[0];
                    var current = el;
                    for (var i = 0; i < 10; i++) {
                        current = current.parentElement;
                        if (!current) break;
                        var toolbar = current.querySelector('div[style*="--x-width: 96px"]');
                        if (toolbar) return current;
                    }
                    return el;
                """, message_element)
            except Exception:
                pass

            # Hover на контейнер повідомлення
            logger.info("Наводимо мишку на повідомлення для Reply...")
            actions = ActionChains(self.driver)
            actions.move_to_element(hover_target).perform()
            time.sleep(2)

            reply_btn = None

            # Спосіб 1: Знаходимо toolbar контейнер (div[style*='--x-width: 96px'])
            # і шукаємо кнопки (span з svg) всередині
            try:
                toolbars = self.driver.find_elements(
                    By.CSS_SELECTOR, "div[style*='--x-width: 96px']"
                )
                for toolbar in toolbars:
                    # Шукаємо елементи з SVG (це іконки кнопок)
                    buttons = toolbar.find_elements(By.XPATH,
                        ".//*[local-name()='svg']/ancestor::span[1] | "
                        ".//*[local-name()='svg']/ancestor::div[@role='button'][1] | "
                        ".//*[local-name()='svg']/ancestor::span[@role='button'][1]"
                    )
                    if not buttons:
                        # Fallback: будь-які span або div що містять svg
                        buttons = toolbar.find_elements(By.XPATH,
                            ".//*[.//*[local-name()='svg']]"
                        )
                    if buttons:
                        logger.info(f"Toolbar знайдено з {len(buttons)} кнопками")
                        # Reply — 2-га кнопка: [emoji/реакція, reply/відповісти, more/поділитися]
                        if len(buttons) >= 2:
                            reply_btn = buttons[1]
                            logger.info(f"Reply кнопка знайдена (позиція 2 з {len(buttons)})")
                        break
            except Exception as e:
                logger.info(f"Toolbar пошук: {e}")

            # Спосіб 2: aria-label (без role='button')
            if not reply_btn:
                for label in ['Reply', 'reply', 'Ответ', 'Відповісти']:
                    try:
                        reply_btn = self.driver.find_element(
                            By.XPATH, f"//*[contains(@aria-label, '{label}')]"
                        )
                        if reply_btn:
                            logger.info(f"Reply знайдено по aria-label '{label}'")
                            break
                    except Exception:
                        continue

            # Спосіб 3: title атрибут
            if not reply_btn and chat_username:
                try:
                    reply_btn = self.driver.find_element(
                        By.XPATH, f"//*[contains(@title, '{chat_username}')]"
                    )
                    logger.info(f"Reply знайдено по title з username")
                except Exception:
                    pass

            if reply_btn:
                reply_btn.click()
                time.sleep(1)
                logger.info("Кнопку Reply натиснуто!")
                return True
            else:
                logger.warning("Кнопку Reply не знайдено після hover")
                return False

        except Exception as e:
            logger.error(f"Помилка hover/reply: {e}")
            return False

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
        """
        Отримати username співрозмовника з відкритого чату.
        Шукаємо a[aria-label^='Open the profile page of'] — це посилання на профіль
        біля повідомлень. В href="/username" лежить справжній username.
        """
        # Спосіб 1: a[aria-label] з повідомлень — найнадійніший (href="/qarbbon")
        try:
            profile_links = self.driver.find_elements(
                By.XPATH, "//a[starts-with(@aria-label, 'Open the profile page')]"
            )
            if profile_links:
                href = profile_links[0].get_attribute('href') or ''
                # Витягуємо username з href: "https://instagram.com/qarbbon" або "/qarbbon"
                username = href.rstrip('/').split('/')[-1]
                if username and len(username) > 0:
                    logger.info(f"Username (profile link): {username}")
                    return username
        except Exception:
            pass

        # Спосіб 2: span[@title] в хедері
        try:
            title_span = self.driver.find_element(By.XPATH, "//header//span[@title]")
            username = title_span.get_attribute('title')
            if username:
                logger.info(f"Username (header title): {username}")
                return username
        except Exception:
            pass

        # Спосіб 3: перший span з текстом в header
        try:
            header_spans = self.driver.find_elements(By.XPATH, "//header//span")
            for span in header_spans:
                text = span.text.strip()
                if text and len(text) > 1:
                    logger.info(f"Username (header span): {text}")
                    return text
        except Exception:
            pass

        logger.warning("Не вдалося отримати username")
        return "unknown_user"

    def get_display_name(self) -> str:
        """Отримати display name (ім'я) з хедера чату."""
        try:
            header_spans = self.driver.find_elements(By.XPATH, "//header//span")
            for span in header_spans:
                text = span.text.strip()
                if text and len(text) > 1:
                    return text
        except Exception:
            pass
        return None

    def process_chat(self, chat_href: str) -> bool:
        """
        Обробка одного чату:
        1. Відкрити чат
        2. Прочитати останнє повідомлення (з визначенням відправника через href)
        3. Hover + Reply на повідомлення користувача
        4. Згенерувати відповідь через AI
        5. Відправити відповідь
        """
        try:
            # 1. Відкриваємо чат
            if not self.open_chat(chat_href):
                return False

            time.sleep(1)

            # 1.5. Перевіряємо чи є кнопка Accept (запит на переписку)
            self.try_accept_request()

            # 2. Отримуємо username та display_name
            username = self.get_chat_username()
            display_name = self.get_display_name()

            logger.info(f"Обробка чату: {username} ({display_name})")

            # 3. Отримуємо останнє повідомлення (з визначенням ролі через profile link)
            last_message = self.get_last_message(chat_username=username)

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

            # 6. Hover + Reply на повідомлення користувача
            msg_element = last_message.get('element') or self._last_user_message_element
            if msg_element:
                self.hover_and_click_reply(msg_element, chat_username=username)

            # 7. Відправляємо відповідь
            success = self.send_message(response)

            if success:
                self.processed_messages.add(msg_key)
                logger.info(f"Успішно відповіли {username}")

            return success

        except Exception as e:
            logger.error(f"Помилка обробки чату: {e}")
            return False

    def open_chat_by_click(self, chat_info: dict) -> bool:
        """
        Відкрити чат через клік по елементу (для requests/hidden де немає прямого href).
        Повертаємось на сторінку локації, знаходимо елемент заново і клікаємо.
        """
        try:
            location_url = chat_info.get('location_url')
            username = chat_info.get('username', 'unknown')

            # Повертаємось на сторінку де був чат
            if location_url:
                self.go_to_location(location_url)

            # Знаходимо потрібний чат заново по username через span[@title]
            # (після навігації старі елементи стають stale)
            target_spans = self.driver.find_elements(By.XPATH, f"//span[@title='{username}']")

            if not target_spans:
                logger.warning(f"Не знайдено span[@title='{username}'] на сторінці")
                return False

            for target_span in target_spans:
                try:
                    # Піднімаємось до клікабельного батька
                    clickable = None
                    try:
                        clickable = target_span.find_element(
                            By.XPATH, "./ancestor::div[@role='button']"
                        )
                    except Exception:
                        try:
                            clickable = target_span.find_element(
                                By.XPATH, "./ancestor::div[@role='listitem']"
                            )
                        except Exception:
                            continue

                    # Клікаємо на елемент щоб відкрити чат
                    logger.info(f"Клікаю на чат: {username}")
                    clickable.click()
                    time.sleep(3)

                    logger.info(f"Чат {username} відкрито через клік")
                    return True

                except Exception as e:
                    logger.error(f"Помилка кліку по чату {username}: {e}")
                    continue

            logger.warning(f"Не знайдено чат {username} для кліку")
            return False

        except Exception as e:
            logger.error(f"Помилка open_chat_by_click: {e}")
            return False

    def process_chat_by_click(self, chat_info: dict) -> bool:
        """
        Повна обробка чату: відкрити → Accept → визначити ролі → hover+reply → AI → відповідь.
        """
        try:
            username = chat_info.get('username', 'unknown')

            # 1. Відкриваємо чат кліком
            if not self.open_chat_by_click(chat_info):
                return False

            # 2. Перевіряємо чи є кнопка Accept (запит на переписку)
            accepted = self.try_accept_request()
            if accepted:
                logger.info(f"Accept натиснуто для {username}, чекаємо завантаження...")
                time.sleep(2)

            # 3. Отримуємо username та display_name
            chat_username = self.get_chat_username()
            display_name = self.get_display_name()

            # Якщо не вдалось отримати username з хедера — беремо з chat_info
            if chat_username == "unknown_user":
                chat_username = username
                display_name = username

            logger.info(f"Обробка чату (клік): {chat_username} ({display_name})")

            # 4. Отримуємо останнє повідомлення (з визначенням ролі через profile link)
            last_message = self.get_last_message(chat_username=chat_username)
            if not last_message or not last_message.get('is_from_user'):
                logger.info(f"Немає нових повідомлень від користувача в {chat_username}")
                return False

            content = last_message['content']
            timestamp = last_message.get('timestamp')

            # 5. Перевіряємо чи не оброблено вже
            msg_key = f"{chat_username}:{content[:50]}"
            if msg_key in self.processed_messages:
                logger.info(f"Повідомлення вже оброблено: {msg_key}")
                return False

            # 6. AI обробка
            response = self.ai_agent.process_message(
                username=chat_username,
                content=content,
                display_name=display_name,
                message_type='text',
                message_timestamp=timestamp
            )

            if not response:
                return False

            # 7. Hover + Reply на повідомлення користувача
            msg_element = last_message.get('element') or self._last_user_message_element
            if msg_element:
                self.hover_and_click_reply(msg_element, chat_username=chat_username)

            # 8. Відправка відповіді
            success = self.send_message(response)
            if success:
                self.processed_messages.add(msg_key)
                logger.info(f"Успішно відповіли {chat_username}")

            return success

        except Exception as e:
            logger.error(f"Помилка process_chat_by_click: {e}")
            return False

    def run_inbox_loop(self, check_interval: int = 30, heartbeat_callback=None):
        """
        Головний цикл: перевіряє локації ПО ЧЕРЗІ.
        Директ → знайшли → відповіли на всі → Запити → відповіли → Приховані → відповіли.

        Args:
            check_interval: інтервал перевірки в секундах
            heartbeat_callback: функція для оновлення heartbeat (watchdog)
        """
        logger.info(f"Запуск inbox loop, інтервал: {check_interval}с")
        logger.info(f"Локації для перевірки: {[loc['name'] for loc in self.DM_LOCATIONS]}")
        if self.DEBUG_ONLY_USERNAME:
            logger.info(f"[DEBUG] Фільтр: відповідаємо тільки користувачу '{self.DEBUG_ONLY_USERNAME}'")

        def heartbeat(msg: str = None):
            if heartbeat_callback:
                heartbeat_callback(msg)

        while True:
            try:
                heartbeat("Ітерація inbox loop")
                total_processed = 0

                # Обходимо кожну локацію ПО ЧЕРЗІ: знайшли чати → відповіли → наступна
                for location in self.DM_LOCATIONS:
                    url = location['url']
                    name = location['name']

                    heartbeat(f"Перевірка: {name}")
                    logger.info(f"Перевіряю: {name} ({url})")

                    if not self.go_to_location(url):
                        logger.warning(f"Не вдалося відкрити {name}, пропускаю")
                        continue

                    # Знаходимо чати на цій сторінці
                    # [DEBUG] get_all_chats() — всі чати
                    found_chats = self.get_all_chats()
                    # found_chats = self.get_unread_chats()  # TODO: розкоментувати після дебагу

                    if not found_chats:
                        logger.info(f"  {name}: чатів не знайдено")
                        time.sleep(random.uniform(1, 2))
                        continue

                    logger.info(f"  {name}: знайдено {len(found_chats)} чатів, обробляю...")

                    # Відповідаємо на кожен чат ЗРАЗУ в цій локації
                    for i, chat in enumerate(found_chats):
                        chat_username = chat.get('username', 'unknown')

                        # [DEBUG] Фільтр по username
                        if self.DEBUG_ONLY_USERNAME:
                            # Пропускаємо всіх крім debug username
                            # Перевіряємо і display name і можливий username
                            if self.DEBUG_ONLY_USERNAME.lower() not in chat_username.lower():
                                logger.info(f"  [DEBUG] Пропускаю {chat_username} (не {self.DEBUG_ONLY_USERNAME})")
                                continue

                        heartbeat(f"Обробка: {chat_username} [{name}]")
                        logger.info(f"  Обробка [{i+1}/{len(found_chats)}]: {chat_username}")

                        # Додаємо location_url для process_chat_by_click
                        chat['location_url'] = url
                        chat['location'] = name

                        if chat.get('href'):
                            self.process_chat(chat['href'])
                        else:
                            self.process_chat_by_click(chat)

                        total_processed += 1
                        time.sleep(random.uniform(2, 5))

                    time.sleep(random.uniform(1, 2))

                logger.info(f"Оброблено {total_processed} чатів. Чекаємо {check_interval}с...")
                heartbeat("Очікування наступної перевірки")
                time.sleep(check_interval)

            except KeyboardInterrupt:
                logger.info("Зупинка за запитом користувача")
                raise
            except Exception as e:
                logger.error(f"Помилка в inbox loop: {e}")
                heartbeat("Помилка в циклі, повтор")
                time.sleep(check_interval)
