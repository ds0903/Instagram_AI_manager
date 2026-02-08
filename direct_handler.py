"""
Instagram Direct Handler
Читання та відправка повідомлень в Direct через Selenium
"""
import os
import time
import random
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from selenium.webdriver.common.by import By

load_dotenv()
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
        # Наш username акаунта (для визначення де чиє повідомлення)
        self.bot_username = os.getenv('BOT_USERNAME', '').strip().lower()
        if self.bot_username:
            logger.info(f"BOT_USERNAME: {self.bot_username}")
        else:
            logger.warning("BOT_USERNAME не вказано в .env! Визначення ролей може бути неточним.")

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
        Визначити чи повідомлення від користувача.

        Стратегія (2 рівні):
        1. Profile link: <a href="/username"> в предках
           - href = BOT_USERNAME → наше (False)
           - href = інший → користувач (True)
        2. Fallback: X-позиція елемента
           - Зліва → користувач (True), Справа → наше (False)
           (В Instagram DM: чужі повідомлення зліва, свої справа)
        """
        try:
            return self.driver.execute_script("""
                var msg = arguments[0];
                var botUsername = arguments[1];

                // === СТРАТЕГІЯ 1: Profile link ===
                var current = msg;
                for (var i = 0; i < 8; i++) {
                    current = current.parentElement;
                    if (!current || current === document.body) break;

                    var role = current.getAttribute('role');
                    if (role === 'grid' || role === 'main' ||
                        current.tagName === 'MAIN' || current.tagName === 'SECTION') {
                        break;
                    }

                    var presentations = current.querySelectorAll('div[role="presentation"]');
                    if (presentations.length > 4) break;

                    var link = current.querySelector('a[aria-label^="Open the profile page"]');
                    if (link) {
                        var href = (link.getAttribute('href') || '').toLowerCase();
                        if (botUsername && href.includes('/' + botUsername)) {
                            return false;  // наш профіль → наше повідомлення
                        }
                        return true;  // інший профіль → користувач
                    }
                }

                // === СТРАТЕГІЯ 2: X-позиція (fallback) ===
                // В Instagram DM: повідомлення клієнта зліва, наші справа
                var rect = msg.getBoundingClientRect();
                var chatContainer = document.querySelector('div[role="grid"]')
                                 || document.querySelector('main')
                                 || document.documentElement;
                var containerRect = chatContainer.getBoundingClientRect();
                var containerCenter = containerRect.left + containerRect.width / 2;
                var msgCenter = rect.left + rect.width / 2;

                // Якщо центр повідомлення лівіше за центр контейнера → користувач
                return msgCenter < containerCenter;
            """, msg_element, self.bot_username)
        except Exception as e:
            logger.error(f"Помилка визначення відправника: {e}")
            return False

    def get_user_messages(self, chat_username: str = None) -> list:
        """
        Отримати ВСІ повідомлення КОРИСТУВАЧА з відкритого чату (текст + зображення).
        Повертає list dicts відсортований за Y-позицією (хронологічний порядок).
        Кожен dict: {content, element, message_type, image_src, y_position, timestamp}
        """
        if not chat_username:
            chat_username = self.get_chat_username()

        all_messages = []

        # === ТЕКСТОВІ ПОВІДОМЛЕННЯ ===
        msg_divs = self.driver.find_elements(
            By.XPATH, "//div[@role='presentation']//div[@dir='auto']"
        )
        if not msg_divs:
            msg_divs = self.driver.find_elements(
                By.XPATH, "//span[@dir='auto']//div[@dir='auto']"
            )

        for msg_div in msg_divs:
            text = msg_div.text.strip()
            if not text:
                continue
            is_from_user = self._is_message_from_user(msg_div, chat_username)
            y = msg_div.location.get('y', 0)
            all_messages.append({
                'content': text,
                'is_from_user': is_from_user,
                'element': msg_div,
                'message_type': 'text',
                'image_src': None,
                'y_position': y,
                'timestamp': datetime.now()
            })

        # === ЗОБРАЖЕННЯ (фото/скріншоти всередині повідомлень) ===
        # Шукаємо ВСІ img на сторінці (фото можуть бути поза div[@role='presentation'])
        # Фільтруємо по CDN URL, розміру, виключаємо аватарки
        try:
            all_page_imgs = self.driver.find_elements(
                By.XPATH,
                "//img[not(@alt='user-profile-picture')]"
            )
            logger.info(f"📷 Пошук зображень: знайдено {len(all_page_imgs)} img на сторінці")
            for img in all_page_imgs:
                try:
                    src = img.get_attribute('src') or ''
                    # Тільки CDN зображення Instagram/Facebook
                    if 'cdninstagram' not in src and 'fbcdn' not in src:
                        continue
                    # Фільтр: профільні фото (t51.2885-19) — НЕ фото з чату
                    if '/t51.2885-19/' in src:
                        continue
                    w = int(img.get_attribute('width') or '0')
                    h = int(img.get_attribute('height') or '0')
                    if w < 100 or h < 100:
                        try:
                            natural = self.driver.execute_script(
                                "return [arguments[0].naturalWidth, arguments[0].naturalHeight]", img
                            )
                            w, h = natural[0], natural[1]
                        except Exception:
                            pass
                    if w < 100 or h < 100:
                        continue

                    logger.info(f"📷 Знайдено фото в чаті: {w}x{h}, src={src[:80]}...")
                    is_from_user = self._is_message_from_user(img, chat_username)
                    y = img.location.get('y', 0)
                    all_messages.append({
                        'content': '[Фото]',
                        'is_from_user': is_from_user,
                        'element': img,
                        'message_type': 'image',
                        'image_src': src,
                        'y_position': y,
                        'timestamp': datetime.now()
                    })
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"Помилка пошуку зображень: {e}")

        if not all_messages:
            logger.warning("Не знайдено повідомлень в чаті")
            return []

        # Сортуємо за Y-позицією (хронологічний порядок)
        all_messages.sort(key=lambda m: m['y_position'])

        # Логуємо ВСІ повідомлення
        for i, msg in enumerate(all_messages):
            role_str = 'USER' if msg['is_from_user'] else 'ASSISTANT'
            type_str = msg['message_type'].upper()
            logger.info(f"  [{i+1}] {role_str} ({type_str}): '{msg['content'][:60]}'")

        # Фільтруємо тільки повідомлення КОРИСТУВАЧА
        user_messages = [m for m in all_messages if m['is_from_user']]

        # Зберігаємо елемент останнього повідомлення для hover+reply
        self._last_user_message_element = user_messages[-1]['element'] if user_messages else None

        if not user_messages:
            logger.warning("Не знайдено жодного повідомлення від користувача")
            return []

        logger.info(f"Знайдено {len(user_messages)} повідомлень від користувача")
        return user_messages

    def _filter_unanswered(self, screen_messages: list, username: str) -> list:
        """
        Фільтрація: залишити тільки НЕВІДПОВІДЖЕНІ повідомлення.
        Перевіряємо кожне повідомлення з екрану проти БД:
        - Якщо content збігається і answer_id НЕ NULL → вже відповіли (пропускаємо)
        - Якщо content не знайдено в БД або answer_id NULL → невідповіджене
        (Логіка 1:1 з Dia_Travel_AI)
        """
        db_history = self.ai_agent.db.get_conversation_history(username, limit=50)

        unanswered = []
        for msg in screen_messages:
            already_answered = False

            for db_msg in db_history:
                if db_msg['role'] != 'user':
                    continue
                if db_msg['content'] != msg['content']:
                    continue
                # Content збігається — перевіряємо answer_id
                if db_msg.get('answer_id'):
                    already_answered = True
                break

            if not already_answered:
                unanswered.append(msg)

        return unanswered

    def _download_image(self, img_src: str, img_element=None) -> bytes:
        """
        Отримати зображення з чату у максимальній якості.

        Спосіб 1: Клік на зображення → відкривається full-size viewer →
                  скріншот великого зображення → закрити (Escape)
        Спосіб 2: Витягнути srcset (більший URL) і завантажити з cookies
        Спосіб 3: Скріншот маленького елемента (fallback)
        """
        # === Спосіб 1: Клік → full-size viewer → скріншот ===
        if img_element:
            try:
                # Знаходимо клікабельний батьківський div[role='button'] для зображення
                try:
                    click_target = img_element.find_element(
                        By.XPATH, "./ancestor::div[@role='button']"
                    )
                    logger.info("Клік на div[role='button'] батька зображення...")
                except Exception:
                    click_target = img_element
                    logger.info("Клік на сам img елемент...")

                click_target.click()
                time.sleep(2)

                # Шукаємо НАЙБІЛЬШЕ CDN-зображення на сторінці (viewer показує його великим)
                fullsize_img = None
                all_imgs = self.driver.find_elements(By.TAG_NAME, 'img')
                best_img = None
                best_area = 0

                for img in all_imgs:
                    try:
                        src = img.get_attribute('src') or ''
                        if 'cdninstagram' not in src and 'fbcdn' not in src:
                            continue
                        # Пропускаємо профільні фото
                        if '/t51.2885-19/' in src:
                            continue
                        dims = self.driver.execute_script(
                            "var r = arguments[0].getBoundingClientRect();"
                            "return [r.width, r.height, arguments[0].naturalWidth, arguments[0].naturalHeight]",
                            img
                        )
                        disp_w, disp_h, nat_w, nat_h = dims
                        area = disp_w * disp_h
                        logger.info(f"  img: display={disp_w:.0f}x{disp_h:.0f}, natural={nat_w}x{nat_h}, src={src[:60]}...")
                        if area > best_area:
                            best_area = area
                            best_img = img
                    except Exception:
                        continue

                if best_img and best_area > 90000:  # мінімум ~300x300
                    fullsize_img = best_img
                    logger.info(f"Full-size знайдено: area={best_area:.0f}px²")

                if fullsize_img:
                    # Скріншот великого зображення
                    png_bytes = fullsize_img.screenshot_as_png
                    logger.info(f"Full-size скріншот: {len(png_bytes)} байт")

                    # Також спробуємо завантажити по URL (ще краща якість)
                    fullsize_src = fullsize_img.get_attribute('src') or ''
                    if fullsize_src:
                        try:
                            selenium_cookies = self.driver.get_cookies()
                            cookies = {c['name']: c['value'] for c in selenium_cookies}
                            resp = requests.get(
                                fullsize_src,
                                cookies=cookies,
                                headers={
                                    'User-Agent': self.driver.execute_script("return navigator.userAgent"),
                                    'Referer': 'https://www.instagram.com/',
                                },
                                timeout=15
                            )
                            if resp.status_code == 200 and len(resp.content) > len(png_bytes):
                                logger.info(f"Full-size URL download: {len(resp.content)} байт (краще за скріншот)")
                                png_bytes = resp.content
                        except Exception as e:
                            logger.warning(f"Full-size URL fallback: {e}")

                    # Закриваємо viewer (Escape)
                    try:
                        self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                        time.sleep(1)
                    except Exception:
                        pass

                    if png_bytes and len(png_bytes) > 5000:
                        return png_bytes
                else:
                    logger.warning("Full-size зображення не знайдено в overlay")
                    # Закриваємо viewer
                    try:
                        self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                        time.sleep(1)
                    except Exception:
                        pass

            except Exception as e:
                logger.warning(f"Full-size viewer не вдався: {e}")
                # Закриваємо на всякий випадок
                try:
                    self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                    time.sleep(0.5)
                except Exception:
                    pass

        # === Спосіб 2: srcset з оригінального елемента (більший URL) ===
        if img_element:
            try:
                srcset = img_element.get_attribute('srcset') or ''
                if srcset:
                    # srcset = "url1 320w, url2 640w, url3 1080w" — беремо найбільший
                    parts = [p.strip() for p in srcset.split(',') if p.strip()]
                    best_url = None
                    best_w = 0
                    for part in parts:
                        tokens = part.split()
                        if len(tokens) >= 2:
                            url = tokens[0]
                            w_str = tokens[1].replace('w', '')
                            try:
                                w = int(w_str)
                                if w > best_w:
                                    best_w = w
                                    best_url = url
                            except ValueError:
                                pass
                        elif len(tokens) == 1:
                            best_url = tokens[0]

                    if best_url and best_w > 300:
                        logger.info(f"srcset: знайдено URL {best_w}w")
                        selenium_cookies = self.driver.get_cookies()
                        cookies = {c['name']: c['value'] for c in selenium_cookies}
                        resp = requests.get(
                            best_url,
                            cookies=cookies,
                            headers={
                                'User-Agent': self.driver.execute_script("return navigator.userAgent"),
                                'Referer': 'https://www.instagram.com/',
                            },
                            timeout=15
                        )
                        if resp.status_code == 200 and len(resp.content) > 5000:
                            logger.info(f"srcset download: {len(resp.content)} байт")
                            return resp.content
            except Exception as e:
                logger.warning(f"srcset помилка: {e}")

        # === Спосіб 3: Скріншот маленького елемента (fallback) ===
        if img_element:
            try:
                png_bytes = img_element.screenshot_as_png
                if png_bytes and len(png_bytes) > 2000:
                    logger.info(f"Зображення (small screenshot): {len(png_bytes)} байт")
                    return png_bytes
            except Exception as e:
                logger.warning(f"Small screenshot не вдався: {e}")

        # === Спосіб 4: URL download (original src) ===
        try:
            selenium_cookies = self.driver.get_cookies()
            cookies = {c['name']: c['value'] for c in selenium_cookies}
            response = requests.get(
                img_src,
                cookies=cookies,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/133.0.0.0 Safari/537.36',
                    'Referer': 'https://www.instagram.com/',
                },
                timeout=15
            )
            if response.status_code == 200 and len(response.content) > 2000:
                logger.info(f"Зображення завантажено (URL): {len(response.content)} байт")
                return response.content
            else:
                logger.warning(f"URL завантаження: {response.status_code}, {len(response.content)} байт (замало)")
        except Exception as e:
            logger.warning(f"URL завантаження не вдалося: {e}")

        return None

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

            # Спосіб 1: aria-label містить текст Reply/Ответ/Відповісти (3 мови)
            for label in ['Ответьте на сообщение', 'Reply to message', 'Відповісти на повідомлення',
                          'Ответить', 'Reply', 'Відповісти', 'Ответ']:
                try:
                    reply_btn = self.driver.find_element(
                        By.XPATH, f"//*[contains(@aria-label, '{label}')]"
                    )
                    if reply_btn:
                        logger.info(f"Reply знайдено по aria-label '{label}'")
                        break
                except Exception:
                    continue

            # Спосіб 2: title атрибут (tooltip)
            if not reply_btn:
                for label in ['Ответьте', 'Reply', 'Відповісти']:
                    try:
                        reply_btn = self.driver.find_element(
                            By.XPATH, f"//*[contains(@title, '{label}')]"
                        )
                        if reply_btn:
                            logger.info(f"Reply знайдено по title '{label}'")
                            break
                    except Exception:
                        continue

            # Спосіб 3: Toolbar контейнер (div[style*='--x-width: 96px'])
            # Шукаємо SVG іконки напряму — кожна SVG = 1 кнопка
            # Кнопки: [emoji, reply, more] — Reply = 2-га (індекс 1)
            if not reply_btn:
                try:
                    toolbars = self.driver.find_elements(
                        By.CSS_SELECTOR, "div[style*='--x-width: 96px']"
                    )
                    for toolbar in toolbars:
                        # Знаходимо саме SVG елементи (не вкладені контейнери)
                        svgs = toolbar.find_elements(By.CSS_SELECTOR, "svg")
                        if svgs:
                            logger.info(f"Toolbar знайдено з {len(svgs)} SVG іконками")
                            # Reply = 2-га SVG іконка (індекс 1)
                            if len(svgs) >= 2:
                                # Клікаємо на батька SVG (span/div кнопку)
                                reply_btn = svgs[1].find_element(By.XPATH, "..")
                                logger.info(f"Reply кнопка знайдена (SVG позиція 2 з {len(svgs)})")
                            break
                except Exception as e:
                    logger.info(f"Toolbar пошук: {e}")

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

    def _process_opened_chat(self, username: str, display_name: str) -> bool:
        """
        Обробка вже відкритого чату (спільна логіка).
        Алгоритм (як Dia_Travel):
        1. Читаємо ВСІ повідомлення користувача з екрану
        2. Перевіряємо БД: які вже мають answer_id (відповідь)
        3. Фільтруємо — залишаємо тільки НЕВІДПОВІДЖЕНІ
        4. Об'єднуємо тексти невідповіджених
        5. Зберігаємо КОЖНЕ повідомлення окремо в БД
        6. Генеруємо ОДНУ відповідь AI
        7. Зберігаємо відповідь і зв'язуємо ВСІ повідомлення з нею (answer_id)
        8. Hover + Reply + відправка
        """
        try:
            logger.info(f"Обробка чату: {username} ({display_name})")

            # 1. Читаємо ВСІ повідомлення користувача з екрану
            user_messages = self.get_user_messages(chat_username=username)
            if not user_messages:
                logger.info(f"Немає повідомлень від користувача в {username}")
                return False

            # 2. Фільтруємо: тільки НЕВІДПОВІДЖЕНІ (перевірка answer_id в БД)
            unanswered = self._filter_unanswered(user_messages, username)
            if not unanswered:
                logger.info(f"Всі повідомлення від {username} вже оброблені (є answer_id)")
                return False

            logger.info(f"Нових (невідповіджених) повідомлень: {len(unanswered)}")
            for i, msg in enumerate(unanswered, 1):
                logger.info(f"  📨 {i}. [{msg['message_type']}] '{msg['content'][:80]}'")

            # 3. Перевірка in-session дедуплікації
            combined_key = f"{username}:" + "|".join([m['content'][:30] for m in unanswered])
            if combined_key in self.processed_messages:
                logger.info("Вже оброблено в цій сесії")
                return False

            # 4. Об'єднуємо тексти + обробка зображень
            text_parts = []
            image_data = None
            message_type = 'text'
            for msg in unanswered:
                if msg['message_type'] == 'image' and msg.get('image_src'):
                    if not image_data:
                        logger.info(f"📷 Завантажуємо зображення: {msg['image_src'][:80]}...")
                        image_data = self._download_image(msg['image_src'], msg.get('element'))
                        if image_data:
                            message_type = 'image'
                            logger.info(f"📷 Зображення готове: {len(image_data)} байт → відправимо в Gemini Vision")
                        else:
                            logger.warning("📷 Не вдалося завантажити зображення!")
                    # Не додаємо "[Фото]" в текст
                else:
                    text_parts.append(msg['content'])

            if text_parts:
                combined_content = " ".join(text_parts)
                if image_data:
                    combined_content += " (клієнт також прикріпив фото, опиши що на ньому)"
            else:
                combined_content = "Клієнт надіслав фото товару. Опиши детально що зображено на фото (бренд, колір, тип товару) і допоможи з вибором."

            logger.info(f"Об'єднаний текст для AI: '{combined_content[:100]}'")

            # 5. Зберігаємо КОЖНЕ повідомлення окремо в БД
            user_msg_ids = []
            phone = None
            for msg in unanswered:
                p = self.ai_agent._extract_phone(msg['content'])
                if p:
                    phone = p
                msg_id = self.ai_agent.db.add_user_message(
                    username=username,
                    content=msg['content'],
                    display_name=display_name
                )
                user_msg_ids.append(msg_id)
                logger.info(f"Збережено user message id={msg_id}")

            # 6. Створюємо/оновлюємо ліда
            self.ai_agent.db.create_or_update_lead(
                username=username,
                display_name=display_name,
                phone=phone
            )

            # 7. Перевіряємо ескалацію
            if self.ai_agent._check_escalation(combined_content):
                logger.info(f"Ескалація для {username}")
                self.ai_agent.escalate_to_human(
                    username=username,
                    display_name=display_name,
                    reason="Клієнт просить зв'язку з оператором",
                    last_message=combined_content
                )
                response = self.ai_agent.prompts.get('escalation_response',
                    'Зрозуміло! Передаю ваше запитання нашому менеджеру. Він зв\'яжеться з вами найближчим часом.')
            else:
                # 8. Перевіряємо правила поведінки (Google Sheets)
                behavior_rule = self.ai_agent._check_behavior_rules(combined_content)
                if behavior_rule and behavior_rule.get('Відповідь'):
                    response = behavior_rule.get('Відповідь')
                    logger.info(f"Застосовано правило: {behavior_rule.get('Ситуація')}")
                else:
                    # 9. Генеруємо відповідь через AI
                    response = self.ai_agent.generate_response(
                        username=username,
                        user_message=combined_content,
                        display_name=display_name,
                        message_type=message_type,
                        image_data=image_data
                    )

            if not response:
                return False

            # 10. Зберігаємо відповідь асистента в БД
            assistant_msg_id = self.ai_agent.db.add_assistant_message(
                username=username,
                content=response,
                display_name=display_name
            )

            # 11. Зв'язуємо ВСІ повідомлення користувача з ОДНІЄЮ відповіддю (answer_id)
            for msg_id in user_msg_ids:
                self.ai_agent.db.update_answer_id(msg_id, assistant_msg_id)
            logger.info(f"Зв'язано {len(user_msg_ids)} повідомлень → answer #{assistant_msg_id}")

            # 12. Сповіщення про нового ліда (перший контакт)
            lead = self.ai_agent.db.get_lead(username)
            if lead and lead.get('messages_count') == 1 and self.ai_agent.telegram:
                self.ai_agent.telegram.notify_new_lead(
                    username=username,
                    display_name=display_name,
                    phone=phone,
                    products=combined_content[:100]
                )

            # 13. Hover + Reply на останнє повідомлення користувача
            msg_element = self._last_user_message_element
            if msg_element:
                self.hover_and_click_reply(msg_element, chat_username=username)

            # 14. Відправляємо відповідь
            success = self.send_message(response)
            if success:
                self.processed_messages.add(combined_key)
                logger.info(f"Успішно відповіли {username}")

            return success

        except Exception as e:
            logger.error(f"Помилка обробки чату: {e}")
            return False

    def process_chat(self, chat_href: str) -> bool:
        """Обробка чату по href (inbox)."""
        try:
            if not self.open_chat(chat_href):
                return False
            time.sleep(1)
            self.try_accept_request()

            username = self.get_chat_username()
            display_name = self.get_display_name()
            return self._process_opened_chat(username, display_name)

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
        """Обробка чату через клік (requests/hidden)."""
        try:
            username = chat_info.get('username', 'unknown')

            if not self.open_chat_by_click(chat_info):
                return False

            accepted = self.try_accept_request()
            if accepted:
                logger.info(f"Accept натиснуто для {username}, чекаємо завантаження...")
                time.sleep(2)

            chat_username = self.get_chat_username()
            display_name = self.get_display_name()

            if chat_username == "unknown_user":
                chat_username = username
                display_name = username

            return self._process_opened_chat(chat_username, display_name)

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
