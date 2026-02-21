"""
Instagram Direct Handler
Читання та відправка повідомлень в Direct через Selenium
"""
import os
import time
import random
import logging
import json
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

    # [DEBUG] Зберігати скріншоти сторіз локально для перевірки
    DEBUG_SAVE_STORY_SCREENSHOTS = True
    STORY_SCREENSHOTS_DIR = "debug_story_screenshots"

    def __init__(self, driver, ai_agent):
        self.driver = driver
        self.ai_agent = ai_agent
        self.processed_messages = set()  # Вже оброблені повідомлення
        self._sent_photos = {}  # {username: set(photo_url)} — вже надіслані фото
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
                            // Знайшли profile link бота — але перевіримо X-позицію
                            // КОНТЕЙНЕРА (current), а не дрібного текстового елемента.
                            // Якщо контейнер ліворуч (сторона клієнта), то лінк бота
                            // знаходиться всередині цитати (reply quote) — це повідомлення клієнта.
                            var containerRect = current.getBoundingClientRect();
                            var chatEl = document.querySelector('div[role="grid"]')
                                      || document.querySelector('main')
                                      || document.documentElement;
                            var chatRect = chatEl.getBoundingClientRect();
                            var center = chatRect.left + chatRect.width / 2;
                            if (containerRect.left + containerRect.width / 2 < center) {
                                // Контейнер ліворуч — це клієнт, лінк бота з цитати
                                return true;
                            }
                            return false;  // наш профіль, наше повідомлення (справа)
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

        # === ЗОБРАЖЕННЯ та ВІДЕО-ПРЕВʼЮ (фото/скріншоти/відео всередині повідомлень) ===
        # Instagram показує відео в DM як img-thumbnail + playButton.png (без <video> тегу!)
        # Тому тут визначаємо: якщо є playButton поруч → це відео, інакше → фото
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

                    # Перевіряємо чи це відео (playButton.png поруч або t15.3394-10 в URL)
                    is_video = False
                    try:
                        is_video = self.driver.execute_script("""
                            var img = arguments[0];
                            // Піднімаємось до контейнера повідомлення (div[role='button'])
                            var container = img;
                            for (var i = 0; i < 10; i++) {
                                container = container.parentElement;
                                if (!container) break;
                                if (container.getAttribute('role') === 'button') break;
                                if (container.getAttribute('role') === 'grid') return false;
                            }
                            if (!container) return false;
                            // Шукаємо playButton.png в контейнері
                            var playBtn = container.querySelector('img[src*="playButton"]');
                            if (playBtn) return true;
                            // Також перевіряємо URL: t15.3394-10 = відео thumbnail
                            var src = img.getAttribute('src') || '';
                            if (src.indexOf('/t15.3394-10/') !== -1) return true;
                            return false;
                        """, img)
                    except Exception:
                        pass

                    is_from_user = self._is_message_from_user(img, chat_username)
                    y = img.location.get('y', 0)

                    if is_video:
                        # Знаходимо клікабельний контейнер div[role='button'] для відео
                        video_click_container = img
                        try:
                            video_click_container = img.find_element(
                                By.XPATH, "./ancestor::div[@role='button']"
                            )
                        except Exception:
                            pass

                        logger.info(f"🎬 Знайдено ВІДЕО в чаті (через thumbnail+playButton): {w}x{h}, src={src[:80]}...")
                        all_messages.append({
                            'content': '[Відео]',
                            'is_from_user': is_from_user,
                            'element': video_click_container,
                            'message_type': 'video',
                            'image_src': src,
                            'y_position': y,
                            'timestamp': datetime.now()
                        })
                    else:
                        logger.info(f"📷 Знайдено фото в чаті: {w}x{h}, src={src[:80]}...")
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

        # === ГОЛОСОВІ ПОВІДОМЛЕННЯ (voice notes) ===
        # Instagram НЕ зберігає <audio> в DOM — аудіо завантажується при кліку Play.
        # Тому шукаємо UI-маркери: waveform SVG або audio progress bar.
        try:
            voice_waveforms = self.driver.find_elements(
                By.XPATH,
                "//svg[@aria-label='Waveform for audio message']"
            )
            if not voice_waveforms:
                # Fallback: audio progress bar
                voice_waveforms = self.driver.find_elements(
                    By.XPATH,
                    "//div[@aria-label='Audio progress bar']"
                )
            logger.info(f"🎤 Пошук голосових: знайдено {len(voice_waveforms)} голосових повідомлень")

            for waveform in voice_waveforms:
                try:
                    is_from_user = self._is_message_from_user(waveform, chat_username)
                    y = waveform.location.get('y', 0)
                    all_messages.append({
                        'content': '[Голосове]',
                        'is_from_user': is_from_user,
                        'element': waveform,
                        'message_type': 'voice',
                        'image_src': None,
                        'audio_src': None,  # URL буде захоплено при кліку Play
                        'y_position': y,
                        'timestamp': datetime.now()
                    })
                    logger.info(f"🎤 Голосове повідомлення знайдено, user={is_from_user}")
                except Exception as e:
                    logger.warning(f"🎤 Помилка обробки голосового: {e}")
                    continue
        except Exception as e:
            logger.warning(f"Помилка пошуку голосових: {e}")

        # === ВІДЕО ПОВІДОМЛЕННЯ (video messages) — додатковий пошук <video> тегів ===
        # Основний пошук відео тепер через thumbnail+playButton (вище).
        # Цей блок — fallback для випадків коли <video> тег вже є в DOM.
        try:
            video_elements = self.driver.find_elements(
                By.XPATH,
                "//div[@role='presentation']//video | //div[contains(@class,'x78zum5')]//video"
            )
            # Y-позиції вже знайдених відео та голосових — для дедуплікації
            voice_y_positions = {m['y_position'] for m in all_messages if m['message_type'] == 'voice'}
            video_y_positions = {m['y_position'] for m in all_messages if m['message_type'] == 'video'}
            logger.info(f"🎬 Пошук <video> тегів: знайдено {len(video_elements)} елементів")

            for video_el in video_elements:
                try:
                    y = video_el.location.get('y', 0)
                    # Пропускаємо якщо вже знайдено (через thumbnail або голосове)
                    is_duplicate = any(abs(y - vy) < 50 for vy in voice_y_positions | video_y_positions)
                    if is_duplicate:
                        continue
                    w = video_el.size.get('width', 0)
                    h = video_el.size.get('height', 0)
                    if w < 80 or h < 80:
                        continue

                    is_from_user = self._is_message_from_user(video_el, chat_username)
                    all_messages.append({
                        'content': '[Відео]',
                        'is_from_user': is_from_user,
                        'element': video_el,
                        'message_type': 'video',
                        'image_src': None,
                        'y_position': y,
                        'timestamp': datetime.now()
                    })
                    logger.info(f"🎬 Відео (<video> тег) знайдено: {w}x{h}, user={is_from_user}")
                except Exception as e:
                    logger.warning(f"🎬 Помилка обробки відео: {e}")
                    continue
        except Exception as e:
            logger.warning(f"Помилка пошуку відео: {e}")

        # === ВІДПОВІДІ НА STORIES (story replies/shares) ===
        # Ідентифікація: лінк _a6hd з href="/stories/username/..."
        # Витягуємо: username автора сторіз, превʼю-зображення, текст "Shared X's story"
        try:
            story_links = self.driver.find_elements(
                By.CSS_SELECTOR, 'a._a6hd[role="link"][href*="/stories/"]'
            )
            seen_stories = set()  # Дедуплікація

            valid_stories = 0
            for story_el in story_links:
                try:
                    story_data = self.driver.execute_script("""
                        var link = arguments[0];
                        var href = link.getAttribute('href') || '';

                        // Витягуємо username автора сторіз з /stories/username/id...
                        var match = href.match(/\\/stories\\/([^\\/\\?]+)/);
                        if (!match) return null;
                        var storyAuthor = match[1];

                        // Превʼю зображення сторіз (thumbnail)
                        var imageUrl = '';
                        var imgs = link.querySelectorAll('img');
                        for (var i = 0; i < imgs.length; i++) {
                            var src = imgs[i].src || '';
                            if (src.includes('cdninstagram') || src.includes('fbcdn')) {
                                imageUrl = src;
                                break;
                            }
                        }

                        // Текст-індикатор ("Shared X's story" / "Відповідь на story")
                        // Шукаємо в батьківському контейнері
                        var container = link;
                        for (var j = 0; j < 10; j++) {
                            container = container.parentElement;
                            if (!container) break;
                        }
                        var storyText = '';
                        if (container) {
                            var spans = container.querySelectorAll('span[dir="auto"]');
                            for (var k = 0; k < spans.length; k++) {
                                var text = spans[k].textContent.trim();
                                if (text.toLowerCase().includes('story') ||
                                    text.toLowerCase().includes('сторіз') ||
                                    text.toLowerCase().includes('истори')) {
                                    storyText = text;
                                    break;
                                }
                            }
                        }

                        return {storyAuthor: storyAuthor, imageUrl: imageUrl, storyText: storyText};
                    """, story_el)

                    if not story_data:
                        continue

                    story_author = story_data.get('storyAuthor', '')
                    image_url = story_data.get('imageUrl', '')
                    story_text = story_data.get('storyText', '')

                    # Пропускаємо сторіз нашого бота
                    if story_author.lower() == self.bot_username:
                        continue

                    # Дедуплікація
                    dedup_key = f"story:{story_author}"
                    if dedup_key in seen_stories:
                        continue
                    seen_stories.add(dedup_key)

                    # Сторіз завжди від користувача — бот відповідає лише текстом
                    is_from_user = True
                    y = story_el.location.get('y', 0)

                    content = f"[Сторіз від @{story_author}]"
                    if story_text:
                        content += f": {story_text}"

                    all_messages.append({
                        'content': content,
                        'is_from_user': is_from_user,
                        'element': story_el,
                        'message_type': 'story_reply',
                        'image_src': image_url,
                        'story_author': story_author,
                        'y_position': y,
                        'timestamp': datetime.now()
                    })
                    valid_stories += 1
                    logger.info(f"📖 Сторіз від @{story_author}, img={'yes' if image_url else 'no'}, text: '{story_text[:60]}'")

                except Exception as e:
                    logger.warning(f"📖 Помилка обробки сторіз: {e}")
                    continue

            logger.info(f"📖 Пошук сторіз: {len(story_links)} лінків → {valid_stories} валідних")
        except Exception as e:
            logger.warning(f"Помилка пошуку сторіз: {e}")

        # === ПЕРЕСЛАННІ ПОСТИ/REELS (shared posts) ===
        # Ідентифікація: лінк з класом _a6hd — автор поста (Instagram-специфічний маркер)
        # Фільтрація: тільки всередині повідомлень чату (є sender profile link + велике фото)
        # НЕ включає /stories/ — вони обробляються вище
        try:
            post_links = self.driver.find_elements(By.CSS_SELECTOR, 'a._a6hd[role="link"]')
            seen_captions = set()  # Дедуплікація

            valid_posts = 0
            for link_el in post_links:
                try:
                    post_data = self.driver.execute_script("""
                        var link = arguments[0];
                        var href = link.getAttribute('href') || '';

                        // Пропускаємо сторіз — вони обробляються окремо
                        if (href.includes('/stories/')) return null;

                        // Витягуємо username автора поста
                        var postAuthor = href.replace(/^\\//, '').replace(/\\/$/, '').trim();

                        // Фільтр: пропускаємо навігаційні лінки
                        var navPaths = ['reels', 'explore', 'direct', 'directinbox',
                                        'accounts', '#', '', 'p'];
                        if (navPaths.indexOf(postAuthor) !== -1) return null;
                        if (postAuthor.includes('/')) return null;

                        // Перевіряємо: лінк повинен бути всередині повідомлення чату
                        // (має бути sender profile link в предках)
                        var container = link;
                        var hasSenderLink = false;
                        for (var i = 0; i < 15; i++) {
                            container = container.parentElement;
                            if (!container) break;
                            if (container.querySelector('a[aria-label^="Open the profile page"]')) {
                                hasSenderLink = true;
                                break;
                            }
                        }
                        if (!hasSenderLink) return null;

                        // Фото поста (>= 150px — не аватарка)
                        var imageUrl = '';
                        var imgs = container.querySelectorAll('img');
                        for (var k = 0; k < imgs.length; k++) {
                            var w = parseInt(imgs[k].getAttribute('width') || '0');
                            var h = parseInt(imgs[k].getAttribute('height') || '0');
                            if (w >= 150 && h >= 150) {
                                imageUrl = imgs[k].src;
                                break;
                            }
                        }
                        if (!imageUrl) return null;  // Без фото — не пост

                        // Текст опису — шукаємо span з line-clamp (caption поста)
                        var caption = '';
                        var spans = container.querySelectorAll('span');
                        var bestLen = 0;
                        for (var m = 0; m < spans.length; m++) {
                            var style = spans[m].getAttribute('style') || '';
                            var text = spans[m].textContent.trim();
                            // Пріоритет: span з line-clamp (точно caption)
                            if (style.includes('line-clamp') && text.length > 20) {
                                caption = text;
                                break;
                            }
                            // Fallback: найдовший текст
                            if (text.length > bestLen && text.length > 30) {
                                bestLen = text.length;
                                caption = text;
                            }
                        }

                        return {postAuthor: postAuthor, caption: caption, imageUrl: imageUrl};
                    """, link_el)

                    if not post_data:
                        continue

                    post_author = post_data.get('postAuthor', '')
                    caption = post_data.get('caption', '')

                    # Пропускаємо лінк нашого бота
                    if post_author.lower() == self.bot_username:
                        continue

                    # Дедуплікація: один і той же пост — один запис
                    dedup_key = f"{post_author}:{caption[:50]}"
                    if dedup_key in seen_captions:
                        continue
                    seen_captions.add(dedup_key)

                    # Пост завжди від користувача — бот відповідає лише текстом
                    is_from_user = True
                    y = link_el.location.get('y', 0)

                    content = f"[Пост від @{post_author}]: {caption}" if caption else f"[Пост від @{post_author}]"

                    all_messages.append({
                        'content': content,
                        'is_from_user': is_from_user,
                        'element': link_el,
                        'message_type': 'post_share',
                        'image_src': post_data.get('imageUrl'),
                        'post_author': post_author,
                        'y_position': y,
                        'timestamp': datetime.now()
                    })
                    valid_posts += 1
                    logger.info(f"📎 Пост від @{post_author}, user={is_from_user}, caption: '{caption[:80]}...'")

                except Exception as e:
                    logger.warning(f"📎 Помилка обробки поста: {e}")
                    continue

            logger.info(f"📎 Пошук постів: {len(post_links)} лінків → {valid_posts} валідних постів")
        except Exception as e:
            logger.warning(f"Помилка пошуку постів: {e}")

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

        # Зберігаємо Y-позицію і текст останнього повідомлення бота (для фільтрації медіа і перевірки менеджера)
        assistant_messages = [m for m in all_messages if not m['is_from_user']]
        self._last_assistant_y = assistant_messages[-1]['y_position'] if assistant_messages else 0
        self._last_assistant_text = assistant_messages[-1]['content'] if assistant_messages else None

        if not user_messages:
            logger.warning("Не знайдено жодного повідомлення від користувача")
            return []

        logger.info(f"Знайдено {len(user_messages)} повідомлень від користувача")
        return user_messages

    def _filter_unanswered(self, screen_messages: list, username: str) -> list:
        """
        Фільтрація: залишити тільки НЕВІДПОВІДЖЕНІ повідомлення.

        Текст: збіг content в БД + answer_id NOT NULL → вже відповіли.
        Медіа ([Голосове], [Відео], [Фото]): контент завжди однаковий,
        тому перевіряємо по Y-позиції: якщо медіа НИЖЧЕ останньої відповіді бота
        на екрані → це нове повідомлення (бот ще не відповів на нього).
        Це надійно працює незалежно від lazy loading.
        """
        db_history = self.ai_agent.db.get_conversation_history(username, limit=50)
        media_labels = {'[Голосове]', '[Відео]', '[Фото]'}

        # Y-позиція останньої відповіді бота (збережена в get_user_messages)
        last_bot_y = getattr(self, '_last_assistant_y', 0)
        logger.info(f"Фільтр: last_bot_y={last_bot_y}")

        unanswered = []
        for msg in screen_messages:
            if msg['content'] in media_labels:
                # Медіа — нове якщо НИЖЧЕ останньої відповіді бота (більша Y)
                if msg['y_position'] > last_bot_y:
                    logger.info(f"Медіа '{msg['content']}' y={msg['y_position']} > bot_y={last_bot_y} → НОВЕ")
                    unanswered.append(msg)
                else:
                    logger.info(f"Медіа '{msg['content']}' y={msg['y_position']} <= bot_y={last_bot_y} → вже відповіли")
            else:
                # Текст — стара логіка по content match в БД
                already_answered = False
                for db_msg in db_history:
                    if db_msg['role'] != 'user':
                        continue
                    if db_msg['content'] != msg['content']:
                        continue
                    if db_msg.get('answer_id'):
                        already_answered = True
                    break
                if not already_answered:
                    unanswered.append(msg)

        return unanswered

    def _close_image_viewer(self):
        """Закрити overlay перегляду зображення (кілька стратегій)."""
        # Стратегія 1: Keys.ESCAPE через ActionChains (надійніше ніж body.send_keys)
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(1)
            # Перевіряємо чи закрився — шукаємо кнопку закриття, якщо є — не закрився
            close_btns = self.driver.find_elements(
                By.XPATH,
                "//svg[@aria-label='Закрыть' or @aria-label='Закрити' or @aria-label='Close']"
            )
            if not close_btns:
                logger.info("Viewer закрито через Escape")
                return
        except Exception as e:
            logger.debug(f"Escape не спрацював: {e}")

        # Стратегія 2: Клік на хрестик (SVG з aria-label)
        for label in ['Закрыть', 'Закрити', 'Close']:
            try:
                close_btn = self.driver.find_element(
                    By.XPATH, f"//svg[@aria-label='{label}']"
                )
                close_btn.click()
                time.sleep(1)
                logger.info(f"Viewer закрито кліком на '{label}'")
                return
            except Exception:
                continue

        # Стратегія 3: Клік на title елемент всередині SVG
        for label in ['Закрыть', 'Закрити', 'Close']:
            try:
                close_btn = self.driver.find_element(
                    By.XPATH, f"//svg[title='{label}']"
                )
                close_btn.click()
                time.sleep(1)
                logger.info(f"Viewer закрито через title '{label}'")
                return
            except Exception:
                continue

        # Стратегія 4: body.send_keys (старий спосіб)
        try:
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            time.sleep(1)
            logger.info("Viewer закрито через body.send_keys(ESC)")
        except Exception:
            logger.warning("Не вдалося закрити viewer жодним способом")

    def _screenshot_video_element(self, video_element, label: str = "відео") -> list:
        """
        Знімає скріншоти з <video> елемента кожні 5 сек + фінальний кадр.

        Args:
            video_element: Selenium WebElement <video>
            label: мітка для логів (сторіз/пост/відео)

        Returns:
            list[bytes] — список PNG скріншотів
        """
        screenshots = []
        try:
            duration = self.driver.execute_script("return arguments[0].duration;", video_element)
            if not duration or duration <= 0:
                logger.warning(f"🎬 [{label}] Не вдалося отримати тривалість, робимо один скріншот")
                screenshot = video_element.screenshot_as_png
                if screenshot:
                    screenshots.append(screenshot)
                return screenshots

            logger.info(f"🎬 [{label}] Тривалість: {duration:.1f} сек")
            self.driver.execute_script("arguments[0].pause();", video_element)
            time.sleep(0.3)

            max_screenshots = 12
            step = 5
            current_time = 0
            while current_time < duration and len(screenshots) < max_screenshots:
                self.driver.execute_script(
                    "arguments[0].currentTime = arguments[1];", video_element, current_time
                )
                time.sleep(0.5)
                try:
                    WebDriverWait(self.driver, 3).until(
                        lambda d: d.execute_script(
                            "return !arguments[0].seeking;", video_element
                        )
                    )
                except Exception:
                    time.sleep(1)

                screenshot = video_element.screenshot_as_png
                if screenshot:
                    screenshots.append(screenshot)
                    logger.info(f"🎬 [{label}] Скріншот @ {current_time:.0f}с ({len(screenshot)} байт)")

                current_time += step

            # Фінальний скріншот якщо ще не покрили кінець
            last_captured = current_time - step
            if last_captured + 2 < duration and len(screenshots) < max_screenshots:
                final_time = max(duration - 0.5, 0)
                self.driver.execute_script(
                    "arguments[0].currentTime = arguments[1];", video_element, final_time
                )
                time.sleep(0.5)
                try:
                    WebDriverWait(self.driver, 3).until(
                        lambda d: d.execute_script(
                            "return !arguments[0].seeking;", video_element
                        )
                    )
                except Exception:
                    time.sleep(1)
                screenshot = video_element.screenshot_as_png
                if screenshot:
                    screenshots.append(screenshot)
                    logger.info(f"🎬 [{label}] Фінальний скріншот @ {final_time:.1f}с ({len(screenshot)} байт)")

            logger.info(f"🎬 [{label}] Всього скріншотів: {len(screenshots)}")

        except Exception as e:
            logger.warning(f"🎬 [{label}] Помилка при захопленні відео: {e}")
            try:
                screenshot = video_element.screenshot_as_png
                if screenshot:
                    screenshots.append(screenshot)
            except Exception:
                pass

        return screenshots

    def _save_debug_screenshots(self, screenshots: list, username: str, label: str = "story"):
        """Зберігає скріншоти локально якщо DEBUG увімкнено."""
        if not self.DEBUG_SAVE_STORY_SCREENSHOTS or not screenshots:
            return
        try:
            os.makedirs(self.STORY_SCREENSHOTS_DIR, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            for i, s in enumerate(screenshots):
                path = os.path.join(self.STORY_SCREENSHOTS_DIR, f"{username}_{label}_{ts}_{i}.png")
                with open(path, 'wb') as f:
                    f.write(s)
                logger.info(f"🎬 DEBUG: збережено {path} ({len(s)} байт)")
        except Exception as e:
            logger.warning(f"🎬 DEBUG: не вдалося зберегти скріншоти: {e}")

    def _capture_story_content(self, story_element, username: str = "unknown") -> list:
        """
        Відкриває сторіз, робить скріншоти фото або відео.

        Returns:
            list[bytes] — список PNG скріншотів (порожній якщо сторіз expired)
        """
        screenshots = []
        current_url = self.driver.current_url

        try:
            logger.info("📖 Відкриваємо сторіз для захоплення контенту...")
            try:
                story_element.click()
            except Exception as e:
                logger.warning(f"📖 Не вдалося клікнути на сторіз: {e}")
                return screenshots

            time.sleep(3)

            # Визначаємо тип: відео чи фото
            video_el = None
            try:
                video_el = self.driver.find_element(By.CSS_SELECTOR, "video")
                logger.info("📖 Знайдено відео в сторіз")
            except Exception:
                logger.info("📖 Відео не знайдено, це фото-сторіз")

            if video_el:
                screenshots = self._screenshot_video_element(video_el, "сторіз")
            else:
                # Фото: один скріншот
                try:
                    img_element = None
                    for selector in ["img[style*='object-fit']", "div[role='dialog'] img", "img[crossorigin]"]:
                        try:
                            img_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if img_element and img_element.size.get('width', 0) > 100:
                                break
                        except Exception:
                            continue
                    if img_element:
                        screenshot = img_element.screenshot_as_png
                        if screenshot:
                            screenshots.append(screenshot)
                            logger.info(f"📖 Скріншот фото-сторіз: {len(screenshot)} байт")
                    else:
                        logger.warning("📖 Не знайдено зображення в story viewer")
                except Exception as e:
                    logger.warning(f"📖 Помилка при скріншоті фото: {e}")

            self._save_debug_screenshots(screenshots, username, "story")

        except Exception as e:
            logger.error(f"📖 Помилка при захопленні сторіз: {e}")
        finally:
            try:
                ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(1)
            except Exception:
                pass
            try:
                if self.driver.current_url != current_url:
                    self.driver.get(current_url)
                    time.sleep(2)
            except Exception:
                pass

        logger.info(f"📖 Результат захоплення сторіз: {len(screenshots)} скріншотів")
        return screenshots

    def _capture_post_content(self, post_element, username: str = "unknown") -> list:
        """
        Відкриває пост, робить скріншоти фото або відео.
        Клікає на зображення-превʼю поста (не на лінк автора!).

        Returns:
            list[bytes] — список PNG скріншотів
        """
        screenshots = []
        current_url = self.driver.current_url

        try:
            logger.info("📎 Відкриваємо пост для захоплення контенту...")

            # Шукаємо зображення-превʼю поста в контейнері (піднімаємось по DOM)
            clickable = None
            container = post_element
            for _ in range(10):
                try:
                    container = container.find_element(By.XPATH, "..")
                except Exception:
                    break
                # Шукаємо img з CDN URL всередині контейнера
                try:
                    imgs = container.find_elements(By.TAG_NAME, "img")
                    for img in imgs:
                        src = img.get_attribute('src') or ''
                        w = img.size.get('width', 0)
                        h = img.size.get('height', 0)
                        if ('cdninstagram' in src or 'fbcdn' in src) and w > 100 and h > 100:
                            clickable = img
                            logger.info(f"📎 Знайдено превʼю поста для кліку: {w}x{h}")
                            break
                except Exception:
                    continue
                if clickable:
                    break

            if not clickable:
                logger.warning("📎 Не знайдено превʼю поста, клікаємо на елемент напряму")
                clickable = post_element

            try:
                clickable.click()
            except Exception as e:
                logger.warning(f"📎 Не вдалося клікнути на пост: {e}")
                return screenshots

            time.sleep(3)

            # Визначаємо тип: відео чи фото
            video_el = None
            try:
                video_el = self.driver.find_element(By.CSS_SELECTOR, "div[role='dialog'] video, article video, video")
                logger.info("📎 Знайдено відео в пості")
            except Exception:
                logger.info("📎 Відео не знайдено, це фото-пост")

            if video_el:
                screenshots = self._screenshot_video_element(video_el, "пост")
            else:
                # Фото: скріншот найбільшого зображення
                try:
                    img_element = None
                    best_size = 0
                    for selector in [
                        "div[role='dialog'] img[style*='object-fit']",
                        "div[role='dialog'] img[crossorigin]",
                        "article img[style*='object-fit']",
                        "article img",
                    ]:
                        try:
                            imgs = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            for img in imgs:
                                w = img.size.get('width', 0)
                                h = img.size.get('height', 0)
                                if w * h > best_size and w > 100 and h > 100:
                                    best_size = w * h
                                    img_element = img
                        except Exception:
                            continue

                    if img_element:
                        screenshot = img_element.screenshot_as_png
                        if screenshot:
                            screenshots.append(screenshot)
                            logger.info(f"📎 Скріншот фото-поста: {len(screenshot)} байт")
                    else:
                        logger.warning("📎 Не знайдено зображення в пості")
                except Exception as e:
                    logger.warning(f"📎 Помилка при скріншоті поста: {e}")

            self._save_debug_screenshots(screenshots, username, "post")

        except Exception as e:
            logger.error(f"📎 Помилка при захопленні поста: {e}")
        finally:
            try:
                ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(1)
            except Exception:
                pass
            try:
                if self.driver.current_url != current_url:
                    self.driver.get(current_url)
                    time.sleep(2)
            except Exception:
                pass

        logger.info(f"📎 Результат захоплення поста: {len(screenshots)} скріншотів")
        return screenshots

    def _capture_inline_video(self, video_container, username: str = "unknown") -> list:
        """
        Знімає скріншоти з відео-повідомлення в чаті.

        Стратегія (як для фото — відкриваємо full-size viewer):
        1. Клік на контейнер (div[role='button'] батько) → відкривається overlay
        2. Знаходимо <video> в overlay → скріншоти (початок, кожні 5 сек, кінець)
        3. Закриваємо viewer (Escape)
        Fallback: якщо viewer не відкрився — скріншотимо відео прямо в чаті

        Returns:
            list[bytes] — список PNG скріншотів
        """
        screenshots = []
        viewer_opened = False
        try:
            # === Стратегія 1: Клік на контейнер → full-size viewer ===
            try:
                # video_container вже є div[role='button'] (передано з get_user_messages)
                # або сам video елемент — клікаємо напряму
                click_target = video_container
                role = None
                try:
                    role = video_container.get_attribute('role')
                except Exception:
                    pass
                if role == 'button':
                    logger.info("🎬 Клік на div[role='button'] контейнер відео...")
                else:
                    # Fallback: піднімаємось до div[role='button']
                    try:
                        click_target = video_container.find_element(
                            By.XPATH, "./ancestor::div[@role='button']"
                        )
                        logger.info("🎬 Клік на div[role='button'] батька відео...")
                    except Exception:
                        logger.info("🎬 Клік на сам елемент відео...")

                click_target.click()
                time.sleep(2)

                # Шукаємо <video> в overlay (повноекранний viewer)
                # Overlay зазвичай містить більший video елемент
                overlay_video = None
                all_videos = self.driver.find_elements(By.TAG_NAME, "video")
                logger.info(f"🎬 Після кліку: знайдено {len(all_videos)} video елементів")

                if len(all_videos) > 0:
                    # Шукаємо найбільший video (overlay показує повноекранне)
                    best_video = None
                    best_area = 0
                    for v in all_videos:
                        try:
                            w = v.size.get('width', 0)
                            h = v.size.get('height', 0)
                            area = w * h
                            logger.info(f"🎬   video: {w}x{h}, area={area}")
                            if area > best_area:
                                best_area = area
                                best_video = v
                        except Exception:
                            continue

                    if best_video and best_area > 10000:  # мінімум ~100x100
                        overlay_video = best_video
                        viewer_opened = True
                        logger.info(f"🎬 Full-size video знайдено в overlay: area={best_area}")

                if overlay_video:
                    # Натискаємо play щоб відео завантажилось
                    try:
                        self.driver.execute_script("""
                            var v = arguments[0];
                            if (v.paused) v.play();
                        """, overlay_video)
                        time.sleep(1.5)
                    except Exception:
                        pass

                    screenshots = self._screenshot_video_element(overlay_video, "відео-чат-fullsize")
                    self._save_debug_screenshots(screenshots, username, "video")

                    # Закриваємо viewer
                    self._close_image_viewer()
                    return screenshots
                else:
                    logger.warning("🎬 Overlay video не знайдено, закриваємо viewer")
                    self._close_image_viewer()
                    viewer_opened = False

            except Exception as e:
                logger.warning(f"🎬 Full-size viewer не вдався: {e}")
                if viewer_opened:
                    self._close_image_viewer()

            # === Fallback: скріншотимо video прямо в чаті ===
            logger.info("🎬 Fallback: скріншотимо відео прямо в чаті")
            video_el = None
            try:
                video_el = video_container.find_element(By.TAG_NAME, "video")
            except Exception:
                # Піднімаємось по DOM
                container = video_container
                for _ in range(5):
                    try:
                        container = container.find_element(By.XPATH, "..")
                        video_el = container.find_element(By.TAG_NAME, "video")
                        break
                    except Exception:
                        continue

            if not video_el:
                logger.warning("🎬 Не знайдено <video> елемент (fallback)")
                return screenshots

            # Натискаємо play
            try:
                video_el.click()
                time.sleep(1)
            except Exception:
                pass

            screenshots = self._screenshot_video_element(video_el, "відео-чат-inline")
            self._save_debug_screenshots(screenshots, username, "video")

        except Exception as e:
            logger.error(f"🎬 Помилка при захопленні відео з чату: {e}")

        return screenshots

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

                    # Закриваємо viewer
                    self._close_image_viewer()

                    if png_bytes and len(png_bytes) > 5000:
                        return png_bytes
                else:
                    logger.warning("Full-size зображення не знайдено в overlay")
                    # Закриваємо viewer
                    self._close_image_viewer()

            except Exception as e:
                logger.warning(f"Full-size viewer не вдався: {e}")
                # Закриваємо на всякий випадок
                self._close_image_viewer()

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

    def _download_audio(self, audio_src: str) -> bytes:
        """
        Завантажити голосове повідомлення з CDN.
        Повертає raw bytes аудіо або None.
        """
        try:
            selenium_cookies = self.driver.get_cookies()
            cookies = {c['name']: c['value'] for c in selenium_cookies}
            response = requests.get(
                audio_src,
                cookies=cookies,
                headers={
                    'User-Agent': self.driver.execute_script("return navigator.userAgent"),
                    'Referer': 'https://www.instagram.com/',
                },
                timeout=15
            )
            if response.status_code == 200 and len(response.content) > 1000:
                logger.info(f"🎤 Аудіо завантажено: {len(response.content)} байт")
                return response.content
            else:
                logger.warning(f"🎤 Аудіо завантаження: статус {response.status_code}, {len(response.content)} байт")
        except Exception as e:
            logger.warning(f"🎤 Помилка завантаження аудіо: {e}")
        return None

    def _capture_and_download_audio(self, voice_element) -> bytes:
        """
        Захопити аудіо з голосового повідомлення Instagram.
        Instagram не зберігає URL аудіо в DOM — він завантажується при натисканні Play.

        Стратегії (в порядку надійності):
        A. Resource Timing API — бачить ВСІ мережеві запити (включно з media engine)
        B. CDP Network.enable + performance logs
        C. JS monkey-patch HTMLMediaElement.src
        D. Пошук <audio> в DOM після кліку Play
        """
        try:
            # 1. Знаходимо кнопку Play (поруч з waveform)
            play_btn = self.driver.execute_script("""
                var el = arguments[0];
                var parent = el;
                for (var i = 0; i < 10; i++) {
                    parent = parent.parentElement;
                    if (!parent) break;
                    var btns = parent.querySelectorAll('div[role="button"][aria-label]');
                    for (var j = 0; j < btns.length; j++) {
                        var label = (btns[j].getAttribute('aria-label') || '').toLowerCase();
                        if (label.includes('воспроизвести') || label.includes('play') ||
                            label.includes('відтворити')) {
                            return btns[j];
                        }
                    }
                }
                return null;
            """, voice_element)

            if not play_btn:
                logger.warning("🎤 Кнопка Play не знайдена")
                return None

            # 2. Готуємо перехоплення ПЕРЕД кліком Play

            # 2a. Resource Timing API — знімок поточних ресурсів
            self.driver.execute_script(
                "window.__audioResourcesBefore = performance.getEntriesByType('resource').length;"
            )

            # 2b. CDP Network.enable
            try:
                self.driver.execute_cdp_cmd('Network.enable', {})
            except Exception:
                pass

            # 2c. Очищаємо performance logs
            try:
                self.driver.get_log('performance')
            except Exception:
                pass

            # 2d. JS monkey-patch (setAttribute + src setter)
            self.driver.execute_script("""
                window.__capturedAudioUrls = [];
                if (!window.__audioInterceptorInstalled) {
                    // Patch src setter
                    var origDesc = Object.getOwnPropertyDescriptor(HTMLMediaElement.prototype, 'src');
                    if (origDesc && origDesc.set) {
                        Object.defineProperty(HTMLMediaElement.prototype, 'src', {
                            set: function(val) {
                                if (val && typeof val === 'string') {
                                    window.__capturedAudioUrls.push(val);
                                }
                                return origDesc.set.call(this, val);
                            },
                            get: origDesc.get
                        });
                    }
                    // Patch setAttribute
                    var origSetAttr = HTMLMediaElement.prototype.setAttribute;
                    HTMLMediaElement.prototype.setAttribute = function(name, val) {
                        if (name === 'src' && val && typeof val === 'string') {
                            window.__capturedAudioUrls.push(val);
                        }
                        return origSetAttr.call(this, name, val);
                    };
                    window.__audioInterceptorInstalled = true;
                } else {
                    window.__capturedAudioUrls = [];
                }
            """)

            # 3. Натискаємо Play
            logger.info("🎤 Натискаємо Play для захоплення URL аудіо...")
            play_btn.click()
            time.sleep(3)

            audio_url = None

            # 4. Стратегія A: Resource Timing API (найнадійніша)
            # Шукаємо в УСІХ ресурсах (аудіо може бути кешоване з попереднього відтворення)
            try:
                all_resources = self.driver.execute_script("""
                    var all = performance.getEntriesByType('resource');
                    var results = [];
                    for (var i = 0; i < all.length; i++) {
                        results.push(all[i].name);
                    }
                    return results;
                """)
                before_count = self.driver.execute_script(
                    "return window.__audioResourcesBefore || 0;"
                )
                new_count = len(all_resources) - before_count
                logger.info(f"🎤 Resource Timing: {len(all_resources)} всього, {new_count} нових після Play")

                # Спочатку шукаємо audioclip в УСІХ ресурсах (включно з кешованими)
                for res_url in all_resources:
                    if 'audioclip' in res_url:
                        audio_url = res_url
                        logger.info(f"🎤 Resource Timing (audioclip): {audio_url[:120]}...")
                        break

                # Логуємо нові ресурси для дебагу
                if not audio_url:
                    new_resources = all_resources[before_count:]
                    for res_url in new_resources:
                        logger.info(f"🎤   новий ресурс: {res_url[:120]}")
            except Exception as e:
                logger.debug(f"🎤 Resource Timing помилка: {e}")

            # 5. Стратегія B: CDP performance logs
            if not audio_url:
                try:
                    logs = self.driver.get_log('performance')
                    logger.info(f"🎤 CDP: {len(logs)} записів в performance logs")
                    for entry in logs:
                        try:
                            log_msg = json.loads(entry['message'])
                            method = log_msg.get('message', {}).get('method', '')
                            if method in ('Network.requestWillBeSent', 'Network.responseReceived'):
                                params = log_msg['message']['params']
                                url = ''
                                if 'request' in params:
                                    url = params['request'].get('url', '')
                                if 'response' in params:
                                    url = url or params['response'].get('url', '')
                                if url and 'audioclip' in url:
                                    audio_url = url
                                    logger.info(f"🎤 CDP logs захопив URL: {audio_url[:120]}...")
                                    break
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug(f"🎤 CDP logs недоступні: {e}")

            # 6. Стратегія C: JS monkey-patch результати
            if not audio_url:
                try:
                    captured = self.driver.execute_script("return window.__capturedAudioUrls || [];")
                    logger.info(f"🎤 JS interceptor: {len(captured)} перехоплених URL")
                    for url in captured:
                        if 'audioclip' in url or 'cdninstagram' in url or 'fbcdn' in url:
                            audio_url = url
                            logger.info(f"🎤 JS interceptor захопив URL: {audio_url[:100]}...")
                            break
                except Exception:
                    pass

            # 7. Стратегія D: Пошук <audio> в DOM
            if not audio_url:
                try:
                    audio_els = self.driver.find_elements(By.TAG_NAME, 'audio')
                    logger.info(f"🎤 DOM пошук: знайдено {len(audio_els)} <audio> елементів")
                    for audio_el in audio_els:
                        src = audio_el.get_attribute('src') or ''
                        if src and not src.startswith('blob:'):
                            if 'cdninstagram' in src or 'fbcdn' in src:
                                audio_url = src
                                logger.info(f"🎤 DOM <audio>: {audio_url[:100]}...")
                                break
                        for source_el in audio_el.find_elements(By.TAG_NAME, 'source'):
                            s = source_el.get_attribute('src') or ''
                            if s and ('cdninstagram' in s or 'fbcdn' in s):
                                audio_url = s
                                break
                        if audio_url:
                            break
                except Exception:
                    pass

            # 8. Ставимо на паузу
            try:
                pause_btn = self.driver.execute_script("""
                    var el = arguments[0];
                    var parent = el;
                    for (var i = 0; i < 10; i++) {
                        parent = parent.parentElement;
                        if (!parent) break;
                        var btns = parent.querySelectorAll('div[role="button"][aria-label]');
                        for (var j = 0; j < btns.length; j++) {
                            var label = (btns[j].getAttribute('aria-label') || '').toLowerCase();
                            if (label.includes('пауза') || label.includes('pause') ||
                                label.includes('воспроизвести') || label.includes('play') ||
                                label.includes('відтворити')) {
                                return btns[j];
                            }
                        }
                    }
                    return null;
                """, voice_element)
                if pause_btn:
                    pause_btn.click()
                    logger.info("🎤 Аудіо поставлено на паузу")
            except Exception:
                pass

            if not audio_url:
                logger.warning("🎤 Не вдалося захопити URL аудіо жодним способом")
                return None

            # 9. Завантажуємо аудіо
            return self._download_audio(audio_url)

        except Exception as e:
            logger.error(f"🎤 Помилка захоплення аудіо: {e}")
            return None

    @staticmethod
    def _detect_audio_mime(data: bytes) -> str:
        """Визначити MIME-тип аудіо за magic bytes."""
        if len(data) < 12:
            return 'audio/mp4'
        # OGG: starts with 'OggS'
        if data[:4] == b'OggS':
            return 'audio/ogg'
        # MP3: starts with ID3 tag or sync word 0xFFxFB / 0xFFFB
        if data[:3] == b'ID3' or data[:2] in (b'\xff\xfb', b'\xff\xf3', b'\xff\xf2'):
            return 'audio/mpeg'
        # WAV: starts with 'RIFF'
        if data[:4] == b'RIFF':
            return 'audio/wav'
        # MP4/M4A/AAC: ftyp box (offset 4-7 = 'ftyp')
        if data[4:8] == b'ftyp':
            return 'audio/mp4'
        # Default — MP4 (Instagram зазвичай використовує AAC в MP4 контейнері)
        return 'audio/mp4'

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
            # \n → Shift+Enter (новий рядок в тому ж повідомленні, не відправка)
            for char in text:
                if char == '\n':
                    ActionChains(self.driver).key_down(Keys.SHIFT).send_keys(Keys.RETURN).key_up(Keys.SHIFT).perform()
                else:
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

    def send_photo(self, image_path: str) -> bool:
        """
        Відправити фото в поточний чат Instagram DM.
        Використовує прихований input[type='file'] для завантаження.

        Args:
            image_path: Абсолютний шлях до файлу зображення (JPG/PNG)

        Returns:
            True якщо фото відправлено
        """
        try:
            if not os.path.exists(image_path):
                logger.error(f"Файл не знайдено: {image_path}")
                return False

            file_input = self._get_file_input()
            if not file_input:
                logger.error("Не вдалося знайти input[type='file'] для завантаження фото")
                return False

            abs_path = os.path.abspath(image_path)
            file_input.send_keys(abs_path)
            logger.info(f"Файл завантажено: {abs_path}")

            # Чекаємо поки з'явиться preview
            time.sleep(2)

            send_clicked = self._click_send_button()
            time.sleep(2)
            logger.info(f"Фото відправлено: {image_path}")
            return True

        except Exception as e:
            logger.error(f"Помилка відправки фото: {e}")
            return False

    @staticmethod
    def _convert_gdrive_url(url: str) -> str:
        """
        Конвертує будь-який формат Google Drive посилання в пряме посилання для завантаження.
        Підтримує всі варіанти:
          https://drive.google.com/file/d/ID/view?usp=sharing
          https://drive.google.com/file/d/ID/view
          https://drive.google.com/open?id=ID
          https://drive.google.com/uc?id=ID  (вже правильний)
        """
        import re as _re
        # Варіант 1: /file/d/ID/...
        m = _re.search(r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)', url)
        if m:
            return f'https://drive.google.com/uc?export=view&id={m.group(1)}'
        # Варіант 2: open?id=ID або uc?id=ID (без export=view)
        m = _re.search(r'drive\.google\.com/(?:open|uc)\?(?:.*&)?id=([a-zA-Z0-9_-]+)', url)
        if m:
            return f'https://drive.google.com/uc?export=view&id={m.group(1)}'
        # Не Google Drive або вже правильний формат — повертаємо як є
        return url

    def send_photo_from_url(self, image_url: str) -> bool:
        """
        Завантажити фото з URL та відправити в чат.

        Args:
            image_url: URL зображення (будь-який формат Google Drive підтримується)

        Returns:
            True якщо фото відправлено
        """
        try:
            # Конвертуємо Google Drive посилання в пряме
            image_url = self._convert_gdrive_url(image_url)

            # Завантажуємо зображення
            cookies = {c['name']: c['value'] for c in self.driver.get_cookies()}
            headers = {'User-Agent': self.driver.execute_script("return navigator.userAgent")}
            resp = requests.get(image_url, cookies=cookies, headers=headers, timeout=15)

            if resp.status_code != 200 or len(resp.content) < 1000:
                logger.warning(f"Не вдалося завантажити фото з URL: {resp.status_code}")
                return False

            # Визначаємо розширення
            ext = '.jpg'
            if resp.content[:4] == b'\x89PNG':
                ext = '.png'

            # Зберігаємо тимчасово
            import tempfile
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix='ig_photo_')
            tmp.write(resp.content)
            tmp_path = tmp.name
            tmp.close()

            logger.info(f"Фото завантажено з URL: {len(resp.content)} байт → {tmp_path}")

            # Відправляємо через send_photo
            result = self.send_photo(tmp_path)

            # Видаляємо тимчасовий файл
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

            return result

        except Exception as e:
            logger.error(f"Помилка завантаження/відправки фото з URL: {e}")
            return False

    def _get_file_input(self) -> object:
        """
        Знайти або активувати input[type='file'] для завантаження фото.
        Клікає кнопку фото якщо input не видимий.
        Повертає елемент або None.
        """
        # Стратегія 1: вже є видимий input
        try:
            inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
            for inp in inputs:
                if inp.is_enabled():
                    return inp
        except Exception:
            pass

        # Стратегія 2: клікаємо кнопку фото/галерея в тулбарі
        try:
            photo_btns = self.driver.find_elements(
                By.XPATH,
                "//div[@role='textbox']/ancestor::form//button | "
                "//div[@role='textbox']/ancestor::div[contains(@class,'x')]//svg["
                "contains(@aria-label,'photo') or contains(@aria-label,'image') or "
                "contains(@aria-label,'фото') or contains(@aria-label,'зображення') or "
                "contains(@aria-label,'Photo') or contains(@aria-label,'gallery') or "
                "contains(@aria-label,'Add')]/ancestor::button | "
                "//div[@role='textbox']/ancestor::div[contains(@class,'x')]"
                "//svg[contains(@aria-label,'Photo')]/ancestor::div[@role='button']"
            )
            for btn in photo_btns:
                try:
                    btn.click()
                    time.sleep(1)
                    break
                except Exception:
                    continue
            inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
            for inp in inputs:
                if inp.is_enabled():
                    return inp
        except Exception:
            pass

        # Стратегія 3: JS — робимо input видимим
        try:
            self.driver.execute_script("""
                var inputs = document.querySelectorAll('input[type="file"]');
                for (var i = 0; i < inputs.length; i++) {
                    inputs[i].style.display = 'block';
                    inputs[i].style.opacity = '1';
                    inputs[i].style.position = 'fixed';
                    inputs[i].style.top = '0';
                    inputs[i].style.left = '0';
                    inputs[i].style.zIndex = '99999';
                }
            """)
            time.sleep(0.5)
            inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
            for inp in inputs:
                if inp.is_enabled():
                    return inp
        except Exception:
            pass

        return None

    def _click_send_button(self) -> bool:
        """Натиснути кнопку Send в поточному чаті. Повертає True якщо натиснуто."""
        for xpath in [
            "//button[contains(text(),'Send') or contains(text(),'Надіслати') or contains(text(),'Отправить')]",
            "//div[@role='button'][contains(.,'Send') or contains(.,'Надіслати')]"
        ]:
            try:
                btns = self.driver.find_elements(By.XPATH, xpath)
                for btn in btns:
                    if btn.is_displayed():
                        btn.click()
                        return True
            except Exception:
                pass
        return False

    def send_album(self, image_paths: list) -> bool:
        """
        Відправити кілька фото одним альбомом.
        Логіка: додаємо кожне фото в staging area окремо (БЕЗ Send),
        після всіх фото — один Send. Так Instagram формує карусель/альбом.

        Args:
            image_paths: список абсолютних шляхів до файлів

        Returns:
            True якщо альбом відправлено
        """
        if not image_paths:
            return False
        if len(image_paths) == 1:
            return self.send_photo(image_paths[0])

        try:
            staged = 0
            for i, path in enumerate(image_paths):
                abs_path = os.path.abspath(path)

                # Для кожного фото окремо знаходимо/активуємо file input
                file_input = self._get_file_input()
                if not file_input:
                    logger.warning(f"📸 Не вдалося знайти file input для фото {i+1}, зупиняємось на {staged}")
                    break

                file_input.send_keys(abs_path)
                staged += 1
                logger.info(f"📸 Фото {staged}/{len(image_paths)} додано в альбом: {os.path.basename(abs_path)}")

                # Чекаємо поки фото з'явиться в preview перед додаванням наступного
                time.sleep(2)

            if staged == 0:
                logger.error("📸 Жодне фото не додано в альбом")
                return False

            # Всі фото в staging — один Send
            time.sleep(1)
            send_clicked = self._click_send_button()
            time.sleep(2)

            if send_clicked:
                logger.info(f"📸 Альбом відправлено ({staged} фото)")
            else:
                logger.info(f"📸 Кнопка Send не знайдена, фото могло відправитись автоматично ({staged} фото)")
            return True

        except Exception as e:
            logger.error(f"Помилка відправки альбому: {e}")
            return False

    def send_album_from_urls(self, urls: list) -> bool:
        """
        Завантажити фото з URL та відправити одним альбомом.

        Args:
            urls: список URL (Google Drive або будь-які прямі посилання)

        Returns:
            True якщо альбом відправлено
        """
        import tempfile
        tmp_paths = []
        try:
            cookies = {c['name']: c['value'] for c in self.driver.get_cookies()}
            headers = {'User-Agent': self.driver.execute_script("return navigator.userAgent")}

            for url in urls:
                url = self._convert_gdrive_url(url)
                try:
                    resp = requests.get(url, cookies=cookies, headers=headers, timeout=15)
                    if resp.status_code != 200 or len(resp.content) < 1000:
                        logger.warning(f"Не вдалося завантажити фото для альбому: {url[:60]}")
                        continue
                    ext = '.png' if resp.content[:4] == b'\x89PNG' else '.jpg'
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix='ig_album_')
                    tmp.write(resp.content)
                    tmp_paths.append(tmp.name)
                    tmp.close()
                    logger.info(f"📸 Фото для альбому: {len(resp.content)} байт")
                except Exception as e:
                    logger.warning(f"Помилка завантаження фото для альбому: {e}")

            if not tmp_paths:
                logger.error("send_album_from_urls: жодне фото не завантажено")
                return False

            return self.send_album(tmp_paths)

        finally:
            for p in tmp_paths:
                try:
                    os.unlink(p)
                except Exception:
                    pass

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

            # 2. Перевірка: чи не писав менеджер вручну
            last_bot_text = getattr(self, '_last_assistant_text', None)
            if last_bot_text:
                if not self.ai_agent.db.is_bot_message_in_db(username, last_bot_text):
                    logger.info(
                        f"⚠️ [{username}] Останнє повідомлення бота не в БД — "
                        f"менеджер писав вручну. Пропускаємо."
                    )
                    return False

            # 3. Фільтруємо: тільки НЕВІДПОВІДЖЕНІ (перевірка answer_id в БД)
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

            # 4. Об'єднуємо тексти + обробка зображень/голосових
            text_parts = []
            image_data = None
            story_images_list = []  # Список скріншотів сторіз (list[bytes])
            audio_data_list = []  # Список ВСІХ голосових (кожне окремо)
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
                elif msg['message_type'] == 'voice':
                    logger.info(f"🎤 Захоплюємо голосове повідомлення #{len(audio_data_list)+1}...")
                    audio_bytes = self._capture_and_download_audio(msg['element'])
                    if audio_bytes:
                        audio_data_list.append(audio_bytes)
                        message_type = 'voice'
                        logger.info(f"🎤 Голосове #{len(audio_data_list)} готове: {len(audio_bytes)} байт")
                    else:
                        logger.warning("🎤 Не вдалося отримати голосове!")
                    # Не додаємо "[Голосове]" в текст
                elif msg['message_type'] == 'story_reply':
                    # Відповідь на сторіз — відкриваємо і робимо скріншоти
                    text_parts.append(msg['content'])
                    logger.info(f"📖 Сторіз додано в контекст: '{msg['content'][:80]}...'")
                    if not story_images_list:
                        story_screenshots = self._capture_story_content(
                            msg['element'], username=username
                        )
                        if story_screenshots:
                            story_images_list = story_screenshots
                            message_type = 'story_media'
                            logger.info(f"📖 Захоплено {len(story_images_list)} скріншотів сторіз")
                        else:
                            # Fallback: завантажуємо thumbnail через URL
                            logger.info("📖 Скріншоти не вдалися, пробуємо thumbnail...")
                            if msg.get('image_src') and not image_data:
                                image_data = self._download_image(msg['image_src'])
                                if image_data:
                                    message_type = 'image'
                                    logger.info(f"📖 Превʼю сторіз завантажено: {len(image_data)} байт")
                elif msg['message_type'] == 'post_share':
                    # Пересланий пост — відкриваємо і робимо скріншоти (може бути відео)
                    text_parts.append(msg['content'])
                    logger.info(f"📎 Пост додано в контекст: '{msg['content'][:80]}...'")
                    if not story_images_list:
                        post_screenshots = self._capture_post_content(
                            msg['element'], username=username
                        )
                        if post_screenshots:
                            story_images_list = post_screenshots
                            message_type = 'story_media'
                            logger.info(f"📎 Захоплено {len(story_images_list)} скріншотів поста")
                        else:
                            # Fallback: завантажуємо thumbnail через URL
                            logger.info("📎 Скріншоти поста не вдалися, пробуємо thumbnail...")
                            if msg.get('image_src') and not image_data:
                                image_data = self._download_image(msg['image_src'])
                                if image_data:
                                    message_type = 'image'
                                    logger.info(f"📎 Превʼю поста завантажено: {len(image_data)} байт")
                elif msg['message_type'] == 'video':
                    # Відео повідомлення — знімаємо скріншоти
                    logger.info("🎬 Захоплюємо відео повідомлення...")
                    if not story_images_list:
                        video_screenshots = self._capture_inline_video(
                            msg['element'], username=username
                        )
                        if video_screenshots:
                            story_images_list = video_screenshots
                            message_type = 'story_media'
                            logger.info(f"🎬 Захоплено {len(story_images_list)} скріншотів відео")
                        else:
                            logger.warning("🎬 Не вдалося захопити відео!")
                else:
                    text_parts.append(msg['content'])

            voice_count = len(audio_data_list)
            if text_parts:
                combined_content = " ".join(text_parts)
                if story_images_list:
                    combined_content += (
                        f" (уважно проаналізуй {len(story_images_list)} скріншотів з медіа-контенту:"
                        " розпізнай ВЕСЬ текст на зображеннях (назви моделей, розміри, ціни, написи),"
                        " визнач модель одягу/взуття та доступні розміри."
                        " ВАЖЛИВО: називай ТІЛЬКИ ті товари що є в каталозі нижче!"
                        " Якщо такого товару НЕМАЄ в каталозі — чесно скажи що саме такої моделі немає"
                        " і запропонуй СХОЖИЙ товар тієї ж категорії з каталогу)"
                    )
                elif image_data:
                    combined_content += (
                        " (клієнт прикріпив фото — розпізнай ВЕСЬ текст на зображенні"
                        " (назви моделей, розміри, ціни, написи),"
                        " визнач модель одягу/взуття."
                        " ВАЖЛИВО: називай ТІЛЬКИ ті товари що є в каталозі нижче!"
                        " Якщо такого товару НЕМАЄ в каталозі — чесно скажи що саме такої моделі немає"
                        " і запропонуй СХОЖИЙ товар тієї ж категорії з каталогу)"
                    )
                elif voice_count > 0:
                    combined_content += f" (клієнт також надіслав {voice_count} голосових, прослухай і врахуй)"
            elif voice_count > 0:
                if voice_count == 1:
                    combined_content = "Клієнт надіслав голосове повідомлення. Прослухай і відповідай відповідно."
                else:
                    combined_content = f"Клієнт надіслав {voice_count} голосових повідомлень. Прослухай кожне і відповідай на всі запитання."
            elif image_data:
                combined_content = (
                    "Клієнт надіслав фото — розпізнай ВЕСЬ текст на зображенні"
                    " (назви моделей, розміри, ціни, написи),"
                    " визнач модель одягу/взуття."
                    " ВАЖЛИВО: називай ТІЛЬКИ ті товари що є в каталозі!"
                    " Якщо такого товару НЕМАЄ в каталозі — чесно скажи що саме такої моделі немає"
                    " і запропонуй СХОЖИЙ товар тієї ж категорії з каталогу (штани→штани, куртка→куртка)."
                )
            else:
                combined_content = "Клієнт надіслав повідомлення."

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

            # 6. (Лід створюється тільки при підтвердженні замовлення — в _process_order)

            # 7. Перевіряємо правила поведінки (Google Sheets)
            media_types = {'voice', 'image', 'video', 'story_media'}
            behavior_rule = None
            if message_type not in media_types:
                behavior_rule = self.ai_agent._check_behavior_rules(combined_content)
            if behavior_rule and behavior_rule.get('Відповідь'):
                response = behavior_rule.get('Відповідь')
                logger.info(f"Застосовано правило: {behavior_rule.get('Ситуація')}")
            else:
                # 8. Генеруємо відповідь через AI
                response = self.ai_agent.generate_response(
                    username=username,
                    user_message=combined_content,
                    display_name=display_name,
                    message_type=message_type,
                    image_data=story_images_list if story_images_list else image_data,
                    audio_data=audio_data_list if audio_data_list else None
                )

            # 9. Перевіряємо ескалацію — AI сама вставляє [ESCALATION] якщо клієнт просить менеджера
            if response and '[ESCALATION]' in response:
                logger.info(f"Ескалація для {username} (AI визначила)")
                self.ai_agent.escalate_to_human(
                    username=username,
                    display_name=display_name,
                    reason="Клієнт просить зв'язку з оператором",
                    last_message=combined_content
                )
                response = response.replace('[ESCALATION]', '').strip()

            if not response:
                return False

            # 10. Парсимо замовлення з відповіді AI (якщо є [ORDER]...[/ORDER])
            order_data = self.ai_agent._parse_order(response)
            if order_data:
                self.ai_agent._process_order(
                    username=username,
                    display_name=display_name,
                    order_data=order_data
                )
                # Видаляємо блок [ORDER] з тексту — клієнт не бачить
                response = self.ai_agent._strip_order_block(response)

            # 10.3. Парсимо маркер [SAVE_QUESTION:...] — AI вирішила що це нове питання
            import re as _re
            save_q_match = _re.search(r'\[SAVE_QUESTION:(.*?)\]', response)
            if save_q_match and self.ai_agent.sheets_manager:
                question_text = save_q_match.group(1).strip()
                if question_text:
                    self.ai_agent.sheets_manager.save_unanswered_question(question_text, username)
                # Видаляємо маркер з тексту — клієнт не бачить
                response = _re.sub(r'\[SAVE_QUESTION:.*?\]', '', response).strip()

            # 10.5. Парсимо фото маркери
            # [PHOTO:url] — одне фото (конкретний колір)
            # [ALBUM:url1 url2 url3] — всі кольори одним альбомом
            album_urls = self.ai_agent._parse_album_marker(response)
            photo_urls = self.ai_agent._parse_photo_markers(response)
            if album_urls or photo_urls:
                response = self.ai_agent._strip_photo_markers(response)

            # 11. Зберігаємо відповідь асистента в БД (вже без маркерів)
            assistant_msg_id = self.ai_agent.db.add_assistant_message(
                username=username,
                content=response,
                display_name=display_name
            )

            # 12. Зв'язуємо ВСІ повідомлення користувача з ОДНІЄЮ відповіддю (answer_id)
            for msg_id in user_msg_ids:
                self.ai_agent.db.update_answer_id(msg_id, assistant_msg_id)
            logger.info(f"Зв'язано {len(user_msg_ids)} повідомлень → answer #{assistant_msg_id}")

            # 13. Сповіщення про нового ліда (перший контакт)
            lead = self.ai_agent.db.get_lead(username)
            if lead and lead.get('messages_count') == 1 and self.ai_agent.telegram:
                self.ai_agent.telegram.notify_new_lead(
                    username=username,
                    display_name=display_name,
                    phone=phone,
                    products=combined_content[:100]
                )

            # 14. Hover + Reply на останнє повідомлення користувача
            msg_element = self._last_user_message_element
            if msg_element:
                self.hover_and_click_reply(msg_element, chat_username=username)

            # 15. Відправляємо текстову відповідь
            # Якщо є \n\n — це розділювач між блоками (опис + питання)
            # Кожен блок відправляємо окремим повідомленням
            parts = [p.strip() for p in response.split('\n\n') if p.strip()]
            success = False
            for part in parts:
                success = self.send_message(part)
                time.sleep(0.8)

            # 16. Відправляємо фото / альбом
            if username not in self._sent_photos:
                self._sent_photos[username] = set()

            # 16a. Альбом [ALBUM:...] — всі фото одним повідомленням
            if album_urls:
                new_album_urls = [u for u in album_urls if u not in self._sent_photos[username]]
                if new_album_urls:
                    time.sleep(1)
                    logger.info(f"📸 Відправляємо альбом {len(new_album_urls)} фото для {username}")
                    if self.send_album_from_urls(new_album_urls):
                        for u in new_album_urls:
                            self._sent_photos[username].add(u)
                        # Записуємо в БД які саме фото надіслано — AI бачитиме в історії
                        urls_str = ' '.join(new_album_urls)
                        self.ai_agent.db.add_assistant_message(
                            username=username,
                            content=f'[Фото надіслано (альбом): {urls_str}]',
                            display_name=display_name
                        )
                else:
                    logger.info(f"📸 Альбом вже надсилали, пропускаємо")

            # 16b. Окремі фото [PHOTO:...]
            if photo_urls:
                time.sleep(1)
                for url in photo_urls:
                    if url in self._sent_photos[username]:
                        logger.info(f"📷 Фото вже надсилали, пропускаємо: {url[:80]}")
                        continue
                    logger.info(f"Відправляємо фото: {url[:80]}")
                    if self.send_photo_from_url(url):
                        self._sent_photos[username].add(url)
                        # Записуємо в БД яке саме фото надіслано — AI бачитиме в історії
                        self.ai_agent.db.add_assistant_message(
                            username=username,
                            content=f'[Фото надіслано: {url}]',
                            display_name=display_name
                        )
                    time.sleep(1.5)

            if success:
                self.processed_messages.add(combined_key)
                logger.info(f"Успішно відповіли {username}")

            # 17. Одразу виходимо з чату в Direct (не висимо в переписці)
            try:
                logger.info(f"Виходимо з чату {username} → Direct")
                self.driver.get('https://www.instagram.com/direct/')
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Не вдалося перейти в Direct після відповіді: {e}")

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

    def run_inbox_loop(self, check_interval: int = 30, heartbeat_callback=None, single_run: bool = False):
        """
        Головний цикл: перевіряє локації ПО ЧЕРЗІ.
        Директ → знайшли → відповіли на всі → Запити → відповіли → Приховані → відповіли.

        Args:
            check_interval: інтервал перевірки в секундах
            heartbeat_callback: функція для оновлення heartbeat (watchdog)
            single_run: якщо True — виконати одну ітерацію і повернутись
                        (браузер закривається і перезапускається зовні в bot.py)
        """
        logger.info(f"Запуск inbox loop, інтервал: {check_interval}с")
        logger.info(f"Локації для перевірки: {[loc['name'] for loc in self.DM_LOCATIONS]}")
        if single_run:
            logger.info("Режим single_run: одна ітерація → повернення (браузер перезапускається зовні)")
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

                logger.info(f"Оброблено {total_processed} чатів.")
                heartbeat("Ітерація завершена")

                if single_run:
                    logger.info("single_run: повертаємось (браузер буде закрито і перезапущено зовні)")
                    return

                logger.info(f"Чекаємо {check_interval}с...")
                heartbeat("Очікування наступної перевірки")
                time.sleep(check_interval)

            except KeyboardInterrupt:
                logger.info("Зупинка за запитом користувача")
                raise
            except Exception as e:
                logger.error(f"Помилка в inbox loop: {e}")
                heartbeat("Помилка в циклі, повтор")
                if single_run:
                    raise
                time.sleep(check_interval)
