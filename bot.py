"""
Instagram AI Agent Bot
Вхід через cookies (session файл), перехід в Direct, AI відповіді.

Функції:
- Watchdog (heartbeat) - виявлення зависання (3 хв таймаут)
- Автоперезапуск - до 3 спроб при помилках
- Telegram сповіщення - про помилки, сесію, AI
"""
from camoufox.sync_api import Camoufox
import pickle
import time
import re
import os
import sys
import logging
import threading
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# Вимикаємо зайві логи
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('playwright').setLevel(logging.WARNING)

# Базова директорія проекту
BASE_DIR = Path(__file__).parent
SESSIONS_DIR = BASE_DIR / 'data' / 'sessions'
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# ==================== WATCHDOG (Heartbeat) ====================
_watchdog_running = False
_watchdog_thread = None
_last_heartbeat = time.time()
WATCHDOG_TIMEOUT_MINUTES = 3  # Таймаут зависання


def heartbeat(operation_name: str = None):
    """Оновити heartbeat (повідомити що бот живий)"""
    global _last_heartbeat
    _last_heartbeat = time.time()
    if operation_name:
        logger.debug(f"Heartbeat: {operation_name}")




def _watchdog_loop():
    """Цикл watchdog - перевіряє чи не зависли"""
    global _watchdog_running, _last_heartbeat

    timeout_seconds = WATCHDOG_TIMEOUT_MINUTES * 60

    while _watchdog_running:
        time.sleep(30)  # Перевіряємо кожні 30 секунд

        if not _watchdog_running:
            break

        elapsed = time.time() - _last_heartbeat

        if elapsed > timeout_seconds:
            logger.error("=" * 60)
            logger.error(f"WATCHDOG: ТАЙМАУТ! Операція зависла на {elapsed/60:.1f} хвилин!")
            logger.error("=" * 60)
            logger.error("Перезапускаємо скрипт (Camoufox закриється разом з процесом)...")

            # Сповіщення в Telegram
            try:
                from telegram_notifier import TelegramNotifier
                notifier = TelegramNotifier()
                notifier.notify_error(f"Бот завис на {elapsed/60:.1f} хв. Перезапуск...")
            except Exception:
                pass

            # Перезапускаємо скрипт
            logger.error("Перезапуск скрипта через 5 секунд...")
            time.sleep(5)

            python = sys.executable
            os.execl(python, python, *sys.argv)


def start_watchdog():
    """Запустити watchdog тред"""
    global _watchdog_running, _watchdog_thread, _last_heartbeat

    _last_heartbeat = time.time()
    _watchdog_running = True

    _watchdog_thread = threading.Thread(target=_watchdog_loop, daemon=True)
    _watchdog_thread.start()

    logger.info(f"Watchdog запущено (таймаут: {WATCHDOG_TIMEOUT_MINUTES} хв)")


def stop_watchdog():
    """Зупинити watchdog"""
    global _watchdog_running
    _watchdog_running = False



class InstagramBot:
    def __init__(self):
        self.driver = None  # Playwright Page (alias)
        self.page = None
        self._camoufox = None
        self.browser = None
        self.context = None
        self.temp_profile_dir = None
        self.db = None
        self.ai_agent = None
        self.direct_handler = None

    def init_driver(self, headless=False):
        """Запуск Camoufox (Firefox антидетект) браузера."""
        try:
            logger.info(f"Запуск Camoufox (headless={headless})...")
            self._camoufox = Camoufox(headless=headless, geoip=True)
            self.browser = self._camoufox.__enter__()

            # Завантажуємо сесію якщо є
            session_file = SESSIONS_DIR / os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')
            session_json = str(session_file).replace('.pkl', '.json')

            if os.path.exists(session_json):
                self.context = self.browser.new_context(storage_state=session_json)
                logger.info(f"Camoufox: сесія завантажена з {session_json}")
            else:
                self.context = self.browser.new_context()

            self.page = self.context.new_page()
            self.page.set_viewport_size({"width": 1920, "height": 1080})
            self.driver = self.page  # alias для сумісності
            logger.info("Camoufox запущено успішно")

        except Exception as e:
            logger.error(f"Помилка запуску Camoufox: {e}")
            raise

    def init_ai_components(self):
        """Ініціалізація AI компонентів (DB, AI Agent, Direct Handler)."""
        try:
            from database import Database
            from ai_agent import AIAgent
            from direct_handler import DirectHandler

            # Database
            self.db = Database()
            logger.info("Database підключено")

            # AI Agent
            self.ai_agent = AIAgent(self.db)
            logger.info("AI Agent ініціалізовано")

            # Direct Handler
            self.direct_handler = DirectHandler(self.driver, self.ai_agent)
            logger.info("Direct Handler ініціалізовано")

            return True

        except Exception as e:
            logger.error(f"Помилка ініціалізації AI компонентів: {e}")
            import traceback
            traceback.print_exc()
            return False

    def load_session(self, session_name: str = None):
        """Завантаження сесії — для Camoufox вже зроблено в init_driver через storage_state."""
        if session_name is None:
            session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')

        session_file = SESSIONS_DIR / session_name
        session_json = str(session_file).replace('.pkl', '.json')

        # Якщо JSON сесія вже завантажена в init_driver — просто перевіряємо логін
        if os.path.exists(session_json):
            self.page.goto('https://www.instagram.com')
            time.sleep(3)
            if self.is_logged_in():
                logger.info(f"Сесія завантажена: {session_json}")
                return True

        # Fallback: спробувати завантажити старий pickle як cookies
        if session_file.exists():
            try:
                with open(session_file, 'rb') as f:
                    cookies = pickle.load(f)

                self.page.goto('https://www.instagram.com')
                time.sleep(2)

                for cookie in cookies:
                    try:
                        c = {'name': cookie['name'], 'value': cookie['value'],
                             'domain': cookie.get('domain', '.instagram.com'),
                             'path': cookie.get('path', '/')}
                        if 'expiry' in cookie:
                            c['expires'] = int(cookie['expiry'])
                        self.context.add_cookies([c])
                    except Exception as e:
                        logger.debug(f"Помилка cookie: {e}")

                self.page.reload()
                time.sleep(3)

                if self.is_logged_in():
                    # Зберігаємо як JSON для наступних запусків
                    self.context.storage_state(path=session_json)
                    logger.info(f"Сесія завантажена і збережена як JSON: {session_json}")
                    return True
                else:
                    logger.error("Cookies завантажено, але логін не пройшов")
                    return False
            except Exception as e:
                logger.error(f"Помилка завантаження сесії: {e}")
                return False

        logger.error(f"Session файл не знайдено: {session_file}")
        return False

    def is_logged_in(self):
        """Перевірка чи залогінені в Instagram."""
        try:
            time.sleep(2)
            current_url = self.page.url

            if 'login' in current_url.lower() or 'accounts/login' in current_url:
                return False

            if self.page.locator("xpath=//a[contains(@href, '/accounts/edit/')]").count() > 0:
                return True
            if self.page.locator("xpath=//svg[@aria-label='Profile']").count() > 0:
                return True

            return 'login' not in current_url.lower()

        except Exception:
            return False

    def go_to_direct(self):
        """Перехід в Instagram Direct (повідомлення)."""
        try:
            logger.info("Переходжу в Direct...")
            self.page.goto('https://www.instagram.com/direct/inbox/')
            time.sleep(5)

            current_url = self.page.url
            if 'direct' in current_url:
                logger.info(f"Успішно відкрито Direct: {current_url}")
                return True
            else:
                logger.error(f"Не вдалося відкрити Direct. Поточна URL: {current_url}")
                return False

        except Exception as e:
            logger.error(f"Помилка переходу в Direct: {e}")
            return False

    def close(self):
        """Закриття браузера та очистка."""
        # Закриваємо DB
        if self.db:
            try:
                self.db.close()
            except Exception:
                pass
            self.db = None

        if self._camoufox:
            try:
                self._camoufox.__exit__(None, None, None)
                logger.info("Camoufox браузер закрито")
            except Exception as e:
                logger.warning(f"Помилка закриття Camoufox: {e}")
            self._camoufox = None
            self.browser = None
            self.context = None
            self.page = None
            self.driver = None

    def _notify_telegram(self, message: str):
        """Відправити повідомлення в Telegram про помилку"""
        try:
            from telegram_notifier import TelegramNotifier
            notifier = TelegramNotifier()
            notifier.notify_error(message)
        except Exception as e:
            logger.warning(f"Не вдалося відправити в Telegram: {e}")

    def run(self, session_name: str = None, check_interval: int = 30):
        """
        Головний потік з автоперезапуском.
        - Watchdog (3 хв таймаут)
        - До 3 спроб перезапуску
        - Telegram сповіщення про помилки
        """
        if session_name is None:
            session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')

        restart_count = 0
        max_restarts = 3

        # Запускаємо watchdog
        start_watchdog()
        heartbeat("Старт бота")

        while restart_count < max_restarts:
            try:
                if restart_count > 0:
                    logger.info("=" * 60)
                    logger.info(f"  ПЕРЕЗАПУСК #{restart_count}")
                    logger.info("=" * 60)
                    time.sleep(5)

                logger.info("=" * 60)
                logger.info(f"  ЗАПУСК INSTAGRAM AI AGENT")
                logger.info(f"  Session: {session_name}")
                logger.info("=" * 60)

                heartbeat("Ініціалізація драйвера")

                # 1. Запускаємо браузер
                self.init_driver()

                # 2. Завантажуємо сесію (cookies)
                heartbeat("Завантаження сесії")
                if not self.load_session(session_name):
                    logger.error("Сесія не валідна! Перевір session файл.")
                    self._notify_telegram(f"Сесія не валідна: {session_name}\nПотрібно перезайти в акаунт!")
                    self.close()
                    return False  # Не перезапускаємо - потрібен ручний логін

                logger.info("Успішно залогінено в Instagram!")
                heartbeat("Залогінено")

                # 3. Ініціалізуємо AI компоненти
                if not self.init_ai_components():
                    logger.error("Не вдалося ініціалізувати AI компоненти")
                    self._notify_telegram("Помилка ініціалізації AI компонентів!")
                    raise Exception("AI init failed")

                # 4. Перехід в Direct — не потрібен, бо run_inbox_loop сам переходить на потрібні сторінки
                # heartbeat("Перехід в Direct")
                # if not self.go_to_direct():
                #     logger.error("Не вдалося відкрити Direct.")
                #     raise Exception("Direct open failed")

                logger.info("=" * 60)
                logger.info("  AI AGENT ЗАПУЩЕНО!")
                logger.info(f"  Інтервал перевірки: {check_interval}с")
                logger.info("  Ctrl+C для зупинки")
                logger.info("=" * 60)

                # 5. Запускаємо одну ітерацію (single_run=True → повернеться після обробки)
                heartbeat("Старт циклу inbox")
                self.direct_handler.run_inbox_loop(
                    check_interval=check_interval,
                    heartbeat_callback=heartbeat,
                    single_run=True
                )

                # Ітерація завершена — закриваємо браузер і чекаємо
                logger.info(f"Ітерація завершена. Закриваю браузер, чекаю {check_interval}с...")
                self.close()
                heartbeat("Очікування між ітераціями")
                time.sleep(check_interval)
                restart_count = 0  # Не помилка — скидаємо лічильник

            except KeyboardInterrupt:
                logger.info("Зупинка за запитом користувача (Ctrl+C)")
                self.close()
                stop_watchdog()
                return True

            except Exception as e:
                logger.error(f"Помилка: {e}")
                import traceback
                traceback.print_exc()

                # Закриваємо браузер
                self.close()

                restart_count += 1

                if restart_count < max_restarts:
                    logger.info(f"Перезапуск через 10 секунд... (спроба {restart_count}/{max_restarts})")
                    time.sleep(10)
                    heartbeat("Перезапуск після помилки")
                    continue
                else:
                    break

        # Досягнуто ліміт перезапусків
        if restart_count >= max_restarts:
            logger.error("=" * 60)
            logger.error(f"ДОСЯГНУТО ЛІМІТ {max_restarts} ПЕРЕЗАПУСКІВ!")
            logger.error("Щось серйозно не так. Перевір сесію/інтернет.")
            logger.error("=" * 60)
            self._notify_telegram(f"Бот зупинено: досягнуто ліміт {max_restarts} перезапусків!")

        stop_watchdog()
        return False


def main():
    session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')
    check_interval = int(os.getenv('CHECK_INTERVAL_SECONDS', 30))

    # Аргументи командного рядка
    if '--session' in sys.argv:
        try:
            idx = sys.argv.index('--session')
            session_name = sys.argv[idx + 1]
        except (ValueError, IndexError):
            logger.error("Невірна назва сесії")
            return

    if '--interval' in sys.argv:
        try:
            idx = sys.argv.index('--interval')
            check_interval = int(sys.argv[idx + 1])
        except (ValueError, IndexError):
            pass

    # Запускаємо слухача Telegram-команд (реєстрація адмінів /admin PASSWORD)
    try:
        from telegram_notifier import TelegramAdminListener
        TelegramAdminListener().start()
    except Exception as e:
        logger.warning(f"TelegramAdminListener не запущено: {e}")

    bot = InstagramBot()
    bot.run(session_name, check_interval=check_interval)


if __name__ == '__main__':
    main()
