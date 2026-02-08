"""
Instagram AI Agent Bot
Вхід через cookies (session файл), перехід в Direct, AI відповіді.

Функції:
- Watchdog (heartbeat) - виявлення зависання (3 хв таймаут)
- Автоперезапуск - до 3 спроб при помилках
- Telegram сповіщення - про помилки, сесію, AI
"""
import undetected_chromedriver as uc
import pickle
import time
import re
import os
import sys
import platform
import subprocess
import tempfile
import logging
import threading
import shutil
import psutil
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
logging.getLogger('selenium').setLevel(logging.WARNING)

# Базова директорія проекту
BASE_DIR = Path(__file__).parent
SESSIONS_DIR = BASE_DIR / 'data' / 'sessions'
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# ==================== WATCHDOG (Heartbeat) ====================
_watchdog_running = False
_watchdog_thread = None
_last_heartbeat = time.time()
WATCHDOG_TIMEOUT_MINUTES = 3  # Таймаут зависання

# Зберігаємо PID Chrome процесу
_chrome_pid = None
_chrome_pids_file = BASE_DIR / 'data' / 'chrome_pids.txt'

# Префікс для наших Chrome профілів
CHROME_PROFILE_PREFIX = 'chrome_insta_'


def heartbeat(operation_name: str = None):
    """Оновити heartbeat (повідомити що бот живий)"""
    global _last_heartbeat
    _last_heartbeat = time.time()
    if operation_name:
        logger.debug(f"Heartbeat: {operation_name}")


def _save_chrome_pid(pid: int):
    """Зберегти PID Chrome в файл"""
    global _chrome_pid
    _chrome_pid = pid
    try:
        _chrome_pids_file.parent.mkdir(parents=True, exist_ok=True)
        with open(_chrome_pids_file, 'a') as f:
            f.write(f"{pid}\n")
        logger.debug(f"Chrome PID збережено: {pid}")
    except Exception as e:
        logger.warning(f"Не вдалося зберегти PID: {e}")


def _get_saved_chrome_pids() -> list:
    """Отримати збережені PID з файлу"""
    pids = []
    try:
        if _chrome_pids_file.exists():
            with open(_chrome_pids_file, 'r') as f:
                for line in f:
                    try:
                        pids.append(int(line.strip()))
                    except ValueError:
                        pass
    except Exception:
        pass
    return pids


def _clear_chrome_pids_file():
    """Очистити файл з PID"""
    try:
        if _chrome_pids_file.exists():
            _chrome_pids_file.unlink()
    except Exception:
        pass


def _kill_process_tree(pid: int):
    """Вбити процес і всі його дочірні процеси"""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)

        # Спочатку вбиваємо дітей
        for child in children:
            try:
                child.kill()
                logger.debug(f"Вбито дочірній процес: {child.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        # Потім батька
        try:
            parent.kill()
            logger.info(f"Вбито Chrome процес: {pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    except psutil.NoSuchProcess:
        logger.debug(f"Процес {pid} вже не існує")
    except Exception as e:
        logger.warning(f"Помилка вбивства процесу {pid}: {e}")


def _kill_chrome_by_profile():
    """Вбити всі Chrome процеси з нашим профілем (chrome_insta_*)"""
    killed_count = 0
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name = proc.info['name'] or ''
                cmdline = proc.info['cmdline'] or []

                # Перевіряємо чи це Chrome/chromedriver
                if 'chrome' not in name.lower():
                    continue

                # Перевіряємо чи є наш профіль в командному рядку
                cmdline_str = ' '.join(cmdline)
                if CHROME_PROFILE_PREFIX in cmdline_str:
                    _kill_process_tree(proc.info['pid'])
                    killed_count += 1

            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

    except Exception as e:
        logger.warning(f"Помилка пошуку Chrome процесів: {e}")

    return killed_count


def _kill_all_chrome():
    """
    Вбити ТІЛЬКИ наші Chrome процеси (не чіпає інші):
    1. По збереженому PID
    2. По профілю chrome_insta_*
    3. Очистити тимчасові папки
    """
    global _chrome_pid

    logger.info("Очищення Chrome процесів (тільки наші)...")
    killed_total = 0

    # 1. Вбиваємо по збереженому PID (поточний)
    if _chrome_pid:
        logger.info(f"Вбиваю по поточному PID: {_chrome_pid}")
        _kill_process_tree(_chrome_pid)
        killed_total += 1
        _chrome_pid = None

    # 2. Вбиваємо по збереженим PID з файлу (попередні запуски)
    saved_pids = _get_saved_chrome_pids()
    if saved_pids:
        logger.info(f"Вбиваю по збереженим PID: {saved_pids}")
        for pid in saved_pids:
            _kill_process_tree(pid)
            killed_total += 1

    # 3. Вбиваємо по профілю (гарантовано знайде всі)
    logger.info(f"Шукаю Chrome процеси з профілем '{CHROME_PROFILE_PREFIX}*'...")
    killed_by_profile = _kill_chrome_by_profile()
    killed_total += killed_by_profile

    # Чекаємо щоб Windows звільнив lock-файли
    if platform.system() == 'Windows':
        time.sleep(2)

    # 4. Очищуємо тимчасові профілі
    temp_dir = tempfile.gettempdir()
    cleaned_dirs = 0
    for item in Path(temp_dir).glob(f'{CHROME_PROFILE_PREFIX}*'):
        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
                cleaned_dirs += 1
                logger.debug(f"Видалено профіль: {item}")
        except Exception:
            pass

    # 5. Очищуємо файл з PID
    _clear_chrome_pids_file()

    logger.info(f"Очищено: {killed_total} процесів, {cleaned_dirs} профілів")


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
            logger.error("Вбиваю Chrome і перезапускаю скрипт...")

            # Сповіщення в Telegram
            try:
                from telegram_notifier import TelegramNotifier
                notifier = TelegramNotifier()
                notifier.notify_error(f"Бот завис на {elapsed/60:.1f} хв. Перезапуск...")
            except Exception:
                pass

            # Вбиваємо Chrome
            _kill_all_chrome()

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


def detect_chrome_version():
    """Визначає версію Chrome. Повертає int або None."""
    try:
        if platform.system() == 'Windows':
            try:
                import winreg
                for hkey in [winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE]:
                    try:
                        key = winreg.OpenKey(hkey, r'SOFTWARE\Google\Chrome\BLBeacon')
                        version_str, _ = winreg.QueryValueEx(key, 'version')
                        winreg.CloseKey(key)
                        match = re.search(r'(\d+)\.', version_str)
                        if match:
                            return int(match.group(1))
                    except (FileNotFoundError, OSError):
                        continue
            except ImportError:
                pass
        else:
            for cmd in ['google-chrome', 'google-chrome-stable', 'chromium-browser', 'chromium']:
                try:
                    result = subprocess.run([cmd, '--version'], capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        match = re.search(r'(\d+)\.', result.stdout.strip())
                        if match:
                            return int(match.group(1))
                except (FileNotFoundError, subprocess.TimeoutExpired):
                    continue
    except Exception:
        pass
    return None


class InstagramBot:
    def __init__(self):
        self.driver = None
        self.temp_profile_dir = None
        self.db = None
        self.ai_agent = None
        self.direct_handler = None

    def init_driver(self, headless=False):
        """Запуск Chrome з антидетект налаштуваннями."""
        import uuid
        unique_id = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"
        self.temp_profile_dir = tempfile.mkdtemp(prefix=f'chrome_insta_{unique_id}_')

        options = uc.ChromeOptions()

        if headless:
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            logger.info("Headless режим увімкнено")

        options.add_argument(f'--user-data-dir={self.temp_profile_dir}')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-automation')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-logging')
        options.add_argument('--log-level=3')

        prefs = {
            'profile.default_content_setting_values': {
                'notifications': 2,
                'geolocation': 2,
            },
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False
        }
        options.add_experimental_option('prefs', prefs)

        logger.info(f"Запуск Chrome з профілем: {self.temp_profile_dir}")

        try:
            chrome_version = detect_chrome_version()
            if chrome_version:
                logger.info(f"Версія Chrome: {chrome_version}")

            self.driver = uc.Chrome(
                options=options,
                version_main=chrome_version,
                use_subprocess=True,
                headless=headless
            )

            self.driver.set_window_size(1920, 1080)

            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
            })

            stealth_js = """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [
                {name: 'Chrome PDF Plugin', description: 'Portable Document Format', filename: 'internal-pdf-viewer'},
                {name: 'Chrome PDF Viewer', description: '', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
                {name: 'Native Client', description: '', filename: 'internal-nacl-plugin'}
            ]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
            Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
            Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
            Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 0});

            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({state: Notification.permission}) :
                    originalQuery(parameters)
            );

            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.';
                if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                return getParameter.call(this, parameter);
            };

            if (!window.chrome) window.chrome = {};
            if (!window.chrome.runtime) window.chrome.runtime = {};

            Object.keys(window).forEach(key => {
                if (key.startsWith('$cdc_') || key.startsWith('cdc_')) {
                    delete window[key];
                }
            });
            """
            self.driver.execute_script(stealth_js)

            # Зберігаємо PID Chrome процесу
            try:
                chrome_pid = self.driver.service.process.pid
                _save_chrome_pid(chrome_pid)
                logger.info(f"Chrome запущено успішно (PID: {chrome_pid})")
            except Exception:
                logger.info("Chrome запущено успішно (антидетект)")

        except Exception as e:
            logger.error(f"Помилка запуску Chrome: {e}")
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
        """Завантаження cookies з session файлу."""
        # Використовуємо назву з .env або параметру
        if session_name is None:
            session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')

        session_file = SESSIONS_DIR / session_name
        if not session_file.exists():
            logger.error(f"Session файл не знайдено: {session_file}")
            logger.error(f"Створи його через login_helper.py")
            return False

        try:
            with open(session_file, 'rb') as f:
                cookies = pickle.load(f)

            self.driver.get('https://www.instagram.com')
            time.sleep(2)

            for cookie in cookies:
                if 'expiry' in cookie:
                    cookie['expiry'] = int(cookie['expiry'])
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    logger.debug(f"Помилка додавання cookie: {e}")

            self.driver.refresh()
            time.sleep(3)

            if self.is_logged_in():
                logger.info(f"Сесія завантажена: {session_name}")
                return True
            else:
                logger.error("Cookies завантажено, але логін не пройшов")
                return False

        except Exception as e:
            logger.error(f"Помилка завантаження сесії: {e}")
            return False

    def is_logged_in(self):
        """Перевірка чи залогінені в Instagram."""
        try:
            time.sleep(2)
            current_url = self.driver.current_url

            if 'login' in current_url.lower() or 'accounts/login' in current_url:
                return False

            from selenium.webdriver.common.by import By

            try:
                self.driver.find_element(By.XPATH, "//a[contains(@href, '/accounts/edit/')]")
                return True
            except Exception:
                pass

            try:
                self.driver.find_element(By.XPATH, "//svg[@aria-label='Profile']")
                return True
            except Exception:
                pass

            return 'login' not in current_url.lower()

        except Exception:
            return False

    def go_to_direct(self):
        """Перехід в Instagram Direct (повідомлення)."""
        try:
            logger.info("Переходжу в Direct...")
            self.driver.get('https://www.instagram.com/direct/inbox/')
            time.sleep(5)

            current_url = self.driver.current_url
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
        global _chrome_pid

        # Закриваємо DB
        if self.db:
            try:
                self.db.close()
            except Exception:
                pass
            self.db = None

        if self.driver:
            try:
                self.driver.quit()
                logger.info("Браузер закрито")
            except Exception as e:
                logger.warning(f"Помилка закриття браузера: {e}")
            self.driver = None

        # Скидаємо PID
        _chrome_pid = None

        if self.temp_profile_dir and Path(self.temp_profile_dir).exists():
            try:
                time.sleep(2)
                shutil.rmtree(self.temp_profile_dir, ignore_errors=True)
                logger.info("Тимчасовий профіль видалено")
            except Exception as e:
                logger.warning(f"Не вдалося видалити профіль: {e}")
            self.temp_profile_dir = None

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
                    _kill_all_chrome()
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

                # 5. Запускаємо цикл обробки повідомлень
                heartbeat("Старт циклу inbox")
                self.direct_handler.run_inbox_loop(
                    check_interval=check_interval,
                    heartbeat_callback=heartbeat
                )

                # Якщо дійшли сюди - успішне завершення
                restart_count = 0  # Скидаємо лічильник

            except KeyboardInterrupt:
                logger.info("Зупинка за запитом користувача (Ctrl+C)")
                self.close()
                _kill_all_chrome()
                stop_watchdog()
                return True

            except Exception as e:
                logger.error(f"Помилка: {e}")
                import traceback
                traceback.print_exc()

                # Закриваємо браузер
                self.close()

                # Вбиваємо ВСІ Chrome процеси
                _kill_all_chrome()

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
            _kill_all_chrome()

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

    bot = InstagramBot()
    bot.run(session_name, check_interval=check_interval)


if __name__ == '__main__':
    main()
