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
import random
import re
import os
import sys
import logging
import threading
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

try:
    import pytz
    KYIV_TZ = pytz.timezone('Europe/Kiev')
except ImportError:
    KYIV_TZ = None

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

# Розмір вікна браузера
VIEWPORT = {"width": 1400, "height": 900}

# ==================== PROXY TUNNEL ====================

def _run_socks_tunnel(local_port, socks_host, socks_port, socks_user, socks_pass):
    """Локальний HTTP-проксі що тунелює через SOCKS5 з авторизацією."""
    import socket
    import select
    import socks as pysocks

    def forward_data(client, remote):
        try:
            while True:
                readable, _, _ = select.select([client, remote], [], [], 60)
                if not readable:
                    break
                for sock in readable:
                    data = sock.recv(8192)
                    if not data:
                        return
                    if sock is client:
                        remote.sendall(data)
                    else:
                        client.sendall(data)
        except Exception:
            pass
        finally:
            try:
                client.close()
            except Exception:
                pass
            try:
                remote.close()
            except Exception:
                pass

    def handle_connect(client_sock, host, port):
        """CONNECT метод (HTTPS тунель)"""
        remote = pysocks.socksocket()
        remote.set_proxy(pysocks.SOCKS5, socks_host, int(socks_port), True, socks_user, socks_pass)
        remote.settimeout(30)
        remote.connect((host, port))
        client_sock.sendall(b'HTTP/1.1 200 Connection established\r\n\r\n')
        forward_data(client_sock, remote)

    def handle_http(client_sock, method, url, rest_of_request):
        """Звичайний HTTP запит через SOCKS5"""
        url_str = url.decode() if isinstance(url, bytes) else url
        if '://' in url_str:
            url_str = url_str.split('://', 1)[1]
        if '/' in url_str:
            host_port, path = url_str.split('/', 1)
            path = '/' + path
        else:
            host_port = url_str
            path = '/'
        if ':' in host_port:
            host, port = host_port.rsplit(':', 1)
            port = int(port)
        else:
            host = host_port
            port = 80

        remote = pysocks.socksocket()
        remote.set_proxy(pysocks.SOCKS5, socks_host, int(socks_port), True, socks_user, socks_pass)
        remote.settimeout(30)
        remote.connect((host, port))

        new_first_line = f"{method.decode()} {path} HTTP/1.1\r\n".encode()
        remote.sendall(new_first_line + rest_of_request)
        forward_data(client_sock, remote)

    def handle_client(client_sock):
        try:
            request = b''
            while b'\r\n\r\n' not in request:
                chunk = client_sock.recv(4096)
                if not chunk:
                    return
                request += chunk

            first_line_end = request.index(b'\r\n')
            first_line = request[:first_line_end]
            parts = first_line.split(b' ')
            method = parts[0]

            if method == b'CONNECT':
                target = parts[1].decode()
                host, port = target.rsplit(':', 1)
                handle_connect(client_sock, host, int(port))
            else:
                rest = request[first_line_end + 2:]
                handle_http(client_sock, method, parts[1], rest)
        except Exception:
            pass
        finally:
            try:
                client_sock.close()
            except Exception:
                pass

    import socket as std_socket
    server = std_socket.socket(std_socket.AF_INET, std_socket.SOCK_STREAM)
    server.setsockopt(std_socket.SOL_SOCKET, std_socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', local_port))
    server.listen(50)
    logger.info(f"SOCKS5-тунель запущено на 127.0.0.1:{local_port}")
    while True:
        try:
            client, _ = server.accept()
            threading.Thread(target=handle_client, args=(client,), daemon=True).start()
        except Exception:
            break


def _build_proxy_for_camoufox() -> dict | None:
    """Будує proxy dict для Camoufox. SOCKS5+auth → локальний HTTP тунель."""
    if os.getenv('USE_PROXY', 'false').lower() != 'true':
        return None

    host     = os.getenv('PROXY_1_HOST')
    port     = os.getenv('PROXY_1_PORT')
    login    = os.getenv('PROXY_1_LOGIN')
    password = os.getenv('PROXY_1_PASSWORD')
    ptype    = os.getenv('PROXY_1_TYPE', 'socks5')

    if not all([host, port, login, password]):
        logger.warning("Проксі: не всі змінні задані в .env, пропускаємо")
        return None

    if ptype.startswith('socks') and login:
        # Firefox/Playwright не підтримує SOCKS5+auth — запускаємо локальний тунель
        import socket
        with socket.socket() as s:
            s.bind(('', 0))
            local_port = s.getsockname()[1]

        logger.info(f"Проксі: SOCKS5+auth → локальний тунель 127.0.0.1:{local_port} → {host}:{port}")
        t = threading.Thread(
            target=_run_socks_tunnel,
            args=(local_port, host, port, login, password),
            daemon=True,
        )
        t.start()
        time.sleep(1)
        return {'server': f'http://127.0.0.1:{local_port}'}
    else:
        logger.info(f"Проксі: {ptype}://{host}:{port}")
        return {'server': f'{ptype}://{host}:{port}', 'username': login, 'password': password}


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

            # Сповіщення в Telegram — вимкнено (watchdog часто спрацьовує під час норм. роботи)
            # try:
            #     from telegram_notifier import TelegramNotifier
            #     notifier = TelegramNotifier()
            #     notifier.notify_error(f"Бот завис на {elapsed/60:.1f} хв. Перезапуск...")
            # except Exception:
            #     pass

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



def is_work_time() -> bool:
    """Перевірити чи зараз робочий час (за київським часом)."""
    if os.getenv('WORK_SCHEDULE_ENABLED', 'false').lower() != 'true':
        return True  # Графік вимкнено — завжди працюємо

    start = int(os.getenv('WORK_START_HOUR', 9))
    end = int(os.getenv('WORK_END_HOUR', 23))

    if KYIV_TZ:
        now = datetime.now(KYIV_TZ)
    else:
        # Fallback без pytz — UTC+2/+3
        from datetime import timezone, timedelta
        now = datetime.now(timezone.utc) + timedelta(hours=2)

    return start <= now.hour < end


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

    def init_driver(self, headless=None):
        """Запуск Camoufox (Firefox антидетект) браузера."""
        try:
            if headless is None:
                headless = os.getenv('HEADLESS', 'false').lower() == 'true'
            logger.info(f"Запуск Camoufox (headless={headless})...")

            # Проксі (SOCKS5+auth → локальний HTTP тунель)
            proxy = _build_proxy_for_camoufox()

            self._camoufox = Camoufox(
                headless=headless,
                proxy=proxy,
                geoip=True if proxy else True,
                humanize=True,
                locale='en-US',
                window=(VIEWPORT['width'], VIEWPORT['height']),
            )
            self.browser = self._camoufox.__enter__()

            # Завантажуємо сесію якщо є
            session_file = SESSIONS_DIR / os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')
            session_json = str(session_file).replace('.pkl', '.json')

            if os.path.exists(session_json):
                self.context = self.browser.new_context(storage_state=session_json, viewport=VIEWPORT)
                logger.info(f"Camoufox: сесія завантажена з {session_json}")
            else:
                self.context = self.browser.new_context(viewport=VIEWPORT)

            # Збільшуємо таймаут навігації до 90 секунд (Instagram повільний)
            self.context.set_default_navigation_timeout(90000)
            self.context.set_default_timeout(30000)

            self.page = self.context.new_page()
            self.page.set_viewport_size(VIEWPORT)
            self.page.evaluate(f"window.resizeTo({VIEWPORT['width']}, {VIEWPORT['height']}); window.moveTo(0, 0);")
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
            self.page.goto('https://www.instagram.com', wait_until='domcontentloaded')
            time.sleep(3)
            if self.is_logged_in():
                logger.info(f"Сесія завантажена: {session_json}")
                return True

        # Fallback: спробувати завантажити старий pickle як cookies
        if session_file.exists():
            try:
                with open(session_file, 'rb') as f:
                    cookies = pickle.load(f)

                self.page.goto('https://www.instagram.com', wait_until='domcontentloaded')
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

                self.page.reload(wait_until='domcontentloaded')
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
            logger.info(f"is_logged_in: URL = {current_url}")

            if 'login' in current_url.lower() or 'accounts/login' in current_url:
                logger.info("is_logged_in: URL містить login → не залогінено")
                return False

            # Якщо ми на instagram.com і не на login — залогінені
            if 'instagram.com' in current_url:
                logger.info("is_logged_in: URL instagram.com, не login → залогінено")
                return True

            return False

        except Exception as e:
            logger.warning(f"is_logged_in exception: {e}")
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

    def run(self, session_name: str = None, interval_min: int = 30, interval_max: int = 120):
        """
        Головний потік з автоперезапуском.
        - Watchdog (3 хв таймаут)
        - До 3 спроб перезапуску
        - Telegram сповіщення про помилки
        """
        if session_name is None:
            session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')

        restart_count = 0
        max_restarts = 2
        relogin_attempted = False  # Автологін — тільки одна спроба

        # Запускаємо watchdog
        start_watchdog()
        heartbeat("Старт бота")

        while True:  # зовнішній цикл — для auto-relogin після 3 невдач
          restart_count = 0
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
                    logger.error("Сесія не валідна! Пробуємо auto-relogin...")
                    ig_user = os.getenv('INSTAGRAM_USERNAME', '')
                    ig_pass = os.getenv('INSTAGRAM_PASSWORD', '')
                    session_json = str(SESSIONS_DIR / session_name.replace('.pkl', '.json'))
                    relogin_ok = False
                    if ig_user and ig_pass:
                        try:
                            from auto_login import auto_relogin
                            import concurrent.futures
                            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
                                future = _pool.submit(auto_relogin, session_json, ig_user, ig_pass)
                                relogin_ok = future.result(timeout=300)
                        except Exception as re_err:
                            logger.error(f"Auto-relogin помилка: {re_err}")
                    if relogin_ok:
                        logger.info("Auto-relogin успішний! Перезапускаємо ітерацію...")
                        self.close()
                        continue  # перезапускаємо внутрішній цикл
                    else:
                        logger.error("Auto-relogin не вдався. Зупинка.")
                        self._notify_telegram(f"Сесія не валідна: {session_name}\nAuto-relogin не вдався!")
                        self.close()
                        return False

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
                logger.info(f"  Інтервал перевірки: {interval_min}-{interval_max}с (random)")
                logger.info("  Ctrl+C для зупинки")
                logger.info("=" * 60)

                # 5. Запускаємо одну ітерацію (single_run=True → повернеться після обробки)
                heartbeat("Старт циклу inbox")
                self.direct_handler.run_inbox_loop(
                    check_interval=interval_min,
                    heartbeat_callback=heartbeat,
                    single_run=True
                )

                # Ітерація завершена — рандомна пауза між ітераціями
                sleep_sec = random.randint(interval_min, interval_max)
                logger.info(f"Ітерація завершена. Закриваю браузер, чекаю {sleep_sec}с (діапазон {interval_min}-{interval_max}с)...")
                self.close()
                heartbeat("Очікування між ітераціями")
                time.sleep(sleep_sec)
                restart_count = 0  # Не помилка — скидаємо лічильник

                # Перевірка графіку роботи (після паузи, перед наступною ітерацією)
                while not is_work_time():
                    logger.info("Поза робочим часом — чекаю 5 хв...")
                    heartbeat("Очікування робочого часу")
                    time.sleep(300)

            except KeyboardInterrupt:
                logger.info("Зупинка за запитом користувача (Ctrl+C)")
                self.close()
                stop_watchdog()
                return True

            except Exception as e:
                # Перевіряємо чи це SessionKickedError (сесія скинута Instagram)
                from direct_handler import SessionKickedError
                is_session_kicked = isinstance(e, SessionKickedError)

                if is_session_kicked:
                    restart_count += 1
                    logger.warning(f"Instagram скинув сесію! Спроба {restart_count}/{max_restarts}")
                else:
                    logger.error(f"Помилка: {e}")
                    import traceback
                    traceback.print_exc()
                    restart_count += 1

                # Закриваємо браузер
                self.close()

                if restart_count < max_restarts:
                    logger.info(f"Перезапуск через 10 секунд... (спроба {restart_count}/{max_restarts})")
                    time.sleep(10)
                    heartbeat("Перезапуск після помилки")
                    continue
                else:
                    break

          # ── Внутрішній цикл завершився (3 невдачі) ──
          if restart_count >= max_restarts and not relogin_attempted:
              # Пробуємо автоматично відновити сесію
              ig_user = os.getenv('INSTAGRAM_USERNAME', '')
              ig_pass = os.getenv('INSTAGRAM_PASSWORD', '')
              session_json = str(SESSIONS_DIR / session_name.replace('.pkl', '.json'))

              if ig_user and ig_pass:
                  logger.info("=" * 60)
                  logger.info("  AUTO-RELOGIN: спроба відновити сесію...")
                  logger.info("=" * 60)
                  relogin_attempted = True
                  try:
                      from auto_login import auto_relogin
                      ok = auto_relogin(session_json, ig_user, ig_pass)
                  except Exception as re_err:
                      logger.error(f"Auto-relogin помилка: {re_err}")
                      ok = False

                  if ok:
                      logger.info("Сесію відновлено! Перезапускаю бота...")
                      relogin_attempted = False  # дозволяємо ще одну спробу в майбутньому
                      continue  # перезапускаємо зовнішній цикл
                  else:
                      logger.error("Auto-relogin не вдався")
                      self._notify_telegram(
                          f"🔴 Бот зупинено!\n"
                          f"Instagram {max_restarts} рази скинув сесію.\n"
                          f"Автоматичний вхід також не вдався.\n"
                          f"Потрібне ручне втручання!"
                      )
              else:
                  self._notify_telegram(
                      f"🔴 Бот зупинено!\n"
                      f"Instagram {max_restarts} рази поспіль скинув сесію.\n"
                      f"Задай INSTAGRAM_USERNAME і INSTAGRAM_PASSWORD в .env для авто-відновлення."
                  )
          break  # виходимо з зовнішнього циклу

        stop_watchdog()
        return False


def main():
    session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')
    interval_min = int(os.getenv('CHECK_INTERVAL_MIN', 30))
    interval_max = int(os.getenv('CHECK_INTERVAL_MAX', 120))

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
    bot.run(session_name, interval_min=interval_min, interval_max=interval_max)


if __name__ == '__main__':
    main()
