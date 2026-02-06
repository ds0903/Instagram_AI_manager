"""
Instagram AI Agent Bot
Vkhid cherez cookies (session fajl), perehid v Direct, AI vidpovidi.

Funktsii:
- Watchdog (heartbeat) - vyjavlennia zavisannja (3 hv tajmaut)
- Avtoperezapusk - do 3 sprob pry pomylkakh
- Telegram spovischennia - pro pomylky, sesiju, AI
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

# Lohuvannia
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# Vymykajemo zayvi lohy
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('selenium').setLevel(logging.WARNING)

# Bazova dyrekroriia proektu
BASE_DIR = Path(__file__).parent
SESSIONS_DIR = BASE_DIR / 'data' / 'sessions'
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# ==================== WATCHDOG (Heartbeat) ====================
_watchdog_running = False
_watchdog_thread = None
_last_heartbeat = time.time()
WATCHDOG_TIMEOUT_MINUTES = 3  # Tajmaut zavisannia

# Zberihaemo PID Chrome protsesu
_chrome_pid = None
_chrome_pids_file = BASE_DIR / 'data' / 'chrome_pids.txt'

# Prefiks dlia nashykh Chrome profiliv
CHROME_PROFILE_PREFIX = 'chrome_insta_'


def heartbeat(operation_name: str = None):
    """Onovyty heartbeat (povidomyty shcho bot zhyvyj)"""
    global _last_heartbeat
    _last_heartbeat = time.time()
    if operation_name:
        logger.debug(f"Heartbeat: {operation_name}")


def _save_chrome_pid(pid: int):
    """Zberehty PID Chrome v fajl"""
    global _chrome_pid
    _chrome_pid = pid
    try:
        _chrome_pids_file.parent.mkdir(parents=True, exist_ok=True)
        with open(_chrome_pids_file, 'a') as f:
            f.write(f"{pid}\n")
        logger.debug(f"Chrome PID zberezeno: {pid}")
    except Exception as e:
        logger.warning(f"Ne vdalosja zberehty PID: {e}")


def _get_saved_chrome_pids() -> list:
    """Otrymaty zberezeni PID z fajlu"""
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
    """Ochystyty fajl z PID"""
    try:
        if _chrome_pids_file.exists():
            _chrome_pids_file.unlink()
    except Exception:
        pass


def _kill_process_tree(pid: int):
    """Vbyty protses i vsi joho dochirni protsesy"""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)

        # Spochatku vbyvajemo ditej
        for child in children:
            try:
                child.kill()
                logger.debug(f"Vbyto dochirni protses: {child.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        # Potim batka
        try:
            parent.kill()
            logger.info(f"Vbyto Chrome protses: {pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    except psutil.NoSuchProcess:
        logger.debug(f"Protses {pid} vzhe ne isnuje")
    except Exception as e:
        logger.warning(f"Pomylka vbyvstva protsesu {pid}: {e}")


def _kill_chrome_by_profile():
    """Vbyty vsi Chrome protsesy z nashym profilem (chrome_insta_*)"""
    killed_count = 0
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name = proc.info['name'] or ''
                cmdline = proc.info['cmdline'] or []

                # Pereviriajemo chy tse Chrome/chromedriver
                if 'chrome' not in name.lower():
                    continue

                # Pereviriajemo chy ye nash profil v komandnomu riadku
                cmdline_str = ' '.join(cmdline)
                if CHROME_PROFILE_PREFIX in cmdline_str:
                    _kill_process_tree(proc.info['pid'])
                    killed_count += 1

            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

    except Exception as e:
        logger.warning(f"Pomylka poshuku Chrome protsesiv: {e}")

    return killed_count


def _kill_all_chrome():
    """
    Vbyty TILKY nashi Chrome protsesy (ne chipaje inshi):
    1. Po zberezhenomu PID
    2. Po profileiu chrome_insta_*
    3. Ochystyty tymchasovi papky
    """
    global _chrome_pid

    logger.info("Ochyschennia Chrome protsesiv (tilky nashi)...")
    killed_total = 0

    # 1. Vbyvajemo po zberezhenomu PID (potochnyj)
    if _chrome_pid:
        logger.info(f"Vbyvaju po potochnomu PID: {_chrome_pid}")
        _kill_process_tree(_chrome_pid)
        killed_total += 1
        _chrome_pid = None

    # 2. Vbyvajemo po zberezhenym PID z fajlu (poperedni zapusky)
    saved_pids = _get_saved_chrome_pids()
    if saved_pids:
        logger.info(f"Vbyvaju po zberezhenym PID: {saved_pids}")
        for pid in saved_pids:
            _kill_process_tree(pid)
            killed_total += 1

    # 3. Vbyvajemo po profileiu (garantovano znajde vsi)
    logger.info(f"Shukaju Chrome protsesy z profilem '{CHROME_PROFILE_PREFIX}*'...")
    killed_by_profile = _kill_chrome_by_profile()
    killed_total += killed_by_profile

    # Chekajemo shchob Windows zvilnyv lock-fajly
    if platform.system() == 'Windows':
        time.sleep(2)

    # 4. Ochyschuiemo tymchasovi profili
    temp_dir = tempfile.gettempdir()
    cleaned_dirs = 0
    for item in Path(temp_dir).glob(f'{CHROME_PROFILE_PREFIX}*'):
        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
                cleaned_dirs += 1
                logger.debug(f"Vydaleno profil: {item}")
        except Exception:
            pass

    # 5. Ochyschuiemo fajl z PID
    _clear_chrome_pids_file()

    logger.info(f"Ochyscheno: {killed_total} protsesiv, {cleaned_dirs} profiliv")


def _watchdog_loop():
    """Tsykl watchdog - pereviryaie chy ne zavisly"""
    global _watchdog_running, _last_heartbeat

    timeout_seconds = WATCHDOG_TIMEOUT_MINUTES * 60

    while _watchdog_running:
        time.sleep(30)  # Pereviriajemo kozni 30 sekund

        if not _watchdog_running:
            break

        elapsed = time.time() - _last_heartbeat

        if elapsed > timeout_seconds:
            logger.error("=" * 60)
            logger.error(f"WATCHDOG: TAJMAUT! Operatsiia zavisla na {elapsed/60:.1f} khvylyn!")
            logger.error("=" * 60)
            logger.error("Vbyvaju Chrome i perezapuskaju skrypt...")

            # Spovischennia v Telegram
            try:
                from telegram_notifier import TelegramNotifier
                notifier = TelegramNotifier()
                notifier.notify_error(f"Bot zavis na {elapsed/60:.1f} khv. Perezapusk...")
            except Exception:
                pass

            # Vbyvajemo Chrome
            _kill_all_chrome()

            # Perezapuskajemo skrypt
            logger.error("Perezapusk skrypta cherez 5 sekund...")
            time.sleep(5)

            python = sys.executable
            os.execl(python, python, *sys.argv)


def start_watchdog():
    """Zapustyty watchdog tred"""
    global _watchdog_running, _watchdog_thread, _last_heartbeat

    _last_heartbeat = time.time()
    _watchdog_running = True

    _watchdog_thread = threading.Thread(target=_watchdog_loop, daemon=True)
    _watchdog_thread.start()

    logger.info(f"Watchdog zapuscheno (tajmaut: {WATCHDOG_TIMEOUT_MINUTES} khv)")


def stop_watchdog():
    """Zupynyty watchdog"""
    global _watchdog_running
    _watchdog_running = False


def detect_chrome_version():
    """Vyznachaie versiiu Chrome. Povertaie int abo None."""
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
        """Zapusk Chrome z antydetekt nalashtuvanniamy."""
        import uuid
        unique_id = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"
        self.temp_profile_dir = tempfile.mkdtemp(prefix=f'chrome_insta_{unique_id}_')

        options = uc.ChromeOptions()

        if headless:
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            logger.info("Headless rezhym uvimkneno")

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

        logger.info(f"Zapusk Chrome z profilem: {self.temp_profile_dir}")

        try:
            chrome_version = detect_chrome_version()
            if chrome_version:
                logger.info(f"Versiia Chrome: {chrome_version}")

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

            # Zberigajemo PID Chrome protsesu
            try:
                chrome_pid = self.driver.service.process.pid
                _save_chrome_pid(chrome_pid)
                logger.info(f"Chrome zapuscheno uspishno (PID: {chrome_pid})")
            except Exception:
                logger.info("Chrome zapuscheno uspishno (antydetekt)")

        except Exception as e:
            logger.error(f"Pomylka zapusku Chrome: {e}")
            raise

    def init_ai_components(self):
        """Iniitsializatsiia AI komponentiv (DB, AI Agent, Direct Handler)."""
        try:
            from database import Database
            from ai_agent import AIAgent
            from direct_handler import DirectHandler

            # Database
            self.db = Database()
            logger.info("Database pidkliucheno")

            # AI Agent
            self.ai_agent = AIAgent(self.db)
            logger.info("AI Agent iniitsializovano")

            # Direct Handler
            self.direct_handler = DirectHandler(self.driver, self.ai_agent)
            logger.info("Direct Handler iniitsializovano")

            return True

        except Exception as e:
            logger.error(f"Pomylka iniitsializatsii AI komponentiv: {e}")
            import traceback
            traceback.print_exc()
            return False

    def load_session(self, session_name: str = None):
        """Zavantazhennia cookies z session fajlu."""
        # Vykorystovujemo nazvu z .env abo parametru
        if session_name is None:
            session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')

        session_file = SESSIONS_DIR / session_name
        if not session_file.exists():
            logger.error(f"Session fajl ne znajdeno: {session_file}")
            logger.error(f"Stvor jogo cherez login_helper.py")
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
                    logger.debug(f"Pomylka dodavannia cookie: {e}")

            self.driver.refresh()
            time.sleep(3)

            if self.is_logged_in():
                logger.info(f"Sesiia zavantazhena: {session_name}")
                return True
            else:
                logger.error("Cookies zavantazheno, ale login ne projshov")
                return False

        except Exception as e:
            logger.error(f"Pomylka zavantazhennia sesii: {e}")
            return False

    def is_logged_in(self):
        """Perevirka chy zalohineni v Instagram."""
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
        """Perehid v Instagram Direct (povidomlennia)."""
        try:
            logger.info("Perehodzhu v Direct...")
            self.driver.get('https://www.instagram.com/direct/inbox/')
            time.sleep(5)

            current_url = self.driver.current_url
            if 'direct' in current_url:
                logger.info(f"Uspishno vidkryto Direct: {current_url}")
                return True
            else:
                logger.error(f"Ne vdalosja vidkryty Direct. Potochna URL: {current_url}")
                return False

        except Exception as e:
            logger.error(f"Pomylka perehodu v Direct: {e}")
            return False

    def close(self):
        """Zakryttia brauzera ta ochystka."""
        global _chrome_pid

        # Zakryvajemo DB
        if self.db:
            try:
                self.db.close()
            except Exception:
                pass
            self.db = None

        if self.driver:
            try:
                self.driver.quit()
                logger.info("Brauzer zakryto")
            except Exception as e:
                logger.warning(f"Pomylka zakryttia brauzera: {e}")
            self.driver = None

        # Skydajemo PID
        _chrome_pid = None

        if self.temp_profile_dir and Path(self.temp_profile_dir).exists():
            try:
                time.sleep(2)
                shutil.rmtree(self.temp_profile_dir, ignore_errors=True)
                logger.info("Tymchasovyj profil vydaleno")
            except Exception as e:
                logger.warning(f"Ne vdalosja vydalyty profil: {e}")
            self.temp_profile_dir = None

    def _notify_telegram(self, message: str):
        """Vidpravyty povidomlennia v Telegram pro pomylku"""
        try:
            from telegram_notifier import TelegramNotifier
            notifier = TelegramNotifier()
            notifier.notify_error(message)
        except Exception as e:
            logger.warning(f"Ne vdalosja vidpravyty v Telegram: {e}")

    def run(self, session_name: str = None, check_interval: int = 30):
        """
        Holovnyj potik z avtoperezapuskom.
        - Watchdog (3 khv tajmaut)
        - Do 3 sprob perezapusku
        - Telegram spovischennia pro pomylky
        """
        if session_name is None:
            session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')

        restart_count = 0
        max_restarts = 3

        # Zapuskajemo watchdog
        start_watchdog()
        heartbeat("Start bota")

        while restart_count < max_restarts:
            try:
                if restart_count > 0:
                    logger.info("=" * 60)
                    logger.info(f"  PEREZAPUSK #{restart_count}")
                    logger.info("=" * 60)
                    time.sleep(5)

                logger.info("=" * 60)
                logger.info(f"  ZAPUSK INSTAGRAM AI AGENT")
                logger.info(f"  Session: {session_name}")
                logger.info("=" * 60)

                heartbeat("Init driver")

                # 1. Zapuskajemo brauzer
                self.init_driver()

                # 2. Zavantazhujemo sesiiu (cookies)
                heartbeat("Load session")
                if not self.load_session(session_name):
                    logger.error("Sesiia ne validna! Perevir session fajl.")
                    self._notify_telegram(f"Sesiia ne validna: {session_name}\nPotribno perezajty v akkaunt!")
                    self.close()
                    _kill_all_chrome()
                    return False  # Ne perezapuskajemo - potreben ruchnyj login

                logger.info("Uspishno zalohineno v Instagram!")
                heartbeat("Logged in")

                # 3. Iniitsializujemo AI komponenty
                if not self.init_ai_components():
                    logger.error("Ne vdalosja iniitsializuvaty AI komponenty")
                    self._notify_telegram("Pomylka initsializatsii AI komponentiv!")
                    raise Exception("AI init failed")

                # 4. Perekhodym v Direct
                heartbeat("Go to Direct")
                if not self.go_to_direct():
                    logger.error("Ne vdalosja vidkryty Direct.")
                    raise Exception("Direct open failed")

                logger.info("=" * 60)
                logger.info("  AI AGENT ZAPUSCHENO!")
                logger.info(f"  Interval perevirky: {check_interval}s")
                logger.info("  Ctrl+C dlia zupynky")
                logger.info("=" * 60)

                # 5. Zapuskajemo tsykl obrobky povidomlen
                heartbeat("Start inbox loop")
                self.direct_handler.run_inbox_loop(
                    check_interval=check_interval,
                    heartbeat_callback=heartbeat
                )

                # Jakshcho diyshly siudy - uspishne zavershenia
                restart_count = 0  # Skydajemo lichilnyk

            except KeyboardInterrupt:
                logger.info("Zupynka za zapytom korystuvacha (Ctrl+C)")
                self.close()
                _kill_all_chrome()
                stop_watchdog()
                return True

            except Exception as e:
                logger.error(f"Pomylka: {e}")
                import traceback
                traceback.print_exc()

                # Zakryvajemo brauzer
                self.close()

                # Vbyvajemo VSI Chrome protsesy
                _kill_all_chrome()

                restart_count += 1

                if restart_count < max_restarts:
                    logger.info(f"Perezapusk cherez 10 sekund... (sproba {restart_count}/{max_restarts})")
                    time.sleep(10)
                    heartbeat("Perezapusk pislia pomylky")
                    continue
                else:
                    break

        # Dosiahnuto limit perezapuskiv
        if restart_count >= max_restarts:
            logger.error("=" * 60)
            logger.error(f"DOSIAHNUTO LIMIT {max_restarts} PEREZAPUSKIV!")
            logger.error("Shchos serjozno ne tak. Perevir sesiju/internet.")
            logger.error("=" * 60)
            self._notify_telegram(f"Bot zupyneno: dosiahnuto limit {max_restarts} perezapuskiv!")
            _kill_all_chrome()

        stop_watchdog()
        return False


def main():
    session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')
    check_interval = int(os.getenv('CHECK_INTERVAL_SECONDS', 30))

    # Argumenty komanndoho rjadka
    if '--session' in sys.argv:
        try:
            idx = sys.argv.index('--session')
            session_name = sys.argv[idx + 1]
        except (ValueError, IndexError):
            logger.error("Neviernyj nazva sesii")
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
