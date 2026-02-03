"""
Instagram AI Agent Bot
Vkhid cherez cookies (session fajl), perehid v Direct, AI vidpovidi.
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

    def load_session(self, account_id):
        """Zavantazhennia cookies z session fajlu."""
        session_file = SESSIONS_DIR / f"{account_id}_session.pkl"
        if not session_file.exists():
            logger.error(f"Session fajl ne znajdeno: {session_file}")
            logger.error(f"Stvor jogo cherez login_helper.py dlia akaunta ID={account_id}")
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
                logger.info(f"Sesiia zavantazhena dlia akaunta ID={account_id}")
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

        if self.temp_profile_dir and Path(self.temp_profile_dir).exists():
            try:
                import shutil
                time.sleep(2)
                shutil.rmtree(self.temp_profile_dir, ignore_errors=True)
                logger.info("Tymchasovyj profil vydaleno")
            except Exception as e:
                logger.warning(f"Ne vdalosja vydalyty profil: {e}")
            self.temp_profile_dir = None

    def run(self, account_id, check_interval: int = 30):
        """
        Holovnyj potik: init -> login -> Direct -> AI loop.

        Args:
            account_id: ID akaunta
            check_interval: interval perevirky povidomlen (sekundy)
        """
        logger.info("=" * 60)
        logger.info(f"  ZAPUSK INSTAGRAM AI AGENT")
        logger.info(f"  Account ID: {account_id}")
        logger.info("=" * 60)

        try:
            # 1. Zapuskajemo brauzer
            self.init_driver()

            # 2. Zavantazhujemo sesiiu (cookies)
            if not self.load_session(account_id):
                logger.error("Vkhid ne vdavsia. Perevir session fajl.")
                return False

            logger.info("Uspishno zalohineno v Instagram!")

            # 3. Iniitsializujemo AI komponenty
            if not self.init_ai_components():
                logger.error("Ne vdalosja iniitsializuvaty AI komponenty")
                return False

            # 4. Perekhodym v Direct
            if not self.go_to_direct():
                logger.error("Ne vdalosja vidkryty Direct.")
                return False

            logger.info("=" * 60)
            logger.info("  AI AGENT ZAPUSCHENO!")
            logger.info(f"  Interval perevirky: {check_interval}s")
            logger.info("  Ctrl+C dlia zupynky")
            logger.info("=" * 60)

            # 5. Zapuskajemo tsykl obrobky povidomlen
            self.direct_handler.run_inbox_loop(check_interval=check_interval)

            return True

        except KeyboardInterrupt:
            logger.info("Zupynka za zapytom korystuvacha (Ctrl+C)")
            return True

        except Exception as e:
            logger.error(f"Pomylka: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            self.close()


def main():
    account_id = int(os.getenv('INSTAGRAM_ACCOUNT_ID', 1))
    check_interval = 30  # sekundy

    # Argumenty komanndoho rjadka
    if '--account' in sys.argv:
        try:
            idx = sys.argv.index('--account')
            account_id = int(sys.argv[idx + 1])
        except (ValueError, IndexError):
            logger.error("Neviernyj ID akaunta")
            return

    if '--interval' in sys.argv:
        try:
            idx = sys.argv.index('--interval')
            check_interval = int(sys.argv[idx + 1])
        except (ValueError, IndexError):
            pass

    bot = InstagramBot()
    bot.run(account_id, check_interval=check_interval)


if __name__ == '__main__':
    main()
