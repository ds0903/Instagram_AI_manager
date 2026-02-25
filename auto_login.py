"""
Auto Login - автоматичний вхід в Instagram та збереження сесії.
Використовується коли сесія згоріла і потрібно зайти заново по логін/пароль.

Запуск для тесту:
    python auto_login.py
"""
import os
import time
import random
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
SESSIONS_DIR = BASE_DIR / 'data' / 'sessions'

# Набори рандомних параметрів браузера
_WINDOW_PRESETS = [
    (1366, 768), (1440, 900), (1536, 864),
    (1600, 900), (1920, 1080), (1280, 800),
    (1280, 720), (1360, 768), (1400, 900),
]
_LOCALE_PRESETS = [
    'en-US', 'en-GB', 'en-AU', 'en-CA',
]
def _random_browser_params() -> dict:
    """Повертає рандомні параметри браузера для кожного запуску."""
    w, h = random.choice(_WINDOW_PRESETS)
    # Невеликий jitter ±10px щоб розмір не збігався з шаблоном
    w += random.randint(-10, 10)
    h += random.randint(-5, 5)
    return {
        'window': (w, h),
        'viewport': {'width': w, 'height': h},
        'locale': random.choice(_LOCALE_PRESETS),
    }


def auto_relogin(session_file_path: str, username: str, password: str) -> bool:
    """
    Автоматичний вхід в Instagram по логін/пароль.
    Зберігає сесію в session_file_path (JSON).
    Повертає True при успіху.
    """
    from camoufox.sync_api import Camoufox

    headless = os.getenv('HEADLESS', 'false').lower() == 'true'
    bp = _random_browser_params()
    logger.info(
        f"Auto-login: запускаю браузер (headless={headless}, "
        f"window={bp['window']}, locale={bp['locale']})..."
    )

    try:
        with Camoufox(
            headless=headless,
            geoip=True,
            humanize=True,
            locale=bp['locale'],
            window=bp['window'],
        ) as browser:
            context = browser.new_context(viewport=bp['viewport'])
            context.set_default_navigation_timeout(90000)
            context.set_default_timeout(30000)
            page = context.new_page()
            page.set_viewport_size(bp['viewport'])

            logger.info("Auto-login: відкриваю Instagram...")
            page.goto('https://www.instagram.com/accounts/login/', wait_until='domcontentloaded')
            time.sleep(3)

            # Поле username (name="email" — саме так Instagram називає поле логіну)
            # Fallback: autocomplete, або будь-який text input у формі login_form
            username_input = (
                page.query_selector('input[name="email"]') or
                page.query_selector('input[autocomplete="username webauthn"]') or
                page.query_selector('#login_form input[type="text"]')
            )
            if not username_input:
                logger.error("Auto-login: поле username не знайдено")
                return False
            username_input.click()
            for char in username:
                username_input.type(char)
                time.sleep(random.uniform(0.05, 0.18))
            logger.info(f"Auto-login: введено username={username}")
            time.sleep(random.uniform(0.4, 0.8))

            # Поле password (name="pass" — саме так Instagram називає поле пароля)
            # Fallback: type="password" у формі login_form
            password_input = (
                page.query_selector('input[name="pass"]') or
                page.query_selector('#login_form input[type="password"]') or
                page.query_selector('input[type="password"]')
            )
            if not password_input:
                logger.error("Auto-login: поле password не знайдено")
                return False
            # Клікаємо зліва (position x=20) — щоб не потрапити на іконку "ока" справа
            password_input.click(position={'x': 20, 'y': 10})
            for char in password:
                password_input.type(char)
                time.sleep(random.uniform(0.05, 0.18))
            time.sleep(random.uniform(0.4, 0.8))

            # Кнопка Log in (aria-label="Log In" або submit)
            login_btn = (
                page.query_selector('[aria-label="Log In"]') or
                page.query_selector('button[type="submit"]') or
                page.query_selector('#login_form [role="button"]')
            )
            if login_btn:
                login_btn.click()
                logger.info("Auto-login: натиснуто кнопку Log in")
            else:
                password_input.press('Enter')
                logger.info("Auto-login: натиснуто Enter (кнопку не знайдено)")

            # Чекаємо поки сторінка повністю завантажиться після Log in
            logger.info("Auto-login: очікую завантаження після Log in...")
            try:
                page.wait_for_load_state('networkidle', timeout=30000)
            except Exception:
                pass  # якщо timeout — продовжуємо далі

            # Натискаємо "Save info" якщо з'явився діалог
            save_btn = (
                page.query_selector("xpath=//button[@type='button'][text()='Save info']") or
                page.query_selector("xpath=//button[@type='button'][.//span[text()='Save info']]")
            )
            if save_btn:
                save_btn.click()
                logger.info("Auto-login: натиснуто 'Save info'")
                time.sleep(3)
            else:
                # Або "Not now"
                not_now = page.query_selector("xpath=//div[@role='button'][contains(.,'Not now')]")
                if not_now:
                    not_now.click()
                    logger.info("Auto-login: натиснуто 'Not now'")
                    time.sleep(2)

            # Переходимо на inbox і перевіряємо чи сесія жива
            logger.info("Auto-login: переходжу на inbox для перевірки сесії...")
            page.goto('https://www.instagram.com/direct/inbox/', wait_until='networkidle')

            current_url = page.url
            if 'login' in current_url or 'accounts' in current_url:
                logger.error(f"Auto-login: після переходу на inbox перекинуло на логін ({current_url})")
                logger.error("Auto-login: можливо CAPTCHA або невірні дані — перевір INSTAGRAM_USERNAME/INSTAGRAM_PASSWORD")
                return False

            logger.info(f"Auto-login: inbox завантажився! URL: {current_url}")

            # Зберігаємо сесію
            Path(session_file_path).parent.mkdir(parents=True, exist_ok=True)
            context.storage_state(path=session_file_path)
            logger.info(f"Auto-login: сесію збережено → {session_file_path}")
            return True

    except Exception as e:
        logger.error(f"Auto-login: помилка — {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    # Налаштування логування для ручного запуску
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    username = os.getenv('INSTAGRAM_USERNAME', '')
    password = os.getenv('INSTAGRAM_PASSWORD', '')
    session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')
    session_file = str(SESSIONS_DIR / session_name.replace('.pkl', '.json'))

    print("=" * 60)
    print("  AUTO LOGIN - Instagram")
    print("=" * 60)
    print(f"  Username : {username or '(не задано в .env)'}")
    print(f"  Session  : {session_file}")
    print("=" * 60)

    if not username or not password:
        print("ПОМИЛКА: задай INSTAGRAM_USERNAME і INSTAGRAM_PASSWORD в .env")
        exit(1)

    ok = auto_relogin(session_file, username, password)
    if ok:
        print("\n✓ Сесію збережено успішно! Можна запускати bot.py")
    else:
        print("\n✗ Не вдалося залогінитись. Перевір логін/пароль або CAPTCHA.")
        exit(1)
