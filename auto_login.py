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

VIEWPORT = {'width': 1400, 'height': 900}

_LOCALE_PRESETS = ['en-US', 'en-GB', 'en-AU', 'en-CA']

def _random_browser_params() -> dict:
    return {
        'window': (VIEWPORT['width'], VIEWPORT['height']),
        'viewport': VIEWPORT,
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
            username_input.focus()
            for char in username:
                username_input.type(char)
                time.sleep(random.uniform(0.07, 0.20))
            logger.info(f"Auto-login: введено username={username}")
            time.sleep(random.uniform(0.4, 0.9))

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
            # focus() замість click() — щоб не потрапити на іконку "ока" справа
            password_input.focus()
            typed_pwd = ''
            for char in password:
                password_input.type(char)
                typed_pwd += char
                time.sleep(random.uniform(0.07, 0.20))
            logger.info(f"Auto-login: введено password ({len(typed_pwd)} символів)")
            time.sleep(random.uniform(0.4, 0.9))

            # Натискаємо іконку "ока" щоб показати пароль — перевірка що введено вірно
            eye_btn = (
                page.query_selector('[aria-label="Show password"]') or
                page.query_selector('[aria-label="Show"]') or
                page.query_selector('svg[aria-label="Show password"]') or
                page.query_selector("xpath=//button[.//*[local-name()='svg']][ancestor::form]")
            )
            if eye_btn:
                eye_btn.click()
                logger.info("Auto-login: натиснуто 'показати пароль' (перевірка)")
                time.sleep(1.5)

            # Кнопка Log in — до 3 спроб поки не перекине зі сторінки логіну
            logged_in = False
            for login_attempt in range(1, 4):
                # Log in — div[role="button"] з aria-label="Log in" (маленька i!)
                login_btn = (
                    page.query_selector('[aria-label="Log in"]') or
                    page.query_selector('[aria-label="Log In"]') or
                    page.query_selector("xpath=//div[@role='button'][.//span[text()='Log in']]") or
                    page.query_selector("xpath=//button[normalize-space(.)='Log in']") or
                    page.query_selector('button[type="submit"]')
                )
                if login_btn:
                    login_btn.click()
                    logger.info(f"Auto-login: натиснуто Log in (спроба {login_attempt}/3)")
                else:
                    page.keyboard.press('Enter')  # keyboard замість stale element
                    logger.info(f"Auto-login: натиснуто Enter (спроба {login_attempt}/3, кнопку не знайдено)")

                # Чекаємо 10с поки щось зміниться: URL або "Save info" діалог
                logger.info("Auto-login: чекаю відповіді Instagram (до 15с)...")
                for _ in range(15):
                    time.sleep(1)
                    current = page.url
                    # Успіх 1: URL змінився (перейшли далі)
                    if 'accounts/login' not in current and '/login' not in current:
                        logged_in = True
                        logger.info(f"Auto-login: URL змінився → {current}")
                        break
                    # Успіх 2: з'явився діалог "Save your login info?" (URL ще accounts/login)
                    save_dialog = (
                        page.query_selector("xpath=//button[normalize-space(.)='Save info']") or
                        page.query_selector("xpath=//div[normalize-space(.)='Not now']")
                    )
                    if save_dialog:
                        logged_in = True
                        logger.info("Auto-login: з'явився діалог 'Save info' — вхід успішний!")
                        break

                if logged_in:
                    break

                if login_attempt < 3:
                    logger.warning(f"Auto-login: немає реакції після 10с, повторюю спробу {login_attempt + 1}/3...")
                    time.sleep(random.uniform(2, 4))

            # Натискаємо "Save info" якщо з'явився діалог
            time.sleep(1)
            save_btn = (
                page.query_selector("xpath=//button[normalize-space(.)='Save info']") or
                page.query_selector("xpath=//button[@type='button'][text()='Save info']") or
                page.query_selector("xpath=//button[@type='button'][.//span[text()='Save info']]")
            )
            if save_btn:
                save_btn.click()
                logger.info("Auto-login: натиснуто 'Save info'")
                time.sleep(3)
            else:
                # Або "Not now"
                not_now = page.query_selector("xpath=//div[normalize-space(.)='Not now']")
                if not_now:
                    not_now.click()
                    logger.info("Auto-login: натиснуто 'Not now'")
                    time.sleep(2)

            # Чекаємо поки Instagram сам завершить редирект після Save info / Not now
            logger.info("Auto-login: чекаю автоматичного редиректу Instagram...")
            for _ in range(15):
                time.sleep(1)
                if 'accounts' not in page.url and 'login' not in page.url:
                    break

            # Переходимо на inbox і перевіряємо чи сесія жива
            logger.info("Auto-login: переходжу на inbox для перевірки сесії...")
            try:
                page.goto('https://www.instagram.com/direct/inbox/', wait_until='domcontentloaded', timeout=30000)
            except Exception:
                # Якщо навігація перервана іншим редиректом — чекаємо і пробуємо ще раз
                time.sleep(3)
                page.goto('https://www.instagram.com/direct/inbox/', wait_until='domcontentloaded', timeout=30000)
            time.sleep(2)

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
