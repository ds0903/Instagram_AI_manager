"""
Скрипт для ручного логіну в Instagram і збереження сесії (cookies).
Відкриває Chrome, чекає на ручний логін, зберігає cookies у .pkl файл.

Назви сесій:
- session_writer.pkl - головний акаунт (відповідає на DM)
- session_reader.pkl - резервний акаунт (на майбутнє)
"""
import undetected_chromedriver as uc
import pickle
import time
import os
import sys
import platform
import re
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


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


def manual_login_instagram():
    print("=" * 70)
    print("  РУЧНИЙ ЛОГІН В INSTAGRAM")
    print("=" * 70)

    # Вибір сесії
    print("\nОбери тип сесії:")
    print("  1 - session_writer.pkl (головний акаунт)")
    print("  2 - session_reader.pkl (резервний)")

    while True:
        choice = input("\nВибір (1 або 2): ").strip()
        if choice == '1':
            session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')
            break
        elif choice == '2':
            session_name = os.getenv('SESSION_FILE_READER', 'session_reader.pkl')
            break
        else:
            print("Введи 1 або 2!")

    print(f"\nСесія буде збережена як: {session_name}")

    print("\n" + "=" * 70)
    print("  ВІДКРИВАЮ БРАУЗЕР...")
    print("=" * 70)

    # Визначаємо директорію проекту
    if getattr(sys, 'frozen', False):
        current_dir = Path(os.path.dirname(sys.executable))
    else:
        current_dir = Path(__file__).parent

    # Створюємо папку для Chrome профілю
    profile_dir = current_dir / 'data' / 'chrome_profile'
    profile_dir.mkdir(parents=True, exist_ok=True)

    # Запускаємо Chrome з профілем
    options = uc.ChromeOptions()
    options.add_argument(f'--user-data-dir={profile_dir}')
    options.add_argument('--window-size=1200,900')

    print(f"\nПрофіль: {profile_dir}")

    driver = None

    try:
        driver = uc.Chrome(options=options, version_main=detect_chrome_version())
        print("Chrome запущено")

        # Переходимо на сторінку логіну Instagram
        print("\nПереходжу на https://www.instagram.com/accounts/login")
        driver.get('https://www.instagram.com/accounts/login')
        time.sleep(3)

        print("\n" + "=" * 70)
        print("  ОЧІКУВАННЯ ЛОГІНУ...")
        print("=" * 70)
        print("\nУвійди в акаунт ВРУЧНУ")
        print("Після успішного входу натисни ENTER тут")

        input("\nНатисни ENTER після успішного входу...")

        # Перевіряємо чи браузер ще відкритий
        try:
            driver.current_url
            print("\nЗберігаю cookies...")
            time.sleep(2)
        except Exception:
            print("\nБраузер вже закрито!")
            print("Не можу зберегти сесію!")
            input("\nНатисни Enter для виходу...")
            return

        # Створюємо папку sessions
        sessions_dir = current_dir / 'data' / 'sessions'
        sessions_dir.mkdir(parents=True, exist_ok=True)

        session_file = sessions_dir / session_name

        try:
            cookies = driver.get_cookies()
        except Exception:
            print("Не вдалося отримати cookies - можливо браузер закрито")
            input("\nНатисни Enter для виходу...")
            return

        if len(cookies) > 0:
            with open(session_file, 'wb') as f:
                pickle.dump(cookies, f)

            print(f"Сесію збережено: {session_file}")
            print(f"Збережено {len(cookies)} cookies")

            # Закриваємо браузер
            print("\nЗакриваю браузер...")
            try:
                driver.quit()
                print("Браузер закрито")
            except Exception:
                print("Браузер вже закрито")

            print("\n" + "=" * 70)
            print("  ГОТОВО!")
            print("=" * 70)
            print(f"\nФайл сесії: {session_file}")
            print("Тепер можна запускати bot.py")

            input("\nНатисни ENTER для виходу...")
        else:
            print("Cookies не знайдено. Можливо ти не залогінився?")

            try:
                driver.quit()
            except Exception:
                pass

            input("\nНатисни Enter для виходу...")

    except Exception as e:
        print(f"\nПомилка: {e}")
        import traceback
        traceback.print_exc()

        if driver:
            try:
                driver.quit()
            except Exception:
                pass

        input("\nНатисни Enter для виходу...")


if __name__ == '__main__':
    try:
        manual_login_instagram()
    except KeyboardInterrupt:
        print("\n\nВихід...")
    except Exception as e:
        print(f"\nКритична помилка: {e}")
        import traceback
        traceback.print_exc()
        input("\nНатисни Enter для виходу...")
