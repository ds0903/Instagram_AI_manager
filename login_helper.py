"""
Скрипт для ручного логіну в Instagram і збереження сесії.
Відкриває Camoufox (Firefox), чекає на ручний логін, зберігає сесію у .json файл.

Назви сесій:
- session_writer.pkl → session_writer.json (головний акаунт, відповідає на DM)
- session_reader.pkl → session_reader.json (резервний акаунт, на майбутнє)
"""
import time
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from camoufox.sync_api import Camoufox


VPS_HOST = '185.235.219.33'
VPS_USER = 'root'
VPS_PASSWORD = '1aQG2sktXKA15p8'
VPS_SESSION_DIR = '/root/Instagram_AI_manager/data/sessions'


def upload_session_to_vps(local_path: str):
    """Завантажити файл сесії на VPS через SFTP."""

    try:
        import paramiko
        filename = os.path.basename(local_path)
        remote_path = f"{VPS_SESSION_DIR}/{filename}"

        print(f"\nПідключаюсь до VPS {VPS_HOST}...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(VPS_HOST, username=VPS_USER, password=VPS_PASSWORD, timeout=15)

        sftp = ssh.open_sftp()
        try:
            sftp.mkdir(VPS_SESSION_DIR)
        except Exception:
            pass
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()

        print(f"✓ Сесію завантажено на VPS: {remote_path}")
    except ImportError:
        print("paramiko не встановлено. Запусти: pip install paramiko")
    except Exception as e:
        print(f"Помилка завантаження на VPS: {e}")

load_dotenv()


def manual_login_instagram():
    print("=" * 70)
    print("  РУЧНИЙ ЛОГІН В INSTAGRAM (Camoufox/Firefox)")
    print("=" * 70)

    # Вибір сесії
    print("\nОбери тип сесії:")
    print("  1 - session_writer (головний акаунт)")
    print("  2 - session_reader (резервний)")

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

    # Визначаємо директорію проекту
    if getattr(sys, 'frozen', False):
        current_dir = Path(os.path.dirname(sys.executable))
    else:
        current_dir = Path(__file__).parent

    sessions_dir = current_dir / 'data' / 'sessions'
    sessions_dir.mkdir(parents=True, exist_ok=True)

    # JSON файл сесії (замість pkl)
    session_json = str(sessions_dir / session_name).replace('.pkl', '.json')
    print(f"\nСесія буде збережена: {session_json}")

    print("\n" + "=" * 70)
    print("  ВІДКРИВАЮ БРАУЗЕР (Firefox/Camoufox)...")
    print("=" * 70)

    try:
        headless = os.getenv('HEADLESS', 'false').lower() == 'true'
        with Camoufox(headless=headless, geoip=True, humanize=True, window=(1400, 900)) as browser:
            context = browser.new_context(viewport={"width": 1400, "height": 900})
            context.set_default_navigation_timeout(90000)
            page = context.new_page()
            page.set_viewport_size({"width": 1400, "height": 900})

            print("\nПереходжу на https://www.instagram.com/accounts/login")
            page.goto('https://www.instagram.com/accounts/login', wait_until='domcontentloaded')
            time.sleep(2)

            print("\n" + "=" * 70)
            print("  ОЧІКУВАННЯ ЛОГІНУ...")
            print("=" * 70)
            print("\nУвійди в акаунт ВРУЧНУ у браузері")
            print("Після успішного входу натисни ENTER тут")

            input("\nНатисни ENTER після успішного входу...")

            # Зберігаємо сесію
            print("\nЗберігаю сесію...")
            context.storage_state(path=session_json)

            print(f"\n✓ Сесію збережено: {session_json}")

            # Питаємо чи завантажувати на VPS
            choice = input("\nЗавантажити сесію на VPS? (Enter = так, N = ні): ").strip().lower()
            if choice != 'n':
                upload_session_to_vps(session_json)

            print("\n" + "=" * 70)
            print("  ГОТОВО!")
            print("=" * 70)
            print("Тепер можна запускати bot.py")

            time.sleep(2)

    except Exception as e:
        print(f"\nПомилка: {e}")
        import traceback
        traceback.print_exc()

    input("\nНатисни ENTER для виходу...")


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
