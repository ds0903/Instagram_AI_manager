"""
Skript dlia ruchnoho lohinu v Instagram i zberezhennia sesii (cookies).
Vidkryvaie Chrome, chekaie na ruchnyj login, zberihaje cookies u .pkl fajl.

Nazvy sesij:
- session_writer.pkl - holovnyj akkaunt (vidpovidaje na DM)
- session_reader.pkl - rezervnyj akkaunt (na majbutnie)
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


def manual_login_instagram():
    print("=" * 70)
    print("  RUCHNYJ LOGIN V INSTAGRAM")
    print("=" * 70)

    # Vybir sesii
    print("\nOber typ sesii:")
    print("  1 - session_writer.pkl (holovnyj akkaunt)")
    print("  2 - session_reader.pkl (rezervnyj)")

    while True:
        choice = input("\nVybir (1 abo 2): ").strip()
        if choice == '1':
            session_name = os.getenv('SESSION_FILE_WRITER', 'session_writer.pkl')
            break
        elif choice == '2':
            session_name = os.getenv('SESSION_FILE_READER', 'session_reader.pkl')
            break
        else:
            print("Vvedy 1 abo 2!")

    print(f"\nSesiia bude zberezena jak: {session_name}")

    print("\n" + "=" * 70)
    print("  VIDKRYVAIU BRAUZER...")
    print("=" * 70)

    # Vyznachaiemo dyrekroriiu proektu
    if getattr(sys, 'frozen', False):
        current_dir = Path(os.path.dirname(sys.executable))
    else:
        current_dir = Path(__file__).parent

    # Stvorjujemo papku dlia Chrome profiliu
    profile_dir = current_dir / 'data' / 'chrome_profile'
    profile_dir.mkdir(parents=True, exist_ok=True)

    # Zapuskajemo Chrome z profilem
    options = uc.ChromeOptions()
    options.add_argument(f'--user-data-dir={profile_dir}')
    options.add_argument('--window-size=1200,900')

    print(f"\nProfil: {profile_dir}")

    driver = None

    try:
        driver = uc.Chrome(options=options, version_main=detect_chrome_version())
        print("Chrome zapuscheno")

        # Perekhodym na storinku lohinu Instagram
        print("\nPerekhodju na https://www.instagram.com/accounts/login")
        driver.get('https://www.instagram.com/accounts/login')
        time.sleep(3)

        print("\n" + "=" * 70)
        print("  OCHIKUVANNIA LOHINU...")
        print("=" * 70)
        print("\nUvijdy v akkaunt VRUCHNU")
        print("Pislia uspishnoho vkhodu natysniy ENTER tut")

        input("\nNatysniy ENTER pislia uspishnoho vkhodu...")

        # Perevirijajemo chy brauzer shche vidkrytyj
        try:
            driver.current_url
            print("\nZberigaju cookies...")
            time.sleep(2)
        except Exception:
            print("\nBrauzer vzhe zakryto!")
            print("Ne mozhu zberehty sesiiu!")
            input("\nNatysniy Enter dlia vykhodu...")
            return

        # Stvorjujemo papku sessions
        sessions_dir = current_dir / 'data' / 'sessions'
        sessions_dir.mkdir(parents=True, exist_ok=True)

        session_file = sessions_dir / session_name

        try:
            cookies = driver.get_cookies()
        except Exception:
            print("Ne vdalosja otrymaty cookies - mozhlyvo brauzer zakryto")
            input("\nNatysniy Enter dlia vykhodu...")
            return

        if len(cookies) > 0:
            with open(session_file, 'wb') as f:
                pickle.dump(cookies, f)

            print(f"Sesiiu zberezeno: {session_file}")
            print(f"Zberezeno {len(cookies)} cookies")

            # Zakryvajemo brauzer
            print("\nZakryvaju brauzer...")
            try:
                driver.quit()
                print("Brauzer zakryto")
            except Exception:
                print("Brauzer vzhe zakryto")

            print("\n" + "=" * 70)
            print("  HOTOVO!")
            print("=" * 70)
            print(f"\nFajl sesii: {session_file}")
            print("Teper mozhna zapuskaty bot.py")

            input("\nNatysniy ENTER dlia vykhodu...")
        else:
            print("Cookies ne znajdeno. Mozhlyvo ty ne zalohinyvsia?")

            try:
                driver.quit()
            except Exception:
                pass

            input("\nNatysniy Enter dlia vykhodu...")

    except Exception as e:
        print(f"\nPomylka: {e}")
        import traceback
        traceback.print_exc()

        if driver:
            try:
                driver.quit()
            except Exception:
                pass

        input("\nNatysniy Enter dlia vykhodu...")


if __name__ == '__main__':
    try:
        manual_login_instagram()
    except KeyboardInterrupt:
        print("\n\nVykhid...")
    except Exception as e:
        print(f"\nKrytychna pomylka: {e}")
        import traceback
        traceback.print_exc()
        input("\nNatysniy Enter dlia vykhodu...")
