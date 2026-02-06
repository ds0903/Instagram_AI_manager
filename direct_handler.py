"""
Instagram Direct Handler
Chytannia ta vidpravka povidomlen v Direct cherez Selenium
"""
import time
import random
import logging
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)


class DirectHandler:
    def __init__(self, driver, ai_agent):
        self.driver = driver
        self.ai_agent = ai_agent
        self.processed_messages = set()  # Vzhe obrobleni povidomlennia

    def go_to_inbox(self) -> bool:
        """Perehid v Direct inbox."""
        try:
            self.driver.get('https://www.instagram.com/direct/inbox/')
            time.sleep(3)

            # Chekajemo zavantazhennia
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'x9f619')]"))
            )

            logger.info("Direct inbox vidkryto")
            return True
        except Exception as e:
            logger.error(f"Pomylka vidkryttia inbox: {e}")
            return False

    def get_unread_chats(self) -> list:
        """
        Otrymaty spysok chatov z neprocytanymy povidomlenniamy.
        Povertaie list slovnykiv: [{'username': str, 'element': WebElement, 'unread': bool}]
        """
        chats = []
        try:
            # Shukajemo vsi chaty v spysku
            chat_elements = self.driver.find_elements(
                By.XPATH, "//div[@role='listitem']//a[contains(@href, '/direct/t/')]"
            )

            for chat_elem in chat_elements:
                try:
                    href = chat_elem.get_attribute('href')
                    # Otrymujemo username z chatu (jakshcho ye)
                    username_elem = chat_elem.find_element(By.XPATH, ".//span")
                    username = username_elem.text if username_elem else "unknown"

                    # Pereviriajemo chy ye neprocytani (blakytna krapka)
                    unread = False
                    try:
                        # Shukajemo indykator neprocytanoho
                        chat_elem.find_element(By.XPATH, ".//div[contains(@class, 'unread')]")
                        unread = True
                    except Exception:
                        pass

                    chats.append({
                        'username': username,
                        'href': href,
                        'element': chat_elem,
                        'unread': unread
                    })
                except Exception:
                    continue

            logger.info(f"Znajdeno {len(chats)} chativ, z nykh neprocytanykh: {sum(1 for c in chats if c['unread'])}")
            return chats

        except Exception as e:
            logger.error(f"Pomylka otrymanna chativ: {e}")
            return []

    def open_chat(self, chat_href: str) -> bool:
        """Vidkryty konkretnyj chat."""
        try:
            self.driver.get(chat_href)
            time.sleep(2)

            # Chekajemo zavantazhennia chatu
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
            )

            logger.info(f"Chat vidkryto: {chat_href}")
            return True
        except Exception as e:
            logger.error(f"Pomylka vidkryttia chatu: {e}")
            return False

    def get_chat_messages(self) -> list:
        """
        Otrymaty povidomlennia z vidkrytoho chatu.
        Povertaie list: [{'role': 'user'/'assistant', 'content': str, 'timestamp': datetime}]
        """
        messages = []
        try:
            # Shukajemo vsi povidomlennia v chati
            message_elements = self.driver.find_elements(
                By.XPATH, "//div[contains(@class, 'x1lliihq')]//span"
            )

            for msg_elem in message_elements:
                try:
                    content = msg_elem.text
                    if not content or len(content) < 1:
                        continue

                    # Vyznachajemo chy tse nashe povidomlennia chy klienta
                    # (Tse sproshchena loghika - treba dorobyty dlia tochnoi identyfikatsii)
                    parent = msg_elem.find_element(By.XPATH, "./ancestor::div[contains(@class, 'message')]")
                    is_own = 'own' in parent.get_attribute('class').lower() if parent else False

                    messages.append({
                        'role': 'assistant' if is_own else 'user',
                        'content': content,
                        'timestamp': datetime.now()  # Treba parsynh realnoho chasu
                    })
                except Exception:
                    continue

            return messages

        except Exception as e:
            logger.error(f"Pomylka chytannia povidomlen: {e}")
            return []

    def get_last_message(self) -> dict:
        """Otrymaty ostannie povidomlennia v chati."""
        try:
            # Shukajemo ostannie povidomlennia
            message_divs = self.driver.find_elements(
                By.XPATH, "//div[@role='row']//div[contains(@class, 'x1lliihq')]"
            )

            if not message_divs:
                return None

            last_msg_div = message_divs[-1]

            # Otrymujemo tekst
            try:
                content_span = last_msg_div.find_element(By.XPATH, ".//span")
                content = content_span.text
            except Exception:
                content = last_msg_div.text

            if not content:
                return None

            # Vyznachajemo vid koho povidomlennia
            # Perevirka po styliu/klasam (treba adaptuvaty pid aktualnyj Instagram)
            parent_classes = last_msg_div.get_attribute('class') or ''

            # Sproshchena loghika - tse mozhna dorobyty
            is_from_user = True  # Za zamovchuvannjam vvazhajemo shcho vid korystuvacha

            return {
                'content': content,
                'is_from_user': is_from_user,
                'timestamp': datetime.now()
            }

        except Exception as e:
            logger.error(f"Pomylka otrymanna ostannoho povidomlennia: {e}")
            return None

    def send_message(self, text: str) -> bool:
        """Vidpravyty povidomlennia v potochnyj chat."""
        try:
            # Shukajemo pole vvodu
            textbox = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='textbox']"))
            )

            # Klykajemo na pole
            textbox.click()
            time.sleep(0.5)

            # Vvodym tekst pokharvovo (imitatsija liudyny)
            for char in text:
                textbox.send_keys(char)
                time.sleep(random.uniform(0.02, 0.08))

            time.sleep(0.5)

            # Vidpravliajemo (Enter)
            textbox.send_keys(Keys.RETURN)
            time.sleep(1)

            logger.info(f"Povidomlennia vidpravleno: {text[:50]}...")
            return True

        except Exception as e:
            logger.error(f"Pomylka vidpravky povidomlennia: {e}")
            return False

    def get_chat_username(self) -> str:
        """Otrymaty username spivrozmovnyka z vidkrytoho chatu."""
        try:
            # Shukajemo username v headeri chatu
            header = self.driver.find_element(
                By.XPATH, "//header//a[contains(@href, '/')]//span"
            )
            username = header.text
            return username
        except Exception:
            try:
                # Alternatyvnyj sposib
                header = self.driver.find_element(
                    By.XPATH, "//div[contains(@class, 'x1n2onr6')]//span[contains(@class, 'x1lliihq')]"
                )
                return header.text
            except Exception:
                return "unknown_user"

    def get_display_name(self) -> str:
        """Otrymaty display name (imia) spivrozmovnyka."""
        try:
            # Shukajemo display name v headeri
            name_elem = self.driver.find_element(
                By.XPATH, "//header//div[contains(@class, 'x1lliihq')]//span"
            )
            return name_elem.text
        except Exception:
            return None

    def process_chat(self, chat_href: str) -> bool:
        """
        Obrobka odnoho chatu:
        1. Vidkryty chat
        2. Prochytaty ostannie povidomlennia
        3. Zgeneruvaty vidpovid cherez AI
        4. Vidpravyty vidpovid
        """
        try:
            # 1. Vidkryvajemo chat
            if not self.open_chat(chat_href):
                return False

            time.sleep(1)

            # 2. Otrymujemo username ta display_name
            username = self.get_chat_username()
            display_name = self.get_display_name()

            logger.info(f"Obrobka chatu: {username} ({display_name})")

            # 3. Otrymujemo ostannie povidomlennia
            last_message = self.get_last_message()

            if not last_message or not last_message.get('is_from_user'):
                logger.info(f"Nemaje novykh povidomlen vid korystuvacha v {username}")
                return False

            content = last_message['content']
            timestamp = last_message.get('timestamp')

            # 4. Pereviriajemo chy ne obrobleno vzhe
            msg_key = f"{username}:{content[:50]}"
            if msg_key in self.processed_messages:
                logger.info(f"Povidomlennia vzhe obrobleno: {msg_key}")
                return False

            # 5. Obrobka cherez AI Agent
            response = self.ai_agent.process_message(
                username=username,
                content=content,
                display_name=display_name,
                message_type='text',
                message_timestamp=timestamp
            )

            if not response:
                return False

            # 6. Vidpravliajemo vidpovid
            success = self.send_message(response)

            if success:
                self.processed_messages.add(msg_key)
                logger.info(f"Uspishno vidpovily {username}")

            return success

        except Exception as e:
            logger.error(f"Pomylka obrobky chatu: {e}")
            return False

    def run_inbox_loop(self, check_interval: int = 30, heartbeat_callback=None):
        """
        Holovnyj tsykl: pereviriaje inbox, obroliaje novi povidomlennia.

        Args:
            check_interval: interval perevirky v sekundakh
            heartbeat_callback: funktsiia dlia onovlennia heartbeat (watchdog)
        """
        logger.info(f"Zapusk inbox loop, interval: {check_interval}s")

        def heartbeat(msg: str = None):
            if heartbeat_callback:
                heartbeat_callback(msg)

        while True:
            try:
                heartbeat("Inbox loop iteration")

                # Perekhodym v inbox
                if not self.go_to_inbox():
                    time.sleep(check_interval)
                    continue

                heartbeat("Get unread chats")

                # Otrymujemo neprocytani chaty
                chats = self.get_unread_chats()
                unread_chats = [c for c in chats if c['unread']]

                if unread_chats:
                    logger.info(f"Znajdeno {len(unread_chats)} neprocytanykh chativ")

                    for chat in unread_chats:
                        heartbeat(f"Process chat: {chat.get('username', 'unknown')}")
                        self.process_chat(chat['href'])
                        time.sleep(random.uniform(2, 5))  # Pauza mizh chatamy

                # Chekajemo pered nastupnoju perevirkoiu
                logger.info(f"Chekajemo {check_interval}s...")
                heartbeat("Waiting for next check")
                time.sleep(check_interval)

            except KeyboardInterrupt:
                logger.info("Zupynka za zapytom korystuvacha")
                raise  # Peredajemo vverh dlia korektnoi obrobky
            except Exception as e:
                logger.error(f"Pomylka v inbox loop: {e}")
                heartbeat("Error in loop, retrying")
                time.sleep(check_interval)
