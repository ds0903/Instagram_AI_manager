"""
AI Agent - Gemini API integration
Chytaje prompts.yml, formuje kontekst, vidpravliaje do Gemini
Integratsija z Google Sheets (baza znan) ta Telegram (eskalatsiia)
"""
import os
import re
import yaml
import base64
from google import genai
from google.genai import types
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

# Zavantazhujemo prompty z YAML
PROMPTS_FILE = Path(__file__).parent / 'prompts.yml'

# Tryhery dlia eskalatsii (peredacha operatoru)
ESCALATION_TRIGGERS = [
    'menedzher', 'manager', 'operator', 'liudyna', 'ljudyna', 'chelovek',
    'poklykaty', 'poklychte', 'pozovit', 'pozovite', 'hochu z lydynoiu',
    'zhyva liudyna', 'zhyva ljudyna', 'real person', 'human',
    'skarha', 'skarga', 'complaint', 'povernet', 'return', 'refund',
    'skandal', 'obman', 'shakhraj', 'fraud'
]


class AIAgent:
    def __init__(self, db):
        self.db = db
        self.client = genai.Client(
            api_key=os.getenv('GEMINI_API_KEY')
        )
        self.model = os.getenv('GEMINI_MODEL', 'gemini-3-flash-preview')
        self.prompts = self._load_prompts()

        # Google Sheets Manager (baza znan)
        self.sheets_manager = None
        self._init_google_sheets()

        # Telegram Notifier (eskalatsiia)
        self.telegram = None
        self._init_telegram()

        logger.info(f"AI Agent iniitsializovano, model: {self.model}")

    def _init_google_sheets(self):
        """Iniitsializatsiia Google Sheets Manager."""
        try:
            from google_sheets import GoogleSheetsManager
            self.sheets_manager = GoogleSheetsManager()
            if self.sheets_manager.connect():
                logger.info("Google Sheets pidkliucheno")
            else:
                logger.warning("Google Sheets ne pidkliucheno - bude vykorystano lokalni dani")
                self.sheets_manager = None
        except Exception as e:
            logger.warning(f"Google Sheets nedostupnyj: {e}")
            self.sheets_manager = None

    def _init_telegram(self):
        """Iniitsializatsiia Telegram Notifier."""
        try:
            from telegram_notifier import TelegramNotifier
            self.telegram = TelegramNotifier()
            if not self.telegram.enabled:
                logger.warning("Telegram ne nalashtvano")
                self.telegram = None
        except Exception as e:
            logger.warning(f"Telegram nedostupnyj: {e}")
            self.telegram = None

    def _load_prompts(self) -> dict:
        """Zavantazhennia promptiv z YAML fajlu."""
        try:
            with open(PROMPTS_FILE, 'r', encoding='utf-8') as f:
                prompts = yaml.safe_load(f)
            logger.info("Prompty zavantazheno z prompts.yml")
            return prompts
        except Exception as e:
            logger.error(f"Pomylka zavantazhennia promptiv: {e}")
            return {}

    def reload_prompts(self):
        """Perezavantazhennia promptiv (bez restartu)."""
        self.prompts = self._load_prompts()

    def _build_conversation_context(self, username: str) -> list:
        """
        Formuvannia kontekstu rozmovy dlia Gemini.
        Povertaie list types.Content u formati Gemini API.
        """
        # Otrymujemo istoriiu rozmovy z DB
        history = self.db.get_conversation_history(username, limit=20)

        messages = []
        for msg in history:
            # Gemini vykorystovuje 'model' zamist 'assistant'
            role = 'model' if msg['role'] == 'assistant' else msg['role']
            messages.append(
                types.Content(
                    role=role,
                    parts=[types.Part(text=msg['content'])]
                )
            )

        return messages

    def _get_products_context(self, query: str = None) -> str:
        """Otrymaty kontekst pro tovary dlia promptu (Google Sheets abo DB)."""
        # Sproba Google Sheets
        if self.sheets_manager:
            try:
                return self.sheets_manager.get_products_context_for_ai(query)
            except Exception as e:
                logger.warning(f"Pomylka Google Sheets: {e}")

        # Fallback na DB
        if query:
            products = self.db.search_products(query)
        else:
            products = []

        if not products:
            return "Baza tovariv: (tovary ne znajdeno za zapytom)"

        products_text = "Dostupni tovary:\n"
        for p in products:
            products_text += f"- {p['name']}: {p['price']} grn, rozmiry: {p.get('sizes', 'N/A')}, material: {p.get('material', 'N/A')}\n"

        return products_text

    def _check_escalation(self, message: str) -> bool:
        """Pereviryty chy potrebujetsja eskalatsiia (peredacha operatoru)."""
        message_lower = message.lower()
        for trigger in ESCALATION_TRIGGERS:
            if trigger in message_lower:
                logger.info(f"Znajdeno tryher eskalatsii: '{trigger}'")
                return True
        return False

    def _check_behavior_rules(self, message: str) -> dict:
        """Pereviryty pravyla povedinky z Google Sheets."""
        if self.sheets_manager:
            try:
                return self.sheets_manager.check_triggers(message)
            except Exception as e:
                logger.warning(f"Pomylka perevirky pravyl: {e}")
        return None

    def _extract_phone(self, message: str) -> str:
        """Vytiahuty telefon z povidomlennia."""
        # Shukajemo ukrainski ta mizhnarodni nomery
        patterns = [
            r'\+380\d{9}',           # +380XXXXXXXXX
            r'380\d{9}',             # 380XXXXXXXXX
            r'0\d{9}',               # 0XXXXXXXXX
            r'\d{3}[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}',  # XXX XXX XX XX
        ]
        for pattern in patterns:
            match = re.search(pattern, message)
            if match:
                return match.group()
        return None

    def escalate_to_human(self, username: str, display_name: str,
                          reason: str, last_message: str) -> bool:
        """Vidpravyty povidomlennia pro eskalatsiiu v Telegram."""
        if self.telegram:
            return self.telegram.notify_escalation(
                username=username,
                display_name=display_name,
                reason=reason,
                last_message=last_message
            )
        logger.warning("Telegram ne nalashtvano, eskalatsiia ne vidpravlena")
        return False

    def generate_response(self, username: str, user_message: str,
                          display_name: str = None,
                          message_type: str = 'text',
                          image_data: bytes = None) -> str:
        """
        Heneratsiya vidpovidi vid AI.

        Args:
            username: Instagram username
            user_message: tekst povidomlennia
            display_name: imia korystuvacha (jakshcho vidomo)
            message_type: 'text', 'image', 'voice', 'story_reply', 'post_share'
            image_data: dani zobrazhennia (dlia Vision API)

        Returns:
            Tekst vidpovidi
        """
        try:
            # Systemnyj prompt
            system_prompt = self.prompts.get('system_prompt', '')

            # Dodajemo kontekst pro tovary (jakshcho ye zapyt)
            products_context = self._get_products_context(user_message)
            system_prompt += f"\n\n{products_context}"

            # Dodajemo imia korystuvacha v kontekst
            if display_name:
                system_prompt += f"\n\nImia klienta: {display_name}"

            # Formuujemo istoriiu rozmovy
            messages = self._build_conversation_context(username)

            # Dodajemo potochne povidomlennia
            if message_type == 'image' and image_data:
                # Vision API - analiz zobrazhennia
                text_prompt = user_message or "Shcho tse za tovar? Dopomozhit z vyborom."
                messages.append(
                    types.Content(
                        role="user",
                        parts=[
                            types.Part(text=text_prompt),
                            types.Part(
                                inline_data=types.Blob(
                                    mime_type="image/jpeg",
                                    data=image_data
                                )
                            )
                        ]
                    )
                )
            else:
                # Zvychajne tekstove povidomlennia
                messages.append(
                    types.Content(
                        role="user",
                        parts=[types.Part(text=user_message)]
                    )
                )

            # Vyklykaemo Gemini API
            response = self.client.models.generate_content(
                model=self.model,
                contents=messages,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    max_output_tokens=1024
                )
            )

            # Otrymujemo tekst vidpovidi
            assistant_message = response.text

            logger.info(f"Vidpovid zgenerovano dlia {username}: {assistant_message[:50]}...")

            return assistant_message

        except Exception as e:
            error_str = str(e).lower()
            if 'rate limit' in error_str or '429' in error_str:
                logger.error(f"AI Rate Limit: {e}")
                self._notify_ai_error(f"Rate Limit (tokeny/zapyty): {e}")
            elif 'authentication' in error_str or 'api key' in error_str or '401' in error_str:
                logger.error(f"AI Auth Error: {e}")
                self._notify_ai_error(f"Authentication Error (API key): {e}")
            elif '400' in error_str or '500' in error_str or '503' in error_str:
                logger.error(f"AI API Error: {e}")
                self._notify_ai_error(f"API Error: {e}")
            else:
                logger.error(f"Pomylka generatsii vidpovidi: {e}")
                self._notify_ai_error(f"Nevidoma pomylka AI: {e}")
            return self.prompts.get('fallback', 'Vybachte, stalasja pomylka. Sprobuyte shche raz.')

    def _notify_ai_error(self, error_msg: str):
        """Vidpravyty spovischennia pro pomylku AI v Telegram"""
        try:
            if self.telegram:
                self.telegram.notify_error(f"Pomylka AI Agent:\n{error_msg}")
        except Exception as e:
            logger.warning(f"Ne vdalosja vidpravyty spovischennia: {e}")

    def process_message(self, username: str, content: str,
                        display_name: str = None,
                        message_type: str = 'text',
                        message_timestamp=None,
                        image_data: bytes = None) -> str:
        """
        Povnyj tsykl obrobky povidomlennia:
        1. Zberezhennia user message v DB
        2. Perevirka eskalatsii
        3. Stvorennia/onovlennia lida
        4. Generatsiya vidpovidi
        5. Zberezhennia assistant message v DB

        Returns:
            Tekst vidpovidi dlia vidpravky
        """
        # 1. Pereviriajemo chy ne obrobleno vzhe
        if message_timestamp:
            if self.db.is_message_processed(username, message_timestamp):
                logger.info(f"Povidomlennia vid {username} vzhe obrobleno, propuskajemo")
                return None

        # 2. Zberigajemo povidomlennia korystuvacha
        user_msg_id = self.db.add_user_message(
            username=username,
            content=content,
            display_name=display_name,
            message_timestamp=message_timestamp
        )
        logger.info(f"Zberezeno user message id={user_msg_id} vid {username}")

        # 3. Stvorjujemo/onovljujemo lida
        phone = self._extract_phone(content)
        self.db.create_or_update_lead(
            username=username,
            display_name=display_name,
            phone=phone
        )
        logger.info(f"Lid onovleno: {username}")

        # 4. Pereviriajemo eskalatsiiu
        if self._check_escalation(content):
            logger.info(f"Eskalatsiia dlia {username}")
            self.escalate_to_human(
                username=username,
                display_name=display_name,
                reason="Klient prosyt zviazku z operatorom",
                last_message=content
            )
            # Vse odno generujemo vidpovid, ale z poperedzhennjam
            escalation_note = self.prompts.get('escalation_response',
                'Zrozumilo! Peredaju vashe zapytannia nashomu menedzheru. Vin zviazhetsia z vamy najblyzchym chasom.')
            response_text = escalation_note
        else:
            # 5. Pereviriajemo pravyla povedinky (Google Sheets)
            behavior_rule = self._check_behavior_rules(content)
            if behavior_rule and behavior_rule.get('Відповідь'):
                # Vykorystovujemo vidpovid z pravyla
                response_text = behavior_rule.get('Відповідь')
                logger.info(f"Zastosovano pravylo: {behavior_rule.get('Ситуація')}")
            else:
                # 6. Generujemo vidpovid cherez AI
                response_text = self.generate_response(
                    username=username,
                    user_message=content,
                    display_name=display_name,
                    message_type=message_type,
                    image_data=image_data
                )

        # 7. Zberigajemo vidpovid assistenta
        assistant_msg_id = self.db.add_assistant_message(
            username=username,
            content=response_text,
            display_name=display_name,
            answer_id=user_msg_id
        )
        logger.info(f"Zberezeno assistant message id={assistant_msg_id}")

        # 8. Onovljujemo answer_id v user message
        self.db.update_answer_id(user_msg_id, assistant_msg_id)

        # 9. Spovischennja pro novoho lida (jakshcho tse pershyj kontakt)
        lead = self.db.get_lead(username)
        if lead and lead.get('messages_count') == 1 and self.telegram:
            self.telegram.notify_new_lead(
                username=username,
                display_name=display_name,
                phone=phone,
                products=content[:100] if content else None
            )

        return response_text

    def get_greeting(self) -> str:
        """Otrymaty pryvitannia."""
        return self.prompts.get('greeting', 'Vitaju! Chym mozhu dopomohty?')

    def get_prompt(self, key: str) -> str:
        """Otrymaty prompt za kliuchem."""
        return self.prompts.get(key, '')
