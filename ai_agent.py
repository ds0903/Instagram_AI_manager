"""
AI Agent - Claude API integration
Chytaje prompts.yml, formuje kontekst, vidpravliaje do Claude
"""
import os
import yaml
import anthropic
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

# Zavantazhujemo prompty z YAML
PROMPTS_FILE = Path(__file__).parent / 'prompts.yml'


class AIAgent:
    def __init__(self, db):
        self.db = db
        self.client = anthropic.Anthropic(
            api_key=os.getenv('CLAUDE_API_KEY')
        )
        self.model = os.getenv('CLAUDE_MODEL', 'claude-sonnet-4-20250514')
        self.prompts = self._load_prompts()
        logger.info(f"AI Agent iniitsializovano, model: {self.model}")

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
        Formuvannia kontekstu rozmovy dlia Claude.
        Povertaie list messages u formati Claude API.
        """
        # Otrymujemo istoriiu rozmovy z DB
        history = self.db.get_conversation_history(username, limit=20)

        messages = []
        for msg in history:
            messages.append({
                "role": msg['role'],  # 'user' abo 'assistant'
                "content": msg['content']
            })

        return messages

    def _get_products_context(self, query: str = None) -> str:
        """Otrymaty kontekst pro tovary dlia promptu."""
        if query:
            products = self.db.search_products(query)
        else:
            products = []

        if not products:
            return "Baza tovariv: (tovary ne znajdeno za zapytom)"

        products_text = "Dostupni tovary:\n"
        for p in products:
            products_text += f"- {p['name']}: {p['price']} grn, rozmiry: {p['sizes']}, material: {p['material']}\n"

        return products_text

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
                import base64
                image_base64 = base64.b64encode(image_data).decode('utf-8')
                messages.append({
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/jpeg",
                                "data": image_base64
                            }
                        },
                        {
                            "type": "text",
                            "text": user_message or "Shcho tse za tovar? Dopomozhit z vyborom."
                        }
                    ]
                })
            else:
                # Zvychajne tekstove povidomlennia
                messages.append({
                    "role": "user",
                    "content": user_message
                })

            # Vyklykaemo Claude API
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                system=system_prompt,
                messages=messages
            )

            # Otrymujemo tekst vidpovidi
            assistant_message = response.content[0].text

            logger.info(f"Vidpovid zgenerovano dlia {username}: {assistant_message[:50]}...")

            return assistant_message

        except Exception as e:
            logger.error(f"Pomylka generatsii vidpovidi: {e}")
            return self.prompts.get('fallback', 'Vybachte, stalasja pomylka. Sprobuyte shche raz.')

    def process_message(self, username: str, content: str,
                        display_name: str = None,
                        message_type: str = 'text',
                        message_timestamp=None,
                        image_data: bytes = None) -> str:
        """
        Povnyj tsykl obrobky povidomlennia:
        1. Zberezhennia user message v DB
        2. Generatsiya vidpovidi
        3. Zberezhennia assistant message v DB
        4. Povertaie tekst vidpovidi

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

        # 3. Generujemo vidpovid
        response_text = self.generate_response(
            username=username,
            user_message=content,
            display_name=display_name,
            message_type=message_type,
            image_data=image_data
        )

        # 4. Zberigajemo vidpovid assistenta
        assistant_msg_id = self.db.add_assistant_message(
            username=username,
            content=response_text,
            display_name=display_name,
            answer_id=user_msg_id  # Zviazok z povidomlenniam korystuvacha
        )
        logger.info(f"Zberezeno assistant message id={assistant_msg_id}")

        # 5. Onovljujemo answer_id v user message
        self.db.update_answer_id(user_msg_id, assistant_msg_id)

        return response_text

    def get_greeting(self) -> str:
        """Otrymaty pryvitannia."""
        return self.prompts.get('greeting', 'Vitaju! Chym mozhu dopomohty?')

    def get_prompt(self, key: str) -> str:
        """Otrymaty prompt za kliuchem."""
        return self.prompts.get(key, '')
