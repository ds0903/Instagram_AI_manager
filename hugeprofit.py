"""
HugeProfit CRM Integration
Передача підтвердженого замовлення (ліда) в crm.h-profit.com

Env vars:
    HUGEPROFIT_TOKEN       — API токен (Налаштування → Інтеграції → API)
    HUGEPROFIT_WAREHOUSE_ID — ID складу (GET /bapi/warehouses, необов'язково)
    HUGEPROFIT_CHANNEL_ID   — ID каналу продажів (необов'язково)
"""
import os
import re
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://crm.h-profit.com/bapi"


class HugeProfitCRM:
    def __init__(self):
        self.token = os.getenv('HUGEPROFIT_TOKEN', '').strip()
        self.warehouse_id = os.getenv('HUGEPROFIT_WAREHOUSE_ID', '').strip()
        self.channel_id = os.getenv('HUGEPROFIT_CHANNEL_ID', '').strip()
        self.account_id = os.getenv('HUGEPROFIT_ACCOUNT_ID', '').strip()

        if not self.token:
            logger.warning("HUGEPROFIT_TOKEN не вказано в .env — інтеграція вимкнена")

        self.headers = {
            'Authorization': self.token,
            'Content-Type': 'application/json',
        }

    def _get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        url = f"{BASE_URL}/{endpoint}"
        try:
            logger.info(f"HugeProfit → GET {url} | params: {params}")
            resp = requests.get(url, headers=self.headers, params=params, timeout=15)
            logger.info(f"HugeProfit ← {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            logger.error(f"HugeProfit HTTP {resp.status_code}: {resp.text[:500]}")
            return None
        except Exception as e:
            logger.error(f"HugeProfit request error: {e}")
            return None

    def test_connection(self) -> bool:
        """Перевірити чи токен валідний через GET /bapi/warehouses."""
        result = self._get('warehouses')
        if result is not None:
            logger.info(f"HugeProfit: з'єднання OK, склади: {result}")
            return True
        logger.error("HugeProfit: токен невалідний або немає доступу")
        return False

    def get_first_account_id(self) -> Optional[int]:
        """Отримати account_id з .env (HUGEPROFIT_ACCOUNT_ID)."""
        if self.account_id:
            return int(self.account_id)
        logger.error("HugeProfit: HUGEPROFIT_ACCOUNT_ID не вказано в .env")
        return None

    def _post(self, endpoint: str, data: dict) -> Optional[dict]:
        return self._request('POST', endpoint, data)

    def _put(self, endpoint: str, data: dict) -> Optional[dict]:
        return self._request('PUT', endpoint, data)

    def _request(self, method: str, endpoint: str, data: dict) -> Optional[dict]:
        url = f"{BASE_URL}/{endpoint}"
        try:
            logger.info(f"HugeProfit → {method} {url} | payload: {data}")
            resp = requests.request(method, url, headers=self.headers, json=data, timeout=15)
            logger.info(f"HugeProfit ← {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            logger.error(f"HugeProfit HTTP {resp.status_code}: {resp.text[:500]}")
            return None
        except Exception as e:
            logger.error(f"HugeProfit request error: {e}")
            return None

    # ==================== CLIENTS ====================

    def create_client(self, username: str, phone: str = None,
                      full_name: str = None) -> Optional[int]:
        """
        Створити клієнта в HugeProfit.
        name = Instagram username, phone = номер, comment = ПІБ
        Повертає client_id або None.
        """
        # Форматуємо телефон: 0687034410 → +380687034410
        formatted_phone = phone or ''
        if formatted_phone.startswith('0'):
            formatted_phone = '+38' + formatted_phone

        data = {
            "data": [{
                "name": username,
                "phone": formatted_phone,
            }]
        }
        result = self._post('clients', data)
        if result:
            # Варіант 1: {"id": 123}
            client_id = result.get('id') or result.get('client_id')
            # Варіант 2: {"data": [{"id": 123}]}
            if not client_id and isinstance(result.get('data'), list) and result['data']:
                client_id = result['data'][0].get('id') or result['data'][0].get('client_id')
            if client_id:
                logger.info(f"HugeProfit: клієнт створено id={client_id} ({username})")
                return int(client_id)
        logger.warning(f"HugeProfit: клієнт не створено, відповідь: {result}")
        return None

    # ==================== SALES ====================

    @staticmethod
    def _normalize_name(name: str) -> str:
        """
        Нормалізує назву для порівняння:
        - нижній регістр
        - видаляє ВСІ лапки та дужки (", ', «, », ", ", (, ), [, ], {, })
        - стискає зайві пробіли
        """
        result = name.lower()
        for ch in ('"', "'", '«', '»', '\u201c', '\u201d', '\u2018', '\u2019',
                   '`', '(', ')', '[', ']', '{', '}'):
            result = result.replace(ch, '')
        return ' '.join(result.split())

    def _get_mid_by_ai(self, pid: int, order_line: str) -> Optional[int]:
        """
        Визначає правильний mid (варіацію товару) через Gemini AI.
        1. Отримує всі варіації товару з HugeProfit
        2. Просить AI вибрати відповідну варіацію по рядку замовлення
        Якщо варіація одна — повертає її одразу без запиту до AI.
        """
        result = self._get('products', params={'product_id': pid})
        if not result or not isinstance(result.get('data'), list) or not result['data']:
            logger.warning(f"HugeProfit: не вдалося отримати варіації для pid={pid}")
            return None

        stock = result['data'][0].get('stock', [])
        if not stock:
            logger.warning(f"HugeProfit: варіацій немає для pid={pid}")
            return None

        # Якщо варіація одна — одразу повертаємо
        if len(stock) == 1:
            mid = stock[0].get('mid')
            logger.info(f"HugeProfit: одна варіація mid={mid} для pid={pid}")
            return int(mid) if mid else None

        # Будуємо список варіацій для AI
        variants_text = "\n".join(
            f"mid={v.get('mid')}: розмір={v.get('size', '?')}, sku={v.get('sku', '?')}, "
            f"назва={v.get('name', '')}"
            for v in stock
        )

        prompt = (
            f"Рядок товару із замовлення:\n\"{order_line}\"\n\n"
            f"Доступні варіації товару:\n{variants_text}\n\n"
            f"Визнач який mid найбільше відповідає цьому рядку замовлення "
            f"(враховуй розмір, колір, зріст). "
            f"Відповідай ТІЛЬКИ числом mid, без пояснень."
        )

        try:
            from google import genai
            from dotenv import load_dotenv
            load_dotenv()
            api_key = os.getenv('GEMINI_API_KEY', '')
            if not api_key:
                logger.warning("HugeProfit AI: GEMINI_API_KEY не вказано, беремо першу варіацію")
                mid = stock[0].get('id')
                return int(mid) if mid else None

            from google.genai import types as genai_types
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model='gemini-2.0-flash',
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    safety_settings=[
                        genai_types.SafetySetting(category='HARM_CATEGORY_HARASSMENT', threshold='BLOCK_NONE'),
                        genai_types.SafetySetting(category='HARM_CATEGORY_HATE_SPEECH', threshold='BLOCK_NONE'),
                        genai_types.SafetySetting(category='HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold='BLOCK_NONE'),
                        genai_types.SafetySetting(category='HARM_CATEGORY_DANGEROUS_CONTENT', threshold='BLOCK_NONE'),
                        genai_types.SafetySetting(category='HARM_CATEGORY_CIVIC_INTEGRITY', threshold='BLOCK_NONE'),
                    ]
                )
            )
            mid_str = response.text.strip().split()[0]
            mid = int(re.sub(r'\D', '', mid_str))
            logger.info(f"HugeProfit AI: обрано mid={mid} для '{order_line}'")
            return mid

        except Exception as e:
            logger.error(f"HugeProfit AI: помилка визначення mid: {e}")
            # Fallback — перша варіація
            mid = stock[0].get('id')
            return int(mid) if mid else None

    def _find_pid(self, product_name: str, product_id_map: dict) -> Optional[int]:
        """
        Шукає HugeProfit pid для товару:
        1. По точній нормалізованій назві в product_id_map (з Google Sheets)
        2. По частковому співпадінню (нормалізовано)
        3. Через GET /bapi/products (всі товари, пошук по назві)
        """
        norm_name = self._normalize_name(product_name)

        # Нормалізуємо ключі map для порівняння
        normalized_map = {self._normalize_name(k): v for k, v in product_id_map.items()}

        # 1. Точне співпадіння
        if norm_name in normalized_map:
            pid = normalized_map[norm_name]
            logger.info(f"HugeProfit: знайдено pid={pid} (точна назва) для '{product_name}'")
            return pid

        # 2. Часткове співпадіння
        for catalog_name, pid in normalized_map.items():
            if catalog_name in norm_name or norm_name in catalog_name:
                logger.info(f"HugeProfit: знайдено pid={pid} (часткова назва '{catalog_name}') для '{product_name}'")
                return pid

        # 3. Пошук через API (всі товари)
        result = self._get('products', params={'limit': 500, 'offset': 0})
        if result and isinstance(result.get('data'), list):
            for prod in result['data']:
                api_name = self._normalize_name(prod.get('name') or '')
                if api_name and (api_name in norm_name or norm_name in api_name):
                    pid = prod.get('id')
                    if pid:
                        logger.info(f"HugeProfit: знайдено pid={pid} через API для '{product_name}'")
                        return int(pid)

        logger.warning(f"HugeProfit: pid не знайдено для '{product_name}'")
        return None

    def _parse_products_to_items(self, products_text: str,
                                  total_price: float = None,
                                  product_id_map: dict = None) -> dict:
        """
        Парсить текстовий список товарів з [ORDER] в об'єкт для API.
        Формат рядка: "Назва товару розмір колір — 950 грн"
        product_id_map: {назва_lowercase: pid} з Google Sheets
        Повертає: {"0": {"pid": ..., "count": 1, "discount": 0, "finish_price": ..., "mid": ...}, ...}
        """
        if product_id_map is None:
            product_id_map = {}

        lines = [l.strip() for l in (products_text or '').split('\n') if l.strip()]
        parsed = []

        for line in lines:
            price_match = re.search(r'[—–\-]\s*(\d[\d\s]*)\s*грн', line)
            if price_match:
                price = float(re.sub(r'\s', '', price_match.group(1)))
                name = line[:price_match.start()].strip().rstrip('—–- ')
            else:
                price = 0.0
                name = line
            parsed.append({'name': name or line, 'price': price})

        # Якщо ціни не розпарсились — рівномірно ділимо total_price
        if parsed:
            total_parsed = sum(p['price'] for p in parsed)
            if total_parsed == 0 and total_price and len(parsed) > 0:
                per_item = round(total_price / len(parsed), 2)
                for p in parsed:
                    p['price'] = per_item

        products = {}
        for i, p in enumerate(parsed):
            item = {
                "count": 1,
                "discount": 0,
                "finish_price": p['price'],
            }

            pid = self._find_pid(p['name'], product_id_map)
            if pid:
                item["pid"] = int(pid)
                mid = self._get_mid_by_ai(pid, p['name'])
                if mid is not None:
                    item["mid"] = int(mid)

            products[str(i)] = item

        return products

    def create_sale(self, client_id: int, order_data: dict,
                    product_id_map: dict = None) -> Optional[int]:
        """
        Створити продаж в HugeProfit.
        Повертає sale_id або None.
        """
        # Парсимо суму
        total_price = None
        if order_data.get('total_price'):
            digits = re.sub(r'[^\d.]', '', str(order_data['total_price']))
            try:
                total_price = float(digits)
            except ValueError:
                pass

        products = self._parse_products_to_items(
            order_data.get('products', ''),
            total_price,
            product_id_map or {}
        )

        account_id = self.get_first_account_id()
        if not account_id:
            logger.error("HugeProfit: не вдалося отримати account_id")
            return None

        sale_type = 'Допродаж' if order_data.get('is_upsell') else 'Продаж'
        products_text = order_data.get('products', '').strip()
        address_text = (
            f"{order_data.get('full_name', '')} | "
            f"{order_data.get('city', '')} відд. {order_data.get('nova_poshta', '')}"
        ).strip(" |")
        comment_parts = [sale_type]
        if products_text:
            comment_parts.append(products_text)
        if address_text:
            comment_parts.append(address_text)

        data = {
            "client_id": client_id,
            "order_status": "saled",
            "amount": total_price or 0.0,
            "amount_sale": total_price or 0.0,
            "prepaid_amount": "",
            "account_id": account_id,
            "products": products,
            "comment": "\n".join(comment_parts),
            "channel_id": [],
        }

        if self.warehouse_id:
            data["warehouse_id"] = int(self.warehouse_id)

        result = self._put('sales', data)
        if result:
            sale_id = result.get('id') or result.get('oid') or result.get('sale_id')
            if sale_id:
                logger.info(f"HugeProfit: продаж створено id={sale_id}")
                return int(sale_id)
        logger.warning(f"HugeProfit: продаж не створено, відповідь: {result}")
        return None

    # ==================== MAIN ====================

    def push_order(self, username: str, order_data: dict,
                   product_id_map: dict = None) -> bool:
        """
        Передати підтверджене замовлення в HugeProfit CRM.
        Викликається з _process_order() в ai_agent.py.

        Args:
            username:   Instagram username клієнта
            order_data: dict з полями full_name, phone, city,
                        nova_poshta, products, total_price
        Returns:
            True якщо успішно, False якщо помилка
        """
        if not self.token:
            logger.info("HugeProfit: токен не вказано, пропускаємо")
            return False

        # 1. Клієнт
        client_id = self.create_client(
            username=username,
            phone=order_data.get('phone'),
            full_name=order_data.get('full_name'),
        )
        if not client_id:
            logger.error("HugeProfit: не вдалося створити клієнта, скасовуємо")
            return False

        # 2. Продаж
        sale_id = self.create_sale(client_id, order_data, product_id_map)
        if not sale_id:
            logger.error("HugeProfit: не вдалося створити продаж")
            return False


        logger.info(
            f"HugeProfit: ✓ замовлення передано | "
            f"client_id={client_id} sale_id={sale_id} | {username}"
        )
        return True
