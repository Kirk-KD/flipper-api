import asyncio
import logging
from asyncio import Semaphore
from typing import Optional, TypeAlias

import httpx
import pandas as pd
from httpx import HTTPStatusError


from core.recommender import Recommender

RATE_LIMIT = 429

ItemOrderbookDict: TypeAlias = dict[str, tuple[pd.DataFrame, pd.DataFrame]]

logger = logging.getLogger(__name__)


class DataClient:
    def __init__(self, max_concurrent_requests=1):
        self.req_client = httpx.AsyncClient()
        self.timeout = httpx.Timeout(None)
        self.semaphore = Semaphore(max_concurrent_requests)

    async def get_recommender(self, item_id: str, orderbooks: ItemOrderbookDict) -> Optional[Recommender]:
        if item_id not in orderbooks:
            logger.warning(f'{item_id} skipped: not in orderbooks')
            return None

        try:
            past_hour_task = self.get_past_hour(item_id)

            past_hour = await past_hour_task
            order_book = orderbooks.get(item_id)
            recommender = Recommender(item_id, past_hour, *order_book)

            return recommender
        except KeyError:
            logger.warning(f'{item_id} skipped: not enough data')
        except HTTPStatusError as e:
            logger.error(f'{item_id} failed: HTTP {e.response.status_code}')
        except Exception as e:
            logger.error(f'{item_id} failed: unknown error {e}')

        return None

    async def get_good_products(self) -> list[str]:
        res = await self._get('https://api.hypixel.net/v2/skyblock/bazaar')
        data = res.json()

        def is_good(status: dict) -> bool:
            return (status['sellPrice'] >= 1000 and status['buyPrice'] >= 1000 and
                    status['buyPrice'] - status['sellPrice'] >= 10000 and
                    status['buyMovingWeek'] >= 200 and status['sellMovingWeek'] >= 200)

        return [item_id for item_id in data['products'].keys() if is_good(data['products'][item_id]['quick_status'])]

    async def get_past_hour(self, item_id: str) -> pd.DataFrame:
        res = await self._get(f'https://sky.coflnet.com/api/bazaar/{item_id}/history/hour')
        df = pd.DataFrame(res.json())
        return df

    async def get_orderbooks(self) -> ItemOrderbookDict:
        res = await self._get('https://api.hypixel.net/v2/skyblock/bazaar')
        data = res.json()

        orderbooks = {}

        for item_id, item_data in data['products'].items():
            buy_orderbook = pd.DataFrame(item_data['sell_summary'])
            sell_orderbook = pd.DataFrame(item_data['buy_summary'])
            orderbooks[item_id] = (buy_orderbook, sell_orderbook)

        return orderbooks

    async def get_all_products(self) -> list[str]:
        res = await self._get('https://sky.coflnet.com/api/items/bazaar/tags')
        data = res.json()
        return data

    async def _get(self, url: str):
        logger.debug(f'GET {url}')

        async with self.semaphore:
            res = await self.req_client.get(url, timeout=self.timeout)

            if res.status_code == RATE_LIMIT:
                retry_after = res.headers.get('retry-after', '5')
                wait_time = max(int(retry_after), 5)
                logger.warning(f'{url}: Rate limited. Waiting {wait_time} seconds...')

        if res.status_code == RATE_LIMIT:
            await asyncio.sleep(wait_time)

            async with self.semaphore:
                res = await self.req_client.get(url)
                res.raise_for_status()
                return res

        res.raise_for_status()
        return res

    async def close(self):
        await self.req_client.aclose()
