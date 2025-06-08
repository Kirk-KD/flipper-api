import logging
import asyncio
from typing import Optional
from cachetools import TTLCache

from core.data_client import DataClient, ItemOrderbookDict
from core.recommender import Recommender

logger = logging.getLogger(__name__)


class CacheManager:
    def __init__(self):
        self.data_client = DataClient()
        self.recommender_cache = RecommenderCache(self.data_client)
        self.orderbook_cache = OrderbookCache(self.data_client)
        self.good_products = GoodProductsCache(self.data_client)

    async def refresh(self):
        """
        Populate/refresh the recommender cache with "good" items.
        """
        item_ids = await self.good_products.get()
        orderbooks = await self.orderbook_cache.get()

        for item_id in item_ids:
            await self.recommender_cache.get(item_id, orderbooks)

    def get_recommender(self, item_id: str) -> Optional[Recommender]:
        if item_id not in self.recommender_cache.cache:
            return None

        return self.recommender_cache.cache[item_id]

    async def get_recommenders(self) -> list[Recommender]:
        item_ids = await self.good_products.get()

        recommenders = []

        for item_id in item_ids:
            if item_id in self.recommender_cache.cache:
                recommenders.append(self.recommender_cache.cache[item_id])

        return recommenders

    async def close(self):
        await self.data_client.close()


class RecommenderCache:
    def __init__(self, data_client: DataClient):
        self.data_client = data_client
        self.cache = TTLCache(maxsize=2000, ttl=60 * 5)

    async def get(self, item_id: str, orderbooks: ItemOrderbookDict) -> Optional[Recommender]:
        if item_id in self.cache:
            return self.cache[item_id]

        recommender = await self.data_client.get_recommender(item_id, orderbooks)
        if recommender is not None:
            self.cache[item_id] = recommender
        return recommender


class OrderbookCache:
    def __init__(self, data_client: DataClient):
        self.data_client = data_client
        self.cache = TTLCache(maxsize=1, ttl=60)

    async def get(self) -> ItemOrderbookDict:
        if '_' in self.cache:
            return self.cache['_']

        orderbooks = await self.data_client.get_orderbooks()
        self.cache['_'] = orderbooks
        return orderbooks


class GoodProductsCache:
    def __init__(self, data_client: DataClient):
        self.data_client = data_client
        self.cache = TTLCache(maxsize=1, ttl=60)

    async def get(self) -> list[str]:
        if '_' in self.cache:
            return self.cache['_']

        items = await self.data_client.get_good_products()
        self.cache['_'] = items
        return items
