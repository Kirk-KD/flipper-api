from functools import lru_cache

from core.cache_manager import CacheManager


@lru_cache()
def get_cache_manager() -> CacheManager:
    return CacheManager()
