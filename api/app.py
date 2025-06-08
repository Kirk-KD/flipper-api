import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
import asyncio

from api.dependencies import get_cache_manager
from api.routes import top_flips_router, past_hour_router
from core.cache_manager import CacheManager

api_prefix = '/api/v1'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stdout)]
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info('Starting FlipperAPI server...')

    cache_manager = get_cache_manager()
    task = asyncio.create_task(refresh_loop(cache_manager))

    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    await cache_manager.close()


async def refresh_loop(cache_manager: CacheManager):
    while True:
        try:
            await cache_manager.refresh()
        except Exception as e:
            logger.critical(f'Refresh error: {e}')
            await asyncio.sleep(5)


app = FastAPI(title='Flipper API', lifespan=lifespan)
app.include_router(top_flips_router, prefix=api_prefix)
app.include_router(past_hour_router, prefix=api_prefix)
