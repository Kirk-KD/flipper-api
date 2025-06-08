from fastapi import APIRouter, Depends, Query
from api.dependencies import get_cache_manager
from api.models import TopFlipsListResponse, TopFlipResponse
from core.cache_manager import CacheManager

router = APIRouter(
    prefix='/top_flips',
    tags=['top_flips']
)

@router.get('/')
async def get_top_flips(
        top: int = Query(default=20, ge=1, le=100, description="Number of top flips to return"),
        cache_manager: CacheManager = Depends(get_cache_manager)
):
    recommenders = await cache_manager.get_recommenders()

    flips = sorted([
        {
            'item_id': r.item_id,

            'profit_per_hour': r.profit_per_hour,
            'profit_half_life': r.profit_half_life,
            'competitiveness': r.competitiveness,
            'minutes_per_flip': r.minutes_per_flip,

            'buy_order_price': r.buy_order_price,
            'sell_order_price': r.sell_order_price,
            'buy_order_volume': r.buy_order_volume,
            'sell_order_volume': r.sell_order_volume,
            'insta_buy_volume': r.insta_buy_volume,
            'insta_sell_volume': r.insta_sell_volume,
            'margin': r.margin
        } for r in recommenders
    ], key=lambda x: x['profit_per_hour'], reverse=True)[:top]

    return TopFlipsListResponse(
        flips=[TopFlipResponse(**flip) for flip in flips],
        count=len(flips)
    )