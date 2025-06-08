from fastapi import APIRouter, Depends, HTTPException
from api.dependencies import get_cache_manager
from api.models import PastHourListResponse, PastHourResponse, clean_dataframe_row
from core.cache_manager import CacheManager

router = APIRouter(
    prefix='/past_hour',
    tags=['past-hour']
)


@router.get(
    '/{item_id}',
    summary="Get past hour data for an item",
    description="Retrieve past hour snapshots for a specific item"
)
async def get_past_hour(
        item_id: str,
        cache_manager: CacheManager = Depends(get_cache_manager)
) -> PastHourListResponse:
    recommender = cache_manager.get_recommender(item_id)
    if recommender is None:
        raise HTTPException(status_code=404, detail="Item not found")

    return PastHourListResponse(snapshots=[
        PastHourResponse(**clean_dataframe_row(row), item_id=item_id)
        for row in recommender.past_hour.to_dict('records')
    ])
