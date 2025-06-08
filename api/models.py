import math
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field


class TopFlipResponse(BaseModel):
    item_id: str

    profit_per_hour: float
    competitiveness: float
    profit_half_life: Optional[float]
    minutes_per_flip: Optional[float]

    buy_order_price: Optional[float]
    sell_order_price: Optional[float]
    buy_order_volume: Optional[float]
    sell_order_volume: Optional[float]
    insta_buy_volume: Optional[float]
    insta_sell_volume: Optional[float]
    margin: Optional[float]

    class Config:
        json_encoders = {
            float: lambda v: None if math.isinf(v) or math.isnan(v) else v
        }


class TopFlipsListResponse(BaseModel):
    flips: list[TopFlipResponse]
    count: int = Field(description='Number of profitable flips')


class PastHourResponse(BaseModel):
    buy_order_price: Optional[float]
    sell_order_price: Optional[float]
    buy_order_volume: Optional[int]
    sell_order_volume: Optional[int]
    insta_buy_volume: Optional[int]
    insta_sell_volume: Optional[int]
    margin: Optional[float]
    timestamp: str
    item_id: str

    class Config:
        json_encoders = {
            float: lambda v: None if math.isinf(v) or math.isnan(v) else v,
            pd.Timestamp: lambda ts: None if pd.isna(ts) else ts.isoformat()
        }


class PastHourListResponse(BaseModel):
    snapshots: Optional[list[PastHourResponse]]


def clean_dataframe_row(row_dict: dict) -> dict:
    """Clean a DataFrame row dict for Pydantic model creation"""
    cleaned = {}
    for key, value in row_dict.items():
        if pd.isna(value):
            cleaned[key] = None
        elif hasattr(value, 'item'):  # numpy/pandas scalar
            cleaned[key] = value.item()
        elif isinstance(value, pd.Timestamp):
            cleaned[key] = value.isoformat()
        elif isinstance(value, float) and (math.isinf(value) or math.isnan(value)):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned
