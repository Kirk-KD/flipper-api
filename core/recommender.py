import numpy as np
import pandas as pd
from functools import cached_property


def transformed_past_hour(df: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df = df.sort_values(['timestamp']).reset_index(drop=True)

    fill_cols = ['buy', 'sell', 'buyVolume', 'sellVolume', 'buyMovingWeek', 'sellMovingWeek']
    df[fill_cols] = df[fill_cols].ffill()

    df = df.rename(columns={
        'buy': 'sell_order_price',
        'sell': 'buy_order_price',
        'buyVolume': 'sell_order_volume',
        'sellVolume': 'buy_order_volume',
        'buyMovingWeek': 'insta_buy_volume_week',
        'sellMovingWeek': 'insta_sell_volume_week'
    })

    # margin with 1.125% tax deduction
    df['margin'] = (df['sell_order_price'] - df['buy_order_price']) * (1 - 0.01125)

    # each interval's insta-buy/sell volumes are calculated using the difference of weekly insta volumes
    df['insta_buy_volume'] = df['insta_buy_volume_week'].diff().abs()  # why the hell are there negatives anyway
    df['insta_sell_volume'] = df['insta_sell_volume_week'].diff().abs()

    return df[['buy_order_price',
               'sell_order_price',
               'buy_order_volume',
               'sell_order_volume',
               'insta_buy_volume',
               'insta_sell_volume',
               'insta_buy_volume_week',
               'insta_sell_volume_week',
               'margin',
               'timestamp']]


def transformed_order_book(df: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = df.copy()
    if len(df) > 0:
        df = df.sort_values(['pricePerUnit']).reset_index(drop=True)
        df['out_bid_price'] = df['pricePerUnit'].diff()

    return df


def weighted_rate_of_change(df: pd.DataFrame, column: str,
                            positions_pct: list = [0.01, 0.5, 0.75],
                            weights: list = [0.2, 0.3, 0.5],
                            recent_window: int = 6) -> float:
    n = len(df)
    positions = [int(n * pct) for pct in positions_pct]

    recent_value = df[column].tail(recent_window).mean()

    rates = []
    for pos in positions:
        value_at_pos = df[column].iloc[pos]
        value_diff = recent_value - value_at_pos
        minutes_diff = (df['timestamp'].iloc[-1] - df['timestamp'].iloc[pos]).total_seconds() / 60

        if value_at_pos > 0 and minutes_diff > 0:
            rate_per_minute = value_diff / (value_at_pos * minutes_diff)
            rates.append(rate_per_minute)
        else:
            rates.append(0)

    if len([r for r in rates if r != 0]) == 0:
        return float('nan')

    return float(np.average(rates, weights=weights))


def out_bid_factor(df: pd.DataFrame, ascending: bool) -> float:
    df: pd.DataFrame = df.copy()
    df = df.sort_values('pricePerUnit', ascending=ascending)
    top = int(len(df) * 0.2)
    if top == 0:
        top = 1

    base_out_bid = 0.1
    avg_out_bid = df['out_bid_price'].nlargest(top).mean()

    return avg_out_bid / base_out_bid


class Recommender:
    def __init__(self, item_id: str, past_hour: pd.DataFrame, buy_order_book: pd.DataFrame, sell_order_book: pd.DataFrame):
        self._item_id: str = item_id
        self._past_hour: pd.DataFrame = transformed_past_hour(past_hour)
        self._buy_ob: pd.DataFrame = transformed_order_book(buy_order_book)
        self._sell_ob: pd.DataFrame = transformed_order_book(sell_order_book)

    @property
    def item_id(self) -> str:
        return self._item_id

    @property
    def past_hour(self) -> pd.DataFrame:
        return self._past_hour.copy()

    @property
    def buy_orderbook(self) -> pd.DataFrame:
        return self._buy_ob.copy()

    @property
    def sell_orderbook(self) -> pd.DataFrame:
        return self._sell_ob.copy()

    @cached_property
    def minutes_per_flip(self) -> float:
        """
        Calculates the average number of minutes it takes to complete one flip, based on insta-buy and sell volumes.
        :return: float
        """
        df: pd.DataFrame = self.past_hour

        fill_bo_hr = df['insta_sell_volume'].sum()
        fill_so_hr = df['insta_buy_volume'].sum()

        fill_bo_wait_mins = 60 / fill_bo_hr if fill_bo_hr != 0 else float('inf')
        fill_so_wait_mins = 60 / fill_so_hr if fill_so_hr != 0 else float('inf')

        flip_wait_mins = fill_bo_wait_mins + fill_so_wait_mins

        return flip_wait_mins

    @cached_property
    def profit_per_hour(self) -> float:
        """
        Calculates the profit this item is expected to earn, factoring in the average order filling item, at an optimal
        trading condition where the user has no downtime.
        :return: float
        """
        df: pd.DataFrame = self.past_hour

        flip_wait_mins = self.minutes_per_flip
        num_flips_hr = 60 / flip_wait_mins
        margin_recent = df['margin'].tail(6).mean()  # recent 2 minutes
        margin_hr = margin_recent * num_flips_hr

        return margin_hr

    @cached_property
    def profit_half_life(self) -> float:
        """
        Calculates the estimated number of minutes it takes an item's margin to drop to half its current amount.
        :return: float
        """
        df: pd.DataFrame = self.past_hour

        avg_roc_per_minute = weighted_rate_of_change(df, 'margin')

        if avg_roc_per_minute >= 0:
            return float('inf')

        minutes_to_half = -0.5 / avg_roc_per_minute
        return max(0.0, minutes_to_half)

    @cached_property
    def competitiveness(self) -> float:
        """
        Calculates the competitiveness of bidders and askers based on orderbook data.

        Since the default out-bid amount is 0.1 coins, anything larger than that indicates deliberate flippers trying
        to speed up their order. This leads to bidding wars, thus a higher competitive score.

        :return: float
        """
        bo_ob_factor = out_bid_factor(self.buy_orderbook, ascending=False) if len(self._buy_ob) else 0
        so_ob_factor = out_bid_factor(self.sell_orderbook, ascending=True) if len(self._sell_ob) else 0
        competitiveness = (bo_ob_factor + so_ob_factor) / 2
        return competitiveness

    @cached_property
    def score(self) -> float:
        """
        Calculates the overall score of this flip, factoring in profit per hour and competition.

        :return: float
        """
        competitiveness_factor = 0.1

        return self.profit_per_hour / (self.competitiveness * competitiveness_factor)

    @cached_property
    def buy_order_price(self) -> float:
        return self.past_hour['buy_order_price'].iloc[-1]

    @cached_property
    def sell_order_price(self) -> float:
        return self.past_hour['sell_order_price'].iloc[-1]

    @cached_property
    def buy_order_volume(self) -> float:
        return self.past_hour['buy_order_volume'].iloc[-1]

    @cached_property
    def sell_order_volume(self) -> float:
        return self.past_hour['sell_order_volume'].iloc[-1]

    @cached_property
    def insta_buy_volume(self) -> float:
        return self.past_hour['insta_buy_volume_week'].mean() / 7 / 24

    @cached_property
    def insta_sell_volume(self) -> float:
        return self.past_hour['insta_sell_volume_week'].mean() / 7 / 24

    @cached_property
    def margin(self) -> float:
        return self.past_hour['margin'].iloc[-1]

    @cached_property
    def timestamp(self) -> pd.Timestamp:
        return self.past_hour['timestamp'].iloc[-1]
