import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import numpy as np

from downloader import bao_d, xueqiu_d
from code_formmat import code_formatter


class StrategyNewHighest:

    def new_highest_date_with_baodata(self, code: str):
        dataFrame = bao_d.download_dayline_from_bao(code)
        half_year_df = dataFrame.sort_values(
            by='date', ascending=False)[0:26*5-1]
        highest_date = half_year_df.sort_values(
            by='close', ascending=False).iloc[0]['date']
        return highest_date

    def new_highest_date_with_xiuquedata(self, dataFrame: pd.DataFrame):
        half_year_df = dataFrame.sort_values(
            by='date', ascending=False)[0:26*5-1]
        highest_date = half_year_df.sort_values(
            by='close', ascending=False).iloc[0]['date']
        return highest_date


class StrategyDoubleMa:
    def double_ma_13_21(self, df):
        df['MA13'] = df['close'].ewm(
            span=13, adjust=False).mean()
        df['MA21'] = df['close'].ewm(
            span=21, adjust=False).mean()
        df['PRICE-MA21'] = df['close']-df['MA21']
        df['PRICE-MA13'] = df['close']-df['MA13']
        df['DIFF'] = df['MA13']-df['MA21']
        df = df.sort_values(by='date', ascending=False)
        a = (
            df.iloc[0]['close'],
            df.iloc[0]['percent'],
            df.iloc[0]['MA13'],
            df.iloc[0]['MA21'],
        )
        b = (
            df.iloc[0]['PRICE-MA21']/df.iloc[0]['close'],
            df.iloc[0]['PRICE-MA13']/df.iloc[0]['close'],
            df.iloc[0]['DIFF']/df.iloc[0]['close']
        )
        return a, b


class StrategyVolatilityVol:
    def count_volatility(self, df):
        df['STD20'] = df['percent'].ewm(
            span=20, adjust=False).std()
        df = df.sort_values(by='date', ascending=False)
        a = df.iloc[0]['STD20']
        return a/100

    def count_quantity_ratio(self, df):
        pass


class StrategyHkHolding:
    def count_hk_holding_rate(self, df):
        df['hold_ratio_ma10'] = df['hold_ratio_cn'].ewm(
            span=5, adjust=False).mean()
        df['hold_ratio_ma30'] = df['hold_ratio_cn'].ewm(
            span=15, adjust=False).mean()
        df = df.sort_values(by='date', ascending=False)
        if df.iloc[1]['hold_ratio_cn'] is not None:
            r = (df.iloc[1]['hold_ratio_cn'],
                 df.iloc[1]['hold_ratio_cn']-df.iloc[1]['hold_ratio_ma10'],
                 df.iloc[1]['hold_ratio_cn']-df.iloc[1]['hold_ratio_ma30'])
            return r


vol = StrategyVolatilityVol()
double_ma = StrategyDoubleMa()
new_high = StrategyNewHighest()
hk = StrategyHkHolding()