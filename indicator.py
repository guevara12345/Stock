import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import numpy as np

from downloader import bao_d, xueqiu_d
from code_formmat import code_formatter


class Indicator:

    def new_highest_date_with_baodata(self, code: str):
        dataFrame = bao_d.download_dayline_from_bao(code)
        half_year_df = dataFrame.sort_values(
            by='datetime', ascending=False)[0:26*5-1]
        highest_date = half_year_df.sort_values(
            by='close', ascending=False).iloc[0]['datetime']
        return highest_date

    def new_highest_date(self, series):
        half_year_series = series.sort_index(ascending=False)[0:26*5-1]
        highest_date = half_year_series.sort_values(ascending=False).index[0]
        return highest_date

    def macd(self, series):
        df = pd.DataFrame({'close': series})
        df['MA12'] = df['close'].ewm(
            span=12, adjust=False).mean()
        df['MA26'] = df['close'].ewm(
            span=26, adjust=False).mean()
        df['PRICE-MA26'] = df['close']-df['MA26']
        df['PRICE-MA12'] = df['close']-df['MA12']
        df['DIF'] = df['MA12']-df['MA26']
        df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
        df['macd'] = 2*(df['DIF']-df['DEA'])
        df = df.sort_values(by='datetime', ascending=False)

        macd_change = (df.iloc[0]['macd']-df.iloc[1]
                       ['macd'])/df.iloc[0]['close']
        return {
            # 'close': df.iloc[0]['close'],
            # 'chg_percent': df.iloc[0]['percent'],
            'dif/p': df.iloc[0]['DIF']/df.iloc[0]['close'],
            'macd/p': df.iloc[0]['macd']/df.iloc[0]['close'],
            'macd_chg/p': macd_change,
        }

    def count_volatility(self, data_frame):
        df = data_frame[['close', 'high', 'low']]
        df['h-l'] = df['high']-df['low']
        df['h-last_c'] = df['high']-df['close']+df['close'].diff()
        df['last_c-l'] = df['close']-df['close'].diff()-df['low']
        df['tr'] = df[['h-l', 'h-last_c', 'last_c-l']].max(axis=1)
        df['atr'] = df['tr'].ewm(alpha=1/20, adjust=False).mean()
        df['atr/p'] = df['atr']/df['close']
        df['unit4me'] = 0.01/df['atr/p']

        r = df.sort_index(ascending=False).iloc[0]
        return {
            'atr': r['atr'],
            'atr/p': r['atr/p'],
            'unit4me': r['unit4me'],
        }

    def count_quantity_ratio(self, df):
        df['mount(ma5-ma20)'] = df['amount'].rolling(window=5).mean() - \
            df['amount'].rolling(window=20).mean()
        df = df.sort_values(by='datetime', ascending=False)
        return {
            'turnover': df.iloc[0]['turnoverrate'],
        }

    def count_hk_holding_rate(self, df):
        df['hold_ratio_ma10'] = df['hold_ratio_cn'].ewm(
            span=5, adjust=False).mean()
        df['hold_ratio_ma30'] = df['hold_ratio_cn'].ewm(
            span=15, adjust=False).mean()
        df = df.sort_values(by='datetime', ascending=False)
        if df.iloc[1]['hold_ratio_cn'] is not None:
            return {
                'hk_ratio': df.iloc[1]['hold_ratio_cn'],
                'hk-ma(hk,10)': df.iloc[1]['hold_ratio_cn']-df.iloc[1]['hold_ratio_ma10'],
                'hk-ma(hk,30)': df.iloc[1]['hold_ratio_cn']-df.iloc[1]['hold_ratio_ma30']}

    def count_pe_pb_band(self, df):
        year_df = df.sort_values(by='datetime', ascending=False)[0:52*5-1]
        pe_max = year_df['pe'].max()
        pe_min = year_df['pe'].min()
        year_df['pe_percent'] = year_df['pe'].apply(
            lambda x: (x-pe_min)/(pe_max-pe_min))
        pb_max = year_df['pb'].max()
        pb_min = year_df['pb'].min()
        year_df['pb_percent'] = year_df['pb'].apply(
            lambda x: (x-pb_min)/(pb_max-pb_min))
        return {
            'pe_percent': year_df.iloc[0]['pe_percent'],
            'pb_percent': year_df.iloc[0]['pb_percent']}


indi = Indicator()
