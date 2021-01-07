import pandas as pd

from downloader import bao_d, xueqiu_d
from code_formmat import code_formatter


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
            df.iloc[0]['MA13'],
            df.iloc[0]['MA21']
        )
        b = (
            df.iloc[0]['PRICE-MA21']/df.iloc[0]['close'],
            df.iloc[0]['PRICE-MA13']/df.iloc[0]['close'],
            df.iloc[0]['DIFF']/df.iloc[0]['close']
        )
        return a, b


double_ma = StrategyDoubleMa()
