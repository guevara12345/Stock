import pandas as pd

from downloader import bao_d, xueqiu_d
from code_formmat import code_formatter


class StrategyDoubleMa:
    def double_ma_13_21(self, code):
        capital_code = code_formatter.code2capita(code)
        dataFrame = xueqiu_d.download_dkline_from_xueqiu(capital_code, 10*5)
        dataFrame['MA13'] = dataFrame['close'].ewm(
            span=13, adjust=False).mean()
        dataFrame['MA21'] = dataFrame['close'].ewm(
            span=21, adjust=False).mean()
        dataFrame['PRICE-MA21'] = dataFrame['close']-dataFrame['MA21']
        dataFrame['PRICE-MA13'] = dataFrame['close']-dataFrame['MA13']
        dataFrame['DIFF'] = dataFrame['MA13']-dataFrame['MA21']
        dataFrame = dataFrame.sort_values(by='date', ascending=False)
        a = (
            dataFrame.iloc[0]['close'],
            dataFrame.iloc[0]['MA13'],
            dataFrame.iloc[0]['MA21']
        )
        b = (
            dataFrame.iloc[0]['PRICE-MA21']/dataFrame.iloc[0]['close'],
            dataFrame.iloc[0]['PRICE-MA13']/dataFrame.iloc[0]['close'],
            dataFrame.iloc[0]['DIFF']/dataFrame.iloc[0]['close']
        )
        return a, b


double_ma = StrategyDoubleMa()
