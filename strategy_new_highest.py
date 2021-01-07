import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time

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


new_high = StrategyNewHighest()