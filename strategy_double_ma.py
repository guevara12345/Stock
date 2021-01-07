import pandas as pd

from downloader import bao_d, xueqiu_d
from code_formmat import code_formatter


def double_ma(code):
    capital_code = code_formatter.code2capita(code)
    dataFrame = xueqiu_d.download_dkline_from_xueqiu(capital_code, 10*5)
    
