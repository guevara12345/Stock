import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time


from strategy_new_highest import new_high
from code_formmat import code_formatter



def generate_hs300_report():
    print('start generate hs300 report')
    hs300_df = pd.read_csv(os.path.join(
        os.getcwd(), 'raw_data/hs300_stocks.csv'), index_col=1, encoding="gbk")
    hs300_df = hs300_df.set_index("code")
    for code in hs300_df.index.values.tolist():
        hs300_df.loc[code, 'highest_date'] = new_high.new_highest_date_with_xiuquedata(
            code)

    filename = 'hs300_report_{}_{}'.format(
        datetime.now().strftime('%Y%B%d'), int(time.time()))
    hs300_df.to_csv(
        os.path.join(os.getcwd(), f'raw_data/{filename}.csv'),
        encoding="gbk")


def generate_zz500_report():
    print('start generate zz500 report')
    zz500_df = pd.read_csv(os.path.join(
        os.getcwd(), 'raw_data/zz500_stocks.csv'), index_col=1, encoding="gbk")
    zz500_df = zz500_df.set_index("code")
    for code in zz500_df.index.values.tolist():
        zz500_df.loc[code, 'highest_date'] = new_high.new_highest_date_with_xiuquedata(
            code)

    filename = 'zz500_report_{}_{}'.format(
        datetime.now().strftime('%Y%B%d'), int(time.time()))
    zz500_df.to_csv(
        os.path.join(os.getcwd(), f'raw_data/{filename}.csv'),
        encoding="gbk")

if __name__ == '__main__':
    generate_hs300_report()
    generate_zz500_report()
