import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time


from strategy_new_highest import new_high
from strategy_double_ma import double_ma


def generate_hs300_report():
    print('start generate hs300 report')
    hs300_df = pd.read_csv(os.path.join(
        os.getcwd(), 'raw_data/hs300_stocks.csv'), index_col=1, encoding="gbk")
    hs300_df = hs300_df.set_index("code")
    for code in hs300_df.index.values.tolist():
        hs300_df.loc[code, 'highest_date'] = new_high.new_highest_date_with_xiuquedata(
            code)

        a, b = double_ma.double_ma_13_21(code)
        hs300_df.loc[code, '(price,ma13,ma21)'] = '{};{:.4};{:.4}'.format(
            a[0], a[1], a[2])
        hs300_df.loc[code, '(price-ma21,price-ma13,diff)'] = '{:.2%};{:.2%};{:.2%}'.format(
            b[0], b[1], b[2])

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

        a, b = double_ma.double_ma_13_21(code)
        zz500_df.loc[code, '(price,ma13,ma21)'] = '{};{:.4};{:.4}'.format(
            a[0], a[1], a[2])
        zz500_df.loc[code, '(price-ma21,price-ma13,diff)'] = '{:.2%};{:.2%};{:.2%}'.format(
            b[0], b[1], b[2])

    filename = 'zz500_report_{}_{}'.format(
        datetime.now().strftime('%Y%B%d'), int(time.time()))
    zz500_df.to_csv(
        os.path.join(os.getcwd(), f'raw_data/{filename}.csv'),
        encoding="gbk")


if __name__ == '__main__':
    generate_hs300_report()
    # generate_zz500_report()
