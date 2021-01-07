from downloader import bao_d, xueqiu_d
import baostock as bs
import pandas as pd
import os


def find_record_high_stock_hs300():
    hs300_df = pd.read_csv(os.path.join(
        os.getcwd(), 'raw_data/hs300_stocks.csv'), index_col=1, encoding="gbk")
    hs300_df = hs300_df.set_index("code")
    for code in hs300_df.index.values.tolist():
        stock_info_df = bao_d.download_dayline_from_bao(code)
        hs300_df.loc[code, 'highest_date'] = new_highest_date(stock_info_df)
    hs300_df.to_csv(
        os.path.join(os.getcwd(), 'raw_data/hs300_record_high_date.csv'),
        encoding="gbk")


def find_record_high_stock_zz500():
    zz500_df = pd.read_csv(os.path.join(
        os.getcwd(), 'raw_data/zz500_stocks.csv'), index_col=1, encoding="gbk")
    zz500_df = zz500_df.set_index("code")
    for code in zz500_df.index.values.tolist():
        stock_info_df = bao_d.download_dayline_from_bao(code)
        zz500_df.loc[code, 'highest_date'] = new_highest_date(stock_info_df)
    zz500_df.to_csv(
        os.path.join(os.getcwd(), 'raw_data/zz500_record_high_date.csv'),
        encoding="gbk")


def new_highest_date(dataFrame):
    half_year_df = dataFrame.sort_values(
        by='date', ascending=False)[0:26*5-1]
    highest_date = half_year_df.sort_values(
        by='close', ascending=False).iloc[0]['date']
    return highest_date


if __name__ == '__main__':
    # find_record_high_stock_hs300()
    find_record_high_stock_zz500()
