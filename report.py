import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import xlsxwriter

from downloader import bao_d, xueqiu_d
from code_formmat import code_formatter

from strategy_new_highest import new_high
from strategy_double_ma import double_ma
from strategy_volatility_vol import vol
from get_config import config


class Reporter:
    def generate_hs300_report(self):
        print('start generate hs300 report')
        hs300_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/hs300_stocks.csv'), index_col=1, encoding="gbk")
        hs300_df = hs300_df.set_index("code")

        hs300_df = self.apply_strategy(hs300_df)

        filename = 'hs300_report_{}_{}'.format(
            datetime.now().strftime('%Y%B%d'), int(time.time()))
        self.save2file(filename, hs300_df)

    def generate_zz500_report(self):
        print('start generate zz500 report')
        zz500_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/zz500_stocks.csv'), index_col=1, encoding="gbk")
        zz500_df = zz500_df.set_index("code")

        zz500_df = self.apply_strategy(zz500_df)

        filename = 'zz500_report_{}_{}'.format(
            datetime.now().strftime('%Y%B%d'), int(time.time()))
        self.save2file(filename, zz500_df)

    def generate_holding_stock_report(self):
        print('start generate zz500 report')
        zz500_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/zz500_stocks.csv'), index_col=1, encoding="gbk")
        hs300_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/hs300_stocks.csv'), index_col=1, encoding="gbk")
        df = pd.concat([zz500_df, hs300_df])
        df = df.set_index("code")
        holding_stocks = config.get_stocks()
        holding_df = df[df.index in holding_stocks]
        holding_df = self.apply_strategy(holding_df)

        filename = 'holding_report_{}_{}'.format(
            datetime.now().strftime('%Y%B%d'), int(time.time()))
        self.save2file(filename, holding_df)

    def apply_strategy(self, df):
        for code in df.index.values.tolist():
            capital_code = code_formatter.code2capita(code)
            stock_df = xueqiu_d.download_dkline_from_xueqiu(capital_code, 52*5)

            df.loc[code, 'highest_date'] = new_high.new_highest_date_with_xiuquedata(
                stock_df)

            a, b = double_ma.double_ma_13_21(stock_df)
            df.loc[code, 'price'] = a[0]
            df.loc[code, '(p-ma21)/p'] = b[0]
            df.loc[code, '(p-ma13)/p'] = b[1]
            df.loc[code, 'diff/p'] = b[2]

            df.loc[code, 'pe'] = stock_df.sort_values(
                by='date', ascending=False).iloc[0]['pe']
            df.loc[code, 'pb'] = stock_df.sort_values(
                by='date', ascending=False).iloc[0]['pb']

            df.loc[code, 'std20'] = vol.count_volatility(stock_df)
        return df

    def save2file(self, filename, df: pd.DataFrame):
        df = df[['code_name', 'industry', 'highest_date', 'price', '(p-ma21)/p',
                 '(p-ma13)/p', 'diff/p', 'std20', 'pe', 'pb',
                 'url']]
        with pd.ExcelWriter(os.path.join(os.getcwd(), f'raw_data/{filename}.xlsx'),
                            datetime_format='yyyy-mm-dd',
                            engine='xlsxwriter') as writer:
            # Convert the dataframe to an XlsxWriter Excel object.
            df.to_excel(writer, encoding="gbk", sheet_name='Sheet1')

            # Get the xlsxwriter workbook and worksheet objects.
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']

            # Add some cell formats.
            # format1 = workbook.add_format({'num_format': 'yyyy-mm-dd'})
            format2 = workbook.add_format({'num_format': '0.00%'})
            # row_format = workbook.add_format({'bg_color': 'green'})

            # Note: It isn't possible to format any cells that already have a format such
            # as the index or headers or any cells that contain dates or datetimes.

            # Set the format but not the column width.
            # worksheet.set_column('E:E', None, format1)
            worksheet.set_column('F:I', None, format2)
            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 0)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


if __name__ == '__main__':
    r = Reporter()
    r.generate_hs300_report()
    r.generate_zz500_report()
