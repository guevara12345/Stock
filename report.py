import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import xlsxwriter

from downloader import bao_d, xueqiu_d
from basic_stock_data import basic
from code_formmat import code_formatter
from indicator import indi
from get_config import config


class StockReporter:
    def debug_stock(self, code):
        print('start debug_stock')
        stock_df = pd.DataFrame(index=[code, ])
        stock_df = self.apply_strategy4stocks(stock_df)

    def apply_strategy4hs300(self):
        print('start generate hs300 report')
        hs300_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/hs300_stocks.csv'), index_col=1, encoding="gbk")
        hs300_df = hs300_df.set_index("code")

        hs300_df = self.apply_strategy4stocks(hs300_df)
        return hs300_df

    def apply_strategy4zz500(self):
        print('start generate zz500 report')
        zz500_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/zz500_stocks.csv'), index_col=1, encoding="gbk")
        zz500_df = zz500_df.set_index("code")

        zz500_df = self.apply_strategy4stocks(zz500_df)
        return zz500_df

    def generate_report(self):
        zz500_df = self.apply_strategy4zz500()
        time_str = datetime.now().strftime('%H%M%S')
        self.save2file(f'daily_zz500_{time_str}', zz500_df)

        hs300_df = self.apply_strategy4hs300()
        self.save2file(f'daily_hs300_{time_str}', hs300_df)
        self.save2file(
            f'daily_hs300zz500_{time_str}',
            pd.concat([hs300_df, zz500_df]))

        watching_df = self.apply_strategy4watching()
        self.save2file(f'daily_holding_{time_str}', watching_df)

    def apply_strategy4watching(self):
        print('start generate watching stocks report')
        stocks_dict = {}
        for i in config.watching_stocks:
            stocks_dict[i['code']] = i['code_name']
        watching_df_dict = {}
        for code in stocks_dict.keys():
            watching_df_dict[code] = xueqiu_d.download_dkline_from_xueqiu4daily(
                code, 52*5)

        watching_df = None
        for code in watching_df_dict.keys():
            if watching_df is None:
                watching_df = pd.DataFrame(
                    columns=watching_df_dict[code].columns)
            today = watching_df_dict[code].set_index(
                'datetime').sort_index(ascending=False).iloc[0]
            today['code'] = code
            today['code_name'] = stocks_dict[code]
            today['url'] = 'https://xueqiu.com/S/{}'.format(
                code_formatter.code2capita(code))
            today['industry'] = basic.get_industry(code)
            watching_df = watching_df.append(today)

        watching_df = watching_df.reset_index(drop=True).set_index('code')
        watching_df = self.apply_strategy4stocks(watching_df)
        return watching_df

    def apply_strategy4stocks(self, df):
        for code in df.index.values.tolist():
            stock_df = xueqiu_d.download_dkline_from_xueqiu4daily(code, 52*5)

            df.loc[code, 'highest_date'] = indi.new_highest_date(stock_df)

            ema_info = indi.macd(stock_df['close'])
            df.loc[code, 'price'] = ema_info['close']
            df.loc[code, 'chg_rate'] = ema_info['chg_percent']/100
            df.loc[code, 'dif/p'] = ema_info['dif/p']
            df.loc[code, 'macd/p'] = ema_info['macd/p']
            df.loc[code, 'macd_chg/p'] = ema_info['macd_chg/p']

            df.loc[code, 'pe'] = stock_df.sort_values(
                by='datetime', ascending=False).iloc[0]['pe']
            df.loc[code, 'pb'] = stock_df.sort_values(
                by='datetime', ascending=False).iloc[0]['pb']
            stock_info = xueqiu_d.download_stock_detail_from_xueqiu(code)
            df.loc[code, 'cap'] = stock_info['market_value']//100000000
            df.loc[code, 'f_cap'] = stock_info['float_market_capital']//100000000
            df.loc[code, 'vol_ratio'] = stock_info['vol_ratio']

            df.loc[code, 'std20'] = indi.count_volatility(stock_df)
            c = indi.count_hk_holding_rate(stock_df)
            if c is not None:
                df.loc[code, 'hk_ratio'] = c[0]/100
                df.loc[code, 'hk-ma(hk,10)'] = c[1]/100
                df.loc[code, 'hk-ma(hk,30)'] = c[2]/100
            if df.loc[code, 'pe'] > 0:
                df.loc[code, 'pe_percent'], df.loc[
                    code, 'pb_percent'] = indi.count_pe_pb_band(stock_df)

            vol_info = indi.count_quantity_ratio(stock_df)
            df.loc[code, 'turnover'] = vol_info['turnover']/100

        return df

    def save2file(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'highest_date', 'price', 'chg_rate',
                 'dif/p', 'macd/p', 'macd_chg/p',
                 'turnover', 'vol_ratio', 'std20',
                 'cap', 'f_cap', 'pe', 'pb', 'pe_percent', 'pb_percent',
                 'hk_ratio', 'hk-ma(hk,10)', 'url']]
        with pd.ExcelWriter(f'./raw_data/{folder_name}/{filename}.xlsx',
                            datetime_format='yyyy-mm-dd',
                            engine='xlsxwriter',
                            options={'remove_timezone': True}) as writer:
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
            worksheet.set_column('F:J', None, format2)
            worksheet.set_column('L:L', None, format2)
            worksheet.set_column('Q:T', None, format2)
            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 3)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


class EtfIndexReporter:
    def generate_etf_index_report(self):
        print('start generate etf_index report')
        watch_data_dict = config.wangtching_etf_index
        etf_index_df = None
        for i in watch_data_dict:
            if i['data_source'] == 'xueqiu':
                i['df'] = xueqiu_d.download_dkline_from_xueqiu4daily(
                    i['code'], 52*5)
                i['series'] = i['df'].sort_index(ascending=False).iloc[0]
                i['series']['code'] = i['code']
                i['series']['code_name'] = i['code_name']
                i['series']['url'] = 'https://xueqiu.com/S/{}'.format(
                    code_formatter.code2capita(i['code']))
                self.apply_strategy(i['series'], i['df'])
                if etf_index_df is None:
                    etf_index_df = pd.DataFrame(columns=i['series'].index)
                etf_index_df = etf_index_df.append(i['series'])
        etf_index_df = etf_index_df.reset_index(drop=True).set_index('code')
        time_str = datetime.now().strftime('%H%M%S')
        self.save2file(f'daily_etf_index_{time_str}', etf_index_df)

    def apply_strategy(self, result, df):
        result['highest_date'] = indi.new_highest_date(df['close'])

        ema_info = indi.macd(df['close'])
        result['dif/p'] = ema_info['dif/p']
        result['macd/p'] = ema_info['macd/p']
        result['macd_chg/p'] = ema_info['macd_chg/p']

        result['std20'] = indi.count_volatility(df[['close','high','low']])
        stock_info = xueqiu_d.download_stock_detail_from_xueqiu(result['code'])
        result['vol_ratio'] = stock_info['vol_ratio']
        return result

    def save2file(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'highest_date', 'close', 'percent',
                 'dif/p', 'macd/p', 'macd_chg/p',
                 'vol_ratio', 'std20', 'url']]
        with pd.ExcelWriter(f'./raw_data/{folder_name}/{filename}.xlsx',
                            datetime_format='yyyy-mm-dd',
                            engine='xlsxwriter',
                            options={'remove_timezone': True}) as writer:
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
            worksheet.set_column('E:H', None, format2)
            worksheet.set_column('J:J', None, format2)
            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 0)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


if __name__ == '__main__':
    # sr = StockReporter()
    # sr.generate_report()

    eir = EtfIndexReporter()
    eir.generate_etf_index_report()

    # sr.debug_stock('sh.603288')
