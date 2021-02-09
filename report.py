import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import xlsxwriter

from downloader import bao_d, xueqiu_d
from basic_stock_data import basic
from code_formmat import code_formatter
from strategy_basic import new_high, double_ma, vol, hk, pe_pb
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
        stocks_dict.update(config.watching_stocks)
        stocks_dict.update(config.holding_stocks)
        watching_df_dict = {}
        for code in stocks_dict.keys():
            watching_df_dict[code] = xueqiu_d.download_dkline_from_xueqiu(
                code, 52*5)

        watching_df = None
        for code in watching_df_dict.keys():
            if watching_df is None:
                watching_df = pd.DataFrame(
                    columns=watching_df_dict[code].columns)
            today = watching_df_dict[code].set_index(
                'date').sort_index(ascending=False).iloc[0]
            today['code'] = code
            today['code_name'] = stocks_dict[code]
            today['url'] = 'http://quote.eastmoney.com/{}.html'.format(
                code_formatter.code2nopoint(code))
            today['industry'] = basic.get_industry(code)
            watching_df = watching_df.append(today)

        watching_df = watching_df.reset_index(drop=True).set_index('code')
        watching_df = self.apply_strategy4stocks(watching_df)
        return watching_df

    def apply_strategy4stocks(self, df):
        for code in df.index.values.tolist():
            stock_df = xueqiu_d.download_dkline_from_xueqiu(code, 52*5)

            df.loc[code, 'highest_date'] = new_high.new_highest_date_with_xiuquedata(
                stock_df)

            ema_info = double_ma.double_ma_13_21(stock_df)
            df.loc[code, 'price'] = ema_info['close']
            df.loc[code, 'chg_rate'] = ema_info['chg_percent']/100
            df.loc[code, '(p-ma26)/p'] = ema_info['(p-ema26)/p']
            df.loc[code, 'dif/p'] = ema_info['dif/p']
            df.loc[code, 'dea/p'] = ema_info['dea/p']
            df.loc[code, '(dif-dea)/p'] = ema_info['(dif-dea)/p']

            df.loc[code, 'pe'] = stock_df.sort_values(
                by='date', ascending=False).iloc[0]['pe']
            df.loc[code, 'pb'] = stock_df.sort_values(
                by='date', ascending=False).iloc[0]['pb']

            df.loc[code, 'std20'] = vol.count_volatility(stock_df)
            c = hk.count_hk_holding_rate(stock_df)
            if c is not None:
                df.loc[code, 'hk_ratio'] = c[0]/100
                df.loc[code, 'hk-ma(hk,10)'] = c[1]/100
                df.loc[code, 'hk-ma(hk,30)'] = c[2]/100
            if df.loc[code, 'pe'] > 0:
                df.loc[code, 'pe_percent'], df.loc[
                    code, 'pb_percent'] = pe_pb.count_pe_pb_band(stock_df)

            vol_info = vol.count_quantity_ratio(stock_df)
            df.loc[code, 'turnover'] = vol_info['turnover']
            df.loc[code, 'm(ma5-ma20)/m'] = vol_info['m(ma5-ma20)/m']

        return df

    def save2file(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'highest_date', 'price', 'chg_rate',
                 '(p-ma26)/p', 'dif/p', '(dif-dea)/p',
                 'turnover', 'm(ma5-ma20)/m', 'std20',
                 'pe', 'pb', 'pe_percent', 'pb_percent',
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
            worksheet.set_column('F:I', None, format2)
            worksheet.set_column('K:L', None, format2)
            worksheet.set_column('O:R', None, format2)
            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 3)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


class EtfIndexReporter:
    def generate_etf_index_report(self):
        print('start generate etf_index report')
        single_etf_index_df_dict = dict()
        for code in config.wangtching_etf_index.keys():
            single_etf_index_df_dict[code] = xueqiu_d.download_dkline_from_xueqiu(
                code, 52*5)

        etf_index_df = None
        for code in single_etf_index_df_dict.keys():
            if etf_index_df is None:
                etf_index_df = pd.DataFrame(
                    columns=single_etf_index_df_dict[code].columns)
            today = single_etf_index_df_dict[code].set_index(
                'date').sort_index(ascending=False).iloc[0]
            today['code'] = code
            today['code_name'] = config.wangtching_etf_index[code]
            code_without_point = code_formatter.code2nopoint(code)
            today['url'] = f'http://quote.eastmoney.com/{code_without_point}.html'
            etf_index_df = etf_index_df.append(today)

        etf_index_df = etf_index_df.reset_index(drop=True).set_index('code')
        etf_index_df = self.apply_strategy(
            etf_index_df, single_etf_index_df_dict)

        time_str = datetime.now().strftime('%H%M%S')
        self.save2file(f'daily_etf_index_{time_str}', etf_index_df)

    def apply_strategy(self, etf_index_df, single_etf_index_df_dict):
        for code in single_etf_index_df_dict.keys():
            etf_index_df.loc[code, 'highest_date'] = new_high.new_highest_date_with_xiuquedata(
                single_etf_index_df_dict[code])

            ema_info = double_ma.double_ma_13_21(
                single_etf_index_df_dict[code])
            etf_index_df.loc[code, 'price'] = ema_info['close']
            etf_index_df.loc[code, 'chg_rate'] = ema_info['chg_percent']/100
            etf_index_df.loc[code, '(p-ma26)/p'] = ema_info['(p-ema26)/p']
            etf_index_df.loc[code, 'dif/p'] = ema_info['dif/p']
            etf_index_df.loc[code, 'dea/p'] = ema_info['dea/p']
            etf_index_df.loc[code, '(dif-dea)/p'] = ema_info['(dif-dea)/p']

            etf_index_df.loc[code, 'std20'] = vol.count_volatility(
                single_etf_index_df_dict[code])
            vol_info = vol.count_quantity_ratio(single_etf_index_df_dict[code])
            etf_index_df.loc[code, 'turnover'] = vol_info['turnover']
            etf_index_df.loc[code, 'm(ma5-ma20)/m'] = vol_info['m(ma5-ma20)/m']
        return etf_index_df

    def save2file(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'highest_date', 'price', 'chg_rate',
                 '(p-ma26)/p', 'dif/p', '(dif-dea)/p',
                 'turnover', 'm(ma5-ma20)/m', 'std20', 'url']]
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
            worksheet.set_column('J:K', None, format2)
            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 0)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


if __name__ == '__main__':
    sr = StockReporter()
    sr.generate_report()

    eir = EtfIndexReporter()
    eir.generate_etf_index_report()

    # sr.debug_stock('sh.603288')
