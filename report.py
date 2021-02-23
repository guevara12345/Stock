import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import xlsxwriter

from downloader import bao_d, xueqiu_d, wall_d
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
        watching_df = None
        no_dup_dict = {}
        for x in config.watching_stocks:
            no_dup_dict[x['code']] = x
        for stock in no_dup_dict.values():
            if watching_df is None:
                watching_df = pd.DataFrame()
            series = pd.Series(stock)
            series['url'] = 'https://xueqiu.com/S/{}'.format(
                code_formatter.code2capita(stock['code']))
            series['industry'] = basic.get_industry(stock['code'])
            watching_df = watching_df.append(series, ignore_index=True)

        watching_df = watching_df.reset_index(drop=True).set_index('code')
        watching_df = self.apply_strategy4stocks(watching_df)
        return watching_df

    def apply_strategy4stocks(self, df):
        for code in df.index.values.tolist():
            stock_df = xueqiu_d.download_dkline4daily(code, 52*5)

            df.loc[code, 'highest_date'] = indi.new_highest_date(
                stock_df['close'])

            ema_info = indi.macd(stock_df['close'])

            df.loc[code, 'dif/p'] = ema_info['dif/p']
            df.loc[code, 'macd/p'] = ema_info['macd/p']
            df.loc[code, 'macd_chg/p'] = ema_info['macd_chg/p']

            sorted_df = stock_df.sort_values(by='datetime', ascending=False)
            df.loc[code, 'pe'] = sorted_df.iloc[0]['pe']
            df.loc[code, 'pb'] = sorted_df.iloc[0]['pb']
            df.loc[code, 'price'] = sorted_df.iloc[0]['close']
            df.loc[code, 'chg_rate'] = sorted_df.iloc[0]['percent']/100

            stock_info = xueqiu_d.download_stock_detail(code)
            df.loc[code, 'cap'] = stock_info['market_value']//100000000
            df.loc[code, 'f_cap'] = stock_info['float_market_capital']//100000000
            df.loc[code, 'vol_ratio'] = stock_info['vol_ratio']

            volatility_info = indi.count_volatility(
                stock_df[['close', 'high', 'low']])
            df.loc[code, 'atr/p'] = volatility_info['atr/p']
            df.loc[code, 'unit4me'] = volatility_info['unit4me']

            hk_info = indi.count_hk_holding_rate(stock_df)
            if hk_info is not None:
                df.loc[code, 'hk_ratio'] = hk_info['hk_ratio']/100
                df.loc[code, 'hk-ma(hk,10)'] = hk_info['hk-ma(hk,10)']/100
                df.loc[code, 'hk-ma(hk,30)'] = hk_info['hk-ma(hk,30)']/100

            if df.loc[code, 'pe'] > 0:
                pepb_band = indi.count_pe_pb_band(stock_df)
                df.loc[code, 'pe_percent'] = pepb_band['pe_percent']
                df.loc[code, 'pb_percent'] = pepb_band['pb_percent']
                df.loc[code, 'roe_ttm'] = df.loc[code, 'pb']/df.loc[code, 'pe']

            vol_info = indi.count_quantity_ratio(stock_df)
            df.loc[code, 'turnover'] = vol_info['turnover']/100
        return df

    def save2file(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'highest_date', 'price', 'chg_rate',
                 'dif/p', 'macd/p', 'macd_chg/p',
                 'turnover', 'vol_ratio', 'atr/p', 'unit4me',
                 'cap', 'f_cap',  'pe', 'pb', 'roe_ttm', 'pe_percent', 'pb_percent',
                 'hk_ratio', 'hk-ma(hk,10)', 'url']]
        writer = pd.ExcelWriter(f'./raw_data/{folder_name}/{filename}.xlsx',
                                datetime_format='yyyy-mm-dd',
                                engine='xlsxwriter',
                                options={'remove_timezone': True})
        # Convert the dataframe to an XlsxWriter Excel object.
        df.to_excel(writer, encoding="gbk", sheet_name='Sheet1')

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add some cell formats.
        # format1 = workbook.add_format({'num_format': 'yyyy-mm-dd'})
        format2 = workbook.add_format({'num_format': '0.00%'})
        # row_format = workbook.add_format({'bg_color': 'green'})

        worksheet.set_column('F:J', None, format2)
        worksheet.set_column('L:M', None, format2)
        worksheet.set_column('R:V', None, format2)
        color_format = {'type': 'data_bar', 'bar_solid': True}
        worksheet.conditional_format('F1:F801', color_format)
        worksheet.conditional_format('G1:G801', color_format)
        worksheet.conditional_format('H1:H801', color_format)
        worksheet.conditional_format('I1:I801', color_format)
        worksheet.conditional_format('J1:J801', color_format)
        worksheet.conditional_format('K1:K801', color_format)
        worksheet.conditional_format('R1:R801', color_format)
        worksheet.conditional_format('S1:S801', color_format)
        worksheet.conditional_format('U1:U801', color_format)
        worksheet.conditional_format('V1:V801', color_format)

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
            i['series'] = pd.Series(i)
            if i['data_source'] == 'xueqiu':
                i['df'] = xueqiu_d.download_dkline4daily(
                    i['code'], 52*5)
            elif i['data_source'] == 'wallstreetcn':
                i['df'] = wall_d.download_dkline4daily(i['code'], 52*5)

            self.apply_strategy(i)
            if etf_index_df is None:
                etf_index_df = pd.DataFrame(columns=i['series'].index)
            etf_index_df = etf_index_df.append(i['series'], ignore_index=True)
        etf_index_df = etf_index_df.reset_index(drop=True).set_index('code')
        time_str = datetime.now().strftime('%H%M%S')
        self.save2file(f'daily_etf_index_{time_str}', etf_index_df)

    def apply_strategy(self, stock):
        df = stock['df']

        result = stock['series']
        if stock['data_source'] == 'xueqiu':
            result['url'] = 'https://xueqiu.com/S/{}'.format(
                code_formatter.code2capita(stock['code']))
            stock_info = xueqiu_d.download_stock_detail(result['code'])
            result['vol_ratio'] = stock_info['vol_ratio']
            df_sorted = df.sort_index(ascending=False)
            result['close'] = df_sorted.iloc[0]['close']

        elif stock['data_source'] == 'wallstreetcn':
            result['url'] = 'https://wallstreetcn.com/markets/codes/{}'.format(
                stock['code'])
            df = df.rename(
                columns={'open_px': 'open', 'close_px': 'close', 'high_px': 'high',
                         'low_px': 'low', 'px_change_rate': 'percent', })
            df_sorted = df.sort_index(ascending=False)
            result['close'] = df_sorted.iloc[0]['close']

        result['percent'] = df_sorted.iloc[0]['percent']/100

        result['highest_date'] = indi.new_highest_date(df['close'])

        ema_info = indi.macd(df['close'])
        result['dif/p'] = ema_info['dif/p']
        result['macd/p'] = ema_info['macd/p']
        result['macd_chg/p'] = ema_info['macd_chg/p']

        volatility_info = indi.count_volatility(df[['close', 'high', 'low']])
        result['atr/p'] = volatility_info['atr/p']
        result['unit4me'] = volatility_info['unit4me']

        return result

    def save2file(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'highest_date', 'close', 'percent',
                 'dif/p', 'macd/p', 'macd_chg/p',
                 'vol_ratio', 'atr/p', 'unit4me', 'url']]
        writer = pd.ExcelWriter(f'./raw_data/{folder_name}/{filename}.xlsx',
                                datetime_format='yyyy-mm-dd',
                                engine='xlsxwriter',
                                options={'remove_timezone': True})
        # Convert the dataframe to an XlsxWriter Excel object.
        df.to_excel(writer, encoding="gbk", sheet_name='Sheet1')

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add some cell formats.
        format2 = workbook.add_format({'num_format': '0.00%'})
        worksheet.set_column('E:H', None, format2)
        worksheet.set_column('J:K', None, format2)
        color_format = {'type': 'data_bar', 'bar_solid': True}
        worksheet.conditional_format('E1:E801', color_format)
        worksheet.conditional_format('F1:F801', color_format)
        worksheet.conditional_format('G1:G801', color_format)
        worksheet.conditional_format('H1:H801', color_format)
        worksheet.conditional_format('J1:J801', color_format)

        # Freeze the first row.
        worksheet.freeze_panes(1, 0)

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()


eir = EtfIndexReporter()
sr = StockReporter()
if __name__ == '__main__':
    # sr.generate_report()
    eir.generate_etf_index_report()
    # sr.debug_stock('sh.603288')
