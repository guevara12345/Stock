import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import requests
from lxml import etree
import re

from downloader import bao_d, xueqiu_d, dongcai_d
from code_formmat import code_formatter
from basic_stock_data import basic


class StockProfit:
    def generate_report(self):
        print('start generate hs300 profit report')
        hs300_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/hs300_stocks.csv'), index_col=1, encoding="gbk")
        hs300_df = hs300_df.set_index("code")
        profit_hs300_df = self.get_stock_profit_data(hs300_df)

        print('start generate zz500 profit report')
        zz500_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/zz500_stocks.csv'), index_col=1, encoding="gbk")
        zz500_df = zz500_df.set_index("code")
        profit_zz500_df = self.get_stock_profit_data(zz500_df)
        profit_df = pd.concat([profit_hs300_df, profit_zz500_df])

        # profit_df = profit_hs300_df
        time_str = datetime.now().strftime('%H%M%S')
        self.save2file(f'financial_zz800_{time_str}', profit_df)
        # self.save2file(f'financial_hs300_{time_str}', profit_hs300_df)
        # self.save2file(f'financial_zz500_{time_str}', profit_zz500_df)

    def get_stock_profit_data(self, df_stocks):
        for code in df_stocks.index.tolist():
            print(f'get profit info of {code}')
            code2code_without_char = code_formatter.code2code_without_char(
                code)

            detail = xueqiu_d.download_stock_detail(code)
            df_stocks.loc[code, 'm_cap'] = detail['market_value']
            df_stocks.loc[code, 'f_cap'] = detail['float_market_capital']
            df_stocks.loc[code, 'pe_ttm'] = detail['pe_ttm']
            df_stocks.loc[code, 'pb'] = detail['pb']
            df_stocks.loc[code, 'eps'] = detail['eps']
            if detail['pb'] and detail['pe_ttm']:
                df_stocks.loc[code, 'roe_ttm'] = detail['pb']/detail['pe_ttm']
            df_stocks.loc[code, 'price'] = detail['price']
            df_stocks.loc[code, 'roe'] = detail['roe']

            report = dongcai_d.get_report(code)
            df_stocks.loc[code, 'r_date'] = pd.to_datetime(report['date'])
            df_stocks.loc[code, 'r_eps'] = report['eps']
            df_stocks.loc[code, 'r_kf_eps'] = report['kf_eps']
            df_stocks.loc[code, 'r_proyoy'] = report['profit_yoy']
            df_stocks.loc[code, 'r_revyoy'] = report['revenue_yoy']

            predict = dongcai_d.get_broker_predict(code)
            df_stocks.loc[code, 'rating'] = float(predict['rate'])
            df_stocks.loc[code, 'p_year'] = predict['thisyear']
            df_stocks.loc[code, 'roe-1'] = predict['roe_list'][0]
            df_stocks.loc[code, 'p_roe'] = predict['roe_list'][1]
            df_stocks.loc[code, 'p_roe+1'] = predict['roe_list'][2]
            df_stocks.loc[code, 'eps-1'] = predict['eps_list'][0]
            if predict['eps_list'][1] and predict['eps_list'][0]:
                df_stocks.loc[code, 'p_proyoy'] = (
                    predict['eps_list'][1]-predict['eps_list'][0])/abs(
                        predict['eps_list'][0])
            df_stocks.loc[code, 'p_eps'] = predict['eps_list'][1]
            df_stocks.loc[code, 'p_eps+1'] = predict['eps_list'][2]
            if predict['pro_grow_ratio'] and detail['pe_ttm'] and detail['pe_ttm'] > 0:
                df_stocks.loc[code, 'peg'] = detail['pe_ttm'] / \
                    predict['pro_grow_ratio']
            # if predict[2] is not None:
            #     df_stocks.loc[code,'year2'] = predict[2,'year']
            #     df_stocks.loc[code,'eps2'] = float(predict[2,'value'])
            #     df_stocks.loc[code,'ratio2'] = float(predict[2,'ratio'])

            adv = dongcai_d.get_advance_report(
                code, datetime.fromisoformat(report['date']))
            if adv:
                df_stocks.loc[code, 'adv_date'] = pd.to_datetime(
                    adv['release_date'])
                df_stocks.loc[code, 'is_adv'] = 'Y'

            expr = dongcai_d.get_express_profit(
                code, datetime.fromisoformat(report['date']))
            if expr:
                df_stocks.loc[code, 'expr_date'] = pd.to_datetime(
                    expr['release_date'])
                df_stocks.loc[code, 'expr_rdate'] = pd.to_datetime(
                    expr['report_date'])
                df_stocks.loc[code, 'expr_eps'] = expr['eps']

            df_stocks.loc[code, 'industry'] = df_stocks.loc[code, 'industry']
            df_stocks.loc[
                code, 'url'] = f'https://data.eastmoney.com/stockdata/{code2code_without_char}.html'

            fund_hold = dongcai_d.get_fund_holding(code)
            df_stocks.loc[code, 'f_hold'] = fund_hold['last_quarter']
            df_stocks.loc[code, 'f_last'] = fund_hold['last_2quarter']
            if fund_hold['last_quarter'] and fund_hold['last_2quarter']:
                df_stocks.loc[code, 'f_chg'] = fund_hold['last_quarter'] - \
                    fund_hold['last_2quarter']
            else:
                df_stocks.loc[code, 'f_chg'] = None

        return df_stocks

    def save2file(self, filename, df):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'pe_ttm', 'pb', 'peg', 'price',
                 'm_cap', 'f_cap', 'f_hold', 'f_last', 'f_chg',
                 'rating', 'r_date', 'r_eps', 'r_kf_eps',
                 'p_year', 'eps-1', 'p_eps', 'p_eps+1',
                 'roe-1', 'roe_ttm', 'p_roe', 'p_roe+1',
                 'expr_date', 'expr_rdate', 'expr_eps', 'adv_date', 'is_adv', 'url']]
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
            format1 = workbook.add_format({'num_format': '0.00'})
            format2 = workbook.add_format({'num_format': '0.00%'})
            # row_format = workbook.add_format({'bg_color': 'green'})

            # Note: It isn't possible to format any cells that already have a format such
            # as the index or headers or any cells that contain dates or datetimes.

            # Set the format but not the column width.
            # worksheet.set_column('E:E', None, format1)
            worksheet.set_column('D:I', None, format1)
            worksheet.set_column('J:L', None, format2)
            worksheet.set_column('O:P', None, format1)
            worksheet.set_column('R:T', None, format1)
            worksheet.set_column('U:X', None, format2)

            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 3)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


profit = StockProfit()

if __name__ == '__main__':
    profit.generate_report()
