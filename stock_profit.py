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
        self.save2file(f'financial_hs300zz500_{time_str}', profit_df)
        self.save2file(f'financial_hs300_{time_str}', profit_hs300_df)
        self.save2file(f'financial_zz500_{time_str}', profit_zz500_df)

    def get_stock_profit_data(self, df_stocks):
        for code in df_stocks.index.tolist():
            print(f'get profit info of {code}')
            capital_code = code_formatter.code2capita(code)

            market_capital_info = xueqiu_d.download_stock_detail(
                code)
            df_stocks.loc[code,
                          'm_cap'] = market_capital_info['market_value']//100000000
            df_stocks.loc[code,
                          'f_cap'] = market_capital_info['float_market_capital']//100000000
            df_stocks.loc[code, 'pe_ttm'] = market_capital_info['pe_ttm']
            df_stocks.loc[code, 'pb'] = market_capital_info['pb']
            df_stocks.loc[code, 'eps'] = market_capital_info['eps']
            if df_stocks.loc[code, 'pb'] is not None and df_stocks.loc[code, 'pe_ttm'] is not None:
                df_stocks.loc[code, 'roe_ttm'] = df_stocks.loc[code, 'pb'] / \
                    df_stocks.loc[code, 'pe_ttm']
            df_stocks.loc[code, 'price'] = market_capital_info['price']
            df_stocks.loc[code, 'roe'] = market_capital_info['roe']

            report_info = dongcai_d.get_report(code)
            df_stocks.loc[code, 'r_date'] = pd.to_datetime(report_info['date'])
            if report_info['eps'] is not None:
                df_stocks.loc[code, 'r_eps'] = float(report_info['eps'])
            if report_info['kf_eps'] is not None:
                df_stocks.loc[code, 'r_kf_eps'] = float(report_info['kf_eps'])
            if report_info['profit_yoy'] is not None:
                df_stocks.loc[code, 'r_pro_yoy'] = float(
                    report_info['profit_yoy'])/100
            if report_info['revenue_yoy'] is not None:
                df_stocks.loc[code, 'r_rev_yoy'] = float(
                    report_info['revenue_yoy'])/100

            broker_predict = dongcai_d.get_broker_predict(code)
            if broker_predict['rate'] is not None:
                df_stocks.loc[code, 'rating'] = float(broker_predict['rate'])
            if broker_predict['thisyear'] is not None:
                df_stocks.loc[code, 'p_year'] = broker_predict['thisyear']
            if broker_predict['roe_list'][0] is not None:
                df_stocks.loc[code,
                              'roe-1'] = broker_predict['roe_list'][0]/100
            if broker_predict['roe_list'][1] is not None:
                df_stocks.loc[code,
                              'p_roe'] = broker_predict['roe_list'][1]/100
            if broker_predict['roe_list'][2] is not None:
                df_stocks.loc[code,
                              'p_roe+1'] = broker_predict['roe_list'][2]/100
            if broker_predict['pro_ratio_list'][0] is not None:
                df_stocks.loc[code,
                              'pro-1'] = broker_predict['pro_ratio_list'][0]/100
            if broker_predict['pro_ratio_list'][1] is not None:
                df_stocks.loc[code,
                              'p_pro'] = broker_predict['pro_ratio_list'][1]/100
            if broker_predict['pro_ratio_list'][2] is not None:
                df_stocks.loc[code,
                              'p_pro+1'] = broker_predict['pro_ratio_list'][2]/100

            if broker_predict['pro_grow_ratio'] is not None and broker_predict['pro_grow_ratio'] >= 0:
                if df_stocks.loc[code, 'pe_ttm'] is not None and df_stocks.loc[code, 'pe_ttm'] > 0:
                    df_stocks.loc[code, 'peg'] = df_stocks.loc[code, 'pe_ttm'] / \
                        broker_predict['pro_grow_ratio']
            # if broker_predict[2] is not None:
            #     df_stocks.loc[code,'year2'] = broker_predict[2,'year']
            #     df_stocks.loc[code,'eps2'] = float(broker_predict[2,'value'])
            #     df_stocks.loc[code,'ratio2'] = float(broker_predict[2,'ratio'])

            predict_info = dongcai_d.get_predict_profit(
                code, datetime.fromisoformat(report_info['date']))

            if predict_info:
                df_stocks.loc[code, 'predict_date'] = pd.to_datetime(
                    predict_info['release_date'])
                df_stocks.loc[code, 'pre_r_date'] = pd.to_datetime(
                    predict_info['report_date'])
                df_stocks.loc[code, 'pre_type'] = predict_info['predict_type']
                if predict_info['increase'] is not None:
                    df_stocks.loc[code,
                                  'pre_pro+'] = predict_info['increase']/100

            express_info = dongcai_d.get_express_profit(
                code, datetime.fromisoformat(report_info['date']))
            if express_info:
                df_stocks.loc[code, 'predict_date'] = pd.to_datetime(
                    express_info['release_date'])
                df_stocks.loc[code, 'pre_r_date'] = pd.to_datetime(
                    express_info['report_date'])
                # if express_info[2] is not None:
                #     df_stocks.loc[code,'express_rev_yoy'] = express_info[2]/100
                # if express_info[3] is not None:
                #     df_stocks.loc[code,'express_rev_qoq'] = express_info[3]/100
                if express_info['profit_yoy'] is not None:
                    df_stocks.loc[code,
                                  'pre_pro+'] = express_info['profit_yoy']/100
                # if express_info[5] is not None:
                #     df_stocks.loc[code,'express_pro_qoq'] = express_info[5]/100
            df_stocks.loc[code, 'industry'] = df_stocks.loc[code, 'industry']
            df_stocks.loc[code,
                          'url'] = f'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/Index?type=web&code={capital_code}'

            fund_holding_info = dongcai_d.get_fund_holding(code)
            if fund_holding_info['last_quarter'] != 0:
                df_stocks.loc[code,
                              'f_hold'] = fund_holding_info['last_quarter']
            if fund_holding_info['last_2quarter'] != 0:
                df_stocks.loc[code,
                              'f_last'] = fund_holding_info['last_2quarter']
            if fund_holding_info['last_quarter'] != 0 and fund_holding_info['last_2quarter'] != 0:
                df_stocks.loc[code, 'f_chg'] = fund_holding_info['last_quarter'] - \
                    fund_holding_info['last_2quarter']

        return df_stocks

    def save2file(self, filename, df):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'pe_ttm', 'pb', 'eps', 'peg', 'price',
                 'm_cap', 'f_cap', 'f_hold', 'f_last', 'f_chg',
                 'rating', 'r_date', 'p_year',
                 'pro-1', 'r_pro_yoy', 'r_rev_yoy','p_pro', 'p_pro+1', 
                 'roe-1', 'roe_ttm', 'p_roe', 'p_roe+1',
                 'predict_date', 'pre_r_date', 'pre_type', 'pre_pro+', 'url']]
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
            worksheet.set_column('D:H', None, format1)
            worksheet.set_column('K:M', None, format2)
            worksheet.set_column('Q:Y', None, format2)
            worksheet.set_column('AC:AC', None, format2)

            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 3)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


profit = StockProfit()

if __name__ == '__main__':
    profit.generate_report()
