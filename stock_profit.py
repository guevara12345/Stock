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
        stock_profit_df = None
        for code in df_stocks.index.tolist():
            print(f'get profit info of {code}')
            capital_code = code_formatter.code2capita(code)

            single_stock_price_data = xueqiu_d.download_dkline_from_xueqiu(
                code, 5)
            if stock_profit_df is None:
                stock_profit_df = pd.DataFrame(
                    columns=single_stock_price_data.columns)
            today = single_stock_price_data.set_index(
                'date').sort_index(ascending=False).iloc[0]
            today['code'] = code
            today['code_name'] = df_stocks.loc[code, 'code_name']

            report_info = dongcai_d.get_report(code)
            today['r_date'] = pd.to_datetime(report_info['date'])
            if report_info['eps'] is not None:
                today['r_eps'] = float(report_info['eps'])
            if report_info['kf_eps'] is not None:
                today['r_kf_eps'] = float(report_info['kf_eps'])
            if report_info['profit_yoy'] is not None:
                today['r_pro_yoy'] = float(report_info['profit_yoy'])/100
            if report_info['revenue_yoy'] is not None:
                today['r_rev_yoy'] = float(report_info['revenue_yoy'])/100

            broker_predict = dongcai_d.get_broker_predict(code)
            if broker_predict['rate'] is not None:
                today['rating'] = float(broker_predict['rate'])
            if broker_predict['lastyear'] is not None:
                today['eps'] = float(broker_predict['lastyear']['value'])
                if today['pb'] is not None and today['pe'] is not None:
                    today['roe'] = today['pb']/today['pe']
            if broker_predict['thisyear'] is not None:
                today['bp_year1'] = broker_predict['thisyear']['year']
                today['bp_eps1'] = float(broker_predict['thisyear']['value'])
                if broker_predict['thisyear']['ratio'] != '-':
                    today['bp_ratio1'] = float(
                        broker_predict['thisyear']['ratio'])/100
            if broker_predict['nextyear'] is not None:
                today['bp_year2'] = broker_predict['nextyear']['year']
                today['bp_eps2'] = float(broker_predict['nextyear']['value'])
                if broker_predict['nextyear']['ratio'] != '-':
                    today['bp_ratio2'] = float(
                        broker_predict['nextyear']['ratio'])/100
            if broker_predict['pro_grow_ratio'] is not None and broker_predict['pro_grow_ratio'] >= 0:
                if today['pe'] is not None and today['pe'] > 0:
                    today['peg'] = today['pe']/broker_predict['pro_grow_ratio']
            # if broker_predict[2] is not None:
            #     today['year2'] = broker_predict[2]['year']
            #     today['eps2'] = float(broker_predict[2]['value'])
            #     today['ratio2'] = float(broker_predict[2]['ratio'])

            predict_info = dongcai_d.get_predict_profit(
                code, datetime.fromisoformat(report_info['date']))

            if predict_info:
                today['predict_date'] = pd.to_datetime(
                    predict_info['release_date'])
                today['pre_r_date'] = pd.to_datetime(
                    predict_info['report_date'])
                today['pre_type'] = predict_info['predict_type']
                if predict_info['increase'] is not None:
                    today['pre_pro+'] = predict_info['increase']/100

            express_info = dongcai_d.get_express_profit(
                code, datetime.fromisoformat(report_info['date']))
            if express_info:
                today['predict_date'] = pd.to_datetime(
                    express_info['release_date'])
                today['pre_r_date'] = pd.to_datetime(
                    express_info['report_date'])
                # if express_info[2] is not None:
                #     today['express_rev_yoy'] = express_info[2]/100
                # if express_info[3] is not None:
                #     today['express_rev_qoq'] = express_info[3]/100
                if express_info['profit_yoy'] is not None:
                    today['pre_pro+'] = express_info['profit_yoy']/100
                # if express_info[5] is not None:
                #     today['express_pro_qoq'] = express_info[5]/100
            today['industry'] = basic.get_industry(code)
            today[
                'url'] = f'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/Index?type=web&code={capital_code}'
            # today['predict_date'] = predict_info[0]
            market_capital_info = xueqiu_d.download_stock_detail_from_xueqiu(
                code)
            today['m_cap'] = market_capital_info['market_value']//100000000
            today['f_cap'] = market_capital_info['float_market_capital']//100000000

            fund_holding_info = dongcai_d.get_fund_holding(code)
            if fund_holding_info['last_quarter'] != 0:
                today['f_hold'] = fund_holding_info['last_quarter']
            if fund_holding_info['last_2quarter'] != 0:
                today['f_hold_last'] = fund_holding_info['last_2quarter']
            if fund_holding_info['last_quarter'] != 0 and fund_holding_info['last_2quarter'] != 0:
                today['f_hold_chg'] = fund_holding_info['last_quarter'] - \
                    fund_holding_info['last_2quarter']
            stock_profit_df = stock_profit_df.append(today)
        stock_profit_df = stock_profit_df.reset_index(
            drop=True).set_index('code')
        return stock_profit_df

    def save2file(self, filename, df):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'pe', 'pb', 'eps', 'roe', 'peg', 'close',
                 'm_cap', 'f_cap', 'f_hold', 'f_hold_last', 'f_hold_chg',
                 'r_date', 'r_eps', 'r_kf_eps', 'r_pro_yoy', 'r_rev_yoy',
                 'rating', 'bp_year1', 'bp_eps1', 'bp_eps2', 'bp_ratio1',  'bp_ratio2',
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
            worksheet.set_column('D:F', None, format1)
            worksheet.set_column('G:G', None, format2)
            worksheet.set_column('H:H', None, format1)
            worksheet.set_column('L:N', None, format2)
            worksheet.set_column('R:S', None, format2)
            worksheet.set_column('X:Y', None, format2)
            worksheet.set_column('AC:AC', None, format2)

            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 3)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


profit = StockProfit()

if __name__ == '__main__':
    profit.generate_report()
