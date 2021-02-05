import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import requests
from lxml import etree
import re

from downloader import bao_d, xueqiu_d
from code_formmat import code_formatter
from basic_stock_data import basic


class StockProfit:
    def __init__(self):
        # get session of dongcai
        self.session = requests.Session()
        self.session.headers.update(
            {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'})
        self.session.get('https://www.eastmoney.com/')
        self.session.get('http://data.eastmoney.com/center/')
        self.NO_INTEREST_CONCEPT = [
            'HS300_', 'MSCI中国', '标普概念', '富时概念', '中证500',
            '融资融券', '上证180_', '上证50_', '上证380']

    def get_report(self, code):
        url = f'http://f10.eastmoney.com/NewFinanceAnalysis/MainTargetAjax?type=0&code={code}'
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            p_json = rsp.json()[0]
            return [p_json['date'] if p_json['date'] != '--' else None,
                    # 基本每股收益
                    p_json['jbmgsy'] if p_json['jbmgsy'] != '--' else None,
                    # 扣非每股收益
                    p_json['kfmgsy'] if p_json['kfmgsy'] != '--' else None,
                    # 扣非净利润yoy
                    p_json['kfjlrtbzz'] if p_json['kfjlrtbzz'] != '--' else None,
                    # 营业总收入yoy
                    p_json['yyzsrtbzz'] if p_json['yyzsrtbzz'] != '--' else None, ]

    def get_broker_predict(self, code):
        code2capita = code_formatter.code2capita(code)
        url = f'http://f10.eastmoney.com/ProfitForecast/ProfitForecastAjax?code={code2capita}'
        rsp = self.session.get(url)
        if rsp.status_code == 200 and rsp.json() is not None:
            stock_rating = rsp.json()['pjtj']
            if len(stock_rating) > 0:
                latest_rating = stock_rating[0]['pjxs']
            predict_list = sorted(rsp.json()['mgsy'], key=lambda i: i['year'])
            eps_list = [x['mgsy'].split('(')[0]
                        for x in rsp.json()['yctj']['data']]
            pro_grow_ratio = None
            if eps_list[0] != '--' and eps_list[4] != '--':
                five_year_growth = (
                    float(eps_list[4])-float(eps_list[0]))/abs(float(eps_list[0]))
                if five_year_growth >= 0:
                    pro_grow_ratio = ((1+five_year_growth)**0.2-1)*100
            return [latest_rating, predict_list[0], predict_list[1],
                    predict_list[2], pro_grow_ratio]

    def get_predict_profit(self, code_without_char, last_report_date):
        url = f'http://datacenter.eastmoney.com/api/data/get?st=REPORTDATE&sr=-1&ps=50&p=1&sty=ALL&filter=(SECURITY_CODE%3D%22{code_without_char}%22)&type=RPT_PUBLIC_OP_PREDICT'
        rsp = self.session.get(url)
        if rsp.status_code == 200 and rsp.json()['result'] is not None:
            predict = rsp.json()['result']['data'][0]
            if(datetime.fromisoformat(predict['REPORTDATE']) > last_report_date):
                return [predict['NOTICE_DATE'],
                        predict['REPORTDATE'],
                        predict['FORECASTTYPE'],
                        predict['INCREASEL']]

    def get_express_profit(self, code_without_char, last_report_date):
        url = f'http://datacenter.eastmoney.com/api/data/get?st=REPORT_DATE&sr=-1&ps=50&p=1&sty=ALL&filter=(SECURITY_CODE%3D%22{code_without_char}%22)&type=RPT_FCI_PERFORMANCEE'
        rsp = self.session.get(url)
        if rsp.status_code == 200 and rsp.json()['result'] is not None:
            express = rsp.json()['result']['data'][0]
            if(datetime.fromisoformat(express['REPORT_DATE']) > last_report_date):
                return [express['UPDATE_DATE'],
                        express['REPORT_DATE'],
                        express['YSTZ'],
                        express['DJDYSHZ'],
                        express['JLRTBZCL'],
                        express['DJDJLHZ']]

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
        self.save2file(f'hs300zz500_financial_{time_str}', profit_df)
        self.save2file(f'hs300_financial_{time_str}', profit_hs300_df)
        self.save2file(f'zz500_financial_{time_str}', profit_zz500_df)

    def get_stock_profit_data(self, df_stocks):
        stock_profit_df = None
        for code in df_stocks.index.tolist():
            print(f'get profit info of {code}')
            capital_code = code_formatter.code2capita(code)
            code_without_char = code_formatter.code2code_without_char(
                code)
            code_without_point = code_formatter.code2nopoint(code)

            single_stock_price_data = xueqiu_d.download_dkline_from_xueqiu(
                capital_code, 5)
            if stock_profit_df is None:
                stock_profit_df = pd.DataFrame(
                    columns=single_stock_price_data.columns)
            today = single_stock_price_data.set_index(
                'date').sort_index(ascending=False).iloc[0]
            today['code'] = code
            today['code_name'] = df_stocks.loc[code, 'code_name']

            report_info = self.get_report(capital_code)
            today['r_date'] = pd.to_datetime(report_info[0])
            if report_info[1] is not None:
                today['r_eps'] = float(report_info[1])
            if report_info[2] is not None:
                today['r_kf_eps'] = float(report_info[2])
            if report_info[3] is not None:
                today['r_pro_yoy'] = float(report_info[3])/100
            if report_info[4] is not None:
                today['r_rev_yoy'] = float(report_info[4])/100

            broker_predict = self.get_broker_predict(code)
            if broker_predict[0] is not None:
                today['rating'] = float(broker_predict[0])
            if broker_predict[1] is not None:
                today['eps'] = float(broker_predict[1]['value'])
                if today['pb'] is not None and today['pe'] is not None:
                    today['roe'] = today['pb']/today['pe']
            if broker_predict[2] is not None:
                today['bp_year1'] = broker_predict[2]['year']
                today['bp_eps1'] = float(broker_predict[2]['value'])
                if broker_predict[2]['ratio'] != '-':
                    today['bp_ratio1'] = float(broker_predict[2]['ratio'])/100
            if broker_predict[3] is not None:
                today['bp_year2'] = broker_predict[3]['year']
                today['bp_eps2'] = float(broker_predict[3]['value'])
                if broker_predict[3]['ratio'] != '-':
                    today['bp_ratio2'] = float(broker_predict[3]['ratio'])/100
            if broker_predict[4] is not None and broker_predict[4] >= 0:
                if today['pe'] is not None:
                    today['peg'] = today['pe']/broker_predict[4]
            # if broker_predict[2] is not None:
            #     today['year2'] = broker_predict[2]['year']
            #     today['eps2'] = float(broker_predict[2]['value'])
            #     today['ratio2'] = float(broker_predict[2]['ratio'])

            predict_info = self.get_predict_profit(
                code_without_char, datetime.fromisoformat(report_info[0]))

            if predict_info:
                today['predict_date'] = pd.to_datetime(predict_info[0])
                today['pre_r_date'] = pd.to_datetime(predict_info[1])
                today['pre_type'] = predict_info[2]
                if predict_info[3] is not None:
                    today['pre_pro+'] = predict_info[3]/100

            express_info = self.get_express_profit(
                code_without_char, datetime.fromisoformat(report_info[0]))
            if express_info:
                today['predict_date'] = pd.to_datetime(express_info[0])
                today['pre_r_date'] = pd.to_datetime(express_info[1])
                # if express_info[2] is not None:
                #     today['express_rev_yoy'] = express_info[2]/100
                # if express_info[3] is not None:
                #     today['express_rev_qoq'] = express_info[3]/100
                if express_info[4] is not None:
                    today['pre_pro+'] = express_info[4]/100
                # if express_info[5] is not None:
                #     today['express_pro_qoq'] = express_info[5]/100
            today['industry'] = basic.get_industry(code_without_point)
            today[
                'url'] = f'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/Index?type=web&code={capital_code}'
            # today['predict_date'] = predict_info[0]
            stock_profit_df = stock_profit_df.append(today)
        stock_profit_df = stock_profit_df.reset_index(
            drop=True).set_index('code')
        return stock_profit_df

    def save2file(self, filename, df):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'pe', 'pb', 'eps', 'roe', 'peg', 'close',
                 'r_date', 'r_eps', 'r_kf_eps', 'r_pro_yoy', 'r_rev_yoy',
                 'rating', 'bp_year1', 'bp_eps1', 'bp_ratio1', 'bp_eps2', 'bp_ratio2',
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
            worksheet.set_column('M:N', None, format2)
            worksheet.set_column('R:R', None, format2)
            worksheet.set_column('U:U', None, format2)
            worksheet.set_column('Y:Y', None, format2)

            # worksheet.set_row(0, None, row_format)

            # Freeze the first row.
            worksheet.freeze_panes(1, 3)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()


profit = StockProfit()

if __name__ == '__main__':
    profit.generate_report()
