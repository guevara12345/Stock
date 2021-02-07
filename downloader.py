import baostock as bs
import pandas as pd
from datetime import datetime, timedelta, tzinfo
import time
import os
import json
import requests

from code_formmat import code_formatter


class BaoDownloader:
    def __init__(self):
        #### 登陆系统 ####
        self.lg = bs.login()

        # 显示登陆返回信息
        print('login respond error_code:'+self.lg.error_code)

    # def __del__(self):
    #     #### 登出系统 ####
    #     bs.logout()

    def download_dayline_from_bao2file(self, code, path):
        result = self.download_dayline_from_bao(code)
        result.to_csv(
            os.path.join(os.getcwd(), f'raw_data/{path}/{code}.csv'), index=False)
        print(result)

    def download_dayline_from_bao(self, code):
        now = datetime.now()
        last2year = now - timedelta(days=365*2)
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
            start_date=last2year.strftime('%Y-%m-%d'), end_date=now.strftime('%Y-%m-%d'),
            frequency="d", adjustflag="2")

        print(f'query_history_k_data_plus {code} error_code:{rs.error_code}')
        #### 打印结果集 ####
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        return result

    def get_from_xls(self, code):
        with pd.ExcelFile("D:\\MyProj\\Stock\\raw_data\\K线导出_{}_日线数据.xls".
                          format(code)) as xls:
            df = pd.read_excel(xls, 'Sheet0', parse_dates=True)
            df1 = df[df['交易时间'].notna()]
            print(df1)
            return df


class XueqiuDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'})
        self.session.get('https://xueqiu.com/')

    def download_dkline_from_xueqiu(self, capital_code, day_num):
        XUEQIU_D_KLINE_URL_FORMAT = '''
            https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol={}&begin={}&period=day&type=before&count={}&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance
            '''
        timestamp = int(((datetime.now()+timedelta(days=1)).timestamp())*1000)
        url = XUEQIU_D_KLINE_URL_FORMAT.format(
            capital_code, timestamp, -day_num)
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            print(
                f'download {day_num} days history day_kline data of {capital_code}')
            dkline_json = rsp.json()
            result = pd.DataFrame(
                dkline_json['data']['item'], columns=dkline_json['data']['column'])
            # 时区硬转utc+8，excel不支持时区信息
            result['date'] = pd.to_datetime(
                result['timestamp']+(8*3600)*1000, unit='ms')
            return result

    def download_stock_detail_from_xueqiu(self, code):
        capital_code = code_formatter.code2capita(code)
        URL_FORMAT = '''
            https://stock.xueqiu.com/v5/stock/quote.json?symbol={}&extend=detail
            '''
        url = URL_FORMAT.format(capital_code)
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            print(f'download stock detail of {capital_code}')
            detail_json = rsp.json()['data']
            return {
                'market_value': detail_json['quote']['market_capital'],
                'float_market_capital': detail_json['quote']['float_market_capital'],
            }


class DongcaiDownloader:
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

    # 最近财务报表
    def get_report(self, code):
        url = f'http://f10.eastmoney.com/NewFinanceAnalysis/MainTargetAjax?type=0&code={code}'
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            p_json = rsp.json()[0]
            return {
                'date': p_json['date'] if p_json['date'] != '--' else None,
                'eps': p_json['jbmgsy'] if p_json['jbmgsy'] != '--' else None,
                'kf_eps': p_json['kfmgsy'] if p_json['kfmgsy'] != '--' else None,
                'profit_yoy': p_json['kfjlrtbzz'] if p_json['kfjlrtbzz'] != '--' else None,
                'revenue': p_json['yyzsrtbzz'] if p_json['yyzsrtbzz'] != '--' else None,
            }

    # 券商研报预测
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

    # 业绩预测
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

    # 业绩快报
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


bao_d = BaoDownloader()
xueqiu_d = XueqiuDownloader()
dongcai_d = DongcaiDownloader()

if __name__ == '__main__':
    # bao_d.download_dayline_from_bao('sh.600438')
    # bao_d.get_from_xls('000300')
    xueqiu_d.download_dkline_from_xueqiu('SH600438', 52*5)
