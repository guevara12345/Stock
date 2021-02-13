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

    def download_dkline_from_xueqiu(self, code, day_num):
        XUEQIU_D_KLINE_URL_FORMAT = '''
            https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol={}&begin={}&period=day&type=before&count={}&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance
            '''
        capital_code = code_formatter.code2capita(code)
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
            return result

    def download_dkline_from_xueqiu4backtest(self, code, day_num):
        result = self.download_dkline_from_xueqiu(code, day_num)

    def download_dkline_from_xueqiu4daily(self, code, day_num):
        result = self.download_dkline_from_xueqiu(code, day_num)
        if result is not None:
            # 时区硬转utc+8，excel不支持时区信息
            result['datetime'] = pd.to_datetime(
                result['timestamp']+(8*3600)*1000, unit='ms')
            return result

    def download_stock_detail_from_xueqiu(self, code):
        capital_code = code_formatter.code2capita(code)
        URL_FORMAT = 'https://stock.xueqiu.com/v5/stock/quote.json?symbol={}&extend=detail'
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
        capital_code = code_formatter.code2capita(code)
        url = f'http://f10.eastmoney.com/NewFinanceAnalysis/MainTargetAjax?type=0&code={capital_code}'
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            p_json = rsp.json()[0]
            return {
                'date': p_json['date'] if p_json['date'] != '--' else None,
                'eps': p_json['jbmgsy'] if p_json['jbmgsy'] != '--' else None,
                'kf_eps': p_json['kfmgsy'] if p_json['kfmgsy'] != '--' else None,
                'profit_yoy': p_json['kfjlrtbzz'] if p_json['kfjlrtbzz'] != '--' else None,
                'revenue_yoy': p_json['yyzsrtbzz'] if p_json['yyzsrtbzz'] != '--' else None,
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
            if eps_list[2] != '--' and eps_list[4] != '--':
                two_year_growth = (
                    float(eps_list[4])-float(eps_list[2]))/abs(float(eps_list[2]))
                if two_year_growth >= 0:
                    pro_grow_ratio = ((1+two_year_growth)**0.5-1)*100
            return {
                'rate': latest_rating,
                'lastyear': predict_list[0],
                'thisyear': predict_list[1],
                'nextyear': predict_list[2],
                'pro_grow_ratio': pro_grow_ratio}

    # 业绩预测
    def get_predict_profit(self, code, last_report_date):
        code_without_char = code_formatter.code2code_without_char(code)
        url = f'http://datacenter.eastmoney.com/api/data/get?st=REPORTDATE&sr=-1&ps=50&p=1&sty=ALL&filter=(SECURITY_CODE%3D%22{code_without_char}%22)&type=RPT_PUBLIC_OP_PREDICT'
        rsp = self.session.get(url)
        if rsp.status_code == 200 and rsp.json()['result'] is not None:
            predict = rsp.json()['result']['data'][0]
            if(datetime.fromisoformat(predict['REPORTDATE']) > last_report_date):
                return {
                    'release_date': predict['NOTICE_DATE'],
                    'report_date': predict['REPORTDATE'],
                    'predict_type': predict['FORECASTTYPE'],
                    'increase': predict['INCREASEL']}

    # 业绩快报
    def get_express_profit(self, code, last_report_date):
        code_without_char = code_formatter.code2code_without_char(code)
        url = f'http://datacenter.eastmoney.com/api/data/get?st=REPORT_DATE&sr=-1&ps=50&p=1&sty=ALL&filter=(SECURITY_CODE%3D%22{code_without_char}%22)&type=RPT_FCI_PERFORMANCEE'
        rsp = self.session.get(url)
        if rsp.status_code == 200 and rsp.json()['result'] is not None:
            express = rsp.json()['result']['data'][0]
            if(datetime.fromisoformat(express['REPORT_DATE']) > last_report_date):
                return {
                    'release_date': express['UPDATE_DATE'],
                    'report_date': express['REPORT_DATE'],
                    'revenue': express['YSTZ'],
                    'revenue_qoq': express['DJDYSHZ'],
                    'profit_yoy': express['JLRTBZCL'],
                    'profit_qoq': express['DJDJLHZ']}

    def get_fund_holding(self, code):
        code2capita = code_formatter.code2capita(code)
        today = datetime.now()
        quarter = (today.month-1)//3
        if quarter == 1:
            last_quarter = datetime(today.year, 3, 31).strftime('%Y-%m-%d')
            last_2quarter = datetime(today.year-1, 12, 31).strftime('%Y-%m-%d')
        elif quarter == 2:
            last_quarter = datetime(today.year, 6, 30).strftime('%Y-%m-%d')
            last_2quarter = datetime(today.year, 3, 31).strftime('%Y-%m-%d')
        elif quarter == 3:
            last_quarter = datetime(today.year, 9, 30).strftime('%Y-%m-%d')
            last_2quarter = datetime(today.year, 6, 30).strftime('%Y-%m-%d')
        else:
            last_quarter = datetime(today.year-1, 12, 31).strftime('%Y-%m-%d')
            last_2quarter = datetime(today.year-1, 9, 30).strftime('%Y-%m-%d')

        URL_FORMAT = 'http://f10.eastmoney.com/ShareholderResearch/MainPositionsHodlerAjax?date={}&code={}'
        last_quarter_fund_holding = 0
        last_2quarter_fund_holding = 0
        rsp1 = self.session.get(URL_FORMAT.format(last_quarter, code2capita))
        # ['基金', '保险', '券商', 'QFII', '社保基金', '信托', '其他机构', '合计']
        fund_type = ['基金', ]
        if rsp1.status_code == 200:
            data1 = rsp1.json()
            for i in data1:
                if i['jglx'] in fund_type and i['zltgbl'] != '--':
                    last_quarter_fund_holding = last_quarter_fund_holding+float(
                        i['zltgbl'].split('%')[0])
        rsp2 = self.session.get(URL_FORMAT.format(last_2quarter, code2capita))
        if rsp2.status_code == 200:
            data2 = rsp2.json()
            for i in data2:
                if i['jglx'] in fund_type and i['zltgbl'] != '--':
                    last_2quarter_fund_holding = last_2quarter_fund_holding+float(
                        i['zltgbl'].split('%')[0])
        return {
            'last_quarter': last_quarter_fund_holding/100,
            'last_2quarter': last_2quarter_fund_holding/100,
        }


bao_d = BaoDownloader()
xueqiu_d = XueqiuDownloader()
dongcai_d = DongcaiDownloader()

if __name__ == '__main__':
    # bao_d.download_dayline_from_bao('sh.600438')
    # bao_d.get_from_xls('000300')
    # xueqiu_d.download_dkline_from_xueqiu('sh.600438', 52*5)
    dongcai_d.get_fund_holding('sh.600928')
