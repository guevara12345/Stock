import baostock as bs
import pandas as pd
from datetime import datetime, timedelta, tzinfo
import time
import os
import json
import requests
import math
from lxml import etree

from code_formmat import code_formatter
from sync import AsnycGrab


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

    def download_dkline(self, code, day_num):
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
                f'download {day_num} days day_kline data of {capital_code}')
            dkline_json = rsp.json()
            result = pd.DataFrame(
                dkline_json['data']['item'], columns=dkline_json['data']['column'])
            return result

    def download_dkline4backtest(self, code, day_num):
        result = self.download_dkline(code, day_num)
        if result is not None:
            # 时区硬转utc+8，excel不支持时区信息
            result['datetime'] = pd.to_datetime(
                result['timestamp']+(8*3600)*1000, unit='ms')
            return result.set_index('datetime')

    def download_dkline4daily(self, code, day_num):
        result = self.download_dkline(code, day_num)
        if result is not None:
            # 时区硬转utc+8，excel不支持时区信息
            result['datetime'] = pd.to_datetime(
                result['timestamp']+(8*3600)*1000, unit='ms')
            return result.set_index('datetime')

    def download_wkline4daily(self, code, num):
        XUEQIU_D_KLINE_URL_FORMAT = '''
            https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol={}&begin={}&period=week&type=before&count={}&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance
        '''
        capital_code = code_formatter.code2capita(code)
        timestamp = int(((datetime.now()+timedelta(days=1)).timestamp())*1000)
        url = XUEQIU_D_KLINE_URL_FORMAT.format(
            capital_code, timestamp, -num)
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            print(
                f'download {num} days day_kline data of {capital_code}')
            dkline_json = rsp.json()
            result = pd.DataFrame(
                dkline_json['data']['item'], columns=dkline_json['data']['column'])
            return result
        if result is not None:
            # 时区硬转utc+8，excel不支持时区信息
            result['datetime'] = pd.to_datetime(
                result['timestamp']+(8*3600)*1000, unit='ms')
            return result.set_index('datetime')

    def download_stock_detail(self, code):
        capital_code = code_formatter.code2capita(code)
        URL_FORMAT = 'https://stock.xueqiu.com/v5/stock/quote.json?symbol={}&extend=detail'
        url = URL_FORMAT.format(capital_code)
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            print(f'download stock detail of {capital_code}')
            detail_json = rsp.json()['data']['quote']
            market_json = rsp.json()['data']['market']

            roe = None
            if detail_json.get('pb') and detail_json.get('pe_lyr'):
                roe = detail_json.get('pb') / \
                    detail_json.get('pe_lyr')
            market_capital = None
            if detail_json.get('market_capital'):
                market_capital = detail_json.get('market_capital')/100000000
            float_market_capital = None
            if detail_json.get('float_market_capital'):
                float_market_capital = detail_json.get(
                    'float_market_capital')/100000000
            return {
                'is_open': 'Y' if market_json.get('status_id') == 5 else 'N',
                'roe': roe,
                'price': detail_json.get('last_close'),
                'eps': detail_json.get('eps'),
                'pe_ttm': detail_json.get('pe_ttm'),
                'pb': detail_json.get('pb'),
                'market_value': market_capital,
                'float_market_capital': float_market_capital,
                'vol_ratio': detail_json.get('volume_ratio'),
            }

    def sync_stock_detail(self, code_list):
        def parse(req_info, json):
            print('handle stock detail info of {}'.format(req_info['code']))
            detail_json = json['data']['quote']
            market_json = json['data']['market']

            roe = None
            if detail_json.get('pb') and detail_json.get('pe_lyr'):
                roe = detail_json.get('pb') / \
                    detail_json.get('pe_lyr')
            market_capital = None
            if detail_json.get('market_capital'):
                market_capital = detail_json.get(
                    'market_capital')/100000000
            float_market_capital = None
            if detail_json.get('float_market_capital'):
                float_market_capital = detail_json.get(
                    'float_market_capital')/100000000
            return {
                'is_open': 'Y' if market_json.get('status_id') == 5 else 'N',
                'roe': roe,
                'price': detail_json.get('last_close'),
                'eps': detail_json.get('eps'),
                'pe_ttm': detail_json.get('pe_ttm'),
                'pb': detail_json.get('pb'),
                'market_value': market_capital,
                'float_market_capital': float_market_capital,
                'vol_ratio': detail_json.get('volume_ratio'),
            }

        URL_FORMAT = 'https://stock.xueqiu.com/v5/stock/quote.json?symbol={}&extend=detail'
        reqs_info = [{'code': code, 'url': URL_FORMAT.format(code_formatter.code2capita(code))
                      } for code in code_list]
        downloader = AsnycGrab(reqs_info, parse, 'https://xueqiu.com/')
        downloader.start()
        return downloader.results

    def get_indusrty_list(self):
        rsp = self.session.get('https://xueqiu.com/hq')
        if rsp.status_code == 200:
            html = etree.HTML(rsp.text)
            list_level2info = [{
                'code': item.xpath('@data-level2code')[0], 
                'code_name': item.xpath('@title')[0]
            } for item in html.xpath('//a[@data-level2code]')]
            list_level2info = [
                level2info for level2info in list_level2info if code_formatter.islevel2code(
                    level2info['code'])]
        return list_level2info

    def get_stock_of_indus(self, indus_info):
        print('download industry info of {}'.format(indus_info['code_name']))
        page = 1
        size = 90
        count = 1
        stock_df = pd.DataFrame(
            columns=['code', 'code_name', 'industry'], index=[])
        timestamp = int(((datetime.now()+timedelta(days=1)).timestamp())*1000)
        url_format = 'https://xueqiu.com/service/v5/stock/screener/quote/list?page={page}&size={size}&order=desc&order_by=percent&exchange=CN&market=CN&ind_code={indus_code}&_={timestamp}'
        while (page-1)*size < count:
            rsp = self.session.get(url_format.format(
                page=page, size=size, indus_code=indus_info['code'], timestamp=timestamp))
            if rsp.status_code == 200 and rsp.json()['error_code'] == 0:
                data = rsp.json()['data']
                count = data['count']
                stocks = data['list']
                for s in stocks:
                    stock_df.loc[stock_df.shape[0]] = {
                        'code': s['symbol'],
                        'code_name': s['name'],
                        'industry': indus_info['code_name']
                    }
                page += 1
        return stock_df

    def save_stock_industry_info(self):
        print('download industry info')
        list_level2info = self.get_indusrty_list()
        indus_df = pd.DataFrame()
        for info in list_level2info:
            df = self.get_stock_of_indus(info)
            indus_df = pd.concat([indus_df, df])
        indus_df.to_csv('./raw_data/xueqiu_industry.csv',
                        index=False, encoding="gbk")


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
    def sync_report(self, code_list):
        def parse(req_info, json):
            print('handle stock report of {}'.format(req_info['code']))
            report = json['result']['data'][0]
            account_p = datetime.fromisoformat(report['REPORTDATE'])
            return {
                'update_date': report['UPDATE_DATE'],
                'account_date': report['REPORTDATE'],
                'account_p': '{}-Q{}'.format(
                    account_p.strftime('%Y'), (int(account_p.strftime('%m'))-1)//3+1),
                'eps': report['BASIC_EPS'],
                'kf_eps': report['DEDUCT_BASIC_EPS'],
            }

        url_format = 'http://datacenter.eastmoney.com/api/data/get?st=REPORTDATE&sr=-1&ps=50&p=1&sty=ALL&filter=(SECURITY_CODE%3D%22{}%22)&type=RPT_LICO_FN_CPD'
        req_info_list = [{'url': url_format.format(code_formatter.code2code_without_char(code)),
                          'code': code} for code in code_list]
        downloader = AsnycGrab(
            req_info_list, parse, 'http://data.eastmoney.com/center/')
        downloader.start()
        return downloader.results

    # 券商研报预测
    def sync_broker_predict(self, code_list):
        def parse(req_info, json):
            print('handle broker predict of {}'.format(req_info['code']))
            stock_rating = json['pjtj']
            if len(stock_rating) > 0:
                latest_rating = stock_rating[2]['pjxs']
            roe_list = [x['jzcsyl'].split('(')[0]
                        for x in json['yctj']['data']]
            pro_list = json['gsjlr']
            f_roe_list = [float(x)/100 if x !=
                          '--' else None for x in roe_list]
            f_pro_list = [
                float(x['value'])/100000000 if x['value'] !=
                '0.00' and x['value'] != '--' else None for x in pro_list
            ]
            pro_grow_ratio = None
            if f_pro_list[2] and f_pro_list[0]:
                two_year_growth = (
                    f_pro_list[2]-f_pro_list[0])/abs(f_pro_list[0])
                if two_year_growth >= 0:
                    pro_grow_ratio = ((1+two_year_growth)**0.5-1)*100
            eps_list = [
                float(x['value']) if x['value'] != '0.00' and x['value'] !=
                '--' else None for x in json['mgsy']
            ]

            return {
                'rate': float(latest_rating),
                'thisyear': json['yctj']['data'][3]['rq'],
                'roe_list': f_roe_list[2:5],
                'eps_list': eps_list[0:3],
                'pro_grow_ratio': pro_grow_ratio,
            }

        url_format = 'http://f10.eastmoney.com/ProfitForecast/ProfitForecastAjax?code={}'
        req_info = [{'url': url_format.format(code_formatter.code2capita(code)), 'code': code}
                    for code in code_list]
        downloader = AsnycGrab(
            req_info, parse, 'http://data.eastmoney.com/center/')
        downloader.start()
        return downloader.results

    # 业绩预测
    def sync_advance_report(self, stock_info_list):
        def parse(req_info, json):
            print('handle advance report of {}'.format(req_info['code']))
            if json['result']:
                predict = json['result']['data'][0]
                report_date = datetime.fromisoformat(predict['REPORTDATE'])
                if(report_date > req_info['last_report_date']):
                    account_p = datetime.fromisoformat(predict['REPORTDATE'])
                    return {
                        'release_date': predict['NOTICE_DATE'],
                        'adv_period': '{}-Q{}'.format(
                            account_p.strftime('%Y'), (int(account_p.strftime('%m'))-1)//3+1),
                        'predict_type': predict['FORECASTTYPE'],
                        'increase': predict['INCREASEL']/100 if predict['INCREASEL'] is not None else None,
                    }

        url_format = 'http://datacenter.eastmoney.com/api/data/get?st=REPORTDATE&sr=-1&ps=50&p=1&sty=ALL&filter=(SECURITY_CODE%3D%22{}%22)&type=RPT_PUBLIC_OP_PREDICT'
        req_info = [{'code': item['code'],
                     'url':url_format.format(code_formatter.code2code_without_char(item['code'])),
                     'last_report_date':item['last_report_date']} for item in stock_info_list]
        downloader = AsnycGrab(
            req_info, parse, 'http://data.eastmoney.com/center/')
        downloader.start()
        return downloader.results

    # 业绩快报
    def sync_express_report(self, stock_info_list):
        def parse(req_info, json):
            print('handle express report of {}'.format(req_info['code']))
            if json['result']:
                express = json['result']['data'][0]
                report_date = datetime.fromisoformat(express['REPORT_DATE'])
                if(report_date > req_info['last_report_date']):
                    account_p = datetime.fromisoformat(express['REPORT_DATE'])
                    return {
                        'release_date': express['UPDATE_DATE'],
                        'expr_period': '{}-Q{}'.format(
                            account_p.strftime('%Y'), (int(account_p.strftime('%m'))-1)//3+1),
                        'revenue': express['YSTZ'],
                        'revenue_qoq': express['DJDYSHZ'],
                        'profit_yoy': express['JLRTBZCL']/100 if express['JLRTBZCL'] is not None else None,
                        'profit_qoq': express['DJDJLHZ']/100 if express['DJDJLHZ'] is not None else None,
                        'eps': express['BASIC_EPS']
                    }
        url_format = 'http://datacenter.eastmoney.com/api/data/get?st=REPORT_DATE&sr=-1&ps=50&p=1&sty=ALL&filter=(SECURITY_CODE%3D%22{}%22)&type=RPT_FCI_PERFORMANCEE'
        req_info = [{'code': item['code'],
                     'url':url_format.format(code_formatter.code2code_without_char(item['code'])),
                     'last_report_date':item['last_report_date']} for item in stock_info_list]
        downloader = AsnycGrab(
            req_info, parse, 'http://data.eastmoney.com/center/')
        downloader.start()
        return downloader.results

    # def sync_fund_holding(self, code):
    #     today = datetime.now()
    #     quarter = (today.month-1)//3
    #     if quarter == 1:
    #         last_quarter = datetime(today.year, 3, 31).strftime('%Y-%m-%d')
    #         last_2quarter = datetime(today.year-1, 12, 31).strftime('%Y-%m-%d')
    #     elif quarter == 2:
    #         last_quarter = datetime(today.year, 6, 30).strftime('%Y-%m-%d')
    #         last_2quarter = datetime(today.year, 3, 31).strftime('%Y-%m-%d')
    #     elif quarter == 3:
    #         last_quarter = datetime(today.year, 9, 30).strftime('%Y-%m-%d')
    #         last_2quarter = datetime(today.year, 6, 30).strftime('%Y-%m-%d')
    #     else:
    #         last_quarter = datetime(today.year-1, 12, 31).strftime('%Y-%m-%d')
    #         last_2quarter = datetime(today.year-1, 9, 30).strftime('%Y-%m-%d')
    #     def parse(req_info, json):

    def sync_fund_holding(self, code_list):
        def parse(req_info, json):
            print('handle fund hold info of {}'.format(req_info['code']))
            # ['基金', '保险', '券商', 'QFII', '社保基金', '信托', '其他机构', '合计']
            fund_type = ['基金', ]
            data1 = json
            fund_holding = 0
            for i in data1:
                if i['jglx'] in fund_type and i['zltgbl'] != '--':
                    fund_holding = + float(i['zltgbl'].split('%')[0])
            return fund_holding/100

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

        r = {}
        url_format = 'http://f10.eastmoney.com/ShareholderResearch/MainPositionsHodlerAjax?date={}&code={}'
        last_req_info_list = [{
            'code': code,
            'url': url_format.format(last_quarter, code_formatter.code2capita(code)),
        } for code in code_list]
        last_d = AsnycGrab(
            last_req_info_list, parse, 'http://data.eastmoney.com/center/')
        last_d.start()
        for key in last_d.results.keys():
            r[key] = {'last_quarter': last_d.results[key]}
        last2_req_info_list = [{
            'code': code,
            'url': url_format.format(last_2quarter, code_formatter.code2capita(code)),
        } for code in code_list]
        last2_d = AsnycGrab(
            last2_req_info_list, parse, 'http://data.eastmoney.com/center/')
        last2_d.start()
        for key in last_d.results.keys():
            r[key]['last_2quarter'] = last2_d.results[key]

        return r


class WallcnDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'})
        self.session.get('https://wallstreetcn.com/')

    def download_dkline(self, code, day_num):
        WALL_D_KLINE_URL_FORMAT = '''
            https://api-ddc.wallstcn.com/market/kline?prod_code={}&tick_count={}&period_type=86400&fields=tick_at%2Copen_px%2Cclose_px%2Chigh_px%2Clow_px%2Cturnover_volume%2Cturnover_value%2Caverage_px%2Cpx_change%2Cpx_change_rate%2Cavg_px%2Cma2
            '''
        url = WALL_D_KLINE_URL_FORMAT.format(code, day_num)
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            print(
                f'download {day_num} days day_kline data of {code}')
            dkline_json = rsp.json()
            if dkline_json.get('code') == 20000:
                data = dkline_json['data']
                result = pd.DataFrame(
                    data['candle'][code]['lines'], columns=data['fields'])
                return result

    def download_dkline4backtest(self, code, day_num):
        result = self.download_dkline(code, day_num)
        if result is not None:
            result['datetime'] = pd.to_datetime(result['timestamp'], unit='s')
            return result.set_index('datetime')

    def download_dkline4daily(self, code, day_num):
        result = self.download_dkline(code, day_num)
        if result is not None:
            result['datetime'] = pd.to_datetime(result['tick_at'], unit='s')
            return result.set_index('datetime')


bao_d = BaoDownloader()
xueqiu_d = XueqiuDownloader()
dongcai_d = DongcaiDownloader()
wall_d = WallcnDownloader()

if __name__ == '__main__':
    # bao_d.download_dayline_from_bao('sh.600438')
    # bao_d.get_from_xls('000300')
    # xueqiu_d.download_dkline('sh.600438', 52*5)
    # xueqiu_d.sync_stock_detail(['sh.600438', 'sh.600928'])
    # xueqiu_d.save_stock_industry_info()
    # dongcai_d.get_report('sh.601005')
    # dongcai_d.get_fund_holding('sh.600928')
    dongcai_d.sync_broker_predict(['sz.002371',])
    # dongcai_d.get_express_profit('sh.600875')
    # dongcai_d.get_advance_report('sh.600875')
    # wall_d.download_dkline4daily('US10YR.OTC', 52*5)
