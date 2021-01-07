import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
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
        url = XUEQIU_D_KLINE_URL_FORMAT.format(
            capital_code, int(time.time()*1000), -day_num)
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            print(
                f'download {day_num} days history day_kline data of {capital_code}')
            dkline_json = rsp.json()
            result = pd.DataFrame(
                dkline_json['data']['item'], columns=dkline_json['data']['column'])
            result['date'] = pd.to_datetime(result['timestamp'], unit='ms')
            return result


bao_d = BaoDownloader()
xueqiu_d = XueqiuDownloader()

if __name__ == '__main__':
    # bao_d.download_dayline_from_bao('sh.600438')
    # bao_d.get_from_xls('000300')
    # xueqiu_d.download_dkline_from_xueqiu('SH600438', 52*5)