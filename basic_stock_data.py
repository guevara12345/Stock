import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import requests
from lxml import etree

from downloader import bao_d, xueqiu_d
from code_formmat import code_formatter


class BaiscStockData:
    def __init__(self):
        #### 登陆baostock ####
        self.lg = bs.login()
        print('login respond error_code:'+self.lg.error_code)
        # get session of dongcai
        self.session = requests.Session()
        self.session.headers.update(
            {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'})
        self.session.get('https://www.eastmoney.com/')

    def hs300_index_component(self):
        # 获取沪深300成分股
        rs = bs.query_hs300_stocks()
        print(f'hs300_index_component, error_code:{rs.error_code}')

        # 打印结果集
        hs300_stocks = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            hs300_stocks.append(rs.get_row_data())
        result = pd.DataFrame(hs300_stocks, columns=rs.fields)

        result = result.set_index("code")
        result = self.get_stock_detail_from_dongcai(result)
        # 结果集输出到csv文件
        result.to_csv(
            os.path.join(os.getcwd(), f'raw_data/hs300_stocks.csv'),
            encoding="gbk")

    def zz500_index_component(self):
        # 获取中证500成分股
        rs = bs.query_zz500_stocks()
        print(f'zz500_index_component, error_code:{rs.error_code}')

        # 打印结果集
        zz500_stocks = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            zz500_stocks.append(rs.get_row_data())
        result = pd.DataFrame(zz500_stocks, columns=rs.fields)

        result = result.set_index("code")
        result = self.get_stock_detail_from_dongcai(result)

        # 结果集输出到csv文件
        result.to_csv(
            os.path.join(os.getcwd(), f'raw_data/zz500_stocks.csv'),
            encoding="gbk")

    def get_stock_detail_from_bao(self, df):
        for code in df.index.values.tolist():
            # self.download_dayline_from_bao2file(code, 'hs300_d')
            code_without_point = code_formatter.code2nopoint(code)
            df.loc[code,
                   'url'] = f'http://quote.eastmoney.com/{code_without_point}.html'
            rs = bs.query_stock_industry(code)
            industry_list = []
            while (rs.error_code == '0') & rs.next():
                # 获取一条记录，将记录合并在一起
                industry_list.append(rs.get_row_data())
            df.loc[code, 'industryClassification'] = industry_list[0][3]
            print(f'get stock info of {code}')
        return df

    def get_stock_detail_from_dongcai(self, df):
        for code in df.index.values.tolist():
            code_without_point = code_formatter.code2nopoint(code)
            df[code, 'industry'] = self.get_industry(code_without_point)
            df[code, 'concept'] = self.get_concept(code_without_point)
        return df

    def get_industry(self, code):
        rsp = self.session.get(
            f'http://quote.eastmoney.com/{code}.html')
        if rsp.status_code == 200:
            selector = etree.HTML(rsp.text.encode('utf-8'))
            industry = selector.xpath('//div[@class="xggpbox"]//td')
            return industry

    def get_concept(self, code):
        rsp = self.session.get(
            f'http://f10.eastmoney.com/OperationsRequired/Index?type=web&code={code}')
        if rsp.status_code == 200:
            selector = etree.HTML(rsp.text.encode('utf-8'))
            concept = selector.xpath('//div[@class="xggpbox"]//td')
            return concept


if __name__ == '__main__':
    basic = BaiscStockData()
    basic.hs300_index_component()
    basic.zz500_index_component()
