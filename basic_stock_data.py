import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import requests
from lxml import etree
import re
import matplotlib.pyplot as plt  # 可视化
import seaborn as sns  # 可视化
from scipy.stats import norm

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
        self.NO_INTEREST_CONCEPT = [
            'HS300_', 'MSCI中国', '标普概念', '富时概念', '中证500',
            '融资融券', '上证180_', '上证50_', '上证380']
        modified_time = time.localtime(os.stat('./raw_data/xueqiu_industry.csv').st_mtime)
        if datetime.now()-modified_time > timedelta(days=30):
            xueqiu_d.save_stock_industry_info()
        self.industry = pd.read_csv(
            './raw_data/xueqiu_industry.csv', index_col=1, encoding="gbk")

    def hs300_index_component(self):
        file_path = os.path.join(os.getcwd(), f'raw_data/hs300_stocks.csv')
        # 获取沪深300成分股
        rs = bs.query_hs300_stocks()
        print(f'hs300_index_component, error_code:{rs.error_code}')

        # 打印结果集
        hs300_stocks = []
        is_outdate = None
        while (rs.error_code == '0') & rs.next():
            row = rs.get_row_data()
            if is_outdate is None:
                is_outdate = self.check_index_component_outdate(
                    row[0], file_path)
                # is_outdate = True
                hs300_stocks.append(row)
            elif is_outdate == True:
                # 获取一条记录，将记录合并在一起
                hs300_stocks.append(row)
            else:
                return
        result = pd.DataFrame(hs300_stocks, columns=rs.fields)

        result = result.set_index("code")
        # result = self.get_stock_industry_from_dongcai(result)
        for code in result.index.values.tolist():
            stock_info = self.get_stock_detail(code)
            result.loc[code, 'url'] = stock_info['url']
            result.loc[code, 'industry'] = stock_info['industry']
            result.loc[code, 'pe_max'] = stock_info['pe_max']
            result.loc[code, 'pe_mean'] = stock_info['pe_mean']
            result.loc[code, 'pe_min'] = stock_info['pe_min']
        # 结果集输出到csv文件
        result.to_csv(file_path, encoding="gbk")

    def zz500_index_component(self):
        file_path = os.path.join(os.getcwd(), f'raw_data/zz500_stocks.csv')
        # 获取中证500成分股
        rs = bs.query_zz500_stocks()
        print(f'zz500_index_component, error_code:{rs.error_code}')

        # 打印结果集
        zz500_stocks = []
        is_outdate = None
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            row = rs.get_row_data()
            if is_outdate is None:
                is_outdate = self.check_index_component_outdate(
                    row[0], file_path)
                # is_outdate = True
                zz500_stocks.append(row)
            elif is_outdate == True:
                # 获取一条记录，将记录合并在一起
                zz500_stocks.append(row)
            else:
                return
        result = pd.DataFrame(zz500_stocks, columns=rs.fields)

        result = result.set_index("code")
        # result = self.get_stock_industry_from_dongcai(result)
        for code in result.index.values.tolist():
            stock_info = self.get_stock_detail(code)
            result.loc[code, 'url'] = stock_info['url']
            result.loc[code, 'industry'] = stock_info['industry']
            result.loc[code, 'pe_max'] = stock_info['pe_max']
            result.loc[code, 'pe_mean'] = stock_info['pe_mean']
            result.loc[code, 'pe_min'] = stock_info['pe_min']
        # 结果集输出到csv文件
        result.to_csv(file_path, encoding="gbk")

    def check_index_component_outdate(self, date_str, file_path):
        df = pd.read_csv(file_path, encoding="gbk")
        if date_str > df.iloc[0]['updateDate']:
            return True
        else:
            return False

    def get_stock_detail(self, code):
        print(f'get stock info of {code}')
        # code2capita = code_formatter.code2capita(code)
        # rs = bs.query_stock_industry(code)
        # industry_list = []
        # while (rs.error_code == '0') & rs.next():
        #     # 获取一条记录，将记录合并在一起
        #     industry_list.append(rs.get_row_data())
        # industry = industry_list[0][3]
        pe_distr = self.get_ep_distr(code)
        return {
            'url': f'https://xueqiu.com/S/{code2capita}',
            # 'industry': industry,
            'pe_max': pe_distr['pe_max'],
            'pe_mean': pe_distr['pe_mean'],
            'pe_min': pe_distr['pe_min'],
        }

    def get_ep_distr(self, code):
        stock_df = xueqiu_d.download_dkline4daily(code, 52*5*10)
        ep = stock_df['pe'].apply(lambda x: 1/x)
        mean = ep.mean()
        std = ep.std()
        r = {
            'pe_max': 1/(mean-std),
            'pe_mean': 1/mean,
            'pe_min': 1/(mean+std),
        }
        # sns.distplot(ep, fit=norm)
        # plt.show()
        return r

    def get_stock_industry_from_dongcai(self, df):
        for code in df.index.values.tolist():
            print(f'get stock  of {code}')
            code2capita = code_formatter.code2capita(code)
            df.loc[code, 'industry'] = self.get_industry(code)
            df.loc[code, 'concept'] = self.get_concept(code)
            df.loc[code,
                   'url'] = f'https://xueqiu.com/S/{code2capita}'
        return df

    def get_industry(self, code):
        code_without_point = code_formatter.code2nopoint(code)
        url = f'http://quote.eastmoney.com/{code_without_point}.html'
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            selector = etree.HTML(rsp.text.encode('utf-8'))
            text_list = selector.xpath('//div[@class="nav"]//a/text()')
            if len(text_list) >= 3:
                industry = text_list[2]
                return industry

    def get_concept(self, code):
        code_without_point = code_formatter.code2nopoint(code)
        url = f'http://f10.eastmoney.com/CoreConception/CoreConceptionAjax?code={code_without_point}'
        rsp = self.session.get(url)
        if rsp.status_code == 200:
            concept_list = rsp.json()['hxtc']
            for i in concept_list:
                if i['gjc'] == '所属板块':
                    concept_list = re.split(r'\s+', i['ydnr'])
                    f = list(
                        set([x for x in concept_list if x not in self.NO_INTEREST_CONCEPT]))
                    return ','.join(f)


basic = BaiscStockData()

if __name__ == '__main__':
    # basic.hs300_index_component()
    # basic.zz500_index_component()
    basic.get_ep_distr('sh.601919')
    basic.get_ep_distr('sz.002493')
