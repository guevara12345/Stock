import baostock as bs
import pandas as pd
from pandas.tseries.offsets import *
import os
from datetime import datetime, timedelta
import time
import xlsxwriter
import matplotlib.pyplot as plt  # 可视化
import seaborn as sns  # 可视化
from scipy.stats import norm

from downloader import bao_d, xueqiu_d, wall_d
from basic_stock_data import basic
from code_formmat import code_formatter
from indicator import indi
from get_config import config


plt.rcParams['font.sans-serif'] = ['SimHei']  # 中文字体设置-黑体
plt.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题
sns.set(font='SimHei')  # 解决Seaborn中文显示问题


class StockReporter:
    def debug_stock(self, code):
        print('start debug_stock')
        stock_df = pd.DataFrame(index=[code, ])
        stock_df = self.apply_strategy4stocks(stock_df)

    def generate_zz800_report(self):
        time_str = datetime.now().strftime('%H%M%S')
        stock_df = self.preprocess4stock()
        stock_df = self.apply_strategy4stocks(stock_df)
        self.save2file(f'zz800_{time_str}', stock_df)

    def generate_watching_report(self):
        watching_df = self.preprocess4watching()
        watching_df = self.apply_strategy4stocks(watching_df)
        self.save2file4watching('holding_{}'.format(
            datetime.now().strftime('%H%M%S')), watching_df)

    def preprocess4stock(self):
        hs300_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/hs300_stocks.csv'), index_col=1, encoding="gbk")
        hs300_df = hs300_df.set_index("code")
        hs300_df['belong'] = 'hs300'

        zz500_df = pd.read_csv(os.path.join(
            os.getcwd(), 'raw_data/zz500_stocks.csv'), index_col=1, encoding="gbk")
        zz500_df = zz500_df.set_index("code")
        zz500_df['belong'] = 'zz500'
        return pd.concat([hs300_df, zz500_df])
        # return hs300_df

    def preprocess4watching(self):
        print('start generate watching stocks report')
        no_dup_dict = {}
        for x in config.watching_stocks:
            no_dup_dict[x['code']] = x
        stock_df = self.preprocess4stock()
        watching_df = pd.DataFrame()
        for i in no_dup_dict.values():
            series = pd.Series(i)
            series['url'] = 'https://xueqiu.com/S/{}'.format(
                code_formatter.code2capita(i['code']))
            if i['code'] in stock_df.index:
                series['industry'] = stock_df.loc[i['code'], 'industry']
                series['pe_max'] = stock_df.loc[i['code'], 'pe_max']
                series['pe_mean'] = stock_df.loc[i['code'], 'pe_mean']
                series['pe_min'] = stock_df.loc[i['code'], 'pe_min']
            else:
                stock_info=basic.get_stock_detail_from_bao(i['code'])
                series['industry'] = stock_info['industry']
                series['pe_max'] = stock_info['pe_max']
                series['pe_mean'] = stock_info['pe_mean']
                series['pe_min'] = stock_info['pe_min']
            series['hold'] = 'Y' if i.get('holding') else None
            series['chg_date'] = datetime.fromisoformat(i['chg_date'])
            watching_df = watching_df.append(series, ignore_index=True)
        return watching_df.reset_index(drop=True).set_index('code')

    def apply_strategy4stocks(self, df):
        for code in df.index.values.tolist():
            stock_df = xueqiu_d.download_dkline4daily(code, 52*5)

            df.loc[code, 'highest'] = indi.new_highest_date(
                stock_df['close'])

            ema_info = indi.macd(stock_df['close'])

            df.loc[code, 'dif/p'] = ema_info['dif/p']
            df.loc[code, 'macd/p'] = ema_info['macd/p']

            cci_info = indi.cci(stock_df[['close', 'high', 'low']])
            df.loc[code, 'cci'] = cci_info['cci']

            sorted_df = stock_df.sort_values(by='datetime', ascending=False)
            df.loc[code, 'pe'] = sorted_df.iloc[0]['pe']
            df.loc[code, 'pb'] = sorted_df.iloc[0]['pb']
            df.loc[code, 'price'] = sorted_df.iloc[0]['close']
            df.loc[code, 'chg_rate'] = sorted_df.iloc[0]['percent']/100

            detail = xueqiu_d.download_stock_detail(code)
            df.loc[code, 'cap'] = detail['market_value']
            df.loc[code, 'f_cap'] = detail['float_market_capital']
            df.loc[code, 'vol_ratio'] = detail['vol_ratio']

            volatility_info = indi.count_volatility(
                stock_df[['close', 'high', 'low']])
            df.loc[code, 'atr/p'] = volatility_info['atr/p']
            df.loc[code, 'unit4me'] = volatility_info['unit4me']

            hk_info = indi.count_hk_holding_rate(stock_df)
            if hk_info:
                df.loc[code, 'hk_ratio'] = hk_info['hk_ratio']/100
                df.loc[code, 'hk-ma(hk,10)'] = hk_info['hk-ma(hk,10)']/100
                df.loc[code, 'hk-ma(hk,30)'] = hk_info['hk-ma(hk,30)']/100

            if df.loc[code, 'pe'] > 0:
                #     pepb_band = indi.count_pe_pb_band(stock_df)
                #     df.loc[code, 'pe_percent'] = pepb_band['pe_percent']
                #     df.loc[code, 'pb_percent'] = pepb_band['pb_percent']
                df.loc[code, 'roe_ttm'] = df.loc[code, 'pb']/df.loc[code, 'pe']

            vol_info = indi.count_quantity_ratio(stock_df)
            df.loc[code, 'turnover'] = vol_info['turnover']/100

            if 'hold' in df.columns:
                chg = indi.count_chg_since(
                    df.loc[code, 'chg_date'], stock_df['percent'])
                df.loc[code, 'since_chg'] = chg
        return df

    def save2file(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'highest', 'price', 'chg_rate',
                 'dif/p', 'macd/p', 'cci',
                 'turnover', 'vol_ratio', 'atr/p', 'unit4me',
                 'cap', 'f_cap',  'pe', 'pb', 'pe_max', 'pe_mean', 'pe_min', 'roe_ttm',
                 'hk_ratio', 'hk-ma(hk,10)', 'belong', 'url']]
        writer = pd.ExcelWriter(f'./raw_data/{folder_name}/{filename}.xlsx',
                                datetime_format='yyyy-mm-dd',
                                engine='xlsxwriter',
                                options={'remove_timezone': True})
        # Convert the dataframe to an XlsxWriter Excel object.
        df.to_excel(writer, encoding="gbk", sheet_name='Sheet1')

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add some cell formats.
        format1 = workbook.add_format({'num_format': '0.00'})
        format2 = workbook.add_format({'num_format': '0.00%'})

        worksheet.set_column('F:H', None, format2)
        worksheet.set_column('I:I', None, format1)
        worksheet.set_column('J:J', None, format2)
        worksheet.set_column('L:M', None, format2)
        worksheet.set_column('N:T', None, format1)
        worksheet.set_column('U:W', None, format2)
        color_format = {'type': 'data_bar',
                        'bar_solid': True, 'bar_color': '#4169E1', }
        worksheet.conditional_format('F1:F801', color_format)
        worksheet.conditional_format('G1:G801', color_format)
        worksheet.conditional_format('H1:H801', color_format)
        worksheet.conditional_format('I1:I801', color_format)
        worksheet.conditional_format('J1:J801', color_format)
        worksheet.conditional_format('K1:K801', color_format)
        worksheet.conditional_format('U1:U801', color_format)
        worksheet.conditional_format('V1:V801', color_format)
        worksheet.conditional_format('W1:W801', color_format)

        # worksheet.set_row(0, None, row_format)

        # Freeze the first row.
        worksheet.freeze_panes(1, 3)

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()

    def save2file4watching(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'industry', 'highest', 'price', 'chg_rate',
                 'dif/p', 'macd/p', 'cci',
                 'hold', 'chg_date', 'since_chg',
                 'atr/p', 'unit4me', 'turnover', 'vol_ratio',
                 'cap', 'f_cap',  'pe', 'pb', 'pe_max', 'pe_mean', 'pe_min',  'roe_ttm',
                 'hk_ratio', 'hk-ma(hk,10)', 'url']]
        writer = pd.ExcelWriter(f'./raw_data/{folder_name}/{filename}.xlsx',
                                datetime_format='yyyy-mm-dd',
                                engine='xlsxwriter',
                                options={'remove_timezone': True})
        # Convert the dataframe to an XlsxWriter Excel object.
        df.to_excel(writer, encoding="gbk", sheet_name='Sheet1')

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add some cell formats.
        format1 = workbook.add_format({'num_format': '0.00'})
        format2 = workbook.add_format({'num_format': '0.00%'})

        worksheet.set_column('F:H', None, format2)
        worksheet.set_column('I:I', None, format1)
        worksheet.set_column('L:O', None, format2)
        worksheet.set_column('Q:W', None, format1)
        worksheet.set_column('X:Z', None, format2)

        color_format = {'type': 'data_bar',
                        'bar_solid': True, 'bar_color': '#4169E1', }
        worksheet.conditional_format('F1:F801', color_format)
        worksheet.conditional_format('G1:G801', color_format)
        worksheet.conditional_format('H1:H801', color_format)
        worksheet.conditional_format('I1:I801', color_format)
        worksheet.conditional_format('L1:L801', color_format)
        worksheet.conditional_format('M1:M801', color_format)
        worksheet.conditional_format('N1:N801', color_format)

        worksheet.conditional_format('X1:X801', color_format)
        worksheet.conditional_format('Y1:Y801', color_format)
        worksheet.conditional_format('Z1:Z801', color_format)

        format3 = workbook.add_format({'bg_color': '#4169E1', })
        worksheet.conditional_format(
            'J1:J801', {'type': 'cell', 'criteria': 'equal to',
                        'value': '"Y"', 'format': format3})

        # worksheet.set_row(0, None, row_format)

        # Freeze the first row.
        worksheet.freeze_panes(1, 3)

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()


class EtfIndexReporter:
    def generate_etf_index_report(self):
        print('start generate etf_index report')
        watch_data_dict = self.preprocess4watching(config.wangtching_etf_index)
        etf_index_df = pd.DataFrame()
        for i in watch_data_dict:
            self.apply_strategy(i)
            etf_index_df = etf_index_df.append(i['series'], ignore_index=True)
        etf_index_df = etf_index_df.reset_index(drop=True).set_index('code')
        time_str = datetime.now().strftime('%H%M%S')
        self.save2file(f'etf_index_{time_str}', etf_index_df)

    def preprocess4watching(self, watch_data_dict):
        for i in watch_data_dict:
            i['series'] = pd.Series(i)
            if i['data_source'] == 'xueqiu':
                i['df'] = xueqiu_d.download_dkline4daily(
                    i['code'], 52*5)
            elif i['data_source'] == 'wallstreetcn':
                i['df'] = wall_d.download_dkline4daily(i['code'], 52*5)
        return watch_data_dict

    def apply_strategy(self, stock):
        df = stock['df']

        result = stock['series']
        if stock['data_source'] == 'xueqiu':
            result['url'] = 'https://xueqiu.com/S/{}'.format(
                code_formatter.code2capita(stock['code']))
            detail = xueqiu_d.download_stock_detail(result['code'])
            result['is_open'] = detail['is_open']
            result['vol_ratio'] = detail['vol_ratio']
            df_sorted = df.sort_index(ascending=False)
            result['close'] = df_sorted.iloc[0]['close']

        elif stock['data_source'] == 'wallstreetcn':
            result['url'] = 'https://wallstreetcn.com/markets/codes/{}'.format(
                stock['code'])
            df = df.rename(
                columns={'open_px': 'open', 'close_px': 'close', 'high_px': 'high',
                         'low_px': 'low', 'px_change_rate': 'percent', })
            df_sorted = df.sort_index(ascending=False)
            result['close'] = df_sorted.iloc[0]['close']

        result['percent'] = df_sorted.iloc[0]['percent']/100

        result['highest'] = indi.new_highest_date(df['close'])

        ema_info = indi.macd(df['close'])
        result['dif/p'] = ema_info['dif/p']
        result['macd/p'] = ema_info['macd/p']

        cci_info = indi.cci(df[['close', 'high', 'low']])
        result['cci'] = cci_info['cci']

        volatility_info = indi.count_volatility(df[['close', 'high', 'low']])
        result['atr/p'] = volatility_info['atr/p']
        result['unit4me'] = volatility_info['unit4me']

        return result

    def corr(self):
        def f(x):
            new_time = x['datetime'] - x['datetime'].hour*Hour()
            return pd.Series([new_time, x['percent']], index=['datetime', 'percent'])
        watch_list = self.preprocess4watching(config.wangtching_etf_index)
        df = pd.DataFrame()
        for i in watch_list:
            if i['data_source'] == 'xueqiu':
                chg = i['df']['percent']
                if chg.index[0].hour != 0:
                    chg_df = chg.reset_index()
                    chg_df = chg_df.apply(f, axis=1, result_type='expand')
                    chg_df = chg_df.reset_index(
                        drop=True).set_index('datetime')
                    df[i['code_name']] = chg_df['percent']
                else:
                    df[i['code_name']] = chg
            elif i['data_source'] == 'wallstreetcn':
                df[i['code_name']] = i['df']['px_change_rate']
        df = df[["CN10YR Treasury", "US10YR Treasury", "美元指数", "离岸人民币", "WTI原油",
                 "纽约金", "纽约银", "纽约铜", "BTC/USD", "SP500",
                 "Nasdaq", "恒生指数", "上证指数", "上证50", "沪深300",
                 "中证500", "创业板50", "科创50", ]]
        # df = df[["上证指数", "上证50", "沪深300", "中证500", "创业板50",
        #          "科创50", "证券ETF", "隆基股份", "军工ETF", "芯片ETF",
        #          "有色金属ETF", "新能源车ETF", "银行ETF", "5GETF", "酒ETF",
        #          "农业ETF", "中概互联网ETF", "医疗ETF", "医药ETF", "煤炭ETF",
        #          "钢铁ETF", "计算机ETF", "房地产ETF", ]]
        df = df.sort_index(ascending=False)[0:26*5-1]
        corr = df.corr(method='spearman')
        _, ax = plt.subplots(figsize=(12, 10))  # 分辨率1200×1000
        _ = sns.heatmap(corr,  # 使用Pandas DataFrame数据，索引/列信息用于标记列和行F
                        cmap="RdBu_r",  # 数据值到颜色空间的映射
                        square=True,  # 每个单元格都是正方形
                        cbar_kws={'shrink': .9},  # `fig.colorbar`的关键字参数
                        ax=ax,  # 绘制图的轴
                        annot=True,  # 在单元格中标注数据值
                        fmt=".2f",
                        annot_kws={'fontsize': 'xx-small'})  # 热图，将矩形数据绘制为颜色编码矩阵

        plt.show()

    def save2file(self, filename, df: pd.DataFrame):
        folder_name = datetime.now().strftime('%Y%b%d')
        if not os.path.exists(f'./raw_data/{folder_name}'):
            os.mkdir(f'./raw_data/{folder_name}')

        df = df[['code_name', 'is_open', 'highest', 'close', 'percent',
                 'dif/p', 'macd/p', 'cci',
                 'vol_ratio', 'atr/p', 'unit4me', 'url']]
        writer = pd.ExcelWriter(f'./raw_data/{folder_name}/{filename}.xlsx',
                                datetime_format='yyyy-mm-dd',
                                engine='xlsxwriter',
                                options={'remove_timezone': True})
        # Convert the dataframe to an XlsxWriter Excel object.
        df.to_excel(writer, encoding="gbk", sheet_name='Sheet1')

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add some cell formats.
        format2 = workbook.add_format({'num_format': '0.00%'})
        format1 = workbook.add_format({'num_format': '0.00'})
        worksheet.set_column('F:H', None, format2)
        worksheet.set_column('I:I', None, format1)
        worksheet.set_column('K:L', None, format2)

        color_format = {'type': 'data_bar',
                        'bar_solid': True, 'bar_color': '#4169E1', }
        worksheet.conditional_format('F1:F801', color_format)
        worksheet.conditional_format('G1:G801', color_format)
        worksheet.conditional_format('H1:H801', color_format)
        worksheet.conditional_format('I1:I801', color_format)
        worksheet.conditional_format('K1:K801', color_format)

        format1 = workbook.add_format({'bg_color': '#4169E1', })
        worksheet.conditional_format(
            'C1:C801', {'type': 'cell', 'criteria': 'equal to',
                        'value': '"Y"', 'format': format1})

        # Freeze the first row.
        worksheet.freeze_panes(1, 0)

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()


eir = EtfIndexReporter()
sr = StockReporter()
if __name__ == '__main__':
    # sr.generate_zz800_report()
    sr.generate_watching_report()
    # sr.get_ep('sz.002568')
    # sr.get_ep('sh.600754')
    eir.generate_etf_index_report()
    # eir.corr()
    # sr.debug_stock('sh.603288')
