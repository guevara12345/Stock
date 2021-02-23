from basic_stock_data import basic
from stock_profit import profit
from report import eir, sr


if __name__ == '__main__':
    basic.hs300_index_component()
    basic.zz500_index_component()

    # sr.generate_report()
    eir.generate_etf_index_report()

    # profit.generate_report()
