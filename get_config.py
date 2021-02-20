import json
import requests
from urllib.parse import urljoin
import configparser


class Config:
    def __init__(self):
        with open('config.json', 'r', encoding='utf-8') as f:
            j_config = json.loads(f.read())
            self.__wangtching_etf_index = j_config['wangtching_etf_index']
            self.__watching_stocks = j_config['watching_stocks']

    @ property
    def wangtching_etf_index(self):
        return self.__wangtching_etf_index

    @ property
    def watching_stocks(self):
        return self.__watching_stocks


config = Config()

if __name__ == "__main__":
    config.wangtching_etf_index
