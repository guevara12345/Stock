import json
import requests
from urllib.parse import urljoin
import configparser


class Config:
    def __init__(self):
        with open('config.json', 'r', encoding='utf-8') as f:
            d_data = json.loads(f.read())
            self.__wangtching_etf_index = d_data['wangtching_etf_index']

    @property
    def wangtching_etf_index(self):
        return self.__wangtching_etf_index


config = Config()

if __name__ == "__main__":
    config.get_stocks()
