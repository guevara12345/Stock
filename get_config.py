import json
import requests
from urllib.parse import urljoin
import configparser


class Config:
    def __init__(self):
        self.__cf = configparser.ConfigParser()
        self.__cf.read('config.ini')
        stocks = self.__cf['holding_stocks']['stocks']
        self.__stock_list = stocks.split(',')

    def get_stocks(self):
        return self.__stock_list


config = Config()

if __name__ == "__main__":
    config.get_stocks()
