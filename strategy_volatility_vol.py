import pandas as pd


class StrategyVolatilityVol:
    def count_volatility(self, df):
        df['STD20'] = df['percent'].ewm(
            span=20, adjust=False).std()
        df = df.sort_values(by='date', ascending=False)
        a = df.iloc[0]['STD20']
        return a/100

    def count_quantity_ratio(self, df):
        pass


vol = StrategyVolatilityVol()
