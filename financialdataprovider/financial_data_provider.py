import sqlite3
import pandas as pd
import requests
import json


class FinancialDataProvider(object):

    def __init__(self):
        self._conn = self._create_connection('fdp_daily.db')
        if self._conn is not None:
            self._create_daily_price_table()

        else:
            print('Error! cannot create the database connection.')

    def __del__(self):
        self._conn.close()

    def get(self, date):
        pass

    def _create_connection(self, db_file):
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            print(sqlite3.version)
        except sqlite3.Error as e:
            print(e)

        return conn

    def _create_daily_price_table(self):
        sql_statements = ('CREATE TABLE IF NOT EXISTS daily_data ('
                          'date text NOT NULL,'
                          'symbol text NOT NULL,'
                          'open real NOT NULL,'
                          'high real NOT NULL,'
                          'low real NOT NULL,'
                          'close real NOT NULL,'
                          'volume integer NOT NULL,'
                          'dividend_amt real,'
                          'split_coeff real,'
                          'adj_close real NOT NULL,'
                          'PRIMARY KEY (date, symbol));')
        """
                          'adj_open real NOT NULL,'
                          'adj_high real NOT NULL,'
                          'adj_low real NOT NULL,'

                          'adj_volume integer NOT NULL);')
        """

        try:
            c = self._conn.cursor()
            c.execute(sql_statements)
        except sqlite3.Error as e:
            print(e)

    def get(self, symbol, start_date, end_date):

        # Try an access it
        values = (symbol, start_date, end_date)
        # SELECT * FROM daily_data WHERE symbol='AMZN' AND date between '2018-08-18' AND '2018-08-31';
        sql_statements = "SELECT * FROM daily_data WHERE symbol=? AND date BETWEEN ? AND ?;"

        print(sql_statements)

        try:
            df = pd.read_sql(sql_statements, self._conn, index_col='date', params=values)
            print('Read {}:'.format(symbol))
        except Exception as e:
            print(e)
            df = self._download_and_store(symbol, start_date, end_date)

        return df

    def _download_and_store(self, symbol, start_date, end_date):

        df = self._download(symbol, start_date, end_date)
        df = self._store(df)
        return df

    def _download(self, symbol, start_date, end_date):

        payload = {'apikey': 'XPTB5QP8MF5RDRUD', 'function': 'TIME_SERIES_DAILY_ADJUSTED', 'symbol': 'AMZN'}
        response = requests.get('https://www.alphavantage.co/query', params=payload)

        if response.status_code != requests.codes.ok:
            print('Error getting {} from Alpha Vantage'.format(symbol))
            print('Error: {}'.format(response.json()))
        else:
            print('Downloaded {}'.format((symbol)))
            json_dict = response.json()
            df = pd.DataFrame.from_dict(json_dict['Time Series (Daily)'], orient="index")

            # add a column for the date and symbol
            df['symbol'] = symbol
            df['date'] = df.index

            # The columns we get back from AV are:
            # ['1. open', '2. high', '3. low', '4. close',
            # '5. adjusted close', '6. volume', '7. dividend amount', '8. split coefficient']
            df = df.rename( columns={'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close',
                '5. adjusted close': 'adj_close', '6. volume': 'volume', '7. dividend amount': 'dividend_amt',
                '8. split coefficient': 'split_coeff'})

            # reorder the columns so date and symbol are first
            df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'dividend_amt', 'split_coeff', 'adj_close']]

            return df

    def _store(self, df):

        # generate a list of tuples for each row in the dataframe
        values = list(df.itertuples(index=False))

        sql = ('REPLACE INTO daily_data'
                '(date, symbol, open, high, low, close, volume, dividend_amt, split_coeff, adj_close)'
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);')

        cur = self._conn.cursor()
        cur.executemany(sql, values)
        self._conn.commit()

        return df


def main():

    fdp = FinancialDataProvider()
    df = fdp.get('AMZN', start_date='2018-08-01', end_date='2018-08-31')
    print(df)


if __name__ == '__main__':
    main()
