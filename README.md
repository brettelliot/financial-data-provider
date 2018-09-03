## financial-data-provider
Download, store and access historical financial data for stocks.

### Installation
To install simply:

`pip install -e git://github.com/brettelliot/financial-data-provider.git#egg=financial-data-provider`

### Usage

Create an instance of the `FinancialDataProvider` class and then call `get`.

```python
# Get a dataframe with daily OHLCV and Adjusted OHLC data 
fdp = FinancialDataProvider()
df = fdp.get('AMZN', start_date='2017-10-01', end_date='2018-08-31')
```

Under the hood, FDP checks the data store to see if this data already exists and provides it. Otherwise, it downloads it using the default data provider (Alpha Vantage). When data is downloaded, if an adjusted close is provided, FDP will calculate the adjusted open, high, and low adding those to the data store. 
