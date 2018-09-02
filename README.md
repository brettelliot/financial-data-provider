# financial-data-provider
Download, store and access historical financial data for stocks.

### Goal
To download, store and access historical financial data for stocks. 

### Usage

```python

# Get a dataframe with daily OHLCV and Adjusted OHLCV data 
df = fdp.get('AMZN', start_date='2017-10-01', end_date='2018-03-31')
```

Under the hood, FDP checks the data store to see if this data already exists and provides it if so. Otherwise, it downloads it using the default data provider (Alpha Vantage). When data is downloaded, if an adjusted close is provided, FDP will calculate the adjusted open, high, low and volume and add that to the data store. 
