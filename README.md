# Load history data from the Binance

This Python script downloads history data from Binance and saves it to a CSV file for future use in a dataframe. The script can work with a proxy, as well as downloading can be both on spot and on futures. You can also upload a longer period, the script itself will break the data into subqueries.

##prepare
python -m venv .venv && source .venv/bin/activate
pip install requests pandas

### example

```python
from src.Load import LoadBinanceHistoryData

# SPOT or FUTURE
market='SPOT'
# PAIR
sym = 'BTCUSDT'
# TIMEFRAME: 1m 5m 15m 30m 1h 4h 1d
tf = '1h'
# FROM
f = '2022-01-01 00:00:00'
# To
t = '2021-02-28 23:59:59'

lb = LoadBinanceHistoryData(market, sym, tf, f, t)

# Uncomment if you will be using a proxy
#lb.setProxy("ip","port","login","password")
#lb.checkProxy()

lb.load()
```


