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