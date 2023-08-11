from src.Load import LoadBinanceHistoryData


options = {
    # SPOT or FUTURE
    'market':'FUTURE',
    # PAIR
    'sym': 'BTCUSDT',
    # TIMEFRAME: 1m 5m 15m 30m 1h 4h 1d
    'tf': '5m',
    # FROM
    'f': '2023-07-01 00:00:00',
    # To
    't': '2023-07-31 23:59:59',
    # ADD OPEN INTEREST DATA
    'oi': True,
    # Taker Buy/Sell Volume
    'tbsv': True,
}

lb = LoadBinanceHistoryData(**options)

# Uncomment if you will be using a proxy
# lb.setProxy("78.47.212.29","9999","proxyuser","5SQgfhjkm")
# lb.checkProxy()

lb.load()