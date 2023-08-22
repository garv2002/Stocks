import sqlite3
import config
from alpaca_trade_api.rest import REST, TimeFrame
import alpaca_trade_api as tradeapi
from datetime import datetime, date, timedelta
import tulipy
import numpy

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
               SELECT id,symbol,name FROM stock
               """)
rows = cursor.fetchall()

symbols = []
stock_dict = {}

for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']

cleaned_symbols = [symbol.split('/')[0] for symbol in symbols]

api_v2 = tradeapi.REST(config.API_KEY, config.SECRET_KEY,
                       base_url=config.API_URL, api_version='v2')


chunk_size = 200


for i in range(0, len(cleaned_symbols), chunk_size):
    symbol_chunk = cleaned_symbols[i:i+chunk_size]

    barsets = api_v2.get_bars(symbol_chunk, TimeFrame.Day,
                              "2023-08-01", date.today() - timedelta(days=1))._raw

    # recent_closes = [bar["c"] for bar in barsets[symbol]]

    for bar in barsets:
        symbol = bar["S"]
        print(f"processing symbol ` {symbol}")

        stock_id = stock_dict[bar["S"]]

        iso_date = bar["t"]
        parsed_date = datetime.strptime(iso_date,      "%Y-%m-%dT%H:%M:%SZ")
        formatted_date = parsed_date.strftime("%Y-%m-%d")

        # if len(recent_closes) >= 50 and (date.today() - timedelta(days=1)) == formatted_date:

        #     sma_20 = tulipy.sma(numpy.array(recent_closes), period=20)[-1]
        #     sma_50 = tulipy.sma(numpy.array(recent_closes), period=50)[-1]
        #     rsi_14 = tulipy.rsi(numpy.array(recent_closes), period=14)[-1]
        # else:
        #     sma_20, sma_50, rsi_14 = None, None, None

        cursor.execute("""
        INSERT INTO stock_price (stock_id, date,      open, high, low, close, volume )
        VALUES (?, ?, ?, ?, ?, ?, ? )
        """, (stock_id, formatted_date, bar["o"],      bar["h"], bar["l"], bar["c"], bar["v"]))


connection.commit()
