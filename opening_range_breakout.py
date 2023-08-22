import sqlite3
import config
import alpaca_trade_api as tradeapi
from datetime import datetime, date, timedelta, timezone
from alpaca_trade_api.rest import REST, TimeFrame
from alpha_vantage.timeseries import TimeSeries
import time
import smtplib
import ssl

context = ssl.create_default_context()


connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
       select id from strategy where name = 'opening_range_breakout'
  
               """)

strategy_id = cursor.fetchone()['id']

cursor.execute("""
       select symbol , name
               from stock
               join stock_strategy on stock_strategy.stock_id = stock.id
               where stock_strategy.strategy_id = ?

               
               """, (strategy_id,))
stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY,
                    base_url=config.API_URL)

current_date = '2023-08-18'
start_minute_bar = f"{current_date} 09:30:00"
end_minute_bar = f"{current_date} 09:45:00"


orders = api.list_orders(status='all', limit=500,
                         after=f"{current_date}T13:30:00Z")
existing_order_symbols = [order.symbol for order in orders]

messages = []


def get_minute_data(ticker):
    ts = TimeSeries(key='X08YY1PHQHG6L5GP',
                    output_format='pandas', indexing_type='date')

    df, _ = ts.get_intraday(
        ticker, interval='1min', outputsize='full')

    df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close",
                       "5. volume": "volume", "6. vwap": "vwap",  "date": "date"}, inplace=True)

    df = df.iloc[::-1]

    return df


for symbol in symbols:

    minute_bars = get_minute_data(symbol)

    opening_range_mask = (minute_bars.index >= start_minute_bar) & (
        minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]

    opening_range_low = opening_range_bars['low'].min()
    opening_range_high = opening_range_bars['high'].max()
    opening_range = opening_range_high - opening_range_low

    after_opening_range_mask = minute_bars.index >= end_minute_bar
    after_opening_range_bars = minute_bars.loc[after_opening_range_mask]

    after_opening_range_breakout = after_opening_range_bars[
        after_opening_range_bars['close'] > opening_range_high]
    if not after_opening_range_breakout.empty:
        if symbol not in existing_order_symbols:

            limit_price = after_opening_range_breakout.iloc[0]['close']
            messages.append(
                f"placing order for {symbol} at {limit_price} , closed_above {opening_range_high} \n\n{after_opening_range_breakout.iloc[0]}\n\n")

            rounded_limit_price = round(limit_price, 1)

            stop_price = round(limit_price - opening_range, 2)

            print(
                f"placing order for {symbol} at {limit_price} , closed_above {opening_range_high} at {after_opening_range_breakout.iloc[0]}")

            api.submit_order(
                symbol=symbol,
                side='buy',
                type='limit',
                qty='100',
                time_in_force='day',
                order_class='bracket',
                limit_price=limit_price,
                take_profit=dict(
                     limit_price=rounded_limit_price + opening_range,

                ),
                stop_loss=dict(
                    stop_price=stop_price,
                )

            )
        else:
            print(f"Already an order for {symbol} , skipping")
        time.sleep(13)

print(messages)

with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
    server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)

    email_message = f"Subject: Trade Notifications for {current_date} \n\n"

    email_message += "\n\n".join(messages)

    server.sendmail(config.EMAIL_ADDRESS, config.EMAIL_ADDRESS, email_message)
