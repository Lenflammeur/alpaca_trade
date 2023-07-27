#!/usr/bin/env python3
"""
main algorithmic trading script via Alpaca API
"""
import json
import pandas as pd
import websocket
from alpaca_trade_api import REST
import config
from trading import update_dataframe, check_crossover

# Create an Alpaca API instance
api = REST(config.API_KEY, config.SECRET_KEY, base_url=config.BASE_URL)

# Create a DataFrame to hold our data
df = pd.DataFrame(columns=['MSFT', 'AAPL', 'SPY', 'QQQ', 'NVDA'])

def on_message(ws, message):
    global df
    message_data = json.loads(message)
    for bar in message_data:
        df = update_dataframe(df, bar)
        print(f"{bar['S']} price updated: {bar['c']}")
        check_crossover(df, bar, api)

def on_open(ws):
    print("opened")
    auth_data = {
        "action": "auth",
        "key": config.API_KEY,
        "secret": config.SECRET_KEY
    }
    ws.send(json.dumps(auth_data))
    stream_message = {"action":"subscribe","bars":["AAPL", "MSFT"]}
    ws.send(json.dumps(stream_message))

def on_close(ws):
    print("closed connection")

socket = "wss://stream.data.alpaca.markets/v2/iex"

ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close)
ws.run_forever()
