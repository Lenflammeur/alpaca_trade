#!/usr/bin/env python3
import json
import pandas as pd
from alpaca_trade_api import REST
import config
import websocket

# Create an Alpaca API instance
api = REST(config.API_KEY, config.SECRET_KEY, base_url=config.BASE_URL)

# Create a DataFrame to hold our data
df = pd.DataFrame(columns=['MSFT', 'AAPL'])


def on_message(ws, message):
    global df
    # Parse the incoming JSON message
    message_data = json.loads(message)

    # Extract the bar data
    for bar in message_data:
        # Update our DataFrame
        new_data = pd.DataFrame({
            'symbol': [bar['S']],
            'price': [bar['c']],
            'time': [pd.Timestamp(bar['t'])]
        })
        df = pd.concat([df, new_data], ignore_index=True)

        # Print the updated prices
        print(f"{bar['S']} price updated: {bar['c']}")

        # Calculate moving averages
        short_avg = df[df['symbol'] == bar['S']]['price'].rolling(window=5).mean()
        long_avg = df[df['symbol'] == bar['S']]['price'].rolling(window=20).mean()

        # Check for crossover
        if len(short_avg) > 20:  # ensure we have enough data for the long moving average
            print("We have enough data for long moving average")
            if short_avg.iloc[-1] > long_avg.iloc[-1] and short_avg.iloc[-2] < long_avg.iloc[-2]:
                # Buy when the short moving average crosses above the long moving average
                api.submit_order(
                    symbol=bar['S'],
                    qty=1,
                    side='buy',
                    type='market',
                    time_in_force='gtc',
                )
                print(f"Submitted buy order for {bar['S']}")
                

            elif short_avg.iloc[-1] < long_avg.iloc[-1] and short_avg.iloc[-2] > long_avg.iloc[-2]:
                # Sell when the short moving average crosses below the long moving average
                api.submit_order(
                    symbol=bar['S'],
                    qty=1,
                    side='sell',
                    type='market',
                    time_in_force='gtc',
                )
                print(f"Submitted sell order for {bar['S']}")
                
def on_open(ws):
    """
    open the connection
    """
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
    """
    close the connection
    """
    print("closed connection")

socket = "wss://stream.data.alpaca.markets/v2/iex"

ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close)
ws.run_forever()
