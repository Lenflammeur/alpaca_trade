#!/usr/bin/env python3
import json
import pandas as pd
from alpaca_trade_api import REST
import websocket
import config


# Create an Alpaca API instance
api = REST(config.API_KEY, config.SECRET_KEY, base_url=config.BASE_URL)

# Create a DataFrame to hold our data
df = pd.DataFrame(columns=['MSFT', 'AAPL'])

def on_message(ws, message):
    """
    get message
    """
    #print("received a message")
    #print(message)
    # Parse the incoming JSON message
    message_data = json.loads(message)

    # Extract the bar data
    for bar in message_data:
        # Update our DataFrame
        if bar['S'] == 'MSFT':
            df.loc[pd.Timestamp(bar['t']), 'MSFT'] = bar['c']
            print(f"MSFT price updated: {bar['c']}")
        elif bar['S'] == 'AAPL':
            df.loc[pd.Timestamp(bar['t']), 'AAPL'] = bar['c']
            print(f"AAPL price updated: {bar['c']}")

    print(len(df))
    if 'MSFT' in df.columns and 'AAPL' in df.columns:
        # Drop any rows with missing values
        df.dropna(inplace=True)
        if len(df) > 10:  # wait for enough data to calculate statistics
            # Calculate the mean and std of the ratio of the prices
            print("We have more than 10 data")
            ratio = df['MSFT'] / df['AAPL']
            mean_ratio = ratio.rolling(window=10).mean()
            std_ratio = ratio.rolling(window=10).std()

            # Get the latest ratio
            latest_ratio = ratio.iloc[-1]

            # If the ratio is more than 1 standard deviation above the mean, short the pair
            if latest_ratio > mean_ratio.iloc[-1] + std_ratio.iloc[-1]:
                print("Shorting Pair")
                api.submit_order(
                    symbol='MSFT',
                    qty=1,
                    side='sell',
                    type='market',
                    time_in_force='gtc',
                )
                print("Submitted sell order for MSFT")
                api.submit_order(
                    symbol='AAPL',
                    qty=1,
                    side='buy',
                    type='market',
                    time_in_force='gtc',
                )
                print("Submitted buy order for AAPL")

            # If the ratio is more than 1 standard deviation below the mean, long the pair
            elif latest_ratio < mean_ratio.iloc[-1] - std_ratio.iloc[-1]:
                print("Longing Pair")
                api.submit_order(
                    symbol='MSFT',
                    qty=1,
                    side='buy',
                    type='market',
                    time_in_force='gtc',
                )
                print("Submitted buy order for MSFT")
                api.submit_order(
                    symbol='AAPL',
                    qty=1,
                    side='sell',
                    type='market',
                    time_in_force='gtc',
                )
                print("Submitted sell order for AAPL")

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
