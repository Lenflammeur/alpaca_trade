"""
script for the trading strategy
"""
import pandas as pd
import boto3

sns = boto3.client('sns', region_name='us-east-1')
response = sns.publish(
    TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message="Connected to SNS"
)

lookback = 20
threshold = 1.5

def update_dataframe(df, bar):
    """
    update the df with the new bar fetched at 1min interval
    """
    new_data = pd.DataFrame({
        'symbol': [bar['S']],
        'price': [bar['c']],
        'time': [pd.Timestamp(bar['t'])]
    })
    return pd.concat([df, new_data], ignore_index=True)

def pair_trading(df, api):
    """
    pair trading strategy with Apple and Microsoft
    """
    if len(df) < lookback:
        return
    
    aapl_prices = df[df['symbol'] == 'AAPL']['price'].astype(float)
    msft_prices = df[df['symbol'] == 'MSFT']['price'].astype(float)

    if len(aapl_prices) < lookback or len(msft_prices) < lookback:
        print("Not enough data for trading")
        return
    
    if len(aapl_prices) == lookback and len(msft_prices) == lookback:
        sns.publish(TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message="It's Show Time!")

    aapl_prices = aapl_prices.tail(lookback)
    msft_prices = msft_prices.tail(lookback)

    spread = aapl_prices.values - msft_prices.values
    spread_series = pd.Series(spread)
    mean_spread = spread_series.rolling(window=lookback).mean().iloc[-1]
    std_spread = spread_series.rolling(window=lookback).std().iloc[-1]
    z_score = (spread[-1] - mean_spread) / std_spread

    portfolio_value = float(api.get_account().cash)
    qty_aapl = int(0.4 * portfolio_value / aapl_prices.iloc[-1])
    qty_msft = int(0.4 * portfolio_value / msft_prices.iloc[-1])

    positions = api.list_positions()
    long_aapl = any(pos.symbol == 'AAPL' and pos.side == 'long' for pos in positions)
    short_aapl = any(pos.symbol == 'AAPL' and pos.side == 'short' for pos in positions)
    long_msft = any(pos.symbol == 'MSFT' and pos.side == 'long' for pos in positions)
    short_msft = any(pos.symbol == 'MSFT' and pos.side == 'short' for pos in positions)

    if z_score > threshold:
        if not long_aapl and not short_msft:
            api.submit_order(symbol='AAPL', qty=qty_aapl, side='buy', type='market', time_in_force='gtc')
            api.submit_order(symbol='MSFT', qty=qty_msft, side='sell', type='market', time_in_force='gtc')
            sns.publish(TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message=f"Opened positions: Bought {qty_aapl} AAPL & sold {qty_msft} MSFT")

    elif z_score < -threshold:
        if not short_aapl and not long_msft:
            api.submit_order(symbol='AAPL', qty=qty_aapl, side='sell', type='market', time_in_force='gtc')
            api.submit_order(symbol='MSFT', qty=qty_msft, side='buy', type='market', time_in_force='gtc')
            sns.publish(TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message=f"Opened positions: Sold {qty_aapl} AAPL & bought {qty_msft} MSFT")

    elif abs(z_score) < 0.5:
        if long_aapl and short_msft:
            api.submit_order(symbol='AAPL', qty=qty_aapl, side='sell', type='market', time_in_force='gtc')
            api.submit_order(symbol='MSFT', qty=qty_msft, side='buy', type='market', time_in_force='gtc')
            sns.publish(TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message=f"Closed positions: Sold {qty_aapl} AAPL & bought {qty_msft} MSFT")
        elif short_aapl and long_msft:
            api.submit_order(symbol='AAPL', qty=qty_aapl, side='buy', type='market', time_in_force='gtc')
            api.submit_order(symbol='MSFT', qty=qty_msft, side='sell', type='market', time_in_force='gtc')
            sns.publish(TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message=f"Closed positions: Bought {qty_aapl} AAPL & sold {qty_msft} MSFT")
