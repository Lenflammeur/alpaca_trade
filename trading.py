"""
script for the trading strategy
"""
import pandas as pd
import boto3

sns = boto3.client('sns', region_name='us-east-1')
response = sns.publish(
    TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message="Connected to SNS"
)

lookback = 120
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
        if lookback % 10 == 0:
             response = sns.publish(
                TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message=f"We have {lookback} lookback"
            )
        return
    
    if len(df) == lookback:
            #print("We have enough data for long moving average")
            response = sns.publish(
                TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message="We have enough data for pair trading strategy"
            )

    aapl_prices = df[df['symbol'] == 'AAPL']['price']
    msft_prices = df[df['symbol'] == 'MSFT']['price']

    spread = aapl_prices - msft_prices
    mean_spread = spread.rolling(window=lookback).mean()
    std_spread = spread.rolling(window=lookback).std()
    z_score = (spread - mean_spread) / std_spread

    portfolio_value = api.get_account().cash

    qty_aapl = int(0.5 * portfolio_value / aapl_prices.iloc[-1])
    qty_msft = int(0.5 * portfolio_value / msft_prices.iloc[-1])

    if z_score[-1] > threshold:
        api.submit_order(
            symbol='AAPL',
            qty=qty_aapl,
            side='sell',
            type='market',
            time_in_force='gtc',
        )
        api.submit_order(
            symbol='MSFT',
            qty=qty_msft,
            side='buy',
            type='market',
            time_in_force='gtc',
        )
        # After submitting the order, publish a message to SNS
        response = sns.publish(
            TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message=f"Submitted {qty_aapl} sell for AAPL & {qty_msft} buy for MSFT"
        )
    elif z_score[-1] < -threshold:
        api.submit_order(
            symbol='AAPL',
            qty=qty_aapl,
            side='buy',
            type='market',
            time_in_force='gtc',
        )
        api.submit_order(
            symbol='MSFT',
            qty=qty_msft,
            side='sell',
            type='market',
            time_in_force='gtc',
        )
        response = sns.publish(
            TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message=f"Submitted {qty_aapl} buy for AAPL & {qty_msft} sell for MSFT"
        )
    elif abs(z_score[-1]) < 0.5:
        api.submit_order(
            symbol='AAPL',
            qty=qty_aapl,
            side='sell',
            type='market',
            time_in_force='gtc',
        )
        api.submit_order(
            symbol='MSFT',
            qty=qty_msft,
            side='sell',
            type='market',
            time_in_force='gtc',
        )
        response = sns.publish(
            TopicArn='arn:aws:sns:us-east-1:429690615505:MyOrders', Message=f"Submitted {qty_aapl} sell for AAPL & {qty_msft} sell for MSFT"
        )
