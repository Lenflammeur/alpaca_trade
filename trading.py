"""
script for the trading strategy
"""
import pandas as pd
import boto3

sns = boto3.client('sns')

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

def check_crossover(df, bar, api):
    """
    Simple Moving Average strategy
    """
    short_avg = df[df['symbol'] == bar['S']]['price'].rolling(window=5).mean()
    long_avg = df[df['symbol'] == bar['S']]['price'].rolling(window=20).mean()

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
            # After submitting the order, publish a message to SNS
            response = sns.publish(
                TopicArn='arn:aws:sns:us-east-1:123456789012:MyTopic',    # TODO: replace with your actual ARN
                Message=f"Submitted buy order for {bar['S']}",    # TODO: replace with your actual message
            )

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
            # After submitting the order, publish a message to SNS
            response = sns.publish(
                TopicArn='arn:aws:sns:us-east-1:123456789012:MyTopic',    # TODO: replace with your actual ARN
                Message=f"Submitted sell order for {bar['S']}",    # TODO: replace with your actual message
            )
