import json
import pandas as pd
from alpaca_trade_api import REST
import config
from pykalman import KalmanFilter

# Create an Alpaca API instance
api = REST(config.API_KEY, config.SECRET_KEY, base_url=config.BASE_URL)

# Create a DataFrame to hold our data
df = pd.DataFrame(columns=['MSFT', 'AAPL'])

# In your on_message function

if 'MSFT' in df.columns and 'AAPL' in df.columns and not df.empty:
    # Drop any rows with missing values
    df.dropna(inplace=True)

    # Only run the Kalman Filter if we have enough data
    if len(df) > 1:
        # Update Kalman filter with latest data
        kf = KalmanFilter(initial_state_mean=[0,0], n_dim_obs=2)
        states_pred = kf.em(df.values).smooth(df.values)[0]
        hedge_ratios = -states_pred[:, 0] / states_pred[:, 1]

        # Get the latest hedge ratio
        latest_hedge_ratio = hedge_ratios[-1]
        print(f'Hedge Ratio calculated: {latest_hedge_ratio}')




def on_message(ws, message):
    """
    get message
    """
    print("received a message")
    print(message)
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

    if 'MSFT' in df.columns and 'AAPL' in df.columns:
        # Drop any rows with missing values
        df.dropna(inplace=True)

        # Update Kalman filter with latest data
        state_means, _ = kf.filter(df[['MSFT', 'AAPL']].values)

        # Get latest hedge ratio
        hedge_ratio = - state_means[-1, 0] / state_means[-1, 1]
        print(f'Hedge Ratio calculated: {hedge_ratio}')

        # If hedge_ratio > 1, short pair
        if hedge_ratio > 1:
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
                qty=int(hedge_ratio),
                side='buy',
                type='market',
                time_in_force='gtc',
            )
            print(f"Submitted buy order for AAPL with quantity: {int(hedge_ratio)}")

        # If hedge_ratio < -1, long pair
        elif hedge_ratio < -1:
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
                qty=int(-hedge_ratio),
                side='sell',
                type='market',
                time_in_force='gtc',
            )
            print(f"Submitted sell order for AAPL with quantity: {int(-hedge_ratio)}")

