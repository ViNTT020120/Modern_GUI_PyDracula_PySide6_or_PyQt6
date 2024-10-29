import pandas as pd
import aiohttp


async def get_futures():
    payload = {'tickers': ['VNC1 Index', 'VNC2 Index'],
               'fields': ['LAST_TRADEABLE_DT', 'FUT_ACT_DAYS_EXP', 'PX_BID', 'PX_ASK']}
    #

    async with aiohttp.ClientSession() as session:
        async with session.get('http://172.25.4.3:5555/getField', params=payload) as r:
            data = await r.json()

    _result = format_ticker_data(data)

    return _result


def format_ticker_data(data):
    # Dictionary to hold the result
    _result = {}

    # Function to extract VN30FYYMM format
    def extract_vn30f_format(item):
        # Parse the date
        date = pd.to_datetime(item['LAST_TRADEABLE_DT'])
        _time_to_maturity = item['FUT_ACT_DAYS_EXP']
        _bid = item['PX_BID']
        _ask = item['PX_ASK']
        # Extract year and month in the required format (YYMM)
        year = date.year % 100  # Get last two digits of the year
        month = f'{date.month:02d}'  # Zero-padded month
        # Construct the ticker string
        _formatted_ticker = f'VN30F{year}{month}'
        # Return the formatted ticker and date
        return _formatted_ticker, _time_to_maturity, _bid, _ask

    # Loop through each item and process it
    for item in data:
        # Get the ticker key (vnc1, vnc2, etc.) by splitting the ticker value
        key = item['ticker'].split()[0].lower()  # 'VNC1 Index' becomes 'vnc1'
        # Get the formatted ticker and the expiring date
        formatted_ticker, time_to_maturity, bid, ask = extract_vn30f_format(item)
        # Add to result dictionary
        _result[key] = (formatted_ticker, time_to_maturity, bid, ask)

    return _result


def parse_futures_bid_ask(data):
    parts = data.split('|')
    ticker = parts[0].split('#')[1]

    # Prepare bid and ask pairs
    bids = [(parts[i], parts[i + 1]) for i in range(1, 21, 2) if parts[i]]  # 10 levels of bids
    asks = [(parts[i], parts[i + 1]) for i in range(21, 41, 2) if parts[i]]  # 10 levels of asks

    return {'ticker': ticker, 'bids': bids, 'asks': asks}


def parse_futures_trade(data):
    parts = data.split('|')
    ticker = parts[0].split('#')[1]
    trade = parts[45]  # Trade price located after the empty fields
    return {'ticker': ticker, 'trade': trade}


def parse_stock_tick(data):
    parts = data.split('|')
    ticker = parts[0].split('#')[1]
    bids = [(parts[i], parts[i + 1]) for i in range(1, 6, 2) if parts[i]]
    asks = [(parts[i], parts[i + 1]) for i in range(21, 26, 2) if parts[i]]
    trade = parts[41]
    return {'ticker': ticker, 'bids': bids, 'asks': asks, 'trade': trade}


if __name__ == '__main__':
    import asyncio

    result = asyncio.run(get_futures())
    print(result)
