from crypto_research_tools.constants import BINANCE_KLINE_API_URL, BINANCE_KLINE_MAX_LIMIT_REQUEST, SUPPORTED_OHLCV_PROVIDERS, OHLCV_COLUMNS, SUPPORTED_SYMBOLS_PROVIDERS, BINANCE_SYMBOL_API_URL

import pandas as pd
import requests
import time

def fetch_binance_raw(symbol: str, time_frame:str , limit:int) -> list[list]:
    """
    Fetch raw data from the binance kline api.
    
    Args:
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        time_frame: Candle interval (e.g., '1m', '5m', '1h', '1d')
        limit: Number of candles to fetch
    
    Returns:
        Raw data in a list
    """

    # Note: The binance klines api can only provide up to BINANCE_KLINE_MAX_LIMIT_REQUEST lines of ohclv data per request.
    # So we need to do multiple requests depending on the limit.

    num_requests = (limit + BINANCE_KLINE_MAX_LIMIT_REQUEST - 1) // BINANCE_KLINE_MAX_LIMIT_REQUEST

    all_data = []
    end_time = None
            
    for i in range(num_requests):
        current_limit = min(BINANCE_KLINE_MAX_LIMIT_REQUEST, limit - len(all_data))
                
        url = f"{BINANCE_KLINE_API_URL}?symbol={symbol}&interval={time_frame}&limit={current_limit}"
        if end_time:
            url += f"&endTime={end_time}"
                
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            raise ConnectionError(f"Binance request failed: {response.status_code}")
        
        data = response.json()
                
        if not data or not isinstance(data, list):
            break
                
        all_data = data + all_data
        end_time = int(data[0][0]) - 1
                
        if i < num_requests - 1:
            time.sleep(0.1)
                
        if len(all_data) >= limit:
            break
            
    all_data = all_data[-limit:]
    return all_data

def fetch_ohclv_data(provider: str, symbol: str, time_frame: str, limit: int) -> pd.DataFrame:
    """
    Fetch OHLCV data from the provider.
    
    Args:
        provider: For the moment there is only "binance"
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        time_frame: Candle interval (e.g., '1m', '5m', '1h', '1d')
        limit: Number of candles to fetch
    
    Returns:
        DataFrame with columns: timestamp, open, high, low, close, volume
    """

    if provider not in SUPPORTED_OHLCV_PROVIDERS:
        raise ValueError(f"The provider you used is not supported / does not exist.\n Current supported provider list: {SUPPORTED_OHLCV_PROVIDERS}")

    if provider == "binance":
        data = fetch_binance_raw(symbol, time_frame, limit)

        # Binance returns 12 columns, we only need the first 6 for OHLCV
        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
        ])
        
        # Select only OHLCV columns
        df = df[OHLCV_COLUMNS]
        
        # Convert data types
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
        
        return df
    

def fetch_tradable_symbols(provider) -> list:

    """
    Fetch all tradable symbols from the provider.
    
    Args:
        provider: For the moment there is only "binance"
    
    Returns:
        List with all the symbols
    """

    if provider not in SUPPORTED_SYMBOLS_PROVIDERS:
        raise ValueError(f"The provider you used is not supported / does not exist.\n Current supported provider list: {SUPPORTED_SYMBOLS_PROVIDERS}")
    
    response = requests.get(BINANCE_SYMBOL_API_URL)
    data = response.json()
    
    symbols = [symbol['symbol'] for symbol in data['symbols'] if symbol['status'] == 'TRADING']
    
    return symbols