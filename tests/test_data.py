from crypto_research_tools.data.fetcher import fetch_ohclv_data, fetch_tradable_symbols
from crypto_research_tools.constants import SUPPORTED_OHLCV_PROVIDERS, OHLCV_COLUMNS, SUPPORTED_SYMBOLS_PROVIDERS
import pandas as pd
from datetime import datetime
import timeit

def fetch_ohclv_data_test(test_symbol, test_time_frame, test_limit) -> bool:
    """
    Test the fetch_ohclv_data function
    
    Args:
        test_symbol: Trading pair symbol (e.g., 'BTCUSDT')
        test_time_frame: Candle interval (e.g., '1m', '5m', '1h', '1d')
        test_limit: Number of candles to fetch

    Returns:
        True if the function works
    """

    for provider in SUPPORTED_OHLCV_PROVIDERS:
        ohclv_data = fetch_ohclv_data(provider, test_symbol, test_time_frame, test_limit)

        if len(ohclv_data) != test_limit:
            raise ValueError(f"Expected {test_limit} rows, got {len(ohclv_data)}")
        
        if len(ohclv_data.columns) != 6:
            raise ValueError(f"Expected 6 columns, got {len(ohclv_data.columns)}")
        
        if not all(ohclv_data.columns == OHLCV_COLUMNS):
            raise ValueError(f"Column names don't match. Expected {OHLCV_COLUMNS}, got {list(ohclv_data.columns)}")
        
        if not pd.api.types.is_datetime64_any_dtype(ohclv_data['timestamp']):
            raise ValueError("Timestamp column is not datetime type")
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if not pd.api.types.is_float_dtype(ohclv_data[col]):
                raise ValueError(f"Column '{col}' is not float type")
        
        if ohclv_data.isnull().any().any():
            raise ValueError("DataFrame contains NaN/null values")
        
        if not (ohclv_data['high'] >= ohclv_data['open']).all():
            raise ValueError("Some 'high' values are less than 'open'")
        
        if not (ohclv_data['high'] >= ohclv_data['close']).all():
            raise ValueError("Some 'high' values are less than 'close'")
        
        if not (ohclv_data['low'] <= ohclv_data['open']).all():
            raise ValueError("Some 'low' values are greater than 'open'")
        
        if not (ohclv_data['low'] <= ohclv_data['close']).all():
            raise ValueError("Some 'low' values are greater than 'close'")
        
        if not (ohclv_data['high'] >= ohclv_data['low']).all():
            raise ValueError("Some 'high' values are less than 'low'")
        
        if not (ohclv_data['volume'] >= 0).all():
            raise ValueError("Some volume values are negative")
        
        if not ohclv_data['timestamp'].is_monotonic_increasing:
            raise ValueError("Timestamps are not in ascending order")
        
        if ohclv_data['timestamp'].duplicated().any():
            raise ValueError("Duplicate timestamps found")
        
        if (ohclv_data['timestamp'] > pd.Timestamp.now()).any():
            raise ValueError("Some timestamps are in the future")
        
        for col in ['open', 'high', 'low', 'close']:
            if not (ohclv_data[col] > 0).all():
                raise ValueError(f"Some '{col}' values are not positive")
        
        print(f"✓ All tests passed for provider '{provider}' with {test_limit} candles")
    
    return True

def test_sequence():
    print(f"Doing fetch_ohclv_data function test...")
    tic=timeit.default_timer()
    fetch_ohclv_data_test("BTCUSDT", "5m", 1345)
    toc=timeit.default_timer()
    print(f"In {(toc - tic)*1000} ms")
    
test_sequence()