import pandas as pd
import numpy as np

def compute_logarithmic_price(df: pd.DataFrame, price_col: str = "close", out_col: str | None = None) -> pd.DataFrame:
    """
    Compute the logarithmic price.

    Args:
        df: The OHLCV data frame
        price_col: The name of the column that will be taken as the price
        out_col: The name of the columnt that will be added to the dataframe
    
    Returns:
        the data frame with a new log_{price_col} column
    """

    if out_col is None:
        out_col = f"log_{price_col}"

    df = df.copy()

    if (df[price_col] <= 0).any():
        raise ValueError("Price column contains non-positive values")

    df[out_col] = np.log(df[price_col])
    return df

def compute_log_returns(df: pd.DataFrame, price_col: str = "close", out_col: str | None = None) -> pd.DataFrame:
    """
    Compute log returns from a price column.

    Args:
        df: The OHLCV data frame
        price_col: The name of the column that will be taken as the price
        out_col: The name of the columnt that will be added to the dataframe
    
    Returns:
        the data frame with a new log_return_{price_col} column
    """

    if out_col is None:
        out_col = f"log_return_{price_col}"

    df = df.copy()
    df[out_col] = np.log(df[price_col]).diff()
    return df