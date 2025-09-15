import os, time, argparse, math
from dotenv import load_dotenv
import requests
import pandas as pd

###############################
# config and helpers
###############################

load_dotenv()
API_KEY = os.getenv("TD_API_KEY")
BASE_TS = "https://api.twelvedata.com/time_series" # time series endpoint

# momentum/trend features
BASE_RSI = "https://api.twelvedata.com/rsi"  # relative strength index endpoint
BASE_MACD = "https://api.twelvedata.com/macd" # moving average convergence divergence endpoint

# long vs short term trend features
BASE_SMA = "https://api.twelvedata.com/sma" # simple moving average endpoint
BASE_EMA = "https://api.twelvedata.com/ema" # exponential moving average endpoint

BASE_QUOTE = "https://api.twelvedata.com/quote" #snapshot fundamental (valuation rations, highs/lows)
BASE_STOCKS = "https://api.twelvedata.com/stocks" # ticker discovery for batch fetching

def check_key():
    """ check if API key is set """
    if not API_KEY:
        raise RuntimeError("API key not set. Please set TD_API_KEY in .env file.")
    
def td_get(url, params, sleep=0.8):
    """
    Generic GET wrapper:
    - adds aPI key to params
    - handles json parsing + basic error surfacing
    - sleeps a bit to respect free-tier rate limits
    """
    p = dict(params or {})
    p["apikey"] = API_KEY
    r = requests.get(url, params=p, timeout=30)
    try:
        data = r.json()
    except Exception:
        # if json parsin fails, raise for satus to expose HTTP error then still return an empty dict
        r.raise_for_status()
        data ={}
    if sleep:
        time.sleep(sleep)
    return data

def cast_ohlcv(df):
    """
    normalize and cast standard OHLCV columsn to numeric types.
    also ensure "datetime" is a proper datetime type
    """
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"])
    return df

##############################
# Core fetchers
##############################

def fetch_time_series(symbols, start, end=None, interval="1day"):
    """
    batch fetch OHLCV time series for oe or multiple symblos. 
    
    - if symbols is a list/tuple, join with commas to use twelve data's batch format: symbol=NVDA,AAPL,MSFT
    - the api will return either:
        - a single block with values  (when you pass oe valid symbol) or 
        - a dict keyed by symbol -> blo
    """
    if isinstance(symbols, (list, tuple)):
        symbol_param = ",".join(symbols)
    else:
        symbol_param = str(symbols)

    # parameters for time_series
    params = {
        "symbol": symbol_param,
        "interval": interval,
        "start_date": start,
        "order": "ASC",
        "outputsize": 5000 # max rows; adjust for longer histories if plan allows
    }

    if end:
        params["end_date"] = end

    #send request
    data = td_get(BASE_TS, params)

    frames = [] 
    # case 1: api collapses rto single symbol response with values
    if isinstance(data, dict) and "values" in data:
        df = pd.DataFrame(data["values"])
        df = cast_ohlcv(df)
        df["symbol"] = symbol_param
        frames.append(df)

    # case 2: multi-symbol resbonse -> {"NVDA": {...}, "AAPL": {...}, ...}
    elif isinstance(data, dict):
        for sym in (symbols if isinstance(symbols, (list,tuple)) else [symbols]):
            block = data.get(sym) # get block for this symbol
            if not block or "values" not in block:
                # could be missing or en error payload for that symbol
                err = data.get(sym, {})
                print(f"[time_series] skipping {sym}: {err if err else 'no values'}")
                continue
            df = pd.DataFrame(block["values"])
            df = cast_ohlcv(df)
            df["symbol"] = sym
            frames.append(df)
    else:
        # unexpected structure - surface it
        raise RuntimeError(f"unexpected response: {data}")  
    
    if not frames:
        raise RuntimeError("no time series returned for requested sysmbols.")
    
    # concatenate and sort by time/symbol for clean panel
    panel = pd.concat(frames, ignore_index=True).sort_values(["datetime", "symbol"])
    return panel    

def fetch_quote(symbol):
    """
    fetch a single quote snapshot for one symbol (non time-indexed)
    useful for metadata and urrent price metrics (veries by plan)
    """
    data = td_get(BASE_QUOTE, {"symbol": symbol}, sleep=0.4)
    if "symbol" not in data:
        # if the api returns an error payload, surface and skip
        print(f"[quote] {symbol}: {data}]")
        return None
    return pd.DataFrame([data])

def fetch_rsi(symbol, interval="1day", period=14, start=None):
    """
    fetch relative strength index (rsi) time series for a symbol; returns datetime-aigned df
    default period is 14 (common setting)
    """
    params = {
        "symbol": symbol,
        "interval": interval,
        "time_period": period,
        "outputsize": 5000
    }
    if start:
        params["start_date"] = start
    data = td_get(BASE_RSI, params)
    if "values" not in data:
        print(f"[rsi] {symbol}: {data}")
        return None
    df = pd.DataFrame(data["values"])
    df.rename(columns={"rsi": "rsi"}, inplace=True)
    df["symbol"] = symbol
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["rsi"] = pd.to_numeric(df["rsi"], errors="coerce")
    return df[["datetime", "symbol", "rsi"]]

def fetch_macd(symbol, interval="1day", fast=12, slow=26, signal=9, start=None):
    """
    fetch moving average convergence divergence (macd) time series for a symbol
    default params are common settings: fast=12, slow=26, signal=9
    returns datetime-aligned df with macd, macd_signal, macd_hist columns
    """
    params ={
        "symbol": symbol,
        "interval": interval,
        "series_type": "close",
        "fastperiod": fast,
        "slowperiod": slow,
        "signalperiod": signal,
        "outputsize": 5000
    }
    if start:
        params["start_date"] = start
    data = td_get(BASE_MACD, params)
    if "values" not in data:
        print(f"[macd] {symbol}: {data}")
        return None
    df = pd.DataFrame(data["values"])
    df["symbol"] = symbol
    df["datetime"] = pd.to_datetime(df["datetime"])
    for c in ["macd", "macd_signal", "macd_hist"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["datetime", "symbol", "macd", "macd_signal", "macd_hist"]]

def fetch_sma(symbol, interval="1day", window=20, start=None):
    """
    fetch simple moving average (sma) time series and rename it ro sma_{window}
    default window is 20 (common setting) - because ~20 trading days in a q trading month
    for quartes it is 50, and for year it is 200
    returns datetime-aligned df with sma_{window} column
    """
    params = {
        "symbol": symbol,
        "interval": interval,
        "time_period": window,
        "outputsize": 5000
    }
    if start:
        params["start_date"] = start
    data = td_get(BASE_SMA, params)
    if "values" not in data:
        print(f"[sma] {symbol}: {data}")
        return None
    df = pd.DataFrame(data["values"])
    df.rename(columns={"sma": f"sma_{window}"}, inplace=True) # unique column name
    df["symbol"] = symbol
    df["datetime"] = pd.to_datetime(df["datetime"])
    df[f"sma_{window}"] = pd.to_numeric(df[f"sma_{window}"], errors="coerce")
    return df[["datetime", "symbol", f"sma_{window}"]]  


def fetc_ema(symbol, interval="1day", window=12, start=None):
    """
    fetch exponential moving average (ema) time series and rename it to ema_{window}
    default window is 12 (common setting)
    returns datetime-aligned df with ema_{window} column
    """
    params = {
        "symbol": symbol,
        "interval": interval,
        "time_period": window,
        "outputsize": 5000
    }
    if start: params["start_date"] = start
    data = td_get(BASE_EMA, params)
    if "values" not in data:
        print(f"[ema] {symbol}: {data}")
        return None
    df = pd.DataFrame(data["values"])
    df.rename(columns={"ema": f"ema_{window}"}, inplace=True) # unique column name
    df["symbol"] = symbol
    df["datetime"] = pd.to_datetime(df["datetime"])
    df[f"ema_{window}"] = pd.to_numeric(df[f"ema_{window}"], errors="coerce")
    return df[["datetime", "symbol", f"ema_{window}"]]

###############################
# discovery
###############################

def list_symbols(exchange=None, country=None, search=None, head=20):
    """
    list available tickers by exchange/country or filter by a search string
    prints a preview and returns a DataFrame of results
    """
    params={}
    if exchange: params["exchange"] = exchange
    if country: params["country"] = country
    if search: params["search"] = search
    data = td_get(BASE_STOCKS, params, sleep=0)
    rows = data.get("data", [])
    df = pd.DataFrame(rows)
    print(df.head(head))
    return df

###############################
# merge util
###############################    