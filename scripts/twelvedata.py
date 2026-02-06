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
    
def _is_credit_exhausted(payload):
    if not isinstance(payload, dict):
        return False
    if payload.get("status") != "error":
        return False
    msg = str(payload.get("message", "")).lower()
    return "credit" in msg or "credits" in msg or "quota" in msg or "limit" in msg

def td_get(url, params, sleep=0.8, max_retries=5, backoff_base=5, stop_on_daily_limit=True):
    """
    Generic GET wrapper:
    - adds aPI key to params
    - handles json parsing + basic error surfacing
    - sleeps a bit to respect free-tier rate limits
    """
    p = dict(params or {})
    p["apikey"] = API_KEY
    for attempt in range(max_retries + 1):
        if os.getenv("TD_STOP_FILE") and os.path.exists(os.getenv("TD_STOP_FILE")):
            raise RuntimeError("stop file detected; aborting request loop")
        r = requests.get(url, params=p, timeout=30)
        try:
            data = r.json()
        except Exception:
            # if json parsin fails, raise for satus to expose HTTP error then still return an empty dict
            r.raise_for_status()
            data = {}
        if isinstance(data, dict) and data.get("status") == "error" and data.get("code") == 429:
            msg = str(data.get("message", "")).lower()
            if "minute" in msg:
                # Minute-level credit limit: wait for the next minute window.
                wait_s = max(60, backoff_base * (attempt + 1))
                print(f"[rate_limit] minute limit hit, sleeping {wait_s}s before retry")
                time.sleep(wait_s)
                continue
            if stop_on_daily_limit and "day" in msg:
                # Daily limit won't clear within the run; return immediately.
                return data
            wait_s = backoff_base * (attempt + 1)
            print(f"[rate_limit] 429 received, sleeping {wait_s}s before retry")
            time.sleep(wait_s)
            continue
        if _is_credit_exhausted(data):
            raise RuntimeError(f"api credits exhausted: {data}")
        if sleep:
            time.sleep(sleep)
        return data
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

def fetch_time_series(symbols, start, end=None, interval="1day", sleep=0.8):
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
    data = td_get(BASE_TS, params, sleep=sleep)
    if isinstance(data, dict) and data.get("status") == "error":
        print(f"[time_series] api error: {data}")
        return pd.DataFrame()

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
        #raise RuntimeError("no time series returned for requested sysmbols.")
        # Return empty DataFrame so callers can decide how to handle no-data batches.
        return pd.DataFrame()
    
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


def fetch_ema(symbol, interval="1day", window=12, start=None):
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
def merge_on_datetime(base_df, extra_df):
    """
    Left-join an indicator DataFrame onto the base OHLCV panel
    on ['datetime', 'symbol'].
    """
    if base_df is None or extra_df is None:
        return base_df
    return pd.merge(base_df, extra_df, on=["datetime", "symbol"], how="left")

def load_symbols_from_csv(path, column="symbol"):
    """
    Load symbols from a CSV file (default column name: symbol).
    Drops blanks and de-duplicates while preserving order.
    """
    df = pd.read_csv(path)
    if column not in df.columns:
        raise ValueError(f"column '{column}' not found in {path}")
    symbols = df[column].astype(str).tolist()
    symbols = [s.strip() for s in symbols if s and s.strip()]
    return list(dict.fromkeys(symbols))

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

###############################
# main for testing
###############################
def main():
    check_key()

    print("Starting data fetch...")

    parser = argparse.ArgumentParser(description="Fetch enriched market data from Twelve Data")
    parser.add_argument("--symbols", type=str, default="NVDA")
    parser.add_argument("--symbols_csv", type=str, default="")
    parser.add_argument("--symbols_col", type=str, default="symbol")
    parser.add_argument("--start", type=str, default="2023-01-01")
    parser.add_argument("--interval", type=str, default="1day")
    parser.add_argument("--rsi", action="store_true")
    parser.add_argument("--macd", action="store_true")
    parser.add_argument("--sma", type=int, default=0)
    parser.add_argument("--ema", type=int, default=0)
    parser.add_argument("--out_panel", type=str, default="data/panel_enriched.csv")
    parser.add_argument("--batch_size", type=int, default=50)
    parser.add_argument("--sleep", type=float, default=8.0)
    parser.add_argument("--stop_file", type=str, default="")

    args = parser.parse_args()

    if args.stop_file:
        os.environ["TD_STOP_FILE"] = args.stop_file

    if args.symbols_csv:
        symbols = load_symbols_from_csv(args.symbols_csv, column=args.symbols_col)
    else:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    # 1) Fetch OHLCV (batch to avoid API limits)
    panels = []
    needs_indicators = any([args.rsi, args.macd, (args.sma and args.sma > 0), (args.ema and args.ema > 0)])
    wrote_header = False
    for batch in chunked(symbols, args.batch_size):
        if args.stop_file and os.path.exists(args.stop_file):
            raise RuntimeError("stop file detected; aborting batch fetch")
        batch_panel = fetch_time_series(batch, start=args.start, interval=args.interval, sleep=args.sleep)
        if batch_panel is None or batch_panel.empty:
            print(f"Fetched OHLCV batch ({len(batch)} symbols): no data")
            continue
        if needs_indicators:
            panels.append(batch_panel)
        else:
            # Save each batch immediately to avoid losing data on limits/interruption.
            out_dir = os.path.dirname(args.out_panel)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            mode = "w" if not wrote_header else "a"
            batch_panel.to_csv(args.out_panel, index=False, mode=mode, header=not wrote_header)
            wrote_header = True
        print(f"Fetched OHLCV batch ({len(batch)} symbols):", batch_panel.shape)
    if needs_indicators:
        if not panels:
            raise RuntimeError("no time series returned for any symbols.")
        panel = pd.concat(panels, ignore_index=True)
    else:
        if not wrote_header:
            raise RuntimeError("no time series returned for any symbols.")
        print(f"Saved {args.out_panel} with per-batch writes")
        print("Done!")
        return

    # 2) Fetch indicators + merge
    for sym in symbols:
        if args.rsi:
            rsi_df = fetch_rsi(sym, interval=args.interval, start=args.start)
            panel = merge_on_datetime(panel, rsi_df)

        if args.macd:
            macd_df = fetch_macd(sym, interval=args.interval, start=args.start)
            panel = merge_on_datetime(panel, macd_df)

        if args.sma and args.sma > 0:
            sma_df = fetch_sma(sym, interval=args.interval, window=args.sma, start=args.start)
            panel = merge_on_datetime(panel, sma_df)

        if args.ema and args.ema > 0:
            ema_df = fetch_ema(sym, interval=args.interval, window=args.ema, start=args.start)
            panel = merge_on_datetime(panel, ema_df)

    # 3) Save CSV (indicators path)
    panel = panel.sort_values(["symbol", "datetime"]).reset_index(drop=True)
    out_dir = os.path.dirname(args.out_panel)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    panel.to_csv(args.out_panel, index=False)

    print(f"Saved {args.out_panel} with shape={panel.shape}")
    print("Done!")

    
if __name__ == "__main__":
    main()
