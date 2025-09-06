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
    
