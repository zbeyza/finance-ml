import os
from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

API_KEY = os.getenv("TD_API_KEY")
print("MY API key is: ", API_KEY)
# twelwe data API endpoint
url = "https://api.twelvedata.com/time_series"

params = {
    "symbol": "NVDA",
    "interval": "1day",
    "start_date": "2023-01-01",
    "apikey": API_KEY,
    "order": "ASC",
}

response = requests.get(url, params=params)
data = response.json()

if "values" in data:
    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.astype({"open": float, 
                    "high": float, 
                    "low": float, 
                    "close": float, 
                    "volume": int})
    print(df.head())
    df.to_csv("nvidia.csv", index=False)
    print("Data saved to csv")
else:
    print("Error:", data)

