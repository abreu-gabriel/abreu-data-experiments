import os
import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

## series_id to explore
## 1. DEXBZUS - Brazilian Real
## 2. DFF - Federal Funds Effective Rate (daily)
# 2.1 DGS10 - 10-Year Treasury Rate (daily)
# 2.2 DGS30 - 30-Year Treasury Rate (daily)
## 3. CPIAUCNS - Consumer Price Index (monthly, not seasonally adjusted)

SERIES = {
    # "DFF": {"filename": "fed_funds_rate.csv", "start": "2019-01-01", "end": "2025-12-31", "frequency": None},
    # "DGS10": {"filename": "treasury_10y_rate.csv", "start": "2019-01-01", "end": "2025-12-31", "frequency": None},
    # "DGS30": {"filename": "treasury_30y_rate.csv", "start": "2019-01-01", "end": "2025-12-31", "frequency": None},
    "CPIAUCNS": {"filename": "us_cpi.csv", "start": "2000-01-01", "end": "2024-12-31", "frequency": "m"},
}

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

def fetch_fred_series(series_id, filename, start, end, frequency=None):
    params = (
        f"?series_id={series_id}"
        f"&api_key={FRED_API_KEY}"
        f"&file_type=json"
        f"&observation_start={start}"
        f"&observation_end={end}"
    )

    # frequency param forces aggregation to monthly when needed
    if frequency:
        params += f"&frequency={frequency}"

    url = BASE_URL + params

    print(f"\nFetching {series_id}...")

    response = requests.get(url)
    print(f"Status Code: {response.status_code}")

    if response.status_code != 200:
        print(f"Error fetching {series_id}: {response.text}")
        return

    data = response.json()

    if not data.get("observations"):
        print(f"No observations found for {series_id}")
        return

    df = pd.DataFrame(data["observations"])
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df.dropna(subset=["value"], inplace=True)

    df.to_csv(filename, index=False)
    print(f"Saved {series_id} → {filename} ({len(df)} rows)")

for series_id, config in SERIES.items():
    fetch_fred_series(
        series_id,
        config["filename"],
        config["start"],
        config["end"],
        config["frequency"]
    )
