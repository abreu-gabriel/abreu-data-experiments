import os
import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

url = f"https://api.stlouisfed.org/fred/series/observations?series_id=SP500&api_key={FRED_API_KEY}&file_type=json&observation_start=2019-01-01&observation_end=2025-12-31"

## series_id to explore
## DEXBZUS - Brazilian Real
## DFF - Federal Funds Effective Rate (daily)
# DGS10 - 10-Year Treasury Rate (daily)
# DGS2 - 2-Year Treasury Rate (daily)
# DGS30 - 30-Year Treasury Rate (daily)
# SP500 - S&P 500 Index
# DJIA - Dow Jones Industrial Average
# GOLDAMGBD228NLBM - Gold Price (daily) -- inverse relation with BRL


print(f"Fetching data...")

response = requests.get(url)

print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    
    if data.get('observations'):
        observations = data['observations']
        print(f"\nSuccess! Found {len(observations)} records")
        
        # Convert to DataFrame
        df = pd.DataFrame(observations)
        
        # Display first few rows
        print(f"\nFirst few records:")
        print(df.head())
        
        # Save to CSV
        df.to_csv('sp500_data_2025.csv', index=False)
        print(f"\nData saved to sp500_data_2025.csv")
    else:
        print("No observations found")
        print(data)
else:
    print(f"Error: {response.text}")
