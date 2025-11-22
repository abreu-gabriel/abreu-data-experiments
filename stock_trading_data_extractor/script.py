import os
import requests
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLIGON_API_KEY")

LIMIT = 1000

url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
tickers = []

data = response.json()
for ticker in data['results']:
    tickers.append(ticker)

# commenting out pagination for now to avoid API maximum requests per minute ERROR

# while 'next_url' in data:
#     print('requesting next page', data['next_url'])
#     response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
#     data = response.json()
#     print(data)
#     for ticker in data['results']:
#         tickers.append(ticker)

# Define CSV columns based on example_ticker schema
example_ticker = {'ticker': 'HIT', 
	'name': 'Health In Tech, Inc. Class A Common Stock', 
	'market': 'stocks', 
	'locale': 'us', 
	'primary_exchange': 'XNAS', 
	'type': 'CS', 
	'active': True, 
	'currency_name': 'usd', 
	'cik': '0002019505', 
	'composite_figi': 'BBG01PK1D0N8', 
	'share_class_figi': 'BBG01PK1D1P4', 
	'last_updated_utc': '2025-11-22T07:06:33.007668375Z'}

# Write tickers to CSV with example_ticker schema
fieldnames = list(example_ticker.keys())
output_csv = 'tickers.csv'
with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for t in tickers:
        row = {key: t.get(key, '') for key in fieldnames}
        writer.writerow(row)
print(f'Wrote {len(tickers)} rows to {output_csv}')


# print(len(tickers))