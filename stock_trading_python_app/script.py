import os
import requests
import csv
import snowflake.connector  # Add this line
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

POLYGON_API_KEY = os.getenv("POLIGON_API_KEY")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

LIMIT = 1000
DS = '2025-12-06'

def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def create_table_if_not_exists(cursor):
    """Create the stock_tickers table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS STOCK_TICKERS (
        TICKER VARCHAR(50),
        NAME VARCHAR(500),
        MARKET VARCHAR(50),
        LOCALE VARCHAR(10),
        PRIMARY_EXCHANGE VARCHAR(50),
        TYPE VARCHAR(50),
        ACTIVE BOOLEAN,
        CURRENCY_NAME VARCHAR(10),
        CIK VARCHAR(50),
        COMPOSITE_FIGI VARCHAR(50),
        SHARE_CLASS_FIGI VARCHAR(50),
        LAST_UPDATED_UTC TIMESTAMP_NTZ,
        DS VARCHAR(50)
    )
    """
    cursor.execute(create_table_sql)
    print("Table created or already exists")

def insert_tickers_to_snowflake(tickers):
    """Insert tickers data into Snowflake table"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # Create table if it doesn't exist
        create_table_if_not_exists(cursor)
        
        # Prepare insert statement
        insert_sql = """
        INSERT INTO STOCK_TICKERS (
            TICKER, NAME, MARKET, LOCALE, PRIMARY_EXCHANGE, 
            TYPE, ACTIVE, CURRENCY_NAME, CIK, COMPOSITE_FIGI, 
            SHARE_CLASS_FIGI, LAST_UPDATED_UTC
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Prepare data for bulk insert
        rows = []
        for ticker in tickers:
            row = (
                ticker.get('ticker', ''),
                ticker.get('name', ''),
                ticker.get('market', ''),
                ticker.get('locale', ''),
                ticker.get('primary_exchange', ''),
                ticker.get('type', ''),
                ticker.get('active', False),
                ticker.get('currency_name', ''),
                ticker.get('cik', ''),
                ticker.get('composite_figi', ''),
                ticker.get('share_class_figi', ''),
                ticker.get('last_updated_utc', None)
            )
            rows.append(row)
        
        # Execute bulk insert
        cursor.executemany(insert_sql, rows)
        conn.commit()
        
        print(f'Successfully inserted {len(tickers)} rows into Snowflake STOCK_TICKERS table')
        
    except Exception as e:
        print(f'Error inserting data into Snowflake: {e}')
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    tickers = []

    data = response.json()
    for ticker in data['results']:
        tickers.append(ticker)

    # Insert data into Snowflake
    insert_tickers_to_snowflake(tickers)

if __name__ == "__main__":
    run_stock_job()



	# commenting out pagination for now to avoid API maximum requests per minute ERROR

	# while 'next_url' in data:
	#     print('requesting next page', data['next_url'])
	#     response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
	#     data = response.json()
	#     print(data)
	#     for ticker in data['results']:
	#         tickers.append(ticker)

	