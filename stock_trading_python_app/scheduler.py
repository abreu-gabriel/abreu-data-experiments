import schedule
import time
from script import run_stock_job

from datetime import datetime

def basic_job():
    print("Job starte at:", datetime.now())

# # Run every minute
# schedule.every().minute.do(basic_job)

# Run every minute
schedule.every().day.at("09:00").do(run_stock_job)

# # Run every minute
# schedule.every().minute.do(basic_job)

while True:
    schedule.run_pending()
    time.sleep(1)