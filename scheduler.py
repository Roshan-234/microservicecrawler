import asyncio
import logging
from datetime import datetime, time
import os
from final_mero_lagani import run_scraper as merolagani_run
from db_module import assign_notify_time, insert_news_to_db
from sharesansar import run_scraper as sharesansar_run
import aiohttp
import ssl

# Ensure logs directory exists
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
log_file_path = os.path.join(logs_dir, 'schedule.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

def initialize_logging():
    # Check if log file exists; if not, create it
    if not os.path.exists(log_file_path):
        with open(log_file_path, 'w') as file:
            file.write(f"Log file created at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

def log_message(message):
    # Append a message to the log file
    with open(log_file_path, 'a') as file:
        file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

async def run_all_scrapers():
    try:
        current_time = datetime.now()
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        log_message(f"Starting scrapers at {current_time_str}")
        
        merolagani_data = await merolagani_run()
        sharesansar_data = await sharesansar_run()

        insert_news_to_db(merolagani_data, current_time)
        log_message(f"News inserted Successfully at {current_time_str}")

        assign_notify_time(current_time)
        log_message(f"Notification Scheduled Successfully at {current_time_str}")
        # Create an SSL context to ignore certificate verification
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Call the API to schedule notifications
        async with aiohttp.ClientSession() as session:
            url = 'https://hamrolagani.com/api/news/schedule-notification'
            async with session.post(url, ssl=ssl_context) as response:
                if response.status == 200:
                    print("API call successful")
                else:
                    print(f"API call failed with status {response.status}")

    except Exception as e:
        log_message(f"Error running scrapers: {e}")

async def scheduler():
    while True:
        now = datetime.now().time()
        if now.replace(second=0, microsecond=0) in [time(11, 45), time(17, 29)]:
            await run_all_scrapers()
        await asyncio.sleep(60)  # Check every minute

async def start_scheduler():
    initialize_logging()  # Ensure logging is initialized
    logger.info("Starting News scheduler...")
    await scheduler()

if __name__ == "__main__":
    # If this script is run directly, start the scheduler
    asyncio.run(start_scheduler())
