import os
import sys
import logging
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
from pyppeteer import launch
import asyncio

from db_module import announcement_data, fetch_stock_symbols

# Ensure logs directory exists
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
log_file_path = os.path.join(logs_dir, 'announcements.log')
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

# Load environment variables
load_dotenv()

# Main scraping and processing function
async def scrape_and_process_announcements(stock_symbols):
    for stock in stock_symbols:
        STOCK_NAME = stock
        URL = f"https://www.sharesansar.com/company/{STOCK_NAME}"
        log_message(f"Processing stock: {STOCK_NAME}")

        try:
            browser = await launch(headless=True, executablePath='/usr/bin/chromium', args=['--no-sandbox', '--disable-gpu', '--disable-software-rasterizer'])
            page = await browser.newPage()
            await page.goto(URL)
            log_message(f"Opened URL: {URL}")
            await asyncio.sleep(2)

            announcements_button = await page.waitForXPath('//*[@id="btn_cannouncements"]', {'timeout': 60000})
            await page.evaluate('(element) => element.scrollIntoView()', announcements_button)

            await announcements_button.click()
            await asyncio.sleep(5)

            await page.waitForXPath('//*[@id="myTableCAnnouncements"]/tbody', {'timeout': 60000})
            await asyncio.sleep(10)

            headers = await page.xpath('//*[@id="myTableCAnnouncements"]/thead/tr/th')
            header_list = [await page.evaluate('(element) => element.textContent', header) for header in headers]

            rows = await page.xpath('//*[@id="myTableCAnnouncements"]/tbody/tr')
            data_list = []
            for row in rows:
                cells = await row.xpath('td')
                cell_data = [await page.evaluate('(element) => element.textContent', cell) for cell in cells]
                data_list.append(cell_data)

            announcement_data(data_list, STOCK_NAME)
        
        except KeyboardInterrupt:
            log_message("\nCtrl+C detected. Saving data and exiting...")
            await browser.close()
            sys.exit(0)

        except Exception as e:
            log_message(f"An error occurred while processing {STOCK_NAME}: {e}")

        finally:
            await browser.close()


# Main function
async def main():
    log_message(f"Announcements extraction Started....")
    stock_symbols = fetch_stock_symbols()
    asyncio.get_event_loop().run_until_complete(await scrape_and_process_announcements(stock_symbols))
    log_message(f"Announcements extraction completed for {len(stock_symbols)} stocks")

if __name__ == "__main__":
    # If this script is run directly, start the scheduler
    asyncio.run(main())
