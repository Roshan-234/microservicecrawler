import logging
from datetime import datetime
import os
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import asyncio
from pyppeteer import launch

from db_module import insert_EPS_data

# Load environment variables from .env file
load_dotenv()

# URL of the site to scrape
url = 'https://chukul.com/stock-filter'

# Ensure logs directory exists
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
log_file_path = os.path.join(logs_dir, 'EPS.log')
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

async def scrape_data():
    log_message("Starting the web scraping process...")
    # Set up Puppeteer
    browser = await launch(headless=True, executablePath='/usr/bin/chromium', args=['--no-sandbox', '--disable-gpu', '--disable-software-rasterizer'])
    page = await browser.newPage()
    
    # Set user agent
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    await page.goto(url)
    
    log_message("Opened the URL in the browser.")

    try:
        # Wait for the table headers to load
        await page.waitForSelector('table thead tr', timeout=60000)
        log_message("Table headers loaded.")
        
        # Wait for the table rows to load
        await page.waitForSelector('table tbody tr', timeout=60000)
        log_message("Table rows loaded.")

        # Interact with dropdown
        dropdown = await page.waitForXPath('//*[@id="q-app"]/div/div[1]/div/div[2]/main/div[2]/div/div/div[3]/div[2]/label/div/div/div[2]/i', timeout=30000)
        await page.evaluate('(element) => element.scrollIntoView()', dropdown)
        await asyncio.sleep(5)
        await dropdown.click()
        log_message("Clicked the dropdown.")

        # Wait for network idle
        await asyncio.sleep(1)

        # Select the last item in the dropdown
        dropdown_values = await page.querySelectorAll('.q-virtual-scroll__content .q-item__label')
        await dropdown_values[-1].click()
        log_message("Selected the last item in the dropdown.")

        # Extract data from the table
        rows = await page.querySelectorAll('table tbody tr')
        extracted_data = []
        for row in rows:
            cells = await row.querySelectorAll('td')
            row_data = []
            for cell in cells:
                cell_text = await page.evaluate('(element) => element.textContent', cell)
                row_data.append(cell_text)
            extracted_data.append(row_data)
        log_message("Extracted data from the table.")

        # Extract headers from the table
        headers = await page.querySelectorAll('table thead tr th')
        extracted_header = []
        for header in headers:
            header_text = await page.evaluate('(element) => element.textContent', header)
            header_text = header_text.replace('arrow_upward', "")
            extracted_header.append(header_text)
    finally:
        # Close the browser
        await browser.close()
        log_message("Closed the browser.")

    # Filter data
    filtering_columns = ["Symbol", "EPS", "P/E Ratio"]
    filtering_indices = [extracted_header.index(col) for col in filtering_columns]

    final_data = []
    for row in extracted_data:
        final_row = [row[idx] for idx in filtering_indices]
        final_data.append(final_row)

    return final_data

async def eps_scraper():
    data = await scrape_data()
    insert_EPS_data(data)

def main():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # If the loop is already running, create a task
        loop.create_task(eps_scraper())
    else:
        # If no loop is running, run the loop
        asyncio.run(eps_scraper())

if __name__ == "__main__":
    main()
