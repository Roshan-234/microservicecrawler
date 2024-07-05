from datetime import datetime, timedelta
import asyncio
import os
from pyppeteer import launch
from fake_useragent import UserAgent
import logging

import asyncio
from db_module import insert_floorsheet_data, fetch_stock_symbols

URL = "https://www.sharesansar.com/floorsheet"
number_of_days = 30

# Ensure logs directory exists
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
log_file_path = os.path.join(logs_dir, 'floorsheet.log')
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

async def scrape_floorsheet(stock_name, number_of_days):
    user_agent = UserAgent()
    browser = None
    try:
        # Launch the browser
        log_message(f"Launching browser for stock: {stock_name}")
        browser = await launch(
            headless=True,
            executablePath='/usr/bin/chromium',
            args=['--no-sandbox', '--disable-gpu', '--disable-software-rasterizer']
        )
        page = await browser.newPage()
        await page.setViewport({"width": 1080, "height": 720})  # Full screen resolution
        await asyncio.sleep(5)
        
        # Navigate to URL
        await page.goto(URL)
        await asyncio.sleep(5)

        # Select stock
        log_message(f"Selecting stock: {stock_name}")
        stock_selection = await page.waitForXPath('//*[@id="frm_floorsheet"]/div[1]/span')
        await stock_selection.click()
        await asyncio.sleep(2)
        
        stock_input = await page.waitForXPath('/html/body/span/span/span[1]/input')
        await stock_input.type(stock_name)
        await asyncio.sleep(3)

        await page.waitForXPath('//*[@id="select2-company-results"]/li')
        stock_options = await page.xpath('//*[@id="select2-company-results"]/li')
        for stock_option in stock_options:
            stock_text = await page.evaluate('(element) => element.textContent.split("/").pop().trim()', stock_option)
            if stock_text == stock_name:
                await stock_option.click()
                log_message(f"Stock '{stock_name}' selected")
                break
        else:
            log_message(f"Stock '{stock_name}' not found")
            return
        
        await asyncio.sleep(3)

        # Get table headers
        log_message(f"Fetching table headers")
        headers = await page.waitForXPath('//*[@id="myTable"]/thead')
        headers_elements = await headers.xpath('.//th[position() > 1]')
        headers_list = [await page.evaluate('(element) => element.textContent.toLowerCase().replace(" (rs)", "").replace(" (in rs)", "").replace(" ", "_")', header) for header in headers_elements]

        await asyncio.sleep(3)

        # Set date range
        today_date = datetime.now().date()
        start_date = today_date - timedelta(days=number_of_days - 1)
        current_date = today_date

        while current_date >= start_date:
            all_data_list = []

            # Setting the date
            extraction_date = current_date.strftime('%Y-%m-%d')
            log_message(f"Setting date to: {extraction_date}")
            date_input = await page.waitForXPath('//*[@id="date"]')
            await page.evaluate('''(element) => { element.value = ''; }''', date_input)
            log_message(f"Date field cleared")
            await asyncio.sleep(2)
            await date_input.type(extraction_date)
            log_message(f"Date set to: {extraction_date}")
            await asyncio.sleep(2)

            # Clicking search button
            log_message(f"Clicking search button")
            search = await page.waitForXPath('//*[@id="btn_flsheet_submit"]')
            await search.click()
            await asyncio.sleep(2)

            # Selecting 500 rows per page
            log_message(f"Selecting 500 rows per page")
            await page.waitForXPath('//*[@id="myTable_length"]/label/select')
            await page.select('select[name="myTable_length"]', '500')
            await asyncio.sleep(2)

            # Extracting data from the table
            try:
                log_message(f"Extracting data for {stock_name} on {extraction_date}")
                rows = await page.xpath('//*[@id="myTable"]/tbody/tr')
                if len(rows) > 1:
                    for row in rows:
                        cells = await row.xpath('.//td[position() > 1]')
                        cell_data = [await page.evaluate('(element) => element.textContent.trim()', cell) for cell in cells]
                        all_data_list.append(dict(zip(headers_list, cell_data)))
                    log_message(f"Data extracted for {stock_name} on {extraction_date}")
                    insert_floorsheet_data(all_data_list)
            except Exception as e:
                log_message(f"An error occurred while processing {stock_name} on {extraction_date}: {e}")

            # Navigating through pagination
            while True:
                all_data_list = []
                try:
                    next_button = await page.xpath('//*[@id="myTable_next"]')
                    if next_button:  # Check if the list is not empty
                        class_property = await next_button[0].getProperty('className')
                        class_value = await class_property.jsonValue()
                        if "disabled" in class_value:
                            break
                    await asyncio.sleep(2)
                    await next_button.click()
                    await asyncio.sleep(2)
                    try:
                        rows = await page.xpath('//*[@id="myTable"]/tbody/tr')
                        if len(rows) > 1:
                            for row in rows:
                                cells = await row.xpath('.//td[position() > 1]')
                                cell_data = [await page.evaluate('(element) => element.textContent.trim()', cell) for cell in cells]
                                all_data_list.append(dict(zip(headers_list, cell_data)))
                            log_message(f"Data extracted for {stock_name} on {extraction_date}")
                            insert_floorsheet_data(all_data_list)
                    except Exception as e:
                        log_message(f"An error occurred while processing {stock_name} on {extraction_date}: {e}")
                except Exception as e:
                    log_message(f"An error occurred while navigating pages for {stock_name} on {extraction_date}: {e}")
                    break

            current_date -= timedelta(days=1)

    except KeyboardInterrupt:
        log_message("\nCtrl+C detected. Exiting...")
        if browser:
            await browser.close()
    except RuntimeError:
        log_message("\nRuntime error detected. Exiting...")
        if browser:
            await browser.close()
    except Exception as e:
        log_message(f"An error occurred while processing {stock_name}: {e}")
        if browser:
            await browser.close()
    finally:
        if browser:
            await browser.close()
        log_message(f"Browser closed for stock: {stock_name}")

async def main():
    initialize_logging()
    log_message("Fetching stock symbols from database")
    symbols_list = fetch_stock_symbols()
    log_message(f"Number of Stocks fetched: {len(symbols_list)}")

    for symbol in symbols_list:
        log_message(f"Starting scraping for stock: {symbol}")
        asyncio.run(await scrape_floorsheet(symbol, number_of_days))


async def floor_start():
    initialize_logging()  # Ensure logging is initialized
    log_message("Starting Floor sheet scraping...")
    await main()


if __name__ == "__main__":
    # If this script is run directly, start the scheduler
    asyncio.run(floor_start())

