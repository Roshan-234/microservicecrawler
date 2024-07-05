import os
import json
from datetime import datetime, time as dt_time, timedelta
import asyncio
import schedule
from pyppeteer import launch
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import time
import logging
from db_module import insert_Nepse_index_data
from marketStatus import scrape_market_status

# Ensure logs directory exists
current_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(current_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
log_file_path = os.path.join(logs_dir, 'live_nepse_index.log')
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

# URL of the site to scrape
url = 'https://www.sharesansar.com/live-trading'

async def scrape_website():
    initialize_logging()
    log_message("Setting up the Pyppeteer Browser...")
    browser = await launch(headless=True, executablePath='/usr/bin/chromium', args=['--no-sandbox', '--disable-gpu', '--disable-software-rasterizer'])
    page = await browser.newPage()
    await page.goto(url)

    try:
        # Wait for the main container to ensure the page is loaded
        await page.waitForSelector('.bx-viewport', timeout=60000)

        # Define a list of all index names to search for
        index_names = [
            'NEPSE Index', 'Non Life Insurance', 'Others Index', 'Sensitive Float Inde.',
            'Sensitive Index', 'Trading Index', 'Banking SubIndex', 'Development Bank Ind.',
            'Finance Index', 'Float Index', 'Hotels And Tourism', 'HydroPower Index',
            'Investment', 'Life Insurance', 'Manufacturing And Pr.', 'Microfinance Index',
            'Mutual Fund'
        ]

        extracted_data = []

        for name in index_names:
            try:
                parent_container_xpath = '/html/body/div[2]/div/section[2]/div[3]/div/div/div/div/div[1]/div[3]/div[1]/div/div/div/div[1]/div'
                await page.waitForXPath(parent_container_xpath, timeout=60000)

                index_elements = await page.xpath(f"{parent_container_xpath}//div[h4/text()='{name}']")

                found_matching_element = False

                for index_element in index_elements:
                    found_matching_element = True

                    turnover_element = await index_element.querySelector('p.mu-price')
                    turnover = await page.evaluate('(element) => element.textContent', turnover_element)
                    turnover = turnover.strip()

                    index_value_element = await index_element.querySelector('p span.mu-value')
                    index_value = await page.evaluate('(element) => element.textContent', index_value_element)
                    index_value = index_value.strip()

                    percentage_change_element = await index_element.querySelector('p span.mu-percent')
                    percentage_change_text = await page.evaluate('(element) => element.textContent', percentage_change_element)
                    percentage_change_str = percentage_change_text.split('%')[0] + '%'
                    percentage_change = percentage_change_str.strip().replace('\n', '')

                    # Clean and convert data
                    turnover_cleaned = float(turnover.replace(',', '')) if turnover else 0.0
                    index_value_cleaned = float(index_value.replace(',', '')) if index_value else 0.0
                    percentage_change_cleaned = float(percentage_change.replace('%', '')) if percentage_change else 0.0

                    extracted_data.append({
                        'index_name': name,
                        'turnover': turnover_cleaned,
                        'last_trading_index': index_value_cleaned,
                        'percentage_change': percentage_change_cleaned
                    })

                    log_message(f"Index Name: {name}, Turnover: {turnover_cleaned}, Last Trading Index: {index_value_cleaned}, Percentage Change: {percentage_change_cleaned}")
                    break

                if not found_matching_element:
                    while True:
                        button = await page.waitForXPath('/html/body/div[2]/div/section[2]/div[3]/div/div/div/div/div[1]/div[3]/div[1]/div/div/div/div[2]/div/a[2]', timeout=10000)
                        await button.click()
                        await page.waitForTimeout(1000)  # Wait for the page to update

                        index_elements = await page.xpath(f"{parent_container_xpath}//div[h4/text()='{name}']")

                        for index_element in index_elements:
                            found_matching_element = True

                            turnover_element = await index_element.querySelector('p.mu-price')
                            turnover = await page.evaluate('(element) => element.textContent', turnover_element)
                            turnover = turnover.strip()

                            index_value_element = await index_element.querySelector('p span.mu-value')
                            index_value = await page.evaluate('(element) => element.textContent', index_value_element)
                            index_value = index_value.strip()

                            percentage_change_element = await index_element.querySelector('p span.mu-percent')
                            percentage_change_text = await page.evaluate('(element) => element.textContent', percentage_change_element)
                            percentage_change_str = percentage_change_text.split('%')[0] + '%'
                            percentage_change = percentage_change_str.strip().replace('\n', '')

                            # Clean and convert data
                            turnover_cleaned = float(turnover.replace(',', '')) if turnover else 0.0
                            index_value_cleaned = float(index_value.replace(',', '')) if index_value else 0.0
                            percentage_change_cleaned = float(percentage_change.replace('%', '')) if percentage_change else 0.0

                            extracted_data.append({
                                'index_name': name,
                                'turnover': turnover_cleaned,
                                'last_trading_index': index_value_cleaned,
                                'percentage_change': percentage_change_cleaned
                            })

                            log_message(f"Index Name: {name}, Turnover: {turnover_cleaned}, Last Trading Index: {index_value_cleaned}, Percentage Change: {percentage_change_cleaned}")
                            break

                        if found_matching_element:
                            break

            except Exception as e:
                log_message(f"Error extracting data for '{name}': {e}")

    except Exception as e:
        log_message(f"Error during web scraping: {e}")

    finally:
        # Close the browser
        await browser.close()
        log_message("Closed the browser.")
    
    log_message("Data Extracted, Moving to Database")
    return extracted_data



def job():
    is_live = scrape_market_status()
    log_message(f"Market is {'live' if is_live else 'not live, Scrapper not Running.'}")
    if is_live:
        log_message("Starting the scraping process...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        extracted_data = loop.run_until_complete(scrape_website())
        loop.close()
        log_message("Scraping process completed.")
        if extracted_data:
            insert_Nepse_index_data(extracted_data)


async def schedule_jobs():
    while True:
        try:
            log_message("Initializing schedule_jobs...")
            start_time = dt_time(11, 00)
            end_time = dt_time(15, 1)

            schedule.every(1).minutes.do(job)
            log_message("Job scheduled successfully.")

            while True:
                current_time = datetime.now().time()
                current_day = datetime.now().weekday()

                if current_day in [6, 0, 1, 2, 3] and start_time <= current_time <= end_time:
                    log_message(f"Current time is {current_time}. Running scheduled jobs.")
                    await schedule.run_pending()
                else:
                    if current_day not in [6, 0, 1, 2, 3]:
                        log_message("Today is not a scheduled day. Market is Closed.")
                    else:
                        log_message(f"Current time {current_time} is outside the scheduled time range. Market is Closed.")

                time.sleep(10)  # Check every 10 seconds

        except Exception as e:
            log_message(f"An unexpected error occurred in schedule_jobs: {e}")
            log_message("Attempting to restart schedule_jobs after a brief pause...")
            time.sleep(60)
        except KeyboardInterrupt as k:
            log_message(f"Keyboard Interrupted, Exiting the program.")
            break  # Wait for 60 seconds before trying again

async def runner():
    initialize_logging()  # Ensure logging is initialized
    logger.info("Starting Nepse Index scraping.....")
    await schedule_jobs()

if __name__ == "__main__":
    asyncio.run(runner())