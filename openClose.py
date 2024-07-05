import asyncio
import os
import logging
from datetime import datetime, time as dt_time, timedelta
from dotenv import load_dotenv
from pyppeteer import launch
import mysql.connector
from mysql.connector import Error
import schedule
import time

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create handlers
file_handler = logging.FileHandler('market_status.log')
console_handler = logging.StreamHandler()

# Set logging level for handlers
file_handler.setLevel(logging.INFO)
console_handler.setLevel(logging.INFO)

# Create formatters and add them to handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Update market status in the database
def update_market_status(is_live):
    try:
        # Create database connection
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        if connection.is_connected():
            logger.info("Database connection established successfully")
        
        cursor = connection.cursor()
        
        # Check if the record exists
        cursor.execute("SELECT * FROM application_config WHERE `key` = 'market_status'")
        result = cursor.fetchone()
        
        current_time = datetime.now()
        
        if result:
            # Update existing record
            update_query = """
            UPDATE application_config 
            SET `value` = %s, updated_at = %s 
            WHERE `key` = 'market_status'
            """
            cursor.execute(update_query, (is_live, current_time))
        else:
            # Insert new record
            insert_query = """
            INSERT INTO application_config (`key`, `value`, created_at, updated_at) 
            VALUES ('market_status', %s, %s, %s)
            """
            cursor.execute(insert_query, (is_live, current_time, current_time))
        
        connection.commit()
        logger.info(f"Market status updated: {'Live' if is_live else 'Not Live'}")
    except Error as e:
        logger.error(f"Error while updating database: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# Main scraping function
async def scrape_market_status():
    browser = None
    
    try:
        browser = await launch(headless=True, executablePath='/usr/bin/chromium', args=['--no-sandbox', '--disable-gpu', '--disable-software-rasterizer'])
        page = await browser.newPage()
        
        # Set user agent
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        website = 'https://www.nepalstock.com.np/'
        
        try:
            await page.goto(website, {'waitUntil': 'networkidle0'})
            logger.info("Page loaded successfully")

            # Wait for the element to be visible
            await page.waitForXPath('/html/body/app-root/div/main/div/app-dashboard/div[1]/div[1]/div/div[1]/div[1]/div[2]/span[2]', {'visible': True, 'timeout': 60000})
            
            # Get the text from the element
            element = await page.xpath('/html/body/app-root/div/main/div/app-dashboard/div[1]/div[1]/div/div[1]/div[1]/div[2]/span[2]')
            market_status_text = await page.evaluate('(element) => element.textContent', element[0])
            
            logger.info(f"Retrieved market status text: {market_status_text}")

            # Check if the market is live
            is_live = 1 if "Live Market" in market_status_text else 0

            # Update the database
            update_market_status(is_live)

        except Exception as e:
            logger.error(f"An error occurred during scraping: {e}")

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Exiting...")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if browser:
            await browser.close()

# Schedule the scraping function to run every day at 11:20 AM
def job():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scrape_market_status())
    loop.close()

def schedule_jobs():
    try:
        logger.info("Initializing schedule_jobs...")
        # Schedule the job to run at 11:20 AM every day
        schedule.every().day.at("18:22").do(job)
        schedule.every().day.at("18:25").do(job)
        logger.info("Job scheduled successfully.")

        while True:
            current_time = datetime.now().time()
            current_day = datetime.now().weekday()

            if current_day in [6, 0, 1, 2, 3]:  # Sunday to Thursday
                schedule.run_pending()
            else:
                logger.info("Today is not a scheduled day. Market is Closed.")

            time.sleep(1)  # Check every 10 seconds

    except Exception as e:
        logger.error(f"An unexpected error occurred in schedule_jobs: {e}")

if __name__ == "__main__":
    schedule_jobs()
