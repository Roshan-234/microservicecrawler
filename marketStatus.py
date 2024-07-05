import asyncio
import logging
from pyppeteer import launch


# Configure logging
logger = logging.getLogger()

# Main scraping function
async def scrape_market_status():
    browser = None
    
    try:
        browser = await launch(headless=True, executablePath='/usr/bin/chromium', args=['--no-sandbox', '--disable-gpu', '--disable-software-rasterizer'])
        page = await browser.newPage()
        
        # Set user agent
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        website = 'https://www.nepalstock.com.np/'
        
        await page.goto(website, {'waitUntil': 'networkidle0'})

        # Wait for the element to be visible
        await page.waitForXPath('/html/body/app-root/div/main/div/app-dashboard/div[1]/div[1]/div/div[1]/div[1]/div[2]/span[2]', {'visible': True, 'timeout': 60000})
        
        # Get the text from the element
        element = await page.xpath('/html/body/app-root/div/main/div/app-dashboard/div[1]/div[1]/div/div[1]/div[1]/div[2]/span[2]')
        market_status_text = await page.evaluate('(element) => element.textContent', element[0])
        

        # Check if the market is live
        is_live = "Live Market" in market_status_text

        return is_live

    except Exception as e:
        logger.error(f"An error occurred during status checking: {e}")
        return False

    finally:
        if browser:
            await browser.close()
