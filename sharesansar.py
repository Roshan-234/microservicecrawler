import asyncio
import logging
from urllib.parse import urlparse
from pyppeteer import launch
from datetime import datetime, timedelta
from dateutil import parser

from db_module import insert_news_to_db


# Configure logging
logging.basicConfig(filename='logs/scheduler.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

sites_list = [
    'https://www.sharesansar.com/category/ipo-fpo-news',
    'https://www.sharesansar.com/category/dividend-right-bonus',
    'https://www.sharesansar.com/category/exclusive',
    'https://www.sharesansar.com/category/financial-analysis',
    'https://www.sharesansar.com/category/share-listed'
]

# Mapping URLs to categories
category_map = {
    'https://www.sharesansar.com/category/ipo-fpo-news': 'ipo',
    'https://www.sharesansar.com/category/dividend-right-bonus': 'dividend/right/bonus',
    'https://www.sharesansar.com/category/exclusive': 'general',
    'https://www.sharesansar.com/category/financial-analysis': 'financial analysis',
    'https://www.sharesansar.com/category/share-listed': 'listed shares'
}

async def scrape_news_sharesansar():
    
    browser = await launch({
        'args':['--no-sandbox', '--disable-gpu', '--disable-software-rasterizer'],
        'headless': False,  # Set to True if you want to run headless
        'executablePath': '/usr/bin/chromium'  # Replace with your Chrome executable path
    })
    
    async def scrape_site(site_url, category):
        page = await browser.newPage()
        await page.goto(site_url)
        logger.info(f"Page loaded: {site_url}")

        async def scrape_news():
            news_items = await page.evaluate('''() => {
                const items = document.querySelectorAll('.featured-news-list');
                return Array.from(items).map(item => {
                    const titleElement = item.querySelector('h4.featured-news-title');
                    const imgElement = item.querySelector('img');
                    const linkElement = item.querySelector('a');
                    const dateElement = item.querySelector('span.text-org');
                    return {
                        title: titleElement ? titleElement.innerText : null,
                        image: imgElement ? imgElement.src : null,
                        link: linkElement ? linkElement.href : null,
                        date: dateElement ? dateElement.innerText.trim() : null,
                    };
                });
            }''')
            return news_items

        async def news_description(page, url):
            await page.goto(url)
            await page.waitForSelector('div#newsdetail-content')
            logger.info(f"Page loaded for description: {url}")
            short_description = await page.evaluate('''() => {
                const contentDiv = document.querySelector('div#newsdetail-content');
                if (!contentDiv) return '';
                const paragraphs = contentDiv.querySelectorAll('p');
                let concatenatedText = '';
                paragraphs.forEach(paragraph => {
                    concatenatedText += paragraph.innerText.trim() + ' ';
                });
                return concatenatedText.trim();
            }''')
            return short_description
        
        all_news_items = []
        one_week_ago = datetime.now() - timedelta(days=7)

        while True:
            await page.waitForSelector('.featured-news-list', {'timeout': 10000})
            news_items = await scrape_news()
            recent_news_items = [
                news for news in news_items
                if news['date'] and parser.parse(news['date'], fuzzy=True) >= one_week_ago
            ]

            for news in recent_news_items:
                news['category'] = category
                all_news_items.append(news)

            if any(parser.parse(news['date'], fuzzy=True) < one_week_ago for news in news_items):
                break

            next_page_link = await page.querySelector('a.page-link[rel="next"]')
            if not next_page_link:
                break

            next_page_href = await page.evaluate('(link) => link.href', next_page_link)
            await page.goto(next_page_href)
            await page.waitFor(3000)

        def extract_domain(link):
            parsed_uri = urlparse(link)
            domain = '{uri.netloc}'.format(uri=parsed_uri)
            return domain
        
        news_data = []
        for news in all_news_items:
            parsed_date = parser.parse(news['date'], fuzzy=True)
            formatted_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            domain = extract_domain(news['link'])
            des = await news_description(page, news['link'])
            if des:
                des = des.encode('utf-8', errors='ignore').decode('utf-8')
                des = des.split('. ')
                summary = '. '.join(des[:3])
                if len(des) > 3:
                    summary += '...'
            news_data.append({
                'title': news['title'],
                'image': news['image'],
                'link': news['link'],
                'date': formatted_date,
                'domain': domain,
                'description': summary,
                'category': news['category']
            })
        logger.info("News data scraped")
        current_time = datetime.now().time()
        insert_news_to_db(news_data, current_time)
        await page.close()

    for site in sites_list:
        category = category_map.get(site, 'general')
        await scrape_site(site, category)

    await browser.close()

async def run_scraper():
    return await scrape_news_sharesansar()
