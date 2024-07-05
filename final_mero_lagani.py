import logging
from urllib.parse import urlparse
from pyppeteer import launch
from datetime import datetime, timedelta
from dateutil import parser

from db_module import insert_news_to_db, assign_notify_time


# Configure logging
logging.basicConfig(filename='logs/scheduler.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)
async def scrape_news():

    browser = await launch({
        'args': ['--no-sandbox', '--disable-gpu', '--disable-software-rasterizer'],
        'headless': False,  # Set to True if you want to run headless
        'executablePath': '/usr/bin/chromium'  # Replace with your Chrome executable path
    })
    page = await browser.newPage()
    await page.goto('https://merolagani.com/NewsList.aspx')
    logger.info("Page loaded")

    await page.select('#ctl00_ContentPlaceHolder1_ddlNewsCategory', '6')  # '6' is the value for "Stock Market"
    logger.info("Stock market category selected")

    await page.click('#ctl00_ContentPlaceHolder1_lbtnSearch')
    logger.info("Search button clicked")

    async def scrape_news_items():
        news_items = await page.evaluate('''() => {
            const items = document.querySelectorAll('.media-news');
            return Array.from(items).map(item => {
                const titleElement = item.querySelector('h4.media-title a');
                const imgElement = item.querySelector('img');
                const linkElement = item.querySelector('a');
                const dateElement = item.querySelector('span.media-label');
                return {
                    title: titleElement ? titleElement.innerText.trim() : null,
                    image: imgElement ? imgElement.src : null,
                    link: linkElement ? linkElement.href : null,
                    date: dateElement ? dateElement.innerText.trim() : null,
                };
            });
        }''')
        return news_items

    async def news_short_description(page, url):
        await page.goto(url)
        await page.waitForSelector('div.col-md-9')
        logger.info("Page loaded")
        short_description = await page.evaluate('''() => {
            const spans = document.querySelectorAll('div.col-md-9 .media-content p span');
            let concatenatedText = '';
            spans.forEach(span => {
                concatenatedText += span.innerText.trim() + ' ';
            });
            return concatenatedText.trim();
        }''')
        return short_description

    all_news_items = []
    scraped_links = set()
    one_week_ago = datetime.now() - timedelta(days=7)

    while True:
        await page.waitForSelector('.media-news', {'timeout': 10000})
        news_items = await scrape_news_items()
        if not news_items:
            break
        unique_news_items = [news for news in news_items if news['link'] not in scraped_links]
        all_news_items.extend(unique_news_items)
        scraped_links.update([news['link'] for news in unique_news_items])
        
        if unique_news_items:
            last_news_date = parser.parse(unique_news_items[-1]['date'].split(',')[0], fuzzy=True)
            if last_news_date < one_week_ago:
                break
        else:
            break

        try:
            load_more_button = await page.querySelector('a.btn.btn-primary.btn-block[data-load="news-block-three"]')
            if load_more_button:
                await page.evaluate('(button) => button.click()', load_more_button)
                await page.waitFor(2000)
            else:
                break
        except Exception as e:
            logger.error(f"Error clicking the load more button: {e}")
            break

    filtered_news_items = [news for news in all_news_items if news['date'] and len(news['date'].split(',')) == 2 and (datetime.now() - parser.parse(news['date'].split(',')[0], fuzzy=True)) <= timedelta(days=7)]

    def extract_domain(link):
        parsed_uri = urlparse(link)
        domain = '{uri.netloc}'.format(uri=parsed_uri)
        return domain

    def is_valid_url(link):
        if ('.jpg') in link:
            return link
        else:
            return None
        
    news_data = []
    for news in filtered_news_items:
        domain = extract_domain(news['link'])
        des = await news_short_description(page, news['link'])
        img_link = is_valid_url(news['image'])
        parsed_date = parser.parse(news['date'], fuzzy=True)
        formatted_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
        print('Date formatted :', formatted_date)
        print('exact date', news['date'])
        if des:
            des = des.encode('utf-8', errors='ignore').decode('utf-8')
            des = des.split('। ')
            summary = '। '.join(des[:3])
            if len(des) > 3:
                summary += '...'
        news_data.append({
            'title': news['title'],
            'image': img_link,
            'link': news['link'],
            'date': formatted_date,
            'domain': domain,
            'description': summary,
            'category': 'general'
        })

    logger.info("News data scraped")
    current_time = datetime.now().time()
    insert_news_to_db(news_data, current_time)
    assign_notify_time(current_time)

    try:
        await browser.close()
    except IOError as e:
        logger.error(f"Error closing browser: {e}")

    return news_data

async def run_scraper():
    return await scrape_news()
