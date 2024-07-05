import logging
import multiprocessing
import os
from flask import Flask, render_template, request
import asyncio

app = Flask(__name__)

# Ensure logs directory exists for app.py
logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging for the Flask app
log_file_path = os.path.join(logs_dir, 'app.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

# Example routes to display logs
@app.route('/')
def home():
    logger.info("Home page accessed.")
    return render_template('home.html')

@app.route('/logs', methods=['POST'])
def show_logs():
    log_type = request.form.get('log_type')

    if log_type == 'news':
        log_content = get_news_logs()
    elif log_type == 'nepse':
        log_content = get_nepse_logs()
    elif log_type == 'announcements':
        log_content = get_announcement_logs()
    elif log_type == 'eps':
        log_content = get_eps_logs()
    elif log_type == 'floorsheet':
        log_content = get_floorsheet_logs()
    else:
        log_content = "Select a log type above."

    return render_template('home.html', log_content=log_content)

def get_news_logs():
    return read_log_file('schedule.log')

def get_nepse_logs():
    return read_log_file('live_nepse_index.log')

def get_announcement_logs():
    return read_log_file('announcements.log')

def get_eps_logs():
    return read_log_file('EPS.log')

def get_floorsheet_logs():
    return read_log_file('floorsheet.log')

def read_log_file(file_name):
    file_path = os.path.join(logs_dir, file_name)
    try:
        with open(file_path, 'r') as file:
            log_content = file.read()
    except FileNotFoundError:
        logger.error(f"Log file '{file_path}' not found.")
        log_content = f"Log file '{file_path}' not found."
    return log_content

async def run_async_task(func):
    await func()

async def run_scraping_tasks():
    from scheduler import start_scheduler
    from floorsheetPypps import floor_start
    from live_nepse_data_scrapper import runner
    from announcemetsPupp import main
    from epsPupeeter import eps_scraper

    tasks = [
        start_scheduler(),
        floor_start(),
        runner(),
        main(),
        eps_scraper()
    ]
    
    await asyncio.gather(*tasks)

def run_flask():
    port = os.environ.get('PORT', 5000)
    app.run(host='0.0.0.0', port=port, debug=False)
    logger.info(f"Flask app running on http://0.0.0.0:{port}/")

if __name__ == '__main__':
    # Start Flask in a separate process
    flask_process = multiprocessing.Process(target=run_flask)
    flask_process.start()

    # Run the scraping tasks in the main process
    asyncio.run(run_scraping_tasks())

    # Wait for the Flask process to finish (which it never will in normal operation)
    flask_process.join()
