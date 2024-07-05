from datetime import datetime, timedelta
import logging
import random
import sys
import time
import mysql.connector
from dateutil import parser
from dotenv import load_dotenv
import os
from mysql.connector import errorcode
import pytz

logger = logging.getLogger()
# Load environment variables from .env file
load_dotenv()

# MySQL database credentials from .env file
mysql_config = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
    "connection_timeout": int(os.getenv("MYSQL_CONNECTION_TIMEOUT"))
}

# Inserting news into the db
def insert_news_to_db(news_data, current_time):   
    # Connect to MySQL database
    db_connection = mysql.connector.connect(**mysql_config)
    cursor = db_connection.cursor()

    # Get current date
    today_date = datetime.now().strftime('%Y-%m-%d 00:00:00')
    print("Today's Date for Comparison:", today_date)

    for news in news_data:
        is_day_start = (current_time.hour == 11) 
        # Check if the news already exists in the database
        cursor.execute("SELECT COUNT(*) FROM news WHERE title = %s AND link = %s", (news['title'], news['link']))
        result = cursor.fetchone()
        print("News Date:", news['date'])
        parsed_date = parser.parse(news['date'], fuzzy=True)
        formatted_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
        formatted_date_for_comparison = parsed_date.strftime('%Y-%m-%d 00:00:00')
        notify_time = parsed_date.strftime('%H:%M:%S')  # Extract only the time part
        print("Formatted Date for Comparison:", formatted_date_for_comparison)

        # Determine notify_time and should_notify based on news date
        if formatted_date_for_comparison == today_date:
            should_notify = 1  # True
        else:
            notify_time = None
            should_notify = 0  # False

        print("Should Notify:", should_notify)

        if result[0] == 0:
            # Insert news into the database
            cursor.execute("INSERT INTO news (title, image, link, date, domain, description, category, notify_time, should_notify, is_day_start) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s)",
                           (news['title'], news['image'], news['link'], news['date'], news['domain'], news['description'], news['category'], notify_time, should_notify, is_day_start))
            logging.info("Inserted:", news['title'])
        else:
            logging.info("Skipped:", news['title'], "- Already exists in the database")

    # Commit changes to the database
    db_connection.commit()
    logging.info("News inserted Successfully.......... " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # Close database connection
    cursor.close()
    db_connection.close()

# Notification time
def assign_notify_time(current_time):
    try:
        # Connect to MySQL database
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor(dictionary=True)  # Use dictionary cursor

        logging.info("Database connection established successfully.")
        print("current Time: ", current_time)

        if current_time.hour == 11:  # If the current time is 10:00 (day start)
            # Fetch news for today's date with is_day_start = True
            query = "SELECT * FROM news WHERE should_notify = TRUE AND is_day_start = TRUE"
            cursor.execute(query)
            results = cursor.fetchall()

            print(f"Fetched {len(results)} rows.")

            # Check if the result is non-empty
            if results:
                # Generate random notification times between 10:30 and 16:00 in 2-hour intervals
                notify_times = []
                notify_start_time = datetime.strptime("11:30", "%H:%M")
                end_time = datetime.strptime("16:00", "%H:%M")

                current_time = notify_start_time
                while current_time < end_time:
                    notify_times.append(current_time)
                    current_time += timedelta(hours=2)

                # Shuffle notify_times to randomize the order
                random.shuffle(notify_times)

                # Assign notify_times to the results
                for result, notify_time in zip(results, notify_times):
                    news_id = result['id']  # Assuming 'id' is the primary key

                    # Update notify_time to today's date
                    notify_time = datetime.combine(datetime.now().date(), notify_time.time())

                    cursor.execute("UPDATE news SET notify_time = %s WHERE id = %s", (notify_time, news_id))
                    db_connection.commit()  # Commit the transaction

                    logging.info(f"Updated notify_time for news id {news_id} to {notify_time}")

                logging.info("Notification times updated successfully.")
            else:
                logging.info("No results to notify.")

        elif current_time.hour == 16:  # If the current time is 16:00 (day end)
            # Fetch new news inserted today with is_day_start = False
            query = "SELECT * FROM news WHERE should_notify = TRUE AND is_day_start = FALSE"
            cursor.execute(query)
            results = cursor.fetchall()

            logging.info(f"Fetched {len(results)} rows.")

            # Check if the result is non-empty
            if results:
                # Select one random news item and assign a notification time
                random_news = random.choice(results)
                news_id = random_news['id']

                # Generate a random notification time between 16:00 and 18:00
                notify_time = datetime.strptime(str(random.randint(16, 18)) + ":00", "%H:%M")
                notify_time = datetime.combine(datetime.now().date(), notify_time.time())

                cursor.execute("UPDATE news SET notify_time = %s WHERE id = %s", (notify_time, news_id))
                db_connection.commit()  # Commit the transaction

                logging.info(f"Updated notify_time for news id {news_id} to {notify_time}")
            else:
                logging.error("No new results to notify.")

    except mysql.connector.Error as err:
        logging.error(f"MySQL Error: {err}")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")

    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()

# Live nepse index 
def insert_Nepse_index_data(final_data):
    try:
        # Connect to the database
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()
        logger.info("Connection established.")

        # Prepare the insert statement with placeholders
        sql = """
            INSERT INTO live_indices_price (index_id, last_trading_price, percentage_change, created_at, updated_at, turnover)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_trading_price = VALUES(last_trading_price),
                percentage_change = VALUES(percentage_change),
                turnover = VALUES(turnover),
                updated_at = VALUES(updated_at);
        """

        # Retrieve stock symbols and their IDs
        cursor.execute("SELECT id, index_display_name FROM sector")
        symbols = cursor.fetchall()

        index_dict = {index_name: index_id for index_id, index_name in symbols}

        # Insert data into the database
        for row in final_data:
            indexName = row['index_name']
            turnover = row['turnover']
            lastTradingPrice = row['last_trading_index']
            percentChange = row['percentage_change']
            
            index_id = index_dict.get(indexName)
            if index_id is not None:
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                values = (index_id, lastTradingPrice, percentChange, now, now, turnover)
                cursor.execute(sql, values)

        # Commit the transaction
        db_connection.commit()
        logger.info("Data committed to the database.")

    except mysql.connector.Error as err:
        logger.error(f"Error: {err}")
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.warning("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.warning("Database does not exist")
        else:
            logger.error(err)
    finally:
        if db_connection.is_connected():
            cursor.close()
            db_connection.close()
            logger.info("Closed the database connection.")

# Insert EPS datas

def insert_EPS_data(final_data):
    # Connect to the database
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()
        logger.debug("Connection established.")

        # Retrieve stock symbols and their IDs
        cursor.execute("SELECT id, symbol FROM stock")
        symbols = cursor.fetchall()
        logger.debug("Fetched stock symbols and IDs from the database.")

        symbol_dict = {symbol: stock_id for stock_id, symbol in symbols}

        # Insert or update data into the database
        for row in final_data:
            symbol = row[0].strip()
            eps = row[1]
            pe_ratio = row[2]

            # Handle NaN values
            eps = '0.00' if eps == 'NaN' or eps == '' else eps
            pe_ratio = '0.00' if pe_ratio == 'NaN' or pe_ratio == '' else pe_ratio

            current_date = datetime.now()

            stock_id = symbol_dict.get(symbol)
            if stock_id is not None:
                # Check if the stock_id already exists in stock_eps_pe
                cursor.execute("SELECT stock_id FROM stock_eps_pe WHERE stock_id = %s", (stock_id,))
                result = cursor.fetchone()
                
                if result:
                    # If the stock_id exists, update the record
                    sql_update = """
                        UPDATE stock_eps_pe
                        SET EPS = %s, PE_Ratio = %s, updated_at = %s
                        WHERE stock_id = %s
                    """
                    cursor.execute(sql_update, (eps, pe_ratio, current_date, stock_id))
                else:
                    # If the stock_id does not exist, insert a new record
                    sql_insert = """
                        INSERT INTO stock_eps_pe (stock_id, EPS, PE_Ratio, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql_insert, (stock_id, eps, pe_ratio, current_date, current_date))
        logger.debug("Data Inserted/Updated for EPS and PERatio")
        # Commit the transaction
        db_connection.commit()
        logger.debug("Data committed to the database.")

    except mysql.connector.Error as err:
        logger.error(f"Error: {err}")
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(err)
    finally:
        if db_connection.is_connected():
            cursor.close()
            db_connection.close()
            logger.debug("Closed the database connection.")


#floor sheet
def insert_floorsheet_data(final_data):
    logger.info("Connecting to the database...")
    
    connection = None
    cursor = None
    
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()
        logger.info("Database connection established.")
    
        current_datetime = datetime.now(pytz.timezone('Asia/Kathmandu')).strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('SELECT * FROM stock')
        stock_rows = cursor.fetchall()

        logger.info("Inserting/updating Floorsheet Data")
        if stock_rows:
            for row in final_data:
                stock = next((stock for stock in stock_rows if stock['symbol'] == row['symbol']), None)
                
                if stock:
                    stock_id = stock['id']
                    buyer_broker_id = int(row['buyer_broker'])
                    sell_broker_id = int(row['sell_broker'])
                    share_quantity = float(row['share_quantity'].replace(",", ""))
                    rate = float(row['rate'].replace(",", ""))
                    amount = float(row['amount'].replace(",", ""))
                    traded_date = row['traded_date']
                    
                    # logger.debug(f"Stock: {stock['symbol']}, Stock ID: {stock_id}, Buyer Broker ID: {buyer_broker_id}, Sell Broker ID: {sell_broker_id}")

                    sql_live_floorsheet = """
                        INSERT INTO floorsheet (stock_id, transaction_no, buyer_broker_id, sell_broker_id, share_quantity, rate, amount, date, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        stock_id = VALUES(stock_id),
                        transaction_no = VALUES(transaction_no),
                        buyer_broker_id = VALUES(buyer_broker_id),
                        sell_broker_id = VALUES(sell_broker_id),
                        share_quantity = VALUES(share_quantity),
                        rate = VALUES(rate),
                        amount = VALUES(amount),
                        date = VALUES(date),
                        created_at = VALUES(created_at),
                        updated_at = VALUES(updated_at);
                    """
                    try:
                        cursor.execute(sql_live_floorsheet, (
                            stock_id,
                            row['transaction_no.'],
                            buyer_broker_id,
                            sell_broker_id,
                            share_quantity,
                            rate,
                            amount,
                            traded_date,
                            current_datetime,
                            current_datetime
                        ))
                    except Exception as error:
                        logger.error(f'Error during Floorsheet Data Insertion: {error}')
                else:
                    logger.warning(f"Skipping row due to missing stock: {row}")

            connection.commit()  # Ensure the transaction is committed
            logger.info("Floorsheet Data Inserted.")
        else:
            logger.warning("No stock or broker data found")

    except Exception as e:
        logger.error(f"Connection to database failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()
            logger.info("Database connection closed.")


def fetch_stock_symbols():
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()
        cursor.execute("SELECT symbol FROM stock")
        stocks = cursor.fetchall()
        stock_symbols = [symbol for (symbol,) in stocks]
        cursor.close()
        db_connection.close()
        return stock_symbols
    except mysql.connector.Error as err:
        logger.error(f"Error fetching stock symbols: {err}")
        sys.exit(1)

# Announcements
def generate_random_time():
    start_time = time(11, 0)  # 11 AM
    end_time = time(18, 0)    # 6 PM
    
    random_seconds = random.randint(
        0, 
        int((datetime.combine(datetime.today(), end_time) - datetime.combine(datetime.today(), start_time)).total_seconds())
    )
    
    random_time = (datetime.combine(datetime.today(), start_time) + timedelta(seconds=random_seconds)).time()
    return random_time

def announcement_data(data_list, stock_name):
    try:
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor()
        logger.info("Database connection established.")

        cursor.execute("SELECT id, symbol FROM stock")
        stocks = cursor.fetchall()
        stock_ids = {symbol: stock_id for stock_id, symbol in stocks}
        
        stock_id = stock_ids.get(stock_name)
        if not stock_id:
            logger.info(f"Stock ID for {stock_name} not found")
            return

        current_date = datetime.now().date()
        for row in data_list:
            announcement_date = datetime.strptime(row[0], "%Y-%m-%d").date()  # Adjust the format as necessary
            announcement_text = row[1]
            should_notify = announcement_date == current_date
            notify_time = generate_random_time() if should_notify else None
            
            cursor.execute(
                "SELECT COUNT(*) FROM announcements WHERE stock_id = %s AND date = %s AND announcement = %s",
                (stock_id, announcement_date, announcement_text)
            )
            exists = cursor.fetchone()[0] > 0
            
            if not exists:
                cursor.execute(
                    "INSERT INTO announcements (stock_id, date, announcement, should_notify, notify_time, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (stock_id, announcement_date, announcement_text, should_notify, notify_time, current_date, current_date)
                )

        db_connection.commit()
        logger.info(f"Announcements processed for {stock_name}")
    
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
    
    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()
        logger.info("Database connection closed.")