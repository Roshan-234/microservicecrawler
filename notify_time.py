import mysql.connector
import random
from datetime import datetime, timedelta


def assign_notify_time(current_time):
    mysql_config = {
        'host': '46.16.236.25',
        'database': 'quizwebs_scrapuUI',
        'user': 'quizwebs_scrapyui',
        'password': 'e6AAj;I.gK3R',
        "connection_timeout": 3600  # Increase the connection timeout (in seconds)
    }

    try:
        # Connect to MySQL database
        db_connection = mysql.connector.connect(**mysql_config)
        cursor = db_connection.cursor(dictionary=True)  # Use dictionary cursor

        print("Database connection established successfully.")
        print("current Time: ", current_time)

        if current_time.hour == 12:  # If the current time is 10:00 (day start)
            # Fetch news for today's date with is_day_start = True
            query = "SELECT * FROM news WHERE should_notify = TRUE AND is_day_start = TRUE"
            cursor.execute(query)
            results = cursor.fetchall()

            print(f"Fetched {len(results)} rows.")

            # Check if the result is non-empty
            if results:
                # Generate random notification times between 10:30 and 16:00 in 2-hour intervals
                notify_times = []
                notify_start_time = datetime.strptime("10:30", "%H:%M")
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

                    print(f"Updated notify_time for news id {news_id} to {notify_time}")

                print("Notification times updated successfully.")
            else:
                print("No results to notify.")

        elif current_time.hour == 15:  # If the current time is 16:00 (day end)
            # Fetch new news inserted today with is_day_start = False
            query = "SELECT * FROM news WHERE should_notify = TRUE AND is_day_start = FALSE"
            cursor.execute(query)
            results = cursor.fetchall()

            print(f"Fetched {len(results)} rows.")

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

                print(f"Updated notify_time for news id {news_id} to {notify_time}")
            else:
                print("No new results to notify.")

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")

    except Exception as e:
        print(f"Unexpected error: {e}")

    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()