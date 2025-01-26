import mariadb
import requests
import logging
from datetime import datetime
from requests.exceptions import RequestException, Timeout
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from db_connection import DB_USER, DB_PASSWORD, DB_HOST, DB_DATABASE

# Set up logging
logging.basicConfig(
    filename='/you/logfile/path/goes/here/primary_ping_logs.txt',  # Replace with the path to your log file
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_url(session, db_config, url, phrase, row_id):
    try:
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url

        response = session.get(url, timeout=60)
        status_code = response.status_code
        
        # Database connection and cursor are created inside the function to ensure proper closing
        connection = mariadb.connect(**db_config)
        cursor = connection.cursor()

        if status_code == 200:
            insert_query = "INSERT INTO bronze (link, phrase, service, status, time) VALUES (%s, %s, %s, %s, %s)"
            data = (url, phrase, "onion", status_code, datetime.now())
        else:
            insert_query = "INSERT INTO removed (id, link, phrase, service, status, time) VALUES (%s, %s, %s, %s, %s, %s)"
            data = (row_id, url, phrase, "onion", status_code, datetime.now())

        cursor.execute(insert_query, data)
        connection.commit()

        delete_query = "DELETE FROM raw WHERE id = %s"
        cursor.execute(delete_query, (row_id,))
        connection.commit()

    except (RequestException, Timeout) as e:
        logging.error(f"Error fetching URL {url}: {str(e)}")
        try:
            connection = mariadb.connect(**db_config)
            cursor = connection.cursor()
            insert_query = "INSERT INTO removed (id, link, phrase, service, status, time) VALUES (%s, %s, %s, %s, %s, %s)"
            data = (row_id, url, phrase, "onion", 408, datetime.now())
            cursor.execute(insert_query, data)
            connection.commit()

            delete_query = "DELETE FROM raw WHERE id = %s"
            cursor.execute(delete_query, (row_id,))
            connection.commit()
        except mariadb.Error as db_error:
            logging.error(f"Error handling URL {url}: {str(db_error)}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def fetch_and_process_urls(session, connection, db_config, row_limit, total_workers):
    cursor = connection.cursor()

    try:
        query = "SELECT id, link, phrase FROM raw LIMIT %s"
        cursor.execute(query, (row_limit,))
        rows = cursor.fetchall()
        progress_bar = tqdm(total=len(rows), desc="Requesting URL Status Codes", unit="URL")

        with ThreadPoolExecutor(max_workers=total_workers) as executor:
            futures = {executor.submit(process_url, session, db_config, url, phrase, row_id): (url, row_id) for row_id, url, phrase in rows}
            
            for future in as_completed(futures):
                url, row_id = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing URL {url} with row ID {row_id}: {str(e)}")
                progress_bar.update(1)
        
        progress_bar.close()

    except mariadb.Error as e:
        logging.error(f"Error accessing database: {str(e)}")
    finally:
        cursor.close()

def main(session, row_limit, total_workers):
    db_config = {
        'user': DB_USER,
        'password': DB_PASSWORD,
        'host': DB_HOST,
        'database': DB_DATABASE
    }
    
    try:
        db_connection = mariadb.connect(**db_config)
        fetch_and_process_urls(session, db_connection, db_config, row_limit, total_workers)

    except mariadb.Error as e:
        logging.error(f"Error connecting to MariaDB: {str(e)}")
    
    finally:
        if db_connection:
            db_connection.close()

if __name__ == "__main__":
    session = requests.Session()
    session.proxies = {
        'http': 'socks5h://127.0.0.1:9050',
        'https': 'socks5h://127.0.0.1:9050'
    }
    row_limit = 36
    total_workers = 12  # Adjust the number of workers here
    main(session, row_limit, total_workers)
