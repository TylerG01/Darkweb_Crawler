import requests
import mariadb
from db_connection import DB_USER, DB_PASSWORD, DB_HOST, DB_DATABASE
import duplicatesV2
import URL_Cleaner
import onion_ping_multiV2
import content_checkV4
import b2sV2
import common_wordsV3
import s2gV2
import External_LinksV4
import duplicate_gold_externalLinks
import gtr_externLinksV2

def configure_socks_proxy():
    socks_proxy = 'socks5h://127.0.0.1:9050'
    proxies = {
        'http': socks_proxy,
        'https': socks_proxy
    }
    # Create a global requests session with the proxy configuration
    session = requests.Session()
    session.proxies.update(proxies)
    return session

def get_raw_table_row_count():
    try:
        conn = mariadb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_DATABASE
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM raw")
        row_count = cur.fetchone()[0]
        conn.close()
        return row_count
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        return 0

def run_modules_in_order(session, row_limit, total_workers):
    duplicatesV2.delete_duplicate_rows()
    duplicatesV2.non_onion()
    URL_Cleaner.main()
    onion_ping_multiV2.main(session, row_limit, total_workers)
    content_checkV4.main(session, total_workers)
    b2sV2.move_all_rows()
    common_wordsV3.main(session, total_workers)
    s2gV2.main()
    External_LinksV4.main(session, total_workers)
    duplicate_gold_externalLinks.main()
    gtr_externLinksV2.main()

if __name__ == "__main__":
    try:
        session = configure_socks_proxy()
        total_workers = 12
        # total_workers = int(input("Number of workers: "))
        row_limit = 36  # Example predefined value, change as needed
        
        while get_raw_table_row_count() > 1:
            run_modules_in_order(session, row_limit, total_workers)
    
    except Exception as e:
        print(f"An error occurred: {e}")
