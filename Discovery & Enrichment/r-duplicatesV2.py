import mariadb
from db_connection import DB_USER, DB_PASSWORD, DB_HOST, DB_DATABASE
from progress.spinner import Spinner

def connect_to_db():
    try:
        conn = mariadb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_DATABASE
        )
        print("Starting to remove duplicates from raw table...")
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        return None

def non_onion():
    conn = connect_to_db()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        delete_non_onion_query = """
        DELETE FROM raw
        WHERE link NOT LIKE '%.onion%';
        """

        spinner = Spinner('Removing non-onion rows... ')
        cursor.execute(delete_non_onion_query)
        spinner.next()
        deleted_rows = cursor.rowcount
        conn.commit()
        spinner.finish()
        print(f"Deleted {deleted_rows} non-onion rows.")

    except mariadb.Error as err:
        print(f"Error: {err}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()
        print("MariaDB connection closed.")

def delete_duplicate_rows():
    conn = connect_to_db()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        # SQL query to delete duplicate rows
        delete_query = """
        DELETE t1
        FROM raw t1
        JOIN (
            SELECT link, MIN(id) as min_id
            FROM raw
            GROUP BY link
            HAVING COUNT(*) > 1
        ) t2 ON t1.link = t2.link AND t1.id > t2.min_id;
        """

        spinner = Spinner('Processing... ')
        cursor.execute(delete_query)
        spinner.next()
        deleted_rows = cursor.rowcount
        conn.commit()
        spinner.finish()
        print(f"Deleted {deleted_rows} duplicate rows.")

    except mariadb.Error as err:
        print(f"Error: {err}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        conn.close()
        # print("MariaDB connection closed.")

if __name__ == "__main__":
    non_onion()
    delete_duplicate_rows()
