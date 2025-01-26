import mariadb
from db_connection import DB_USER, DB_PASSWORD, DB_HOST, DB_DATABASE

# Strings to be removed from the URLs
STRINGS_TO_REMOVE = [
    "http://goto.php?href=",
    "http:///redir?q=leak&url=",
    # Add other strings here as needed
]

def clean_url(url):
    url = url.strip()  # Removes whitespace in the URL
    for pattern in STRINGS_TO_REMOVE:
        if pattern in url:
            print(f"Original URL: {url}")  # Debugging: Print original URL
            url = url.replace(pattern, "")
            print(f"Cleaned URL: {url}")   # Debugging: Print cleaned URL
    return url

def main():
    # Connect to MariaDB
    try:
        conn = mariadb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_DATABASE
        )
        cursor = conn.cursor()

        # Fetch all URLs from the 'link' column in the 'removed' table
        cursor.execute("SELECT id, link FROM removed")
        rows = cursor.fetchall()

        # Initialize a counter for modified URLs
        modified_count = 0

        # Clean each URL and update the database
        for row in rows:
            url_id, url = row
            cleaned_url = clean_url(url)
            if cleaned_url != url:
                cursor.execute("UPDATE removed SET link = ? WHERE id = ?", (cleaned_url, url_id))
                modified_count += 1

        # Commit the changes to the database
        conn.commit()

        # Print the total number of URLs modified
        print(f"Total URLs modified: {modified_count}")

    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
    
    finally:
        # Close the connection
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
