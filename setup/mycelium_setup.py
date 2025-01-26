import requests
import mariadb
from db_connection import DB_USER, DB_PASSWORD, DB_HOST, DB_DATABASE
import db_construction
import seederV2
import duplicatesV2

def main():
    db_construction_input = input("Would you like to execute db_construction.py? (yes/no): ").strip().lower()
    run_db_construction = db_construction_input == 'yes'
    
    seeder_input = input("Would you like to execute seederV2.py? (yes/no): ").strip().lower()
    run_seeder = seeder_input == 'yes'
    if run_db_construction:
        db_construction.main()
    if run_seeder:
        seederV2.main()

    duplicatesV2.delete_duplicate_rows()
    duplicatesV2.non_onion()

    return run_db_construction, run_seeder

if __name__ == "__main__":
    main()
