import zipfile
import sqlite3
import pandas as pd
import os
import logging
import sys
import shutil
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
ftp_data_dir = os.path.join(script_dir, '..','ftp', 'data')

log_file_path = os.path.join(ftp_data_dir, 'email_attachment_downloader.log')
try:
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')  
except Exception as e:
    print(f"Error setting up logging: {e}")



# SQLite database file
sqlite_db_path = os.path.join(ftp_data_dir, 'database.sqlite')

print("Database path: ", sqlite_db_path)
table_newcolumns = {

    '2G': ["R373:Cell Out-of-Service Duration(s)", "2G_interference_samples"],
    '3G': ['VS.Cell.UnavailTime.Sys(s)'],
    '5G': ['N.Cell.Unavail.Dur.System(s)'],
}

def add_columns():
    with sqlite3.connect(sqlite_db_path) as conn:
        for table_name, new_columns in table_newcolumns.items():
            for new_column in new_columns:
                try:
                    conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{new_column}" REAL')
                    logging.info(f"Added column {new_column} to table '{table_name}'")
                except sqlite3.OperationalError as e:
                    logging.warning(f"Error adding column {new_column} to table '{table_name}': {e}")
                    continue
                except Exception as e:
                    logging.error(f"Error adding column {new_column} to table '{table_name}': {e}")
                    continue
    print("Columns added successfully")
def get_table_info():
    with sqlite3.connect(sqlite_db_path) as conn:
        # Get the table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        # print table names and column names
        for table in tables:
            table_name = table[0]
            print(f"Table: {table_name}")
            cursor.execute(f"PRAGMA table_info('{table_name}');")
            columns = cursor.fetchall()
            for column in columns:
                print(column)
            print("\n")

def write_to_mysql():
    # Write the data to MySQL
    pass


def main():
    add_columns()
    get_table_info()

# What is the output of the code above?
if __name__ == "__main__":
    main()
