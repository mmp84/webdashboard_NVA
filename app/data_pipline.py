import zipfile
import sqlite3
import pandas as pd
import os
import logging

# Set up logging
logging.basicConfig(filename='../ftp/data/email_attachment_downloader.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

    

download_folder = '../ftp/data'
csv_folder_path = '../ftp/data'
# SQLite database file
sqlite_db_path = '../ftp/data/database.sqlite'

def unzip_file(file_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(download_folder)
        logging.info(f'Unzipped: {file_path}')



# Function to determine the appropriate table based on the filename
def determine_table_name(filename):
    if '2G' in filename:
        return '2G'
    elif '3G' in filename:
        return '3G'
    elif '4G' in filename:
        return '4G'
    elif '5G' in filename:
        return '5G'
    else:
        return None  # No matching table

# Import CSV files into SQLite database
def import_csv_files():
    try:
        with sqlite3.connect(sqlite_db_path) as conn:
            for filename in os.listdir(csv_folder_path):
                if filename.endswith('.csv'):
                    table_name = determine_table_name(filename)
                    if table_name is None:
                        logging.warning(f"Skipping {filename}: No matching table found.")
                        continue

                    csv_file_path = os.path.join(csv_folder_path, filename)

                    try:
                        df = pd.read_csv(csv_file_path, skiprows=6)
                        df = df.iloc[:-1]  # Drop the last row
                        
                        if validate_data(df):
                            df.to_sql(table_name, conn, if_exists='append', index=False)
                            logging.info(f"Appended {filename} to {table_name} table in SQLite database.")

                            # Delete the CSV file after successful append
                            os.remove(csv_file_path)
                            logging.info(f"Deleted {filename} after successful import.")

                        else:
                            logging.warning(f"Data validation failed for {filename}. Skipping import.")
                    except Exception as e:
                        logging.error(f"Error reading or appending {filename}: {e}")
    except Exception as e:
        logging.error(f"Database connection error: {e}")

# Validate data in DataFrame
def validate_data(df):
    # Implement validation logic here
    # Example: return False if df is empty
    return not df.empty

# Query and print data from a specified table
def query_data(table_name):
    try:
        with sqlite3.connect(sqlite_db_path) as conn:
            # cursor = conn.cursor()
            # cursor.execute(f"SELECT * FROM '{table_name}'")
            query = f"SELECT * FROM '{table_name}'"
            df = pd.read_sql_query(query, conn)

            print(df)
            # results = cursor.fetchall()
            # for row in results:
            #     print(row)
            # results.to_csv('D:/FTP/2Gdf.csv')
    except Exception as e:
        logging.error(f"Error querying {table_name} table: {e}")
def main():
    # unzipping the file
    for filename in os.listdir(download_folder):
        if filename.endswith('.zip'):
            file_path = os.path.join(download_folder, filename)
            unzip_file(file_path)
            os.rename(file_path, '../ftp/data/processed/' + filename)



    # Run the import process
    import_csv_files()
    query_data('4G')
         
if __name__ == "__main__":
    main()
