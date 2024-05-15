import zipfile
import sqlite3
import pandas as pd
import os
import logging
import sys
import shutil
import mysql.connector
from sqlalchemy import create_engine


# Connect to MySQL database
engine = create_engine('mysql+mysqlconnector://root:Mobily123@10.27.64.25:3306/dash')

mydb =  mysql.connector.connect(
    host= "10.27.64.25",
    port = "3306",
    user= "root",
    password= "Mobily123",
    database= "dash"
)

mycursor = mydb.cursor()

# Set up logging
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
ftp_data_dir = os.path.join(script_dir, '..', '..','ftp', 'data')

log_file_path = os.path.join(ftp_data_dir, 'email_attachment_downloader.log')
try:
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')  
except Exception as e:
    print(f"Error setting up logging: {e}")


download_folder = ftp_data_dir
csv_folder_path = ftp_data_dir
# SQLite database file
sqlite_db_path = os.path.join(ftp_data_dir, 'database.sqlite')

def unzip_file(file_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(download_folder)
        logging.info(f'Unzipped: {file_path}')



# Function to determine the appropriate table based on the filename
def determine_table_name(filename):
    if '2G_geo' in filename:
        return '2G'
    elif '3G_geo' in filename:
        return '3G'
    elif '4G_geo' in filename:
        return '4G'
    elif '5G_geo' in filename:
        return '5G'
    elif '2G_daily_kpi' in filename:
        return '2g_kpi_daily'
    elif 'UMTS_daily_kpi' in filename:
        return '3g_kpi_daily'
    elif '4G_daily_kpi' in filename:
        return '4g_kpi_daily'
    elif 'NR_daily_kpi' in filename:
        return '5g_kpi_daily'
    elif 'VoLTE_daily_kpi' in filename:
        return 'volte_kpi_daily'
    elif 'NB-IOT_daily_kpi' in filename:
        return 'nbiot_kpi_daily'
    elif '2G_hrly_kpi' in filename:
        return '2g_kpi_hourly'
    elif 'UMTS_hrly_kpi' in filename:
        return '3g_kpi_hourly'
    elif '4G_hrly_kpi' in filename:
        return '4g_kpi_hourly'
    elif 'NR_hrly_kpi' in filename:
        return '5g_kpi_hourly'
    elif 'VoLTE_hrly_kpi' in filename:
        return 'volte_kpi_hourly'
    elif 'NB-IOT_hrly_kpi' in filename:
        return 'nbiot_kpi_hourly' 

    elif 'license' in filename:
        return 'license'
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
                            # replace NIL values with np.nan
                            df.replace('NIL', pd.NA, inplace=True)
                            df.to_sql(table_name, conn, if_exists='append', index=False)
                            write_to_mysql(df,table_name)
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
def write_to_mysql(df,table_name):
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Appended {table_name} to MySQL database.")
    except Exception as e:
        logging.error(f"Error writing {table_name} to MySQL database: {e}")
# Main function     

    

def main():
    # unzipping the file
    #print current working directory
    print(os.getcwd())
    for filename in os.listdir(download_folder):
        if filename.endswith('.zip'):
            file_path = os.path.join(download_folder, filename)
            unzip_file(file_path)
            processed_folder = os.path.join(ftp_data_dir, 'processed')
            os.rename(file_path, os.path.join(processed_folder, filename))


    # License Module 
    for filename in os.listdir(csv_folder_path):
        if filename.endswith('.csv'):
            if '4GRRC' in filename:
            #move the license file to license folder
                print("license file found")
                lic_dir = os.path.join(ftp_data_dir, 'license')   
                destination_path = os.path.join(lic_dir, 'LTE_license_stats.csv')
            elif '5GRRC' in filename:
                print("5G file found")
                lic_dir = os.path.join(ftp_data_dir, 'license')
                destination_path = os.path.join(lic_dir, '5G_license_stats.csv')
            elif '4G_hourly' in filename:
                print("4G hourly file found")
                lic_dir = os.path.join(ftp_data_dir, 'license')
                destination_path = os.path.join(lic_dir, '4G_hourly.csv')
            elif '5G_hourly' in filename:
                print("5G hourly file found")
                lic_dir = os.path.join(ftp_data_dir, 'license')
                destination_path = os.path.join(lic_dir, '5G_hourly.csv')
            else:
                destination_path = None
            if destination_path is not None:
                source_path = os.path.join(csv_folder_path, filename)
                if os.path.exists(destination_path):
                            os.remove(destination_path)  # Remove existing file before moving
                shutil.move(source_path, destination_path)


         
                

        
    import_csv_files()
if __name__ == '__main__':
    main()

