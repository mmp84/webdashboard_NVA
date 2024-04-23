import streamlit as st
st.write("Hello, World! This is a demo page.")
import os
import pandas as pd
import sys
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
ftp_data_dir = os.path.join(script_dir, '..', '..','ftp', 'data', 'daily')




def read_csv():
    #filename contains 4G.csv     
 
    for filename in os.listdir(ftp_data_dir):
        if filename.endswith('.csv'):
            csv_file_path = os.path.join(ftp_data_dir, filename)
            try:
                if 'LTE' in filename:
                    df4g = pd.read_csv(csv_file_path, skiprows=6)
                    df4g = df4g.iloc[:-1]
                    st.write(df4g.head(5))
                elif 'UMTS' in filename:
                    df3g = pd.read_csv(csv_file_path, skiprows=6)
                    df3g = df3g.iloc[:-1]   
                    st.write(df3g.head())
                elif '2G' in filename:
                    df2g = pd.read_csv(csv_file_path, skiprows=6)
                    df2g = df2g.iloc[:-1]
                    st.write(df2g.head())
                elif 'NR' in filename:
                    df5g = pd.read_csv(csv_file_path, skiprows=6)
                    df5g = df5g.iloc[:-1]
                    st.write(df5g.head())
                elif 'NB-IOT' in filename:
                    dfiot = pd.read_csv(csv_file_path,skiprows=6)
                    dfiot = dfiot.iloc[:-1] 
                    st.write(dfiot.head())
                else:
                    st.write("No matching table found.")
            except Exception as e:
                st.write(f"Error reading CSV file: {e}")    
read_csv()


