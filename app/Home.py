from __future__ import (absolute_import, division, print_function)
import base64
import time
import sqlite3
import streamlit as st
from streamlit_folium import folium_static, st_folium
import folium
from folium.plugins import HeatMap
from folium.plugins import GroupedLayerControl
import geopandas as gpd 
import pandas as pd
from shapely.geometry import Point, Polygon
import numpy as np
import branca.colormap as cm
from folium.features import DivIcon
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta
import plotly.express as px
from plotly.subplots import make_subplots
import mysql.connector
from sqlalchemy import create_engine

from folium import Marker

from jinja2 import Template

st.set_page_config( page_title= "Network Visual Analytics", layout="wide")
@st.cache_data
def get_image_as_base64(path):
    with open(path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()
# Path to your local logo image file
local_logo_path = 'app/assets/mobily.png'
# Convert the image to base64
logo_base64 = get_image_as_base64(local_logo_path)
# Display the logo using HTML with base64
# add some white space to the sidebar
  
st.sidebar.markdown(f'<img src="data:image/png;base64,{logo_base64}" alt="Logo" style="height: 70px; width: 100px;">', unsafe_allow_html=True)
#tempdf = pd.read_excel(excel_file_path)
st.sidebar.write("")
st.sidebar.write("")

st.sidebar.page_link("Home.py", label ="Home")
st.sidebar.page_link("pages/2_License Utilization.py", label = "License Utilization")
st.sidebar.page_link("pages/1_Daily Dashboard.py", label = "Daily Dashboard")
st.sidebar.page_link("pages/3_Hourly Dashboard.py", label = "Hourly Dashboard")
st.sidebar.divider()

st_autorefresh(interval=60 * 60 * 1000, key="dataframerefresh")
APP_TITLE = "Network Visual Analytics"
sqlite_db_path = '../ftp/data/database.sqlite'
file_path = '/assets'
#initiate selected_sites, show_labels and show_sites and colormap
selected_sites = []
show_labels = False
show_sites = False

with open('app/style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
# Function to assign color based on Selected KPI
@st.cache_data
def style_function(feature, selected_kpi, _colormap):
    start_time = time.time()
    try:
        # sector = feature['properties'].get('Sector', 'Unknown')  # Use 'Unknown' or similar default if 'Sector' is not found

        value = feature['properties'].get(selected_kpi)
        if value is None or pd.isna(value):
            # Provide a default styling for None values or skip styling
            return {
                'fillColor': 'lightgrey',  # Default color for missing values
                'color': 'lightgrey',
                'weight': 0.5,
                'fillOpacity': 0.7
            }
        else:
            color = _colormap(value)
            # st.write(f"Sector: {sector}, {selected_kpi}: {value}, Color: {color}")
            return {
                'fillColor': color,
                'color': color,
                'weight': 0.5,
                'fillOpacity': 0.8
            }
    except Exception as e:
        print(f"Error in style_function: {e}")
        print(f"Feature data: {feature}")  
  

# Function to create wedge polygons
@st.cache_data
def create_wedges(sitesdf):
    start_time = time.time()
    def create_single_wedge(row):
        num_points = 30  # number of points to define the wedge
        angle_width = 65
        radius = 0.002
        # Convert geographic azimuth (clockwise from north) to mathematical angle (counter-clockwise from east)
        math_azimuth = (450 - row['azimuth']) % 360
        adjusted_azimuth = np.radians(math_azimuth)
        angles = np.linspace(adjusted_azimuth - np.radians(angle_width / 2), 
                             adjusted_azimuth + np.radians(angle_width / 2), 
                             num_points)
        wedge_points = [(row['long'] + radius * np.cos(a), row['lat'] + radius * np.sin(a)) for a in angles]
        wedge_points = [(row['long'], row['lat'])] + wedge_points + [(row['long'], row['lat'])]
        return Polygon(wedge_points)

    # start_time = time.time()
    sitesdf['geometry'] = sitesdf.apply(create_single_wedge, axis=1)
    # st.write("Time of wedge creation:", time.time() - start_time)
    
    return sitesdf






##-----------------------------Data Wrangling -----------------------------------------------------------------------------------------------
@st.cache_data
def load_site_data():
    try:
        sitedf = pd.read_csv('app/assets/site_data.csv')
    except UnicodeDecodeError:
        sitedf = pd.read_csv('app/assets/site_data.csv', encoding='ISO-8859-1')
    sitedf.columns = [col.strip() for col in sitedf.columns]
    return sitedf


@st.cache_data(ttl=3600)
def load_and_process_data():
    # load the data from the sqlite database only last 7 days
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    tables = ['2G', '3G', '4G', '5G']  # List of table names
    # tables = ['2G']
    dataframes = {}  # Dictionary to store DataFrames

    with sqlite3.connect(sqlite_db_path) as conn:
        seven_days_ago = datetime.now() - timedelta(days=7)
        formatted_date = seven_days_ago.strftime('%m/%d/%Y')
        for table in tables:
                query = f'''
                        SELECT *
                        FROM "{table}"
                        WHERE Date >= '{formatted_date}'
                    '''       
        try:
            dataframes[table] = pd.read_sql_query(query, conn)
            dataframes[table].replace('NIL', np.nan, inplace= True)
        except Exception as e:
            print(f"Error while fetching data from table {table}:{e}")

    dataframes['2G']['Cell CI'] = dataframes['2G']['Cell CI'].astype('int64', errors='ignore')
    dataframes['3G']['Cell ID'] = dataframes['3G']['Cell ID'].astype('int64', errors='ignore')
    dataframes['4G']['LocalCell Id'] = dataframes['4G']['LocalCell Id'].astype('int64', errors='ignore')
    dataframes['5G']['NR Cell ID'] = dataframes['5G']['NR Cell ID'].astype('int64', errors='ignore')
    # dataframes['2G'].shape
    # dataframes['3G'].shape


    return dataframes
#-------------------------- Sector Level Loading and Processing Data --------------------------------------------------------------
@st.cache_data
def load_and_process_data_sector(sector_name):
    # load the data from the sqlite database only last 7 days
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    #take the first 5 characters of the sector name
    sector_name = sector_name[:5]
    # st.write("Sector Name is ", sector_name)
    tables = ['2G', '3G', '4G', '5G']
    dataframes = {}  # Dictionary to store DataFrames
    with sqlite3.connect(sqlite_db_path) as conn:
        seven_days_ago = datetime.now() - timedelta(days=7)
        formatted_date = seven_days_ago.strftime('%m/%d/%Y')
        for table in tables:
            query = f'''
                        SELECT *
                        FROM "{table}"
                        WHERE Date >= '{formatted_date}' AND "Cell Name" LIKE '{sector_name}%'
             '''
            # st.write(query)                  
            dataframes[table] = pd.read_sql_query(query, conn)
            dataframes[table].replace('NIL', np.nan, inplace= True)
    dataframes['2G']['Cell CI'] = dataframes['2G']['Cell CI'].astype('int64', errors='ignore')
    dataframes['3G']['Cell ID'] = dataframes['3G']['Cell ID'].astype('int64', errors='ignore')
    dataframes['4G']['LocalCell Id'] = dataframes['4G']['LocalCell Id'].astype('int64', errors='ignore')
    dataframes['5G']['NR Cell ID'] = dataframes['5G']['NR Cell ID'].astype('int64', errors='ignore')
    # st.write(dataframes['4G'].head(21))   
    return dataframes
@st.cache_data
def create_kpis_sector(df4G, df3G, df2G, df5G):
        #df5G['Sector'] = df5G.apply(lambda row: row['gNodeB Name'][:5] + '_' + str(row['NR Cell ID'] % 3), axis=1)
    if df5G.empty == False:
        # df5G['Sector'] = df5G.apply(lambda row: row['gNodeB Name'][:5] + '_' + str(row['NR Cell ID'] % 3), axis=1)
        df5G['Sector'] = df5G.apply(lambda row: row['Cell Name'][-5] if row['Cell Name'].lower().endswith('new') else row['Cell Name'][-1:], axis=1)
        df5G['Sector'] = df5G['Sector'].str.upper()
        # drop rows where 'Sector' is non alphabetic
        df5G = df5G[df5G['Sector'].str.isalpha()]
        df5G['Sector'] = df5G.apply(lambda row: row['gNodeB Name'][:5] + '_' + row['Sector'], axis=1)
        df5G['N.UL.NI.Avg(dBm)'] = pd.to_numeric(df5G['N.UL.NI.Avg(dBm)'], errors='coerce')
        grouped_df5G = df5G.groupby(['Date', 'Time', 'Sector']).agg({
        '5G_H_Total Traffic (GB)': 'sum',
        'N.User.NsaDc.PSCell.Avg': 'sum',
        'N.UL.NI.Avg(dBm)': 'max',
        'N.Cell.Unavail.Dur.System(s)' : 'max', 
        'N.ThpVol.DL(kbit)' : 'sum',
        'N.ThpVol.DL.LastSlot(kbit)' : 'sum',
        'N.ThpTime.DL.RmvLastSlot(microsecond)' : 'sum',
        'N.ThpVol.UL(kbit)' : 'sum',
        'N.ThpVol.UE.UL.SmallPkt(kbit)' : 'sum',
        'N.ThpTime.UE.UL.RmvSmallPkt(microsecond)' : 'sum'
    }).reset_index()
    else:
        grouped_df5G = pd.DataFrame(columns= df5G.columns)
        #drop the columns Cell Name and Integirty
        grouped_df5G = grouped_df5G.drop(columns= ['Cell Name', 'Integrity', 'gNodeB Name', 'NR Cell ID', 'gNodeB Function Name'])
        grouped_df5G['Sector'] = None
     


    # df5G['N.UL.NI.Avg(dBm)'].unique()
    if df4G.empty == False:
        # df4G['Sector'] = df4G.apply(lambda row: row['eNodeB Name'][:5] + '_' + str(row['LocalCell Id'] % 3), axis=1)
        df4G['Sector'] = df4G.apply(lambda row: row['Cell Name'][-5] if row['Cell Name'].lower().endswith('new') else row['Cell Name'][-1:], axis=1)
        df4G['Sector'] = df4G['Sector'].str.upper()
        # drop rows where 'Sector' is non alphabetic
        df4G = df4G[df4G['Sector'].str.isalpha()]
        df4G['Sector'] = df4G.apply(lambda row: row['eNodeB Name'][:5] + '_' + row['Sector'], axis=1)
        df4G['L.UL.Interference.Avg(dBm)'] = pd.to_numeric(df4G['L.UL.Interference.Avg(dBm)'], errors='coerce')

        # Grouping by 'Date', 'Time', 'Sector'
        grouped_df4G = df4G.groupby(['Date', 'Time', 'Sector']).agg({
            'Total Traffic Volume (GB)': 'sum',
            'L.Traffic.User.Avg': 'sum',
            'L.ChMeas.PRB.DL.Used.Avg': 'sum',
            'L.ChMeas.PRB.DL.Avail': 'sum',
            'L.Thrp.bits.DL(bit)': 'sum',
            'L.Thrp.bits.DL.LastTTI(bit)': 'sum',
            'L.Thrp.Time.DL.RmvLastTTI(ms)': 'sum',
            'L.Thrp.bits.UL(bit)': 'sum',
            'L.Thrp.Time.UL(ms)': 'sum',
            'VoLTE_Traffic (Erlang)': 'sum',
            'L.UL.Interference.Avg(dBm)': 'max', 
            'L.Cell.Unavail.Dur.Sys(s)': 'max'
        }).reset_index()
    else:
        grouped_df4G = pd.DataFrame(columns= df4G.columns)
        grouped_df4G = grouped_df4G.drop(columns= ['Cell Name', 'Integrity'])
        grouped_df4G['Sector'] = None
    if df3G.empty == False:
        # df3G['Sector'] = df3G.apply(lambda row: row['NodeB Name'][:5] + '_' + str(int(str(row['Cell ID'])[-1])-1 % 3), axis=1)
        df3G['Sector'] = df3G.apply(lambda row: row['Cell Name'][-5] if row['Cell Name'].lower().endswith('new') else row['Cell Name'][-1:], axis=1)
        df3G['Sector'] = df3G['Sector'].str.upper()
        # drop rows where 'Sector' is non alphabetic
        df3G = df3G[df3G['Sector'].str.isalpha()]
        df3G['Sector'] = df3G.apply(lambda row: row['NodeB Name'][:5] + '_' + row['Sector'], axis=1)
        grouped_df3G = df3G.groupby(['Date', 'Time', 'Sector']).agg({
            'Mab_PS total traffic_GB(GB)': 'sum',
            'Mab_AMR.Erlang.BestCell(Erl)(Erl)': 'sum',
            'VS.MeanRTWP(dBm)': 'max',
            'VS.Cell.UnavailTime.Sys(s)' : 'max'
        }).reset_index()
    else:
        grouped_df3G = pd.DataFrame(columns= df3G.columns)
        grouped_df3G = grouped_df3G.drop(columns= ['Cell Name', 'Integrity', 'RNC', 'Cell ID', 'NodeB Name'])
        grouped_df3G['Sector'] = None

    if df2G.empty == False:
        # df2G['Sector'] = df2G.apply(lambda row: row['Cell Name'][:5] + '_' + str(int(str(row['Cell CI'])[-1])-1 % 3), axis=1)
        df2G['Sector'] = df2G.apply(lambda row: row['Cell Name'][-5] if row['Cell Name'].lower().endswith('new') else row['Cell Name'][-1:], axis=1)
        df2G['Sector'] = df2G['Sector'].str.upper()
        # drop rows where 'Sector' is non alphabetic
        df2G = df2G[df2G['Sector'].str.isalpha()]
        df2G['Sector'] = df2G.apply(lambda row: row['Cell Name'][:5] + '_' + row['Sector'], axis=1)
        df2G['AM_PS Traffic MB'] = df2G['AM_PS Traffic MB'] /1000
        df2G = df2G.rename(columns={'AM_PS Traffic MB': 'PS Traffic GB'})   
    else:
        df2G = pd.DataFrame(columns= df2G.columns)
        #add a column to the dataframe
        df2G['Sector'] = None
        df2G = df2G.rename(columns={'AM_PS Traffic MB': 'PS Traffic GB'})


  

    mergeddf = pd.merge(grouped_df4G,grouped_df3G, on =['Date', 'Time', 'Sector'], how='outer')
    mergeddf = pd.merge(mergeddf, df2G, on = ['Date', 'Time', 'Sector'], how='outer')
    mergeddf = pd.merge(mergeddf, grouped_df5G, on = ['Date', 'Time', 'Sector'], how='outer')
    # st.write(mergeddf.columns)
    Columnstodrop = ['GBSC','Cell CI', 'Cell Name', 'CellIndex', 'Integrity']
    mergeddf = mergeddf.drop(columns= Columnstodrop).reset_index(drop=True)  
    # mergeddf['L.UL.Interference.Avg(dBm)'] =  mergeddf['L.UL.Interference.Avg(dBm)'] .fillna(-120)
    # mergeddf['N.UL.NI.Avg(dBm)'] =  mergeddf['N.UL.NI.Avg(dBm)'].fillna(-116)
    # mergeddf['VS.MeanRTWP(dBm)'] =  mergeddf['VS.MeanRTWP(dBm)'].fillna(-112)
    mergeddf = mergeddf.fillna(np.nan)

    #---------------------------------------KPI Creation -----------------------------------------------------------------------------
    # Adding a new column 'DL User Throughput' based on the given equation
    mergeddf['LTE DL User Throughput Mbps'] = ((mergeddf['L.Thrp.bits.DL(bit)'] - mergeddf['L.Thrp.bits.DL.LastTTI(bit)']) / 1000000) / (mergeddf['L.Thrp.Time.DL.RmvLastTTI(ms)'] / 1000)

    mergeddf['LTE UL User Throughput Mbps'] = (mergeddf['L.Thrp.bits.UL(bit)'] / 1000000) / (mergeddf['L.Thrp.Time.UL(ms)'] / 1000)
    mergeddf['LTE PRB Utilization'] = mergeddf['L.ChMeas.PRB.DL.Used.Avg']/mergeddf['L.ChMeas.PRB.DL.Avail']*100
    # mergeddf['Total CS Traffic Earlang'] = mergeddf['VoLTE_Traffic (Erlang)']+ mergeddf['K3014:Traffic Volume on TCH(Erl)'] + mergeddf['Mab_AMR.Erlang.BestCell(Erl)(Erl)']
    mergeddf['Total CS Traffic Earlang'] = np.nan_to_num(mergeddf['VoLTE_Traffic (Erlang)'], nan=0) + \
                                       np.nan_to_num(mergeddf['K3014:Traffic Volume on TCH(Erl)'], nan=0) + \
                                       np.nan_to_num(mergeddf['Mab_AMR.Erlang.BestCell(Erl)(Erl)'], nan=0)
    # mergeddf['Total PS Traffic GB'] = mergeddf['Total Traffic Volume (GB)'] + mergeddf['5G_H_Total Traffic (GB)']+mergeddf['PS Traffic GB']+ mergeddf['Mab_PS total traffic_GB(GB)']
    mergeddf['Total PS Traffic GB'] = np.nan_to_num(mergeddf['Total Traffic Volume (GB)'], nan=0) + \
                                  np.nan_to_num(mergeddf['5G_H_Total Traffic (GB)'], nan=0) + \
                                  np.nan_to_num(mergeddf['PS Traffic GB'], nan=0) + \
                                  np.nan_to_num(mergeddf['Mab_PS total traffic_GB(GB)'], nan=0)    
    mergeddf['4G Users'] = mergeddf['L.Traffic.User.Avg']
    mergeddf['5G Users'] = mergeddf['N.User.NsaDc.PSCell.Avg']
    mergeddf['3G RTWP'] = mergeddf['VS.MeanRTWP(dBm)']
    mergeddf['LTE UL Interference (dBm)'] = mergeddf['L.UL.Interference.Avg(dBm)']
    mergeddf['5G UL Interference (dBm)'] = mergeddf['N.UL.NI.Avg(dBm)']
    mergeddf['4G Availability'] = (1 - mergeddf['L.Cell.Unavail.Dur.Sys(s)']/3600)*100
    mergeddf['5G Availability'] = (1 - mergeddf['N.Cell.Unavail.Dur.System(s)']/3600)*100
    mergeddf['3G Availability'] = (1 - mergeddf['VS.Cell.UnavailTime.Sys(s)']/3600)*100
    mergeddf['2G Availability'] = (1 - mergeddf['R373:Cell Out-of-Service Duration(s)']/3600)*100
    mergeddf['2G Interference'] = mergeddf['2G_interference_samples(%)']   
    mergeddf['NR DL User Throughput Mbps'] = (np.nan_to_num(mergeddf['N.ThpVol.DL(kbit)'],nan=0) - np.nan_to_num(mergeddf['N.ThpVol.DL.LastSlot(kbit)'] , nan = 0)) / mergeddf['N.ThpTime.DL.RmvLastSlot(microsecond)'] * 1000
    mergeddf['NR UL User Throughput Mbps'] = (np.nan_to_num(mergeddf['N.ThpVol.UL(kbit)'], nan=0) - np.nan_to_num(mergeddf['N.ThpVol.UE.UL.SmallPkt(kbit)'], nan =0)) / mergeddf['N.ThpTime.UE.UL.RmvSmallPkt(microsecond)']*  1000


    # st.write("Time of creating KPIs", time.time())
    # st.write("Time of creating KPIs:", time.time() - start_time)
    return mergeddf



#----------------------End of Sector Level Loading and Processing Data --------------------------------------------------------------
@st.cache_data
def create_kpis(df4G, df3G, df2G, df5G):
    start_time = time.time()

    #df5G['Sector'] = df5G.apply(lambda row: row['gNodeB Name'][:5] + '_' + str(row['NR Cell ID'] % 3), axis=1)
  
    df5G['Sector'] = df5G.apply(lambda row: row['Cell Name'][-5] if row['Cell Name'].lower().endswith('new') else row['Cell Name'][-1:], axis=1)
    df5G['Sector'] = df5G['Sector'].str.upper()
    # drop rows where 'Sector' is non alphabetic
    df5G = df5G[df5G['Sector'].str.isalpha()]
    df5G['Sector'] = df5G.apply(lambda row: row['gNodeB Name'][:5] + '_' + row['Sector'], axis=1)

    # df4G['Sector'] = df4G.apply(lambda row: row['eNodeB Name'][:5] + '_' + str(row['LocalCell Id'] % 3), axis=1)
    df4G['Sector'] = df4G.apply(lambda row: row['Cell Name'][-5] if row['Cell Name'].lower().endswith('new') else row['Cell Name'][-1:], axis=1)
    df4G['Sector'] = df4G['Sector'].str.upper()
    # drop rows where 'Sector' is non alphabetic
    df4G = df4G[df4G['Sector'].str.isalpha()]
    df4G['Sector'] = df4G.apply(lambda row: row['eNodeB Name'][:5] + '_' + row['Sector'], axis=1)
    

    df4G['L.UL.Interference.Avg(dBm)'] = pd.to_numeric(df4G['L.UL.Interference.Avg(dBm)'], errors='coerce')
    df5G['N.UL.NI.Avg(dBm)'] = pd.to_numeric(df5G['N.UL.NI.Avg(dBm)'], errors='coerce')

    # Grouping by 'Date', 'Time', 'Sector'
    grouped_df4G = df4G.groupby(['Date', 'Time', 'Sector']).agg({
        'Total Traffic Volume (GB)': 'sum',
        'L.Traffic.User.Avg': 'sum',
        'L.ChMeas.PRB.DL.Used.Avg': 'sum',
        'L.ChMeas.PRB.DL.Avail': 'sum',
        'L.Thrp.bits.DL(bit)': 'sum',
        'L.Thrp.bits.DL.LastTTI(bit)': 'sum',
        'L.Thrp.Time.DL.RmvLastTTI(ms)': 'sum',
        'L.Thrp.bits.UL(bit)': 'sum',
        'L.Thrp.Time.UL(ms)': 'sum',
        'VoLTE_Traffic (Erlang)': 'sum',
        'L.UL.Interference.Avg(dBm)': 'max', 
        'L.Cell.Unavail.Dur.Sys(s)': 'max',
    }).reset_index()
    # df3G['Sector'] = df3G.apply(lambda row: row['NodeB Name'][:5] + '_' + str(int(str(row['Cell ID'])[-1])-1 % 3), axis=1)
    df3G['Sector'] = df3G.apply(lambda row: row['Cell Name'][-5] if row['Cell Name'].lower().endswith('new') else row['Cell Name'][-1:], axis=1)
    df3G['Sector'] = df3G['Sector'].str.upper()
    # drop rows where 'Sector' is non alphabetic
    df3G = df3G[df3G['Sector'].str.isalpha()]
    df3G['Sector'] = df3G.apply(lambda row: row['NodeB Name'][:5] + '_' + row['Sector'], axis=1)
    grouped_df3G = df3G.groupby(['Date', 'Time', 'Sector']).agg({
        'Mab_PS total traffic_GB(GB)': 'sum',
        'Mab_AMR.Erlang.BestCell(Erl)(Erl)': 'sum',
        'VS.MeanRTWP(dBm)': 'max', 
        'VS.Cell.UnavailTime.Sys(s)' : 'max'
    }).reset_index()

    grouped_df5G = df5G.groupby(['Date', 'Time', 'Sector']).agg({
        '5G_H_Total Traffic (GB)': 'sum',
        'N.User.NsaDc.PSCell.Avg': 'sum',
        'N.UL.NI.Avg(dBm)': 'max',
        'N.Cell.Unavail.Dur.System(s)' : 'max',
        'N.ThpVol.DL(kbit)' : 'sum',
        'N.ThpVol.DL.LastSlot(kbit)' : 'sum',
        'N.ThpTime.DL.RmvLastSlot(microsecond)' : 'sum',
        'N.ThpVol.UL(kbit)' : 'sum',
        'N.ThpVol.UE.UL.SmallPkt(kbit)' : 'sum',
        'N.ThpTime.UE.UL.RmvSmallPkt(microsecond)' : 'sum'
    }).reset_index()


    # df2G['Sector'] = df2G.apply(lambda row: row['Cell Name'][:5] + '_' + str(int(str(row['Cell CI'])[-1])-1 % 3), axis=1)
    df2G['Sector'] = df2G.apply(lambda row: row['Cell Name'][-5] if row['Cell Name'].lower().endswith('new') else row['Cell Name'][-1:], axis=1)
    df2G['Sector'] = df2G['Sector'].str.upper()
    # drop rows where 'Sector' is non alphabetic
    df2G = df2G[df2G['Sector'].str.isalpha()]
    df2G['Sector'] = df2G.apply(lambda row: row['Cell Name'][:5] + '_' + row['Sector'], axis=1)
    df2G['AM_PS Traffic MB'] = df2G['AM_PS Traffic MB'] /1000
    df2G = df2G.rename(columns={'AM_PS Traffic MB': 'PS Traffic GB'})

    mergeddf = pd.merge(grouped_df4G,grouped_df3G, on =['Date', 'Time', 'Sector'], how='outer')
    mergeddf = pd.merge(mergeddf, df2G, on = ['Date', 'Time', 'Sector'], how='outer')
    mergeddf = pd.merge(mergeddf, grouped_df5G, on = ['Date', 'Time', 'Sector'], how='outer')
    Columnstodrop = ['GBSC','Cell CI', 'Cell Name', 'CellIndex', 'Integrity']
    mergeddf = mergeddf.drop(columns= Columnstodrop).reset_index(drop=True)

    # mergeddf['L.UL.Interference.Avg(dBm)'] =  mergeddf['L.UL.Interference.Avg(dBm)'] .fillna(-120)
    # mergeddf['N.UL.NI.Avg(dBm)'] =  mergeddf['N.UL.NI.Avg(dBm)'].fillna(-116)
    # mergeddf['VS.MeanRTWP(dBm)'] =  mergeddf['VS.MeanRTWP(dBm)'].fillna(-112)
    # mergeddf = mergeddf.fillna(0)


    #---------------------------------------KPI Creation -----------------------------------------------------------------------------
    # Adding a new column 'DL User Throughput' based on the given equation
    mergeddf['LTE DL User Throughput Mbps'] = ((mergeddf['L.Thrp.bits.DL(bit)'] - mergeddf['L.Thrp.bits.DL.LastTTI(bit)']) / 1000000) / (mergeddf['L.Thrp.Time.DL.RmvLastTTI(ms)'] / 1000)

    mergeddf['LTE UL User Throughput Mbps'] = (mergeddf['L.Thrp.bits.UL(bit)'] / 1000000) / (mergeddf['L.Thrp.Time.UL(ms)'] / 1000)
    mergeddf['LTE PRB Utilization'] = mergeddf['L.ChMeas.PRB.DL.Used.Avg']/mergeddf['L.ChMeas.PRB.DL.Avail']*100
    # mergeddf['Total CS Traffic Earlang'] = mergeddf['VoLTE_Traffic (Erlang)']+ mergeddf['K3014:Traffic Volume on TCH(Erl)'] + mergeddf['Mab_AMR.Erlang.BestCell(Erl)(Erl)']
    mergeddf['Total CS Traffic Earlang'] = np.nan_to_num(mergeddf['VoLTE_Traffic (Erlang)'], nan=0) + \
                                       np.nan_to_num(mergeddf['K3014:Traffic Volume on TCH(Erl)'], nan=0) + \
                                       np.nan_to_num(mergeddf['Mab_AMR.Erlang.BestCell(Erl)(Erl)'], nan=0)
   
    
    mergeddf['Total PS Traffic GB'] = np.nan_to_num(mergeddf['Total Traffic Volume (GB)'], nan=0) + \
                                  np.nan_to_num(mergeddf['5G_H_Total Traffic (GB)'], nan=0) + \
                                  np.nan_to_num(mergeddf['PS Traffic GB'], nan=0) + \
                                  np.nan_to_num(mergeddf['Mab_PS total traffic_GB(GB)'], nan=0)
    
    mergeddf['4G Users'] = mergeddf['L.Traffic.User.Avg']
    mergeddf['5G Users'] = mergeddf['N.User.NsaDc.PSCell.Avg']
    mergeddf['3G RTWP'] = mergeddf['VS.MeanRTWP(dBm)']
    mergeddf['LTE UL Interference (dBm)'] = mergeddf['L.UL.Interference.Avg(dBm)']
    mergeddf['5G UL Interference (dBm)'] = mergeddf['N.UL.NI.Avg(dBm)']
    mergeddf['4G Availability'] = (1 - mergeddf['L.Cell.Unavail.Dur.Sys(s)']/3600)*100
    mergeddf['5G Availability'] = (1 - mergeddf['N.Cell.Unavail.Dur.System(s)']/3600)*100
    mergeddf['3G Availability'] = (1 - mergeddf['VS.Cell.UnavailTime.Sys(s)']/3600)*100 
    mergeddf['2G Availability'] = (1 - mergeddf['R373:Cell Out-of-Service Duration(s)']/3600)*100
    mergeddf['2G Interference'] = mergeddf['2G_interference_samples(%)']
    mergeddf['NR DL User Throughput Mbps'] = (np.nan_to_num(mergeddf['N.ThpVol.DL(kbit)'],nan=0) - np.nan_to_num(mergeddf['N.ThpVol.DL.LastSlot(kbit)'] , nan = 0)) / mergeddf['N.ThpTime.DL.RmvLastSlot(microsecond)'] * 1000
    mergeddf['NR UL User Throughput Mbps'] = (np.nan_to_num(mergeddf['N.ThpVol.UL(kbit)'], nan=0) - np.nan_to_num(mergeddf['N.ThpVol.UE.UL.SmallPkt(kbit)'], nan =0)) / mergeddf['N.ThpTime.UE.UL.RmvSmallPkt(microsecond)']*  1000
    # st.write("Time of creating KPIs", time.time())
    # st.write("Time of creating KPIs:", time.time() - start_time)
    return mergeddf
# @st.cache_data






# # Helper functions
# def validate_coordinates(data):
#     """Ensure coordinates are in the correct format."""
#     if not all(isinstance(i, (list, tuple)) and len(i) == 2 for i in data):
#         raise ValueError("Data should be a list of [lat, lon] pairs.")
#     return data

# def parse_options(**kwargs):
#     """Parse options for folium markers."""
#     options = {k: v for k, v in kwargs.items() if v is not None}
#     return options


# class FastCircles(Marker):
#     """Add circles using in-browser rendering.
#     Parameters
#     ----------
#     data: list
#         List of list of shape [[], []]. E.g. [[lat, lon], [lat, lon]]
#     radius: float, default 10
#         Radius of the circle marker, in pixels.
#     **kwargs:
#         Options you'd also provide to Circle.
#     """
#     def __init__(self, data, radius=10, **kwargs):
#         super(FastCircles, self).__init__([])
#         self._name = 'FastCircles'
#         self._data = validate_coordinates(data)
#         self.options = parse_options(line=False, radius=radius, **kwargs)

#         self._template = Template(u"""
#             {% macro script(this, kwargs) %}
#             (function(){
#                 var data = {{this._data}};
#                 var options = {{ this.options }};
#                 var row, marker;
#                 for (var i = 0; i < data.length; i++) {
#                     row = data[i];
#                     marker = L.circle([row[0], row[1]], options);
#                     marker.addTo({{ this._parent.get_name() }});
#                 }
#             })();
#             {% endmacro %}""")



def create_map_fast(filtered_gdf, selected_kpi, selected_site, nodebstatus, selected_cluster):
    start_time = time.time()
    # if st.session_state['prev_selected_cluster'] != selected_cluster:
    #     st.session_state['map_center'] = None
    #     st.session_state['map_zoom'] = 15
    #     st.session_state['prev_selected_cluster'] = selected_cluster


    try:
        # Determine initial map location
        if selected_site:
            location = filtered_gdf.loc[filtered_gdf['Site'] == selected_site, ['lat', 'long']].values[0]
        elif st.session_state['center'] is not None:
            location = st.session_state['center']
        elif st.session_state['center'] is not None:
            location = st.session_state['center']
    
        else:
            location = [filtered_gdf['lat'].mean(), filtered_gdf['long'].mean()]
        if not location or len(location) != 2 or not all(isinstance(coord, (int, float)) for coord in location):
            raise ValueError("Invalid map center location")

        # Create folium map
        m = folium.Map(location=location, zoom_start= 15, prefer_canvas=True, tiles='cartodbpositron')
        folium.TileLayer(tiles='openstreetmap', control=True, show=False, attr='openstreetmap').add_to(m)

        # Prepare data for markers
        enodebdf = filtered_gdf[['Site', 'lat', 'long']].drop_duplicates().reset_index(drop=True)
        nodebstatus['Site'] = nodebstatus['NodeB Name'].apply(lambda x: x[:5])
        enodebdf = pd.merge(enodebdf, nodebstatus, on='Site', how='left')

        # KPI and color map configuration
        valid_kpi_values = filtered_gdf[selected_kpi].dropna()
        min_kpi_value = valid_kpi_values.min()
        max_kpi_value = valid_kpi_values.max()

        if selected_kpi in ['4G Availability', '5G Availability', '3G Availability', '2G Availability']:
            site_avg_kpi = filtered_gdf.groupby('Site')[selected_kpi].min()
            linear = cm.LinearColormap(['red', 'orange', 'yellow', 'green'], index=[50, 75, 99.5, 100], vmin=0, vmax=100)
        else:
            site_avg_kpi = filtered_gdf.groupby('Site')[selected_kpi].max()
            linear = cm.LinearColormap(['green', 'yellow', 'red'], vmin=min_kpi_value, vmax=max_kpi_value)

        # Feature groups for layers
        fg_labels = folium.FeatureGroup(name='Labels', show=False)
        fg_sites = folium.FeatureGroup(name='Sites', show=False)
        fg_outage = folium.FeatureGroup(name='Outage', show=False)
        fg_fans = folium.FeatureGroup(name='Fans', show=True)

        for _, row in enodebdf.iterrows():
            avg_kpi_value = site_avg_kpi.get(row['Site'], None)
            marker_color = 'grey' if pd.isna(avg_kpi_value) else linear(avg_kpi_value)

            folium.CircleMarker(
                location=[row['lat'], row['long']],
                radius=2.5,
                color=marker_color,
                fill=True,
                fill_color=marker_color,
                fill_opacity=0.5,
                popup=row['Site']
            ).add_to(fg_sites)

            folium.map.Marker(
                [row['lat'], row['long']],
                icon=folium.DivIcon(
                    icon_size=(150, 36),
                    icon_anchor=(0, 0),
                    html=f'<div style="font-size: 12pt">{row["Site"]}</div>'
                )
            ).add_to(fg_labels)
            
            folium.CircleMarker(
                location=[row['lat'], row['long']],
                radius=3.0,
                color='red' if row['Status'] == 'Site_Down' else 'green',
                fill=True,
                fill_color='red' if row['Status'] == 'Site_Down' else 'green',
                fill_opacity=0.5,
                popup=folium.Popup(
                    f'<b>Site:</b> {row["Site"]}<br><b>Status:</b> {row["Status"]}<br>'
                    f'<b>Down Since:</b> {row["Duration_Hours"]} Hours<br>'
                    f'<b>Outage Days During Last Week:</b> {row["Outage_Days_Last_7_Days"]} Days<br>'
                    f'<b>Outage Days During Last 30 Days:</b> {row["Outage_Days_Last_30_Days"]} Days',
                    max_width=300
                )
            ).add_to(fg_outage)

        m.add_child(fg_labels)
        m.add_child(fg_sites)
        m.add_child(fg_outage)
        enodebdf = pd.DataFrame({
                'lat': [34.0522, 36.1699, 40.7128],
                'long': [-118.2437, -115.1398, -74.0060]
            })

        data = enodebdf[['lat', 'long']].values.tolist()
        st.write(data)  
        print("Data to be passed to FastCircles:", data)


        # fast_circles = FastCircles(data=data, radius=2.5, color='blue', fill=True, fill_opacity=0.5)
        # fast_circles.add_to(fg_sites)     

        folium.LayerControl(collapsed=False).add_to(m)
        # GroupedLayerControl(groups={'Layers': [fg_labels, fg_sites, fg_outage, fg_fans]}, exclusive_groups=False, collapsed=False).add_to(m)
        # enable layer control
        # m.add_child(folium.LayerControl())
        # Capture map state
        st_map = st_folium(m, center = st.session_state['center'], zoom = st.session_state['zoom'], feature_group_to_add= fg_sites,
                     
                           
                           returned_objects=["last_active_drawing", "center", "zoom"], use_container_width=True)

        if st_map and 'last_active_drawing' in st_map and st_map['last_active_drawing'] is not None:
            sector_name = st_map['last_active_drawing']['properties'].get('Sector', None)
        else:
            sector_name = None

        # Update session state with current map center and zoom
        if st_map and 'map_center' in st_map and 'zoom' in st_map:
            st.session_state['center'] = [st_map['center']['lat'], st_map['center']['lng']]
            st.session_state['zoom'] = st_map['zoom']

        st.write("Time of creating map:", time.time() - start_time)
        return sector_name

    except Exception as e:
        import sys
        exc_type, exc_obj, tb = sys.exc_info()
        lineno = tb.tb_lineno
        print(lineno)
        

        st.write(f"An error occurred: {e}")
        return None
# # Initialize session state for map parameters
# if 'center' not in st.session_state:
#     st.session_state['center'] = None
# if 'zoom' not in st.session_state:
#     st.session_state['zoom'] = 15

# if 'prev_selected_cluster' not in st.session_state: 
#     st.session_state['prev_selected_cluster'] = None
# if 'selected_region' not in st.session_state:
#     st.session_state['selected_region'] = None
# @st.cache_resource
def init_map(center = [18.219817, 42.503777], zoom = 15, map_type = 'cartodbpositron'):

    return folium.Map(location=center, zoom_start=zoom, prefer_canvas=True, tiles=map_type)
if 'map_center' not in st.session_state:
    st.session_state['map_center'] = None
if 'map_zoom' not in st.session_state:
    st.session_state['map_zoom'] = 15
if 'prev_selected_cluster' not in st.session_state:
    st.session_state['prev_selected_cluster'] = None

        # Check if region, cluster, or site selection has changed


        # Update session state

def create_map(filtered_gdf, selected_kpi, selected_site, nodebstatus, selected_cluster,selected_region):
    # if st.session_state['prev_selected_cluster'] != selected_cluster:
    #     st.session_state['map_center'] = None
    #     st.session_state['map_zoom'] = 15
    #     st.session_state['prev_selected_cluster'] = selected_cluster

    try:
        # Determine initial map location
        if selected_site:
            location = filtered_gdf.loc[filtered_gdf['Site'] == selected_site, ['lat', 'long']].values[0]
      
        else:
            location = [filtered_gdf['lat'].mean(), filtered_gdf['long'].mean()]

        # Create folium map
        m = folium.Map(location=location, zoom_start = 15, prefer_canvas=True, tiles='cartodbpositron')


        openstreetmap = folium.TileLayer(
            tiles = 'openstreetmap',
            control = True,
            show = False,
            attr= 'openstreetmap')
        openstreetmap.add_to(m) 
  
        #enable layer control

        # Adding markers
        enodebdf = filtered_gdf.drop_duplicates('Site')
        enodebdf = enodebdf[['Site', 'lat', 'long']].reset_index()
        valid_kpi_values = filtered_gdf[selected_kpi].dropna()
        min_kpi_value = valid_kpi_values.min()
        max_kpi_value = valid_kpi_values.max()
        # merge the enodebdf with the nodebstatus
        nodebstatus['Site'] = nodebstatus['NodeB Name'].apply(lambda x: x[:5])
        enodebdf = pd.merge(enodebdf, nodebstatus, on='Site', how='left')
        # map_dict = filtered_gdf.set_index('Sector')[selected_kpi].to_dict()          

        if selected_kpi == '4G Availability' or selected_kpi == '5G Availability' or selected_kpi == '3G Availability' or selected_kpi == '2G Availability':            
            site_avg_kpi = filtered_gdf.groupby('Site')[selected_kpi].min()
            linear = cm.LinearColormap(['red', 'orange', 'yellow', 'green'], index= [50,75,99.5,100], vmin = 0, vmax=100)
        else:
            site_avg_kpi = filtered_gdf.groupby('Site')[selected_kpi].max()
            linear = cm.LinearColormap(['green', 'yellow', 'red'], vmin=min_kpi_value, vmax=max_kpi_value)

        fg_labels = folium.FeatureGroup(name='Labels', show=False)
        fg_sites = folium.FeatureGroup(name='Sites', show=True)
        fg_outage = folium.FeatureGroup(name='Outage', show=False)
        for _, row in enodebdf.iterrows():
            avg_kpi_value = site_avg_kpi.get(row['Site'], None)
            # st.write(avg_kpi_value)
            # Check if avg_kpi_value is NaN
            if pd.isna(avg_kpi_value):
                # If avg_kpi_value is NaN, use a default color like gray or skip this iteration
                marker_color = 'grey'
            else:
                marker_color = linear(avg_kpi_value)
            folium.CircleMarker(
                    location=[row['lat'], row['long']],
                    radius=2.5,
                    color=marker_color,
                    fill=True,
                    fill_color=marker_color,
                    fill_opacity=0.5,
                    popup=row['Site']
                ).add_to(fg_sites)
            folium.map.Marker(
                    [row['lat'], row['long']],
                    icon=folium.DivIcon(
                        icon_size=(150, 36),
                        icon_anchor=(0, 0),
                        html=f'<div style="font-size: 12pt">{row["Site"]}</div>'
                    )
                ).add_to(fg_labels)
            folium.CircleMarker(
                    [row['lat'], row['long']],
                    radius=3.0,
                    color='red' if row['Status'] == 'Site_Down' else 'green',
                    fill=True,
                    fill_color='red' if row['Status'] == 'Site_Down' else 'green',
                    fill_opacity=0.5,
                    popup=folium.Popup(
                        f'<b>Site:</b> {row["Site"]}<br><b>Status:</b> {row["Status"]}<br>'
                        f'<b>Down Since:</b> {row["Duration_Hours"]} Hours<br>'
                        f'<b>Outage Days During Last Week:</b> {row["Outage_Days_Last_7_Days"]} Days<br>'
                        f'<b>Outage Days During Last 30 Days:</b> {row["Outage_Days_Last_30_Days"]} Days',
                        max_width=300
                    )).add_to(fg_outage)
     
        filtered_gdf_geojson = filtered_gdf.to_json()  

        fg_sector = folium.FeatureGroup(name='Sectors', show=True)

        folium.GeoJson(filtered_gdf_geojson,
                    name='sectors',
                    style_function=lambda feature: {
                        "fillColor": linear(feature['properties'][selected_kpi]) if feature['properties'][selected_kpi] is not None else 'lightgrey',
                        "nan_fill_color": "lightgrey",
                        "nan_fill_opacity": 0.7,
                        "color": "lightgray",
                        "weight": 0.5,
                        "fillOpacity": 0.7
                    },

                    tooltip=folium.features.GeoJsonTooltip(
                        fields=['Sector', '4G Availability', '4G Users', 'LTE DL User Throughput Mbps', 'LTE UL User Throughput Mbps', 'LTE PRB Utilization', 'LTE UL Interference (dBm)', 'Total CS Traffic Earlang', 'Total PS Traffic GB', '5G Availability', '5G Users', '5G UL Interference (dBm)','NR DL User Throughput Mbps', 'NR UL User Throughput Mbps', '3G Availability', '3G RTWP', '2G Availability', '2G Interference'], labels=True),
                        #   onEachFeature=whenClicked
                        
                    ).add_to(fg_sector)
        #display legend
        linear.add_to(m)

        # for every lat and long in filtered_gdf take max of selected_kpi
        if selected_kpi == '4G Availability' or selected_kpi == '5G Availability' or selected_kpi == '3G Availability' or selected_kpi == '2G Availability':
            df = filtered_gdf.groupby(['lat', 'long'])[selected_kpi].min().reset_index().dropna()
        else:
            df = filtered_gdf.groupby(['lat', 'long'])[selected_kpi].max().reset_index().dropna()

        if selected_kpi == '4G Availability' or selected_kpi == '5G Availability' or selected_kpi == '3G Availability' or selected_kpi == '2G Availability':
            df[selected_kpi] = df[selected_kpi].apply(lambda x: abs(x-100))       
        if selected_kpi == 'LTE UL Interference (dBm)':
            df[selected_kpi] = df[selected_kpi].apply(lambda x: 0 if x <= -105 else x + 105)
        if selected_kpi == '5G UL Interference (dBm)':
            df[selected_kpi] = df[selected_kpi].apply(lambda x: 0 if x <= -105 else x + 105)
        if selected_kpi == '3G RTWP':
            df[selected_kpi] = df[selected_kpi].apply(lambda x: 0 if x <= -95 else x + 95)
        # st.write df at sector_index
        fg_heatmap = folium.FeatureGroup(name='Heat Map', show=False)

        HeatMap(df, min_opacity= 0.05, max_opacity= 0.9, use_local_extrema = False).add_to(folium.FeatureGroup(name='Heat Map', show=True).add_to(fg_heatmap)) 
        m.add_child(fg_sector)
        m.add_child(fg_sites)
        m.add_child(fg_labels)
        m.add_child(fg_outage)
        m.add_child(fg_heatmap)

        folium.LayerControl(collapsed=False).add_to(m)
        GroupedLayerControl(
            groups={'Layers': [fg_sector, fg_sites, fg_labels, fg_outage, fg_heatmap]},
            exclusive_groups=False,
            collapsed=False,
        ).add_to(m)

   
        sector_name = None
        st_map = st_folium(m, returned_objects= ["last_active_drawing"], use_container_width=True)   

        if st_map['last_active_drawing'] is not None:
            sector_name = st_map['last_active_drawing']['properties']['Sector']
            # st.form_submit_button(label='Refresh Map', disabled= True)
           # Update session state with current map center and zoom
    
      

        
        return sector_name
 
    except Exception as e:
        import sys
        # print line number that error occurred
        # exc_type, exc_obj, tb = sys.exc_info()
        # lineno = tb.tb_lineno
        # print(lineno)
        
        print(f"Error in create_map: {e}")
        return None

def show_map(m):
    # st_map = st_folium(m, width=1200, height=600)
    # if st_map['last_active_drawing'] is not None:
    #     sector_name = st_map['last_active_drawing']['properties']['Sector']
    #     return sector_name
    folium_static(m, width=1200, height=600)
    

def display_kpi(df, column, kpi_name):
    value = int(df[kpi_name].sum())
    with column:
        st.metric(label=kpi_name, value=value)

# Function to create gauge chart
# Function to create gauge chart with reference value
def create_gauge_chart(value, max_value, title, reference):
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = value,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': title},
        gauge = {
            'axis': {'range': [None, max_value]},
            'bar': {'color': "blue"},
            'steps': [{'range': [0, reference], 'color': "lightgray"}],  # Reference range
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': reference}
        }
    ))

    # Adjust layout to fit in a single row
    fig.update_layout(autosize=True, height=160, margin={'t': 40, 'b': 10, 'l': 10, 'r': 10})
    return fig


# st.write(mergeddf.columns)
# mergeddf.head()
# st.write("KPI Creation Done")


# Sort the options if they are not already sorted
def get_time_options_for_date(date,gdf):
    return sorted(gdf[gdf['Date'] == date]['Time'].unique())
def KPIs_of_selected_sector(sector_name):
    KPIs_of_interest = ['4G Availability', '4G Users', 'LTE DL User Throughput Mbps', 'LTE UL User Throughput Mbps', 'LTE PRB Utilization', 'LTE UL Interference (dBm)', 'Total CS Traffic Earlang', 'Total PS Traffic GB', '5G Availability', '5G Users', '5G UL Interference (dBm)','NR DL User Throughput Mbps', 'NR UL User Throughput Mbps', '3G Availability', '3G RTWP', '2G Availability', '2G Interference'] 
   
    
    
     # Replace with actual KPI column names
    df = load_and_process_data_sector(sector_name)
    df4G = df['4G']
    df3G = df['3G']
    df2G = df['2G']
    df5G = df['5G']  
    # st.write(df4G['Date'].unique()) 


    selected_sector = create_kpis_sector(df4G, df3G, df2G, df5G) 
    selected_sector = selected_sector[KPIs_of_interest + ['Sector', 'Date', 'Time']]
    # st.write(selected_sector.head())
    col1,col2,col3 = st.columns(3, gap="medium")    
    with col1.container(border=True):
        cs2gtraffic = df['2G']['K3014:Traffic Volume on TCH(Erl)'].sum()
        cs3gtraffic = df['3G']['Mab_AMR.Erlang.BestCell(Erl)(Erl)'].sum()
        cs4gtraffic = df['4G']['VoLTE_Traffic (Erlang)'].sum()
        # pie chart
        fig = px.pie(values=[cs2gtraffic, cs3gtraffic, cs4gtraffic], names=['2G', '3G', '4G'], title='Total CS Traffic Erlang')
        st.plotly_chart(fig, use_container_width=True)
    with col2.container(border=True):
        # ps2gtraffic = df['2G']['AM_PS Traffic MB'].sum()/1000
        ps3gtraffic = df['3G']['Mab_PS total traffic_GB(GB)'].sum()
        ps4gtraffic = df['4G']['Total Traffic Volume (GB)'].sum()
        ps5gtraffic = df['5G']['5G_H_Total Traffic (GB)'].sum()
        # pie chart
        fig = px.pie(values=[ps3gtraffic, ps4gtraffic, ps5gtraffic], names=['3G', '4G', '5G'], title='Total PS Traffic GB')
        st.plotly_chart(fig, use_container_width=True)
    with col3.container(border=True):
        outagecount4G_date = df['4G'][df['4G']['L.Cell.Unavail.Dur.Sys(s)'] != 0].groupby('Date').size().reset_index(name='Outage Count')  
        outageduration4G_date = df['4G'].groupby('Date')['L.Cell.Unavail.Dur.Sys(s)'].sum().reset_index(name='Outage Duration')
        #merge the two dataframes
        outagecount4G_date = pd.merge(outageduration4G_date,outagecount4G_date, on='Date', how='left')  
        # st.write(outagecount4G_date['Date'].unique())

        if outagecount4G_date.empty:
            st.write("    No Outage    ")
        else: 
            # chart for outage count and duration
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=outagecount4G_date['Date'], y=outagecount4G_date['Outage Count'], name='Outage Count', marker_color='blue'), secondary_y=False)
            fig.add_trace(go.Scatter(x=outagecount4G_date['Date'], y=outagecount4G_date['Outage Duration'], mode='lines+markers', name='Outage Duration', marker_color='red'), secondary_y=True)
            fig.update_layout(title='4G Outage Count and Duration', xaxis_title='Date', yaxis_title='Outage Count', yaxis2_title='Outage Duration (s)', legend=dict(x=0.7, y=1.1))
            st.plotly_chart(fig, use_container_width=True)
    st.subheader("Sector Level KPIs")   
            
    # #merge data and time columns
    # col1, col2 = st.columns(2)
    # with col1.container(border=True):
    #     sector_chioces = selected_sector['Sector'].unique()
    #     sector = st.radio("Select Sector", sector_chioces, horizontal= True)
    # with col2.container(border=True):
    #     chart_display = st.radio("Chart Type", ['Trend', 'Benchmark'], horizontal= True)
    # if sector:
    #     selected_sector = selected_sector[selected_sector['Sector'] == sector]
    # selected_sector = selected_sector[selected_sector['Sector'] == sector_name]
    plot_charts_in_grid(selected_sector, KPIs_of_interest)
    # for kpi in KPIs_of_interest:
    #     selected_sector[kpi] = selected_sector[kpi].round(2)
    #     container = st.container(border = True)
    #     # container.line_chart(data=selected_sector, x='Date', y=kpi, use_container_width=True)
    #     #line chart for each KPI
    #     if chart_display == 'Trend':
    #         selected_sector['combine'] = selected_sector['Date'] + ' ' + selected_sector['Time']
    #         fig = px.line(selected_sector, x='combine', y=kpi, title=kpi).update_layout(xaxis_title='Time', yaxis_title='')
    #         # fig.update_xaxes(rangeslider_visible=True)
    #         container.plotly_chart(fig, use_container_width=True)
    #     else:
    #         #date in legend
    #         fig = px.line(selected_sector, x='Time', y=kpi, title= kpi, color='Date').update_layout(xaxis_title='Time', yaxis_title='')
    #         container.plotly_chart(fig, use_container_width=True)
@st.experimental_fragment
def plot_charts_in_grid(df, KPIs_of_interest):
    figs = []

    col1, col2 = st.columns(2)  

    with col2.container(border=True):
        chart_display = st.radio("Chart Type", ['Trend', 'Benchmark'], horizontal= True)
    with col1.container(border=True):
        sector_choices = df['Sector'].unique()
        if chart_display == 'Benchmark':
            sector = st.radio("Select Sector", sector_choices, horizontal= True)
            df = df[df['Sector'] == sector]
        else:
            sector = st.multiselect("Select Sector", sector_choices, default= sector_choices)
            df = df[df['Sector'].isin(sector)]

    # plot charts in grid of 2 columns
    figs = []
    ncols = len(KPIs_of_interest)
    nrows = (ncols + 1) // 3  # Calculate number of rows required
    # if bm_date is None:
    #     bm_date_str = None
    # else:
    #     bm_date_str = bm_date.strftime('%Y-%m-%d')
    if chart_display == 'Trend':
        df['combine'] = df['Date'] + ' ' + df['Time']   

        for i in range(nrows):
            columns = st.columns(3)  # Create two columns for each row
            for j in range(3):
                idx = i * 3 + j
                if idx < ncols:
                    col = df.columns[0:][idx]                
                    fig = px.line(df, x= df['combine'], y=col, color= 'Sector' , template= 'presentation', height= 350).update_layout(xaxis_title = "", yaxis_title = "", title= { 'text':f'{col}','xanchor':'center', 'x': 0.5, 'yanchor': 'auto'}, legend = dict(
                        title = '',
                        yanchor = 'top',
                        y = 1.2,
                        xanchor = 'center',
                        x = 0.5, 
                        orientation = 'h',                         
                         ))
                    fig.update_yaxes(tickfont_family = 'Arial Black')                  
                    
                    with columns[j].container(height= 375, border= True):
                        st.plotly_chart(fig, use_container_width=True)
                        figs.append(fig)
    else:
        for i in range(nrows):
            columns = st.columns(3)
            for j in range(3):
                idx = i * 3 + j
                if idx < ncols:
                    col = df.columns[0:][idx]
                    fig = px.line(df, x= 'Time', y=col, color= 'Date', template= 'presentation', height= 350, line_shape= "spline").update_layout(xaxis_title = "", yaxis_title = "", title= { 'text':f'{col}','xanchor':'center', 'x': 0.5}, legend = dict(
                        title = '',
                        yanchor = 'bottom',
                        y = -0.5,
                        xanchor = 'center',
                        x = 0.5, 
                        orientation = 'h', 
                        ))
                    fig.update_yaxes(tickfont_family = 'Arial Black')               
                    with columns[j].container(height= 375, border= True):
                        st.plotly_chart(fig, use_container_width=True)
                        figs.append(fig)
    return figs
        
def change_zoom():
    st.session_state['map_zoom'] = 15
def change_center(df):
    st.session_state['map_center'] = [df['lat'].mean(), df['long'].mean()]

def display_region_filter(df):
    region_options = ['South', 'North', 'East', 'West', 'Central']
    return st.sidebar.selectbox('Select Region', region_options, index=0 ,key='selected_region')


# @st.cache_data
def display_cluster_filter(df):
    cluster_options = [''] + list(df['Cluster'].unique())
    cluster_index = cluster_options.index('Abha') if 'Abha' in cluster_options else 0
    return st.sidebar.selectbox('Select Cluster', cluster_options, index=cluster_index, key='selected_cluster')
# @st.cache_data    
def display_KPIs_filter():
    # KPIs_of_interest = ['LTE DL User Throughput Mbps', 'LTE UL User Throughput Mbps','Total CS Traffic Earlang', 'LTE PRB Utilization','Total PS Traffic GB', '4G Users',  '5G Users', '3G RTWP', 'LTE UL Interference (dBm)', '5G UL Interference (dBm)', '4G Availability' , 
                        # '2G Availability', '3G Availability', '5G Availability', '2G Interference']  # Replace with actual KPI column names
    KPIs_of_interest = ['4G Availability', '4G Users', 'LTE DL User Throughput Mbps', 'LTE UL User Throughput Mbps', 'LTE PRB Utilization', 'LTE UL Interference (dBm)', 'Total CS Traffic Earlang', 'Total PS Traffic GB', '5G Availability', '5G Users', '5G UL Interference (dBm)','NR DL User Throughput Mbps', 'NR UL User Throughput Mbps', '3G Availability', '3G RTWP', '2G Availability', '2G Interference']
    return st.sidebar.selectbox('Select KPI', KPIs_of_interest, key='selected_kpi')
@st.cache_data(ttl=3600)
def load_and_process_data_short(selected_date, selected_time):
    # start_time = time.time()
    # load the data from the sqlite database only last 7 days
    # st.write("selected date is ", selected_date)
    # formatted_date = selected_date.strftime('%m/%d/%Y')

    tables = ['2G', '3G', '4G', '5G']  # List of table names
    # tables = ['2G']
    dataframes = {}  # Dictionary to store DataFrames
    
    with sqlite3.connect(sqlite_db_path) as conn:     
        for table in tables:
            query = f'''
                        SELECT *
                        FROM "{table}"
                        WHERE Date == '{selected_date}' AND Time == '{selected_time}'
                    '''            
         
            dataframes[table] = pd.read_sql_query(query, conn)
            dataframes[table].replace('NIL', np.nan, inplace= True)
            dataframes[table].head()
    dataframes['2G']['Cell CI'] = dataframes['2G']['Cell CI'].astype('int64', errors='ignore')
    dataframes['3G']['Cell ID'] = dataframes['3G']['Cell ID'].astype('int64', errors='ignore')
    dataframes['4G']['LocalCell Id'] = dataframes['4G']['LocalCell Id'].astype('int64', errors='ignore')
    dataframes['5G']['NR Cell ID'] = dataframes['5G']['NR Cell ID'].astype('int64', errors='ignore')
    # dataframes['2G'].shape
    # dataframes['3G'].shape

    # st.write("Time of loading and processing data:", time.time() - start_time)
    return dataframes
def date_filter():
    #last 7 days
    # return ['03/21/2024', '03/22/2024', '03/23/2024', '03/24/2024', '03/25/2024', '03/26/2024', '03/27/2024', '03/28/2024']
    min_date = datetime.strptime('03/07/2024', '%m/%d/%Y').date()
    selected_date = st.sidebar.date_input('Select Date', datetime.now(), min_value= min_date, format= "MM/DD/YYYY", key = 'selected_date')
    return selected_date.strftime('%m/%d/%Y')
def time_filter():
    # return ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00']
    last_interval = datetime.now() - timedelta(hours=1, minutes=36)
    hour = last_interval.hour
    hour = datetime.strptime(str(hour), '%H').time()
    selected_time = st.sidebar.time_input('Select Time', value= hour , step= 3600, key= 'selected_time')
    if selected_time:
        return selected_time.strftime('%H:%M')
@st.cache_data(ttl=3600)
def fetch_nodeb_status():

    engine = create_engine('mysql+mysqlconnector://root:Mobily123@10.27.64.25:3306/dash')
    query = """SELECT * FROM NodeB_Outage_Table;"""
    # read data using sqlalchemy
    df = pd.read_sql(query, engine)
    return df
def plotly_map(filtered_gdf, selected_kpi, selected_site, nodebstatus, selected_cluster, selected_region):
    fig = px.scatter_mapbox(filtered_gdf, lat='lat', lon='long', color=selected_kpi, hover_name='Site', hover_data=[selected_kpi], zoom=10, height=1000)
    fig.update_layout(mapbox_style="carto-darkmatter", mapbox_zoom=10, mapbox_center = {"lat": filtered_gdf['lat'].mean(), "lon": filtered_gdf['long'].mean()})
    st.plotly_chart(fig, use_container_width=True)
def main():
    # st.set_page_config(APP_TITLE,layout="wide")
    #timeit
    start_time = time.time()
  
    st.title(APP_TITLE)
    selected_date = date_filter()
    # st.write("Date options", selected_date)
    selected_time = time_filter()
    # st.write("Time options", selected_time)


    sitedf = load_site_data()
    # drop rows with missing values
    sitedf = sitedf.dropna()
    
    selected_region = display_region_filter(sitedf)

    selected_kpi = display_KPIs_filter()
    nodebstatus = fetch_nodeb_status()     


    # dataframes = load_and_process_data()
    dataframes = load_and_process_data_short(selected_date, selected_time)
    # Accessing the individual dataframes
    df2G = dataframes['2G']
    df3G = dataframes['3G']
    df4G = dataframes['4G']
    df5G = dataframes['5G']
    #handle case where all dataframes are empty
    if df2G.empty and df3G.empty and df4G.empty and df5G.empty:
        st.write("No data available for the selected date and time.")
        # rerun the app
        # st.write("Time of loading and processing data:", time.time() - start_time)
    else:
        mergeddf = create_kpis(df4G, df3G, df2G, df5G)


    
        site_df = create_wedges(sitedf)
        # st.write("wedges done")

        Sites_KPIs_df = pd.merge(site_df, mergeddf, on = 'Sector', how='left')
        # copy Sites_KPIs_df to gdf (new dataframe)
        # gdf = Sites_KPIs_df.copy()
        gdf = gpd.GeoDataFrame(Sites_KPIs_df, geometry= 'geometry')

        gdf['Date'] = pd.to_datetime(gdf['Date']).dt.strftime('%Y-%m-%d')  


        #------------------------------------Layout----------------------------------------------------  
        filtered_gdf = gdf.copy()
        filtered_gdf = filtered_gdf[filtered_gdf['Region'] == selected_region]
        selected_cluster = display_cluster_filter(filtered_gdf)

        site_options = [''] + list(filtered_gdf['Site'].unique())
        selected_site = st.sidebar.selectbox('Select Site', site_options, index=0, key='selected_site')
  
        if selected_cluster != '':
            filtered_gdf = filtered_gdf[filtered_gdf['Cluster'] == selected_cluster]
        else:
            filtered_gdf = filtered_gdf  # Use the entire GeoDataFrame

        filtered_gdf = filtered_gdf[filtered_gdf['geometry']!= None]
        numeric_cols = ['LTE DL User Throughput Mbps', 'LTE UL User Throughput Mbps', 
                        'LTE PRB Utilization', 'Total CS Traffic Earlang', 
                        'Total PS Traffic GB', '4G Users', '5G Users', 
                        '3G RTWP', 'LTE UL Interference (dBm)', '5G UL Interference (dBm)', '4G Availability', '5G Availability', '3G Availability', '2G Availability', '2G Interference', 'NR DL User Throughput Mbps', 'NR UL User Throughput Mbps']

        filtered_gdf[numeric_cols] = filtered_gdf[numeric_cols].round(2)
        # -----------------------------------------Guage Charts------------------------------------------------
     
    #     col5, col6 =  st.columns(2)
    #     Penetration_5G = filtered_gdf['5G_H_Total Traffic (GB)'].sum()/filtered_gdf['Total PS Traffic GB'].sum()*100
    #     Penetration_Volte = filtered_gdf['VoLTE_Traffic (Erlang)'].sum()/filtered_gdf['Total CS Traffic Earlang'].sum()*100      
    # # Display the gauge chart in Streamlit
    #     with col5.container(border=True):
    #         gauge_chart1 = create_gauge_chart(Penetration_5G, 100, "5G Traffic Penetration", 20)
    #         st.plotly_chart(gauge_chart1, use_container_width=True)
    #         #add some white space 
    #     st.write("")    
    #     st.write("")    

    #     # Display the gauge chart in Streamlit
    #     with col6.container(border=True):
    #         gauge_chart2 = create_gauge_chart(Penetration_Volte, 100, "Volte Traffic Penetration",50)
    #         st.plotly_chart(gauge_chart2, use_container_width=True)
        
        # dash_3 = st.container()
        # with dash_3:           

        #         col1, col2, col3, col4 = st.columns(4)
        # #
        #         display_kpi(filtered_gdf,col1, 'Total CS Traffic Earlang')
        #         display_kpi(filtered_gdf,col2, 'Total PS Traffic GB')
        #         display_kpi(filtered_gdf,col3, '4G Users')
        #         display_kpi(filtered_gdf, col4, '5G Users')


 
  
        filtered_gdf  = filtered_gdf.drop(['Date'], axis=1)

        # m = init_map()
        maptype = st.sidebar.radio("Select Map Type", ['Folium', 'Plotly'], key='maptype')  
        if maptype == 'Folium':
            sector_name = create_map(filtered_gdf, selected_kpi, selected_site, nodebstatus, selected_cluster, selected_region)
            if sector_name is not None:
                st.subheader("Displaying KPIs of " + sector_name[:5])
                KPIs_of_selected_sector(sector_name)
        else: 
            plotly_map(filtered_gdf, selected_kpi, selected_site, nodebstatus, selected_cluster, selected_region)
   
       

  

    


if __name__ == "__main__":
    main()