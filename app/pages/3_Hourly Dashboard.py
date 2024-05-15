import streamlit as st
import os
import pandas as pd
import sys
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
import streamlit_antd_components as sac
from pygwalker.api.streamlit import StreamlitRenderer
from sqlalchemy import create_engine
import mysql.connector
from fpdf import FPDF
from tempfile import NamedTemporaryFile
st.set_page_config("Hourly Dashboard", layout="wide")

st.sidebar.page_link("dash2.py", label ="Home")
st.sidebar.page_link("pages/2_License Utilization.py", label = "License Utilization")
st.sidebar.page_link("pages/1_Daily Dashboard.py", label = "Daily Dashboard")
st.sidebar.page_link("pages/3_Hourly Dashboard.py", label = "Hourly Dashboard")

with open('app/style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Connect to MySQL database
engine = create_engine('mysql+mysqlconnector://root:Mobily123@10.27.64.25:3306/dash')

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
ftp_data_dir = os.path.join(script_dir, '..', '..','ftp', 'data', 'daily')

def cluster_options(df2g, df3g, df4g, dfvolte, df5g, dfiot):
    options2g = list(df2g['GCell Group'].unique())
    options3g = list(df3g['UCell Group'].unique())
    options4g = list(df4g['LTE Cell Group'].unique())
    optionsvolte = list(dfvolte['LTE Cell Group'].unique())
    options5g = list(df5g['NR Cell Group'].unique())
    optionsiot = list(dfiot['NB-IoT Cell Group'].unique())
    return options2g, options3g, options4g, optionsvolte, options5g, optionsiot

def tech_container():
    tech_container = st.container(border= True)
    with tech_container:
        st.markdown("<h3 style='text-align: left; color:#B09EB0 ;'>Select Technology</h3>", unsafe_allow_html=True)
        tech = sac.buttons([
            sac.ButtonsItem(label="2G", color = 'indigo'),
            sac.ButtonsItem(label="3G", color = 'indigo'),
            sac.ButtonsItem(label="4G", color = 'indigo'),
            sac.ButtonsItem(label="VOLTE", color = 'indigo'),
            sac.ButtonsItem(label="5G", color = 'indigo'),
            sac.ButtonsItem(label="IOT", color = 'indigo'),   
            sac.ButtonsItem(label="DIY", color = 'indigo')     
        ], key = "tech", align = "start", size= "md", use_container_width=True)
    return tech
@st.cache_data(ttl=3600*12)
def fetch_data(start_date, end_date , tech = "None"):
    # connect to MySQL database
    mydb =  mysql.connector.connect(
    host= "10.27.64.25",
    port = "3306",
    user= "root",
    password= "Mobily123",
    database= "dash")
    mycursor = mydb.cursor()
    if tech == "2G":
        mycursor.execute(f"SELECT * FROM 2g_kpi_hourly WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "3G":
        mycursor.execute(f"SELECT * FROM 3g_kpi_hourly WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "4G":
        mycursor.execute(f"SELECT * FROM 4g_kpi_hourly WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "VOLTE":
        mycursor.execute(f"SELECT * FROM volte_kpi_hourly WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "5G":
        mycursor.execute(f"SELECT * FROM 5g_kpi_hourly WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "IOT":
        mycursor.execute(f"SELECT * FROM nbiot_kpi_hourly WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "None":
        dfs = {}
        # data stored in mm/dd/yyyy format
        for tech_type in ("2g", "3g", "4g", "volte", "5g", "nbiot"):
            query = f"SELECT * FROM {tech_type}_kpi_hourly WHERE STR_TO_DATE(Date, '%m/%d/%Y') BETWEEN %s AND %s"
            mycursor.execute(query, (start_date, end_date))            

            dfs[tech_type] = pd.DataFrame(mycursor.fetchall(), columns=[desc[0] for desc in mycursor.description])
        return dfs



    return df
def plot_charts_in_grid(df):
    figs = []
    ncols = len(df.columns[3:])
    nrows = (ncols + 1) // 4  # Calculate number of rows required
    cluster = df.iloc[:,0]
    # if bm_date is None:
    #     bm_date_str = None
    # else:
    #     bm_date_str = bm_date.strftime('%Y-%m-%d')

    for i in range(nrows):
        columns = st.columns(3)  # Create two columns for each row
        for j in range(3):
            idx = i * 3 + j
            if idx < ncols:
                col = df.columns[3:][idx]
                fig = px.line(df, x=df['Time'], y=col, color=df.index, template='presentation', height=350, line_shape="spline").update_layout(
                        xaxis_title="",
                        yaxis_title="",
                        title={'text': f'{col}', 'xanchor': 'center', 'x': 0.5},
                        legend=dict(title = '',
                            yanchor='bottom',
                            y=-0.5,
                            xanchor='center',
                            x=0.5,
                            orientation='h',
                            bordercolor='Black',
                            borderwidth=1.2
                        )
                    ).update_yaxes(tickfont_family = "Arial Black").update_xaxes(tickfont_family = "Arial Black")
            
                
                with columns[j].container(height= 375, border= True):
                    st.plotly_chart(fig, use_container_width=True)
                    figs.append(fig)

    

    return figs
def date_filter():
    #last 7 days
    # return ['03/21/2024', '03/22/2024', '03/23/2024', '03/24/2024', '03/25/2024', '03/26/2024', '03/27/2024', '03/28/2024']
    min_date = datetime.strptime('04/23/2024', '%m/%d/%Y').date()
    selected_date = st.sidebar.date_input('Select Date', datetime.now(), min_value= min_date, format= "MM/DD/YYYY")
    return selected_date.strftime('%m/%d/%Y')
def time_filter():

    # return ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00']
    last_interval = datetime.now() - timedelta(hours=1, minutes=36)
    hour = last_interval.hour
    hour = datetime.strptime(str(hour), '%H').time()
    selected_time = st.sidebar.time_input('Select Time', value= hour , step= 3600)
    if selected_time:
        return selected_time.strftime('%H:%M')
def create_pdf(figs, tech):
    page_width = 210 - 2  # Width of the page in millimeters
    page_height = 297 - 5  # Height of the page in millimeters
    cell_width = 92
    cell_height = 65
    rows = 3
    cols = 3
    pdf = FPDF()
    num_figs = len(figs)

    # Add title on the first page
    pdf.add_page(orientation='L')
    pdf.set_font("Arial", size=16)
    txt = "Hourly Performance Report {}".format(tech)
    x_center = (page_width / 2) - 50
    y_center = page_height / 3
    pdf.set_xy(x_center, y_center)
    pdf.cell(200, 10, txt=txt, ln=True, align="C")
    pdf.ln(10)  # Add a new line

    for i in range(0, num_figs, rows * cols):
        pdf.add_page(orientation='L')
        for row in range(rows):
            for col in range(cols):
                index = i + row * cols + col
                if index < num_figs:
                    with NamedTemporaryFile(delete=False, suffix=".png") as tmpfile:
                        figs[index].write_image(tmpfile.name)
                        x_pos = 5 + col * (cell_width + 5)
                        y_pos = 5 + row * (cell_height + 5)
                        pdf.image(tmpfile.name, x=x_pos, y=y_pos, w = cell_width, type='png')

    return pdf
def main():
    st.html("""<h1 style='text-align: center; color: #B09EB0;'>Hourly Performance Dashboard</h1>""")
    # st.divider()
    default_start = datetime.now() - timedelta(days=8)
    default_start = default_start.strftime('%Y-%m-%d') # YYYY-MM-DD is default sql format
    start_date = default_start
    default_end = datetime.now()
    default_end = default_end.strftime('%Y-%m-%d')
    end_date = default_end
    default_start_time = datetime.now() - timedelta(hours=1, minutes=36)
    hour = default_start_time.hour
    hour = datetime.strptime(str(hour), '%H').time()      

    st.sidebar.divider()
    st.sidebar.write("")
    st.sidebar.write("")
    st.sidebar.write("")
    # options2g, options3g, options4g, optionsvolte, options5g, optionsiot = cluster_options(df2g, df3g, df4g, dfvolte, df5g, dfiot)
    with st.sidebar.form(key="query_data"):
        start_date_custom = st.date_input("Start Date",format= "MM/DD/YYYY", value= datetime.now() - timedelta(days=8), key="start_date")
        end_date_custom = st.date_input("End Date", datetime.now(),format= "MM/DD/YYYY")
        start_date_custom = start_date_custom.strftime('%Y-%m-%d')
        end_date_custom = end_date_custom.strftime('%Y-%m-%d')
        submit = st.form_submit_button("Query Data")
        #submit button on the sidebar
    
    if submit:
        start_date = start_date_custom
        end_date = end_date_custom
        dfs = fetch_data(start_date, end_date, "None")

    dfs = fetch_data(start_date, end_date, "None")
    df2g = dfs['2g']   
    df3g = dfs['3g']
    df4g = dfs['4g']
    dfvolte = dfs['volte']
    df5g = dfs['5g']
    dfiot = dfs['nbiot']
    options2g, options3g, options4g, optionsvolte, options5g, optionsiot = cluster_options(df2g, df3g, df4g, dfvolte, df5g, dfiot)
    tech = tech_container() 
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    last_week = today - timedelta(days=7)
    today = today.strftime('%m/%d/%Y')
    yesterday = yesterday.strftime('%m/%d/%Y')
    last_week = last_week.strftime('%m/%d/%Y')
    # if today is not in the data, remove it from the default dates
    if today not in df2g['Date'].unique() or yesterday not in df2g['Date'].unique() or last_week not in df2g['Date'].unique():
        default_dates= None
    else:
        default_dates = [today, yesterday, last_week]
    if tech == "2G":
        l,r = st.columns(2) 

        clusterindex = options2g.index('Southern Region') if 'Southern Region' in options2g else 0
        cluster2g_selected = l.selectbox("Select Cluster", options2g, index = clusterindex)   


        selected_dates = r.multiselect("Select Date", df2g['Date'].unique(), default= default_dates) 
        # default selected dates are yesterday, today and same day last week

        filtered2g = df2g[df2g['GCell Group'] == cluster2g_selected]
        filtered2g['Date'] = pd.to_datetime(filtered2g['Date'], format='%m/%d/%Y')
        # remove trailing time from 'Date' column 
        filtered2g = filtered2g[(filtered2g['Date'].isin(selected_dates))]  

        # filtered2g = filtered2g[(filtered2g['Date'] >= start_date) & (filtered2g['Date'] <= end_date)]
        filtered2g['Date'] = filtered2g['Date'].dt.date
        # set index to Date
        filtered2g = filtered2g.set_index('Date')
        figs = plot_charts_in_grid(filtered2g)
        output = df2g.copy()    
    elif tech == "3G":
        l,r = st.columns(2)
        clusterindex = options3g.index('Southern Region') if 'Southern Region' in options3g else 0
        cluster3g_selected = l.selectbox("Select Cluster", options3g, index = clusterindex  )
        selected_dates = r.multiselect("Select Date", df3g['Date'].unique(), default= default_dates)
        filtered3g = df3g[df3g['UCell Group'] == cluster3g_selected]
        filtered3g['Date'] = pd.to_datetime(filtered3g['Date'], format='%m/%d/%Y')
        filtered3g = filtered3g[(filtered3g['Date'].isin(selected_dates))]
        filtered3g['Date'] = filtered3g['Date'].dt.date
        filtered3g = filtered3g.set_index('Date')
        figs = plot_charts_in_grid(filtered3g)
        output = df3g.copy()
    elif tech == "4G":
        l,r = st.columns(2)
        clusterindex = options4g.index('Southern Region') if 'Southern Region' in options4g else 0
        cluster4g_selected = l.selectbox("Select Cluster", options4g, index = clusterindex  )
        selected_dates = r.multiselect("Select Date", df4g['Date'].unique(), default= default_dates)
        filtered4g = df4g[df4g['LTE Cell Group'] == cluster4g_selected]
        filtered4g['Date'] = pd.to_datetime(filtered4g['Date'], format='%m/%d/%Y')
        filtered4g = filtered4g[(filtered4g['Date'].isin(selected_dates))]
        filtered4g['Date'] = filtered4g['Date'].dt.date
        filtered4g = filtered4g.set_index('Date')
        figs = plot_charts_in_grid(filtered4g)
        output = df4g.copy()
    elif tech == "VOLTE":
        l,r = st.columns(2)
        clusterindex = optionsvolte.index('Southern Region') if 'Southern Region' in optionsvolte else 0
        clustervolte_selected = l.selectbox("Select Cluster", optionsvolte, index = clusterindex)
        selected_dates = r.multiselect("Select Date", dfvolte['Date'].unique(), default= default_dates)
        filteredvolte = dfvolte[dfvolte['LTE Cell Group'] == clustervolte_selected]
        filteredvolte['Date'] = pd.to_datetime(filteredvolte['Date'], format='%m/%d/%Y')
        filteredvolte = filteredvolte[(filteredvolte['Date'].isin(selected_dates))]
        filteredvolte['Date'] = filteredvolte['Date'].dt.date
        filteredvolte = filteredvolte.set_index('Date')
        figs = plot_charts_in_grid(filteredvolte)
        output = dfvolte.copy()
    elif tech == "5G":
        l,r = st.columns(2)
        clusterindex = options5g.index('Southern Region') if 'Southern Region' in options5g else 0
        cluster5g_selected = l.selectbox("Select Cluster", options5g, index= clusterindex   )
        selected_dates = r.multiselect("Select Date", df5g['Date'].unique(), default= default_dates)
        filtered5g = df5g[df5g['NR Cell Group'] == cluster5g_selected]
        filtered5g['Date'] = pd.to_datetime(filtered5g['Date'], format='%m/%d/%Y')
        filtered5g = filtered5g[(filtered5g['Date'].isin(selected_dates))]
        filtered5g['Date'] = filtered5g['Date'].dt.date
        filtered5g = filtered5g.set_index('Date')
        figs = plot_charts_in_grid(filtered5g)
        output = df5g.copy()
    elif tech == "IOT":
        l,r = st.columns(2)
        clusterindex = optionsiot.index('Southern Region') if 'Southern Region' in optionsiot else 0
        clusteriot_selected = l.selectbox("Select Cluster", optionsiot, index= clusterindex)
        selected_dates = r.multiselect("Select Date", dfiot['Date'].unique(), default= default_dates)
        filterediot = dfiot[dfiot['NB-IoT Cell Group'] == clusteriot_selected]
        filterediot['Date'] = pd.to_datetime(filterediot['Date'], format='%m/%d/%Y')
        filterediot = filterediot[(filterediot['Date'].isin(selected_dates))]
        filterediot['Date'] = filterediot['Date'].dt.date
        filterediot = filterediot.set_index('Date')
        figs = plot_charts_in_grid(filterediot)
        output = dfiot.copy()
    elif tech == "DIY":
        select_df = st.selectbox("Select Technology", ["2G", "3G", "4G", "VOLTE","5G", "IOT"], index = 2)
        if select_df == "2G":
            df = df2g.copy()
        elif select_df == "3G":
            df = df3g.copy()
        elif select_df == "4G":
            df = df4g.copy()
        elif select_df == "VOLTE":
            df = dfvolte.copy()
        elif select_df == "5G":
            df = df5g.copy()
        elif select_df == "IOT":
            df = dfiot.copy()
        if 'Date' in df.columns: 
            #change format to YYYY-MM-DD
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
        pyg_app =  StreamlitRenderer(df)
        pyg_app.explorer()
        output = df.copy()

    st.sidebar.download_button(
        label = "Downoad Raw Data",
        data = output.to_csv(index=False).encode('utf-8'),
        file_name = 'data.csv',
        mime = 'text/csv'
    )
    
    with st.sidebar.form(key="export_pdf_hour", border = False):
        submit = st.form_submit_button("Export Charts to PDF")
        if submit:
            placeholder = st.sidebar.empty()
            with placeholder, st.spinner("Exporting to PDF..."):
                data = create_pdf(figs, tech).output(dest='S').encode('latin1')
                st.sidebar.success("PDF Exported Successfully!")
                st.sidebar.download_button(    

                label="Download PDF", 
                data= data,
                file_name='report.pdf',
                mime='application/pdf'
                    )

if __name__ == '__main__':
    main()