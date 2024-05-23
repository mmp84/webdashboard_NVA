import streamlit as st
import os
import pandas as pd
import sys
import numpy as np
import plotly.express as px
import time 
from datetime import datetime, timedelta
import streamlit_antd_components as sac
from pygwalker.api.streamlit import StreamlitRenderer
from sqlalchemy import create_engine
import mysql.connector
from fpdf import FPDF
import base64
from tempfile import NamedTemporaryFile

st.set_page_config("Daily Dashboard", layout="wide")
st.sidebar.page_link("Home.py", label ="Home")
st.sidebar.page_link("pages/2_License Utilization.py", label = "License Utilization")
st.sidebar.page_link("pages/1_Daily Dashboard.py", label = "Daily Dashboard")
st.sidebar.page_link("pages/3_Hourly Dashboard.py", label = "Hourly Dashboard")

with open('app/style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

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

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
ftp_data_dir = os.path.join(script_dir, '..', '..','ftp', 'data', 'daily')
# df2g = pd.DataFrame()
# df3g = pd.DataFrame()
# df4g = pd.DataFrame()
# dfvolte = pd.DataFrame()
# df5g = pd.DataFrame()
# dfiot = pd.DataFrame()

def write_to_mysql(df,table_name):
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"{table_name} written to MySQL database")
    except Exception as e:
        print(f"Error writing {table_name} to MySQL database: {e}")
        # logging.error(f"Error writing {table_name} to MySQL database: {e}")
@st.cache_data
def read_csv():
    start_time = time.time()
    #filename contains 4G.csv     
    try:
        for filename in os.listdir(ftp_data_dir):
            if filename.endswith('.csv'):
                csv_file_path = os.path.join(ftp_data_dir, filename)
            
                if '4G' in filename:
                    df4g = pd.read_csv(csv_file_path, skiprows=6)
                    df4g = df4g.iloc[:-1]
                    df4g = df4g.replace('NIL', np.nan)
                    # df4g['Date'] = pd.to_datetime(df4g['Date'], format='%m/%d/%Y')
                    # write_to_mysql(df4g, '4g_kpi_daily')
                    # st.write(df4g.head(5))
                elif 'VoLTE' in filename:
                    dfvolte = pd.read_csv(csv_file_path, skiprows=6)
                    dfvolte = dfvolte.iloc[:-1]
                    dfvolte = dfvolte.replace('NIL', np.nan)
                    # st.write(dfvolte.head())
                elif 'UMTS' in filename:
                    df3g = pd.read_csv(csv_file_path, skiprows=6)
                    df3g = df3g.iloc[:-1]   
                    df3g = df3g.replace('NIL', np.nan)
                    # st.write(df3g.head())
                elif '2G' in filename:
                    df2g = pd.read_csv(csv_file_path, skiprows=6)
                    df2g = df2g.iloc[:-1]
                    df2g = df2g.replace('NIL', np.nan)
                    # st.write(df2g.head())
                elif 'NR' in filename:
                    df5g = pd.read_csv(csv_file_path, skiprows=6)
                    df5g = df5g.iloc[:-1]
                    df5g = df5g.replace('NIL', np.nan)
                    # st.write(df5g.head())
                elif 'NB-IOT' in filename:
                    dfiot = pd.read_csv(csv_file_path,skiprows=6)
                    dfiot = dfiot.iloc[:-1] 
                    dfiot = dfiot.replace('NIL', np.nan)
                    # st.write(dfiot.head())
                else:
                    st.write("No matching table found.")
        # st.write("reading data --- %s seconds ---" % (time.time() - start_time))
        return df2g, df3g, df4g, dfvolte, df5g, dfiot
    except Exception as e:
        st.write(f"Error reading CSV file: {e}") 
@st.cache_data   
def cluster_options(df2g, df3g, df4g, dfvolte, df5g, dfiot):
    options2g = df2g['GCell Group'].unique()
    options3g = df3g['UCell Group'].unique()
    options4g = df4g['LTE Cell Group'].unique()
    optionsvolte = dfvolte['LTE Cell Group'].unique() 
    options5g = df5g['NR Cell Group'].unique()
    optionsiot = dfiot['NB-IoT Cell Group'].unique()
    return options2g, options3g, options4g, optionsvolte, options5g, optionsiot

def plot_charts_in_grid(df,bm_date = None):
    figs = []
    ncols = len(df.columns[2:])
    nrows = (ncols + 1) // 3  # Calculate number of rows required
    cluster = df.iloc[:,0]
    if bm_date is None:
        bm_date_str = None
    else:
        bm_date_str = bm_date.strftime('%Y-%m-%d')

    for i in range(nrows):
        columns = st.columns(3)  # Create two columns for each row
        for j in range(3):
            idx = i * 3 + j
            if idx < ncols:
                col = df.columns[2:][idx]
                fig = px.line(df, x=df.index, y=col, title=f'{col}', color = cluster , template= 'presentation', height= 350, line_shape = "spline").update_layout(xaxis_title = "", yaxis_title = "", title= { 'text':f'{col}','xanchor':'center', 'x': 0.5}, legend = dict(
                    title = '',
                    yanchor = 'bottom',
                    y = -0.55,
                    xanchor = 'center',
                    x = 0.5, 
                    orientation = 'h', 
                    ))
                fig.update_yaxes(tickfont_family="Arial Black")
                fig.update_xaxes(tickfont_family="Arial Black")
                
                if bm_date_str:
                    fig.add_vline(x=bm_date_str, line_dash="dash", line_color="red", line_width=1)
            
                
                with columns[j].container(height= 375, border= True):
                    st.plotly_chart(fig, use_container_width=True)
                    figs.append(fig)

    

    return figs
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
        mycursor.execute(f"SELECT * FROM 2g_kpi_daily WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "3G":
        mycursor.execute(f"SELECT * FROM 3g_kpi_daily WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "4G":
        mycursor.execute(f"SELECT * FROM 4g_kpi_daily WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "VOLTE":
        mycursor.execute(f"SELECT * FROM volte_kpi_daily WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "5G":
        mycursor.execute(f"SELECT * FROM 5g_kpi_daily WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "IOT":
        mycursor.execute(f"SELECT * FROM nbiot_kpi_daily WHERE Date BETWEEN {start_date} AND {end_date}")
        df = mycursor.fetchall()
    elif tech == "None":
        dfs = {}
        # data stored in mm/dd/yyyy format
        for tech_type in ("2g", "3g", "4g", "volte", "5g", "nbiot"):
            query = f"SELECT * FROM {tech_type}_kpi_daily WHERE STR_TO_DATE(Date, '%m/%d/%Y') BETWEEN %s AND %s"
            mycursor.execute(query, (start_date, end_date))        
        

            dfs[tech_type] = pd.DataFrame(mycursor.fetchall(), columns=[desc[0] for desc in mycursor.description])
        return dfs



    return df


def create_download_link(val, filename):
    b64 = base64.b64encode(val)  # val looks like b'...'

    return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="{filename}.pdf">Download file</a>'
# def create_pdf(figs, tech):
#     page_width = 210-10  # Width of the page in millimeters
#     page_height = 297-10  # Height of the page in millimeters
#     percentage_width = 50  # Desired width as a percentage
#     percentage_height = 50  # Desired height as a percentage

#     # Convert percentages to millimeters
#     w = 190 
#     h = (percentage_height / 100) * page_height
#     # if export_as_pdf:
#     pdf = FPDF()
#     num_figs = len(figs)
#     #page layout horizontal
    
#         # Add title on the first page
#     pdf.add_page(orientation='L')
#     pdf.set_font("Arial", size=16)
#     txt = "Daily Performance Report {}".format(tech)
#     x_center = (pdf.w / 2)-100
#     y_center = pdf.h / 2
#     pdf.set_xy(x_center, y_center)
#     pdf.cell(200, 10, txt= txt, ln=True, align="C")
#     pdf.ln(10)  # Add a new line
#     for i in range(0, num_figs, 3):
#         pdf.add_page(orientation='L')
#         with NamedTemporaryFile(delete=False, suffix=".png") as tmpfile1:
#             figs[i].write_image(tmpfile1.name)
#             pdf.image(tmpfile1.name, x = 5, y = 10, w = 100, type='png' )
#         if i + 1 < num_figs:
#             with NamedTemporaryFile(delete=False, suffix=".png") as tmpfile2:
#                 figs[i+1].write_image(tmpfile2.name)
#                 pdf.image(tmpfile2.name, x =105, y = 10, w = 100, type='png' )
#         if i + 2 < num_figs:
#             with NamedTemporaryFile(delete=False, suffix=".png") as tmpfile3:
#                 figs[i+2].write_image(tmpfile3.name)
#                 pdf.image(tmpfile3.name, x= 205, y = 10, w = 100,  type='png' )
#         if i + 3 < num_figs:
#             pdf.add_page(orientation='L')
#             with NamedTemporaryFile(delete=False, suffix=".png") as tmpfile4:
#                 figs[i+3].write_image(tmpfile4.name)
#                 pdf.image(tmpfile4.name, x = 5, y = 10, w = 100,  type='png' )
#         if i + 4 < num_figs:
#             with NamedTemporaryFile(delete=False, suffix=".png") as tmpfile5:
#                 figs[i+4].write_image(tmpfile5.name)
#                 pdf.image(tmpfile5.name, x =105, y = 10, w = 100,  type='png' )
#         if i + 5 < num_figs:
#             with NamedTemporaryFile(delete=False, suffix=".png") as tmpfile6:
#                 figs[i+5].write_image(tmpfile6.name)
#                 pdf.image(tmpfile6.name, x= 205, y = 10, w = 100,  type='png' )
#     return pdf


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
    txt = "Daily Performance Report {}".format(tech)
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
    st.html("""<h1 style='text-align: center; color: #B09EB0;'>Daily Performance Dashboard</h1>""")
    # st.divider()
    default_start = datetime.now() - timedelta(days=90)
    default_start = default_start.strftime('%Y-%m-%d') # YYYY-MM-DD is default sql format
    start_date = default_start

    default_end = datetime.now()
    default_end = default_end.strftime('%Y-%m-%d')
    end_date = default_end
    dfs = fetch_data(default_start, default_end, "None")
    st.sidebar.divider()
    st.sidebar.write("")
    st.sidebar.write("")
    st.sidebar.write("")
    # options2g, options3g, options4g, optionsvolte, options5g, optionsiot = cluster_options(df2g, df3g, df4g, dfvolte, df5g, dfiot)
    with st.sidebar.form(key="query_data"):
        start_date_custom = st.date_input("Start Date",format= "MM/DD/YYYY", value= datetime.now() - timedelta(days=90), key="start_date")
        end_date_custom = st.date_input("End Date", datetime.now(),format= "MM/DD/YYYY")
        start_date_custom = start_date_custom.strftime('%Y-%m-%d')
        end_date_custom = end_date_custom.strftime('%Y-%m-%d')
        submit = st.form_submit_button("Query Data")
        #submit button on the sidebar
    if submit:
        start_date = start_date_custom
        end_date = end_date_custom
        dfs = fetch_data(start_date, end_date, "None")

        # submit = st.form_submit_button("Query Data")    
    # # on submit
    # if submit:
    #     dfs = fetch_data(start_date, end_date, "None")
 
    # GSM, UMTS, LTE, VOLTE, NR, IOT = st.tabs(["2G", "3G", "4G", "VOLTE","5G", "IOT"])
    df2g = dfs['2g']    
    df3g = dfs['3g']
    df4g = dfs['4g']
    dfvolte = dfs['volte']
    df5g = dfs['5g']
    dfiot = dfs['nbiot']
    options2g, options3g, options4g, optionsvolte, options5g, optionsiot = cluster_options(df2g, df3g, df4g, dfvolte, df5g, dfiot)

    tech = tech_container() 
    if tech == "2G":
        l,r = st.columns(2)  
        cluster2g_selected = l.multiselect("Select Cluster", options2g, default = "Southern Region")    
        bm_date = r.date_input("Benchmark Date", format= "MM/DD/YYYY", value =None    )
        filtered2g = df2g[df2g['GCell Group'].isin(cluster2g_selected)]
        # filtered2g = filtered2g[(filtered2g['Date'] >= start_date) & (filtered2g['Date'] <= end_date)]
        filtered2g['Date'] = pd.to_datetime(filtered2g['Date'], format='%m/%d/%Y')
        filtered2g = filtered2g[(filtered2g['Date'] >= start_date) & (filtered2g['Date'] <= end_date)]
        # set index to Date
        filtered2g = filtered2g.set_index('Date')
        figs = plot_charts_in_grid(filtered2g, bm_date)
        output = df2g


     

    # with UMTS:
    if tech == "3G":
        l,r = st.columns(2)
        cluster3g_selected = l.multiselect("Select Cluster", options3g, default = "Southern Region")
        bm_date = r.date_input("Benchmark Date", format= "MM/DD/YYYY", value =None    )
        filtered3g = df3g[df3g['UCell Group'].isin(cluster3g_selected)]
        filtered3g['Date'] = pd.to_datetime(filtered3g['Date'], format='%m/%d/%Y' )

        filtered3g = filtered3g[(filtered3g['Date'] >= start_date) & (filtered3g['Date'] <= end_date)]
        # set index to Date
        filtered3g = filtered3g.set_index('Date')
        figs = plot_charts_in_grid(filtered3g, bm_date)
        output = df3g
    # with LTE:
    if tech == "4G":
        l,r = st.columns(2)
        
        cluster4g_selected = l.multiselect("Select Cluster", options4g, default = "Southern Region")  
        bm_date = r.date_input("Benchmark Date", format= "MM/DD/YYYY", value =None    )
        filtered4g = df4g[df4g['LTE Cell Group'].isin(cluster4g_selected)]
        filtered4g['Date'] = pd.to_datetime(filtered4g['Date'], format='%m/%d/%Y' ) 

        filtered4g = filtered4g[(filtered4g['Date'] >= start_date) & (filtered4g['Date'] <= end_date)]
        # set index to Date
        filtered4g = filtered4g.set_index('Date')
        figs = plot_charts_in_grid(filtered4g, bm_date)
        output = df4g
    # with VOLTE:
    if tech == "VOLTE":
        l,r = st.columns(2)
        clustervolte_selected = l.multiselect("Select Cluster", optionsvolte, default = "Southern Region")
        bm_date = r.date_input("Benchmark Date", format= "MM/DD/YYYY", value =None    )
        filteredvolte = dfvolte[dfvolte['LTE Cell Group'].isin(clustervolte_selected)]
        filteredvolte['Date'] = pd.to_datetime(filteredvolte['Date'], format='%m/%d/%Y' )

        filteredvolte = filteredvolte[(filteredvolte['Date'] >= start_date) & (filteredvolte['Date'] <= end_date)]
        # set index to Date
        filteredvolte = filteredvolte.set_index('Date')
        figs = plot_charts_in_grid(filteredvolte, bm_date)
        output = dfvolte

    # with NR:
    if tech == "5G":
        l,r = st.columns(2) 
       
        cluster5g_selected = l.multiselect("Select Cluster", options5g, default = "Southern Region")
        bm_date = r.date_input("Benchmark Date", format= "MM/DD/YYYY", value =None    )
        filtered5g = df5g[df5g['NR Cell Group'].isin(cluster5g_selected)]
        filtered5g['Date'] = pd.to_datetime(filtered5g['Date'], format='%m/%d/%Y' )

        filtered5g = filtered5g[(filtered5g['Date'] >= start_date) & (filtered5g['Date'] <= end_date)]       
        # set index to Date
        filtered5g = filtered5g.set_index('Date')
        figs = plot_charts_in_grid(filtered5g,bm_date)
        output = df5g
    # with IOT:
    if tech == "IOT":
        l,r = st.columns(2) 
        clusteriot_selected = l.multiselect("Select Cluster", optionsiot, default = "Southern Region")
        bm_date = r.date_input("Benchmark Date", format= "MM/DD/YYYY", value =None    )
        filterediot = dfiot[dfiot['NB-IoT Cell Group'].isin(clusteriot_selected)]
        filterediot['Date'] = pd.to_datetime(filterediot['Date'], format='%m/%d/%Y' )

        filterediot = filterediot[(filterediot['Date'] >= start_date) & (filterediot['Date'] <= end_date)]
        # set index to Date
        filterediot = filterediot.set_index('Date')
        figs = plot_charts_in_grid(filterediot, bm_date)
        output = dfiot
    if tech == "DIY":
        select_df = st.selectbox("Select Technology", ["2G", "3G", "4G", "VOLTE","5G", "IOT"], index = 2)
        if select_df == "2G":
            df = df2g
        elif select_df == "3G":
            df = df3g
        elif select_df == "4G":
            df = df4g
        elif select_df == "VOLTE":
            df = dfvolte
        elif select_df == "5G":
            df = df5g
        elif select_df == "IOT":
            df = dfiot
        
        output = df
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
 
        pyg_app =  StreamlitRenderer(df)
        pyg_app.explorer()

# st download button to download the selected data
    st.sidebar.download_button(
            label="Download Raw Data",
            data= output.to_csv(index=False).encode('utf-8'),
            file_name='data.csv',
            mime='text/csv'
        )   


        # html = create_download_link(pdf.output(dest='S').encode('latin1'), 'report')
        # st.markdown(html, unsafe_allow_html=True)
        # st.download_button to download pdf
    with st.sidebar.form(key="export_pdf", border = False):
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


