import streamlit as st
# import zip file
import zipfile
import os
import pandas as pd
import sys
import numpy as np
import plotly.express as px
import glob
import matplotlib
from plotly.subplots import make_subplots
import plotly.graph_objects as go


st.set_page_config("License Utilization", layout="wide")
st.sidebar.page_link("dash2.py", label ="Home")
st.sidebar.page_link("pages/2_License Utilization.py", label = "License Utilization")
st.sidebar.page_link("pages/1_Daily Dashboard.py", label = "Daily Dashboard")
st.sidebar.page_link("pages/3_Hourly Dashboard.py", label = "Hourly Dashboard")
with open('app/style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
lic_dir = os.path.join(script_dir, '..', '..','ftp', 'data', 'license')

@st.cache_data(ttl =3600*12)
def read_license_stats():

#read the csv file
    filename = '*LTE_license*.csv'
    filename5G = '*5G_license*.csv'
    filename4g_hourly = '*4G_hourly*.csv'
    filename5g_hourly = '*5G_hourly*.csv'
    if filename is not None:
        csv_file= glob.glob(os.path.join(lic_dir, filename))
        csv_file = csv_file[0]
    if filename5G is not None:
        csv_file5G = glob.glob(os.path.join(lic_dir, filename5G))
        csv_file5G = csv_file5G[0]
    if filename4g_hourly is not None:
        csv_file4G_hourly = glob.glob(os.path.join(lic_dir, filename4g_hourly))
        csv_file4G_hourly = csv_file4G_hourly[0]
    if filename5g_hourly is not None:
        csv_file5G_hourly = glob.glob(os.path.join(lic_dir, filename5g_hourly))
        csv_file5G_hourly = csv_file5G_hourly[0]

        try:

            df_kpi = pd.read_csv(csv_file, skiprows=6)
            # remove last row
            df_kpi = df_kpi.iloc[:-1]
            if df_kpi is not None:
                df_kpi['L.Traffic.eNodeB.FDD.User.Max'] = pd.to_numeric(df_kpi['L.Traffic.eNodeB.FDD.User.Max'], errors='coerce')
                df_kpi['L.Traffic.eNodeB.TDD.User.Max'] = pd.to_numeric(df_kpi['L.Traffic.eNodeB.TDD.User.Max'], errors='coerce')

                #group by eNodeB Name and find 3 highest values of L.Traffic.eNodeB.FDD.User.Max 
                top_3_fdd= df_kpi.groupby('eNodeB Name')['L.Traffic.eNodeB.FDD.User.Max'].nlargest(3).reset_index()
                # take average of top 3 values round to the nearest integer
                top_3_fdd_avg = top_3_fdd.groupby('eNodeB Name')['L.Traffic.eNodeB.FDD.User.Max'].mean().round().reset_index()

                #group by eNodeB Name and find 3 highest values of L.Traffic.eNodeB.TDD.User.Max
                top_3_tdd= df_kpi.groupby('eNodeB Name')['L.Traffic.eNodeB.TDD.User.Max'].nlargest(3).reset_index()
                # take average of top 3 values round to the nearest integer
                top_3_tdd_avg = top_3_tdd.groupby('eNodeB Name')['L.Traffic.eNodeB.TDD.User.Max'].mean().round().reset_index()

                #merge the two dataframes
                top_3_avg = pd.merge(top_3_fdd_avg, top_3_tdd_avg, on='eNodeB Name', how='outer')  
                # drop 'Integrity' column
                df_kpi = df_kpi.drop(columns='Integrity')
            df_kpi5g = pd.read_csv(csv_file5G, skiprows=6)
            # remove last row
            df_kpi5g = df_kpi5g.iloc[:-1]
            if df_kpi5g is not None:
                df_kpi5g['N.User.gNodeB.RRCConn.Max'] = pd.to_numeric(df_kpi5g['N.User.gNodeB.RRCConn.Max'], errors='coerce')
                #group by gNodeB Name and find 3 highest values of N.User.gNodeB.RRCConn.Max
                top_3_nr= df_kpi5g.groupby('gNodeB Name')['N.User.gNodeB.RRCConn.Max'].nlargest(3).reset_index()
                # take average of top 3 values round to the nearest integer
                top_3_nr_avg = top_3_nr.groupby('gNodeB Name')['N.User.gNodeB.RRCConn.Max'].mean().round().reset_index()
                # drop 'Integrity' column
                df_kpi5g = df_kpi5g.drop(columns='Integrity')
            df_hourly4G = pd.read_csv(csv_file4G_hourly, skiprows=6)
            # remove last row
            df_hourly4G = df_hourly4G.iloc[:-1]
            # replace NIL values with NaN
            df_hourly4G = df_hourly4G.replace('NIL', np.nan)
            df_hourly4G = df_hourly4G.drop(columns='Integrity')
            df_hourly5G = pd.read_csv(csv_file5G_hourly, skiprows=6)
            # remove last row
            df_hourly5G = df_hourly5G.iloc[:-1]
            # replace NIL values with NaN
            df_hourly5G = df_hourly5G.replace('NIL', np.nan)
            df_hourly5G = df_hourly5G.drop(columns='Integrity')


            


            return top_3_avg, df_kpi, top_3_nr_avg, df_kpi5g , df_hourly4G, df_hourly5G

        except Exception as e:
            st.write(f"Error reading CSV file: {e}")


    
                    

@st.cache_data
def read_license(lic_file4G, lic_file5G):
    try: 
        if lic_file4G is not None:
            with open(os.path.join(lic_dir, lic_file4G.name), "wb") as f:
                f.write(lic_file4G.getbuffer())
                st.sidebar.write("File uploaded successfully")
                #read the csv file
                df_lic4G = pd.read_csv(os.path.join(lic_dir, lic_file4G.name))
                # rename old file by adding 'old' to the name
                old_file = '*LTE_ExportSBOM*.csv'
                old_file = glob.glob(os.path.join(lic_dir, old_file))
                os.rename(old_file[0], old_file[0].replace('LTE', 'old_LTE'))
                # save the new file
                             

           
        else:
            #read from csv file containing 'SBOM' in the name
            filename = '*LTE_ExportSBOM*.csv'

            csv_file = glob.glob(os.path.join(lic_dir, filename))
            csv_file = csv_file[0]
            df_lic4G = pd.read_csv(csv_file)
        if lic_file5G is not None:
            with open(os.path.join(lic_dir, lic_file5G.name), "wb") as f:
                f.write(lic_file5G.getbuffer())
                st.sidebar.write("File uploaded successfully")
                #read the csv file
                df_lic5G = pd.read_csv(os.path.join(lic_dir, lic_file5G.name))
                old_file5G = '*NR_ExportSBOM*.csv'
                old_file5G = glob.glob(os.path.join(lic_dir, old_file5G))
                os.rename(old_file5G[0], old_file5G[0].replace('NR', 'old_NR'))

        else:
            #read from csv file containing 'SBOM' in the name
            filename = '*NR_ExportSBOM*.csv'

            csv_file = glob.glob(os.path.join(lic_dir, filename))
            csv_file = csv_file[0]
            df_lic5G = pd.read_csv(csv_file)
                                            
        # keep first two columns and columns name containing 'RRC Connected User'
        df_lic4G = df_lic4G.iloc[:, :2].join(df_lic4G.filter(like='RRC Connected User'))
        #rename first two columns
        df_lic4G = df_lic4G.rename(columns={df_lic4G.columns[0]: 'eNodeB Name', df_lic4G.columns[1]: 'ESN'})
        #skip first four rows
        df_lic4G = df_lic4G.iloc[4:]


        if 'RRC Connected User(FDD).1' in df_lic4G.columns:            
            df_lic4G['Total RRC User License FDD'] = np.where(df_lic4G['RRC Connected User(FDD).1'].notna(), df_lic4G['RRC Connected User(FDD).1'].astype('float')+ df_lic4G['RRC Connected User(FDD)'].astype('float'), df_lic4G['RRC Connected User(FDD)'].astype('float'))
        else: 
            df_lic4G['Total RRC User License FDD'] = df_lic4G['RRC Connected User(FDD)'].astype('float')
        if 'RRC Connected User for LTE TDD(TDD).1' in df_lic4G.columns:
            df_lic4G['Total RRC User License TDD'] = np.where(df_lic4G['RRC Connected User for LTE TDD(TDD).1'].notna(), df_lic4G['RRC Connected User for LTE TDD(TDD).1'].astype('float')+ df_lic4G['RRC Connected User for LTE TDD(TDD)'].astype('float'), df_lic4G['RRC Connected User for LTE TDD(TDD)'].astype('float'))
        else:
            df_lic4G['Total RRC User License TDD'] = df_lic4G['RRC Connected User for LTE TDD(TDD)'].astype('float')
        df_lic4G = df_lic4G[['eNodeB Name', 'ESN','Total RRC User License FDD', 'Total RRC User License TDD']]

        df_lic5G = df_lic5G.iloc[:, :2].join(df_lic5G.filter(like='RRC Connected User License (NR)'))
        df_lic5G = df_lic5G.rename(columns={df_lic5G.columns[0]: 'gNodeB Name', df_lic5G.columns[1]: 'ESN'})
        df_lic5G = df_lic5G.iloc[4:]

        return df_lic4G, df_lic5G
    
    except Exception as e:
        st.write(f"Error reading CSV file: {e}")
        return None
def style_df(value):
    # df.style.applymap(lambda x: 'color: red' if x > 100 else 'color: black', subset=['FDD Utilization', 'TDD Utilization'])

    return f"background-color: {'#FFA07A' if value > 100 else 'white'}"
  

# define main function
def main():
    st.html("""<h1 style='text-align: center; color: #B09EB0;'>LTE NR License Utilization</h1>""")
    st.sidebar.write("")
    st.sidebar.write("")
    st.sidebar.write("Optional: Upload license files if not available in the directory")
    # 4 empty placeholders
    col11, col12, col13, col14 = st.columns(4)
        

    df_stats4G, df_daily4G, df_stats5G, df_daily5G, df_hourly4G, df_hourly5G  = read_license_stats()    
    # yesterday failure count from df_daily
    if df_stats4G is not None:
        yesterday = df_daily4G['Date'].max()
        with col11:

            lte_failures = df_daily4G[df_daily4G['Date'] == yesterday]['L.E-RAB.FailEst.NoRadioRes.RrcUserLic'].sum()
            st.metric(label='Yesterday LTE Failures', value= int(lte_failures))
            # list of sites with failures where L.E-RAB.FailEst.NoRadioRes.RrcUserLic > 0 in yesterday data
            sites_with_failures4G = df_daily4G[df_daily4G['Date'] == yesterday][df_daily4G['L.E-RAB.FailEst.NoRadioRes.RrcUserLic'] > 0]




           
    if df_stats5G is not None:
        yesterday5G = df_daily5G['Date'].max()
        with col12:
            nr_failures = df_daily5G[df_daily5G['Date'] == yesterday5G]['N.NsaDc.SgNB.Add.Fail.Radio.License'].sum()
            st.metric(label='Yesterday 5G Failures', value= int(nr_failures))
            # list of sites with failures where N.NsaDc.SgNB.Add.Fail.Radio.License > 0
            sites_with_failures5G = df_daily5G[df_daily5G['Date'] == yesterday5G][df_daily5G['N.NsaDc.SgNB.Add.Fail.Radio.License'] > 0]
            

    lic_file4G = st.sidebar.file_uploader("Upload LTE License File", type=['csv'])
    lic_file5G = st.sidebar.file_uploader("Upload NR License File", type=['csv'])
    df_lic4G, df_lic5G = read_license(lic_file4G,lic_file5G)
 
    tab1, tab2 = st.tabs(["LTE", "NR"])
    with tab1:
        if df_lic4G is not None:
        
            # merge the two dataframes on 'eNodeB Name'
            df = pd.merge(df_stats4G, df_lic4G, on='eNodeB Name', how='left')
            df['FDD Utilization'] = df['L.Traffic.eNodeB.FDD.User.Max'] / df['Total RRC User License FDD']  * 100
            df['TDD Utilization'] = df['L.Traffic.eNodeB.TDD.User.Max'] / df['Total RRC User License TDD']  * 100
            # merge df_daily with df on 'eNodeB Name'
            df_combined = pd.merge(df_daily4G, df_lic4G, on='eNodeB Name', how='left')
            df_combined['FDD Utilization'] = df_combined['L.Traffic.eNodeB.FDD.User.Max'] / df_combined['Total RRC User License FDD']  * 100
            df_combined['TDD Utilization'] = df_combined['L.Traffic.eNodeB.TDD.User.Max'] / df_combined['Total RRC User License TDD']  * 100
            # sort by FDD Utilization in descending order
            df = df.sort_values(by='FDD Utilization', ascending=False).round(2).reset_index(drop=True)
            # st.write(df.describe())
            FDD_greater_80 = df[df['FDD Utilization'] > 80].count()['FDD Utilization']
            TDD_greater_80 = df[df['TDD Utilization'] > 80].count()['TDD Utilization']
            with col13:
                st.metric(label='LTE Site Count Utilization > 80%', value = FDD_greater_80 + TDD_greater_80)
        
            # styled_df = df.style.background_gradient(cmap='viridis', subset=['FDD Utilization', 'TDD Utilization'])
             #  .applymap(lambda x: 'color: red' if x > 100 else 'color: black', subset=['FDD Utilization', 'TDD Utilization'])
                            #  .applymap(lambda x: 'background-color: #FFA07A' if x > 100 else 'background-color: white', subset=['FDD Utilization', 'TDD Utilization'])
           
                styled_df = df.style.applymap(lambda x: 'color: red; font-size: 16px' if x > 100 else 'color: black; font-size: 16px',
                              subset=['FDD Utilization', 'TDD Utilization']) \
                                    .format({'FDD Utilization': '{:.2f}', 'TDD Utilization': '{:.2f}',
                                            'L.Traffic.eNodeB.FDD.User.Max': '{:.0f}',
                                            'L.Traffic.eNodeB.TDD.User.Max': '{:.0f}',
                                            'Total RRC User License FDD': '{:.0f}',
                                            'Total RRC User License TDD': '{:.0f}'})


# Display styled DataFrame
            with st.expander("LTE License Utilization", expanded=True):
                st.dataframe(styled_df, hide_index=True)
            with st.expander("Sites with Failures"):

                st.dataframe(sites_with_failures4G, hide_index=True)
            st.html("""<h2 style='text-align: left; color: #B09EB0;'>Drill Down</h2>""")

            row = st.columns(2)
            col21, col22 = row[0].columns(2)
            site_options = df_combined['eNodeB Name'].unique().tolist()
            with col21.container(border=True):
                selected_site = st.selectbox('Select Site', site_options)
            # get license of selected site
            df_filter_lic = df_lic4G[df_lic4G['eNodeB Name'] == selected_site]
            filter_df = df_combined[df_combined['eNodeB Name'] == selected_site]
            if df_filter_lic.empty:
                st.write("No license data available for selected site")
                return
            lic_fdd_selected_site = df_filter_lic['Total RRC User License FDD'].values[0]
            lic_tdd_selected_site = df_filter_lic['Total RRC User License TDD'].values[0]
            df_filter_hourly_4g = df_hourly4G[df_hourly4G['eNodeB Name'] == selected_site]
            df_filter_hourly_4g['FDD Utilization'] = df_filter_hourly_4g['L.Traffic.eNodeB.FDD.User.Max'].astype('float') / lic_fdd_selected_site * 100
            df_filter_hourly_4g['TDD Utilization'] = df_filter_hourly_4g['L.Traffic.eNodeB.TDD.User.Max'].astype('float') / lic_tdd_selected_site * 100
            df_filter_hourly_4g['Date'] = pd.to_datetime(df_filter_hourly_4g['Date'] + ' ' + df_filter_hourly_4g['Time'])
            with col22.container(border=True):

                tech = st.radio("Select Technology", ('FDD', 'TDD'), horizontal=True)
            col31, col32 = st.columns(2)
            if tech == 'FDD':
                with col31.container(border=True):
                    fig = go.Figure()   
                    fig.add_trace(go.Scatter(x=filter_df['Date'], y=filter_df['FDD Utilization'], name='FDD Utilization', mode='lines'))
                    fig.update_layout(title='FDD Utilization', xaxis_title='Date', yaxis_title='FDD Utilization')                    
                    fig.update_xaxes(showgrid=False, ticklen = 20, tickangle = 90)

                    

                    st.plotly_chart(fig, use_container_width= True)
                with col32.container(border=True):
                    # fig2 = px.bar(filter_df, x ='Date', y = 'L.Traffic.eNodeB.FDD.User.Max', title = 'Max Users FDD')
                    # st.plotly_chart(fig2)
                    daily_fdd = make_subplots(specs=[[{"secondary_y": True}]])
                    daily_fdd.add_trace(go.Scatter(x=filter_df['Date'], y=filter_df['L.Traffic.eNodeB.FDD.User.Max'], name='Max Users FDD', mode='lines'), secondary_y=False)
                    daily_fdd.add_trace(go.Bar(x=filter_df['Date'], y=filter_df['L.E-RAB.FailEst.NoRadioRes.RrcUserLic'], name='RRC Failures', marker_color='red'), secondary_y=True)
                    daily_fdd.update_layout(title='Daily FDD Users and RRC Failures', xaxis_title='Date', yaxis_title='Max Users FDD', yaxis2_title='RRC Failures')
                    daily_fdd.update_xaxes(showgrid=False)
                    daily_fdd.update_yaxes(showgrid=False)
                    st.plotly_chart(daily_fdd, use_container_width= True)
                 
                    # combine date and time columns
                with st.container(border = True):
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(go.Scatter(x=df_filter_hourly_4g['Date'], y=df_filter_hourly_4g['L.Traffic.eNodeB.FDD.User.Max'], name='Max Users FDD', mode = 'lines'), secondary_y=False)
                    fig.add_trace(go.Bar(x=df_filter_hourly_4g['Date'], y=df_filter_hourly_4g['L.Cell.Unavail.Dur.Sys(s)'], name='Unavailability Duration', marker_color = 'red'), secondary_y=True)
                    fig.update_layout(title='Hourly FDD Users and Unavailability', xaxis_title='Date', yaxis_title='Max Users FDD', yaxis2_title='Unavailability Duration (s)')
                    fig.update_xaxes(showgrid=False)
                    fig.update_yaxes(showgrid=False)
                    st.plotly_chart(fig, use_container_width= True)
                with st.container(border = True):
                    lic_util_hourly = make_subplots(specs=[[{"secondary_y": True}]])    
                    lic_util_hourly.add_trace(go.Scatter(x=df_filter_hourly_4g['Date'], y=df_filter_hourly_4g['FDD Utilization'], name='FDD Utilization', mode='lines'), secondary_y=False)
                    lic_util_hourly.add_trace(go.Bar(x=df_filter_hourly_4g['Date'], y=df_filter_hourly_4g['L.E-RAB.FailEst.NoRadioRes.RrcUserLic'], name='RRC Failures', marker_color='red'), secondary_y=True)
                    lic_util_hourly.update_layout(title='Hourly FDD Utilization and RRC Failures', xaxis_title='Date', yaxis_title='FDD Utilization', yaxis2_title='RRC Failures')
                    # no gridlines
                    lic_util_hourly.update_xaxes(showgrid=False)
                    lic_util_hourly.update_yaxes(showgrid=False)
                    st.plotly_chart(lic_util_hourly, use_container_width= True)
                
         
            else:
                with col31.container(border=True):
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=filter_df['Date'], y=filter_df['TDD Utilization'], name='TDD Utilization', mode='lines'))
                    fig.update_layout(title='TDD Utilization', xaxis_title='Date', yaxis_title='TDD Utilization')
                    fig.update_xaxes(showgrid=False, ticklen = 20, tickangle = 90)
                    st.plotly_chart(fig, use_container_width= True)
                  
                with col32.container(border=True):
                    # fig2 = px.bar(filter_df, x='Date', y= 'L.Traffic.eNodeB.TDD.User.Max', title='Max Users TDD')
                    # st.plotly_chart(fig2)
                    daily_tdd = make_subplots(specs=[[{"secondary_y": True}]])
                    daily_tdd.add_trace(go.Scatter(x=filter_df['Date'], y=filter_df['L.Traffic.eNodeB.TDD.User.Max'], name='Max Users TDD', mode='lines'), secondary_y=False)

                    daily_tdd.add_trace(go.Bar(x=filter_df['Date'], y=filter_df['L.E-RAB.FailEst.NoRadioRes.RrcUserLic'], name='RRC Failures', marker_color='red'), secondary_y=True)

                    daily_tdd.update_layout(title='Daily TDD Users and RRC Failures', xaxis_title='Date', yaxis_title='Max Users TDD', yaxis2_title='RRC Failures')
                    daily_tdd.update_xaxes(showgrid=False)
                    daily_tdd.update_yaxes(showgrid=False)
                 
                    st.plotly_chart(daily_tdd, use_container_width= True)
                with st.container(border = True):
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(go.Scatter(x=df_filter_hourly_4g['Date'], y=df_filter_hourly_4g['L.Traffic.eNodeB.TDD.User.Max'], name='Max Users TDD', mode = 'lines'), secondary_y=False)
                    fig.add_trace(go.Bar(x=df_filter_hourly_4g['Date'], y=df_filter_hourly_4g['L.Cell.Unavail.Dur.Sys(s)'], name='Unavailability Duration', marker_color = 'red'), secondary_y=True)
                    fig.update_layout(title='Hourly TDD Users and Unavailability', xaxis_title='Date', yaxis_title='Max Users TDD', yaxis2_title='Unavailability Duration (s)')
                    fig.update_xaxes(showgrid=False)
                    fig.update_yaxes(showgrid=False)
                    st.plotly_chart(fig, use_container_width= True)
                with st.container(border = True):
                    lic_util_hourly = make_subplots(specs=[[{"secondary_y": True}]])    
                    lic_util_hourly.add_trace(go.Scatter(x=df_filter_hourly_4g['Date'], y=df_filter_hourly_4g['TDD Utilization'], name='TDD Utilization', mode='lines'), secondary_y=False)
                    lic_util_hourly.add_trace(go.Bar(x=df_filter_hourly_4g['Date'], y=df_filter_hourly_4g['L.E-RAB.FailEst.NoRadioRes.RrcUserLic'], name='RRC Failures', marker_color='red'), secondary_y=True)
                    lic_util_hourly.update_layout(title='Hourly TDD Utilization and RRC Failures', xaxis_title='Date', yaxis_title='TDD Utilization', yaxis2_title='RRC Failures')
                    # no gridlines
                    lic_util_hourly.update_xaxes(showgrid=False)
                    lic_util_hourly.update_yaxes(showgrid=False)
                    st.plotly_chart(lic_util_hourly, use_container_width= True)

           
            
            
          
     


          


          
            st.download_button(
            label="Download Data",
            data= df.to_csv(index = False).encode('utf-8'),
            file_name='LTE license_utilization.csv',
            mime='text/csv'
        )   

    with tab2:
        if df_lic5G is not None:
            # merge the two dataframes on 'gNodeB Name'
            df5G = pd.merge(df_stats5G, df_lic5G, on='gNodeB Name', how='left')
            df5G['NR Utilization'] = df5G['N.User.gNodeB.RRCConn.Max'].astype('float') / df5G['RRC Connected User License (NR)'].astype('float')  * 100
            # merge df_daily with df on 'gNodeB Name'
            df_combined5G = pd.merge(df_daily5G, df_lic5G, on='gNodeB Name', how='left')
            df_combined5G['NR Utilization'] = df_combined5G['N.User.gNodeB.RRCConn.Max'].astype('float') / df_combined5G['RRC Connected User License (NR)'].astype('float')  * 100
            # sort by NR Utilization in descending order
            df5G = df5G.sort_values(by='NR Utilization', ascending=False).round(2).reset_index(drop=True)
            NR_greater_80 = df5G[df5G['NR Utilization'] > 80].count()['NR Utilization']
            with col14:
                st.metric(label='5G Site Count Utilization > 80%', value = NR_greater_80)
            styled_df5G = df5G.style.applymap(lambda x: 'color: red; font-size: 16px' if x > 100 else 'color: black; font-size: 16px',
                                  subset=['NR Utilization']) \
                          .format({'NR Utilization': '{:.2f}', 
                                   'N.User.gNodeB.RRCConn.Max': '{:.0f}'})

            
            with st.expander("5G License Utilization", expanded=True):
                st.dataframe(styled_df5G, hide_index=True)   
            with st.expander("Sites with Failures"):
                st.dataframe(sites_with_failures5G, hide_index=True)    
            st.html("""<h2 style='text-align: left; color: #B09EB0;'>Drill Down</h2>""")            
            site_options5G = df_combined5G['gNodeB Name'].unique().tolist()

            selected_site5G = st.selectbox('Select Site', site_options5G)

            col1, col2 = st.columns(2)
            filter_df5G = df_combined5G[df_combined5G['gNodeB Name'] == selected_site5G]
            df_filter_lic5G = df_lic5G[df_lic5G['gNodeB Name'] == selected_site5G]
            lic_dir_selected_site = df_filter_lic5G['RRC Connected User License (NR)'].values[0]
            df_filter_hourly_5g = df_hourly5G[df_hourly5G['gNodeB Name'] == selected_site5G]
            # dtype for 'N.User.gNodeB.RRCConn.Max' and lic_dir_selected_site
            df_filter_hourly_5g['N.User.gNodeB.RRCConn.Max'] = df_filter_hourly_5g['N.User.gNodeB.RRCConn.Max'].astype('float')
            df_filter_hourly_5g['NR Utilization'] = df_filter_hourly_5g['N.User.gNodeB.RRCConn.Max'].astype('float') / float(lic_dir_selected_site) * 100
            df_filter_hourly_5g['Date'] = pd.to_datetime(df_filter_hourly_5g['Date'] + ' ' + df_filter_hourly_5g['Time'])


            with col1.container(border=True):
            
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=filter_df5G['Date'], y=filter_df5G['NR Utilization'], name='NR Utilization', mode='lines'))
                fig.update_layout(title='NR Utilization', xaxis_title='Date', yaxis_title='NR Utilization')
                fig.update_xaxes(showgrid=False, ticklen = 20, tickangle = 90)
                st.plotly_chart(fig, use_container_width= True)
            with col2.container(border = True):
                # fig1 = px.bar(filter_df5G, x='Date', y= 'N.User.gNodeB.RRCConn.Max', title='Max Users NR')
                # st.plotly_chart(fig1, use_container_width= True)
                daily_nr = make_subplots(specs=[[{"secondary_y": True}]])
                daily_nr.add_trace(go.Scatter(x=filter_df5G['Date'], y=filter_df5G['N.User.gNodeB.RRCConn.Max'], name='Max Users NR', mode='lines'), secondary_y=False)
                daily_nr.add_trace(go.Bar(x=filter_df5G['Date'], y=filter_df5G['N.NsaDc.SgNB.Add.Fail.Radio.License'], name='License Failures', marker_color='red'), secondary_y=True)
                daily_nr.update_layout(title='Daily NR Users and License Failures', xaxis_title='Date', yaxis_title='Max Users NR', yaxis2_title='License Failures')
                daily_nr.update_xaxes(showgrid=False)
                daily_nr.update_yaxes(showgrid=False)
                st.plotly_chart(daily_nr, use_container_width= True)

            with st.container(border = True):
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(go.Scatter(x=df_filter_hourly_5g['Date'], y=df_filter_hourly_5g['N.User.gNodeB.RRCConn.Max'], name='Max Users NR', mode = 'lines'), secondary_y=False)
                fig.add_trace(go.Bar(x=df_filter_hourly_5g['Date'], y=df_filter_hourly_5g['N.NsaDc.SgNB.Add.Fail.Radio.License'], name='License Failures', marker_color = 'red'), secondary_y=True)
                fig.update_layout(title='Hourly NR Users and License Failures', xaxis_title='Date', yaxis_title='Max Users NR', yaxis2_title='License Failures')
                fig.update_xaxes(showgrid=False)
                fig.update_yaxes(showgrid=False)
                st.plotly_chart(fig, use_container_width= True)
            with st.container(border = True):
                lic_util_hourly = make_subplots(specs=[[{"secondary_y": True}]])    
                lic_util_hourly.add_trace(go.Scatter(x=df_filter_hourly_5g['Date'], y=df_filter_hourly_5g['NR Utilization'], name='NR Utilization', mode='lines'), secondary_y=False)
                lic_util_hourly.add_trace(go.Bar(x=df_filter_hourly_5g['Date'], y=df_filter_hourly_5g['N.Cell.Unavail.Dur.System(s)'], name='Outage Duration (s)', marker_color='red'), secondary_y=True)
                lic_util_hourly.update_layout(title='Hourly NR Utilization and Outage Duration(s)', xaxis_title='Date', yaxis_title='NR Utilization', yaxis2_title='Outage Duration')
                # no gridlines
                lic_util_hourly.update_xaxes(showgrid=False)
                lic_util_hourly.update_yaxes(showgrid=False)
                st.plotly_chart(lic_util_hourly, use_container_width= True)
           

                # st download button
            st.download_button(
                label="Download Data",
                data=df5G.to_csv(index=False).encode('utf-8'),
                file_name='NR license_utilization.csv',
                mime='text/csv'
            )   












         

if __name__ == "__main__":
    main()
