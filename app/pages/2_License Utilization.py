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


st.set_page_config("License Utilization", layout="wide")
st.sidebar.page_link("dash2.py", label ="Home")
st.sidebar.page_link("pages/2_License Utilization.py", label = "License Utilization")
with open('app/style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
lic_dir = os.path.join(script_dir, '..', '..','ftp', 'data', 'license')

@st.cache_data(ttl =3600*24)
def read_license_stats():

#read the csv file
    filename = '*LTE_license*.csv'
    filename5G = '*5G_license*.csv'
    if filename is not None:
        csv_file= glob.glob(os.path.join(lic_dir, filename))
        csv_file = csv_file[0]
    if filename5G is not None:
        csv_file5G = glob.glob(os.path.join(lic_dir, filename5G))
        csv_file5G = csv_file5G[0]

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
            


            return top_3_avg, df_kpi, top_3_nr_avg, df_kpi5g

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


        

    df_stats4G, df_daily4G, df_stats5G, df_daily5G  = read_license_stats()    
    # yesterday failure count from df_daily
    if df_stats4G is not None:
        yesterday = df_daily4G['Date'].max()
        with col11:

            lte_failures = df_daily4G[df_daily4G['Date'] == yesterday]['L.E-RAB.FailEst.NoRadioRes.RrcUserLic'].sum()
            st.metric(label='Yesterday LTE Failures', value= int(lte_failures))
    if df_stats5G is not None:
        yesterday5G = df_daily5G['Date'].max()
        with col12:
            nr_failures = df_daily5G[df_daily5G['Date'] == yesterday5G]['N.NsaDc.SgNB.Add.Fail.Radio.License'].sum()
            st.metric(label='Yesterday 5G Failures', value= int(nr_failures))

    #

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
            st.dataframe(styled_df, hide_index=True)
             
                
                                
                                
                            

            row = st.columns(2)
            col21, col22 = row[0].columns(2)
            site_options = df_combined['eNodeB Name'].unique().tolist()
            with col21.container(border=True):
                selected_site = st.selectbox('Select Site', site_options)

            filter_df = df_combined[df_combined['eNodeB Name'] == selected_site]
            with col22.container(border=True):

                tech = st.radio("Select Technology", ('FDD', 'TDD'), horizontal=True)
            col31, col32 = st.columns(2)
            if tech == 'FDD':
                with col31.container(border=True):
                    fig = px.line(filter_df, x='Date', y='FDD Utilization', title='FDD Utilization')
                    st.plotly_chart(fig)
                with col32.container(border=True):
                    fig2 = px.bar(filter_df, x ='Date', y = 'L.Traffic.eNodeB.FDD.User.Max', title = 'Max Users FDD')
                    st.plotly_chart(fig2)
                
                with st.container(border = True):
                    fig1 = px.bar(filter_df, x='Date', y= 'L.E-RAB.FailEst.NoRadioRes.RrcUserLic', title='RRC Failures')
                    st.plotly_chart(fig1, use_container_width= True)
            else:
                with col31.container(border=True):
                    fig = px.line(filter_df, x='Date', y='TDD Utilization', title='TDD Utilization')
                    st.plotly_chart(fig)
                with col32.container(border=True):
                    fig2 = px.bar(filter_df, x='Date', y= 'L.Traffic.eNodeB.TDD.User.Max', title='Max Users TDD')
                    st.plotly_chart(fig2)
                with st.container(border = True):
                    fig1 = px.bar(filter_df, x='Date', y= 'L.E-RAB.FailEst.NoRadioRes.RrcUserLic', title='RRC Failures')
                    st.plotly_chart(fig1, use_container_width= True)
            st.download_button(
            label="Download Data",
            data= df.to_csv().encode('utf-8'),
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

            
                              
            
            st.dataframe(styled_df5G, hide_index=True)
            site_options5G = df_combined5G['gNodeB Name'].unique().tolist()

            selected_site5G = st.selectbox('Select Site', site_options5G)

            col1, col2 = st.columns(2)
            filter_df5G = df_combined5G[df_combined5G['gNodeB Name'] == selected_site5G]
            with col1.container(border=True):
                fig = px.line(filter_df5G, x='Date', y='NR Utilization', title='NR Utilization')
                st.plotly_chart(fig, use_container_width= True)
            with col2.container(border = True):
                fig1 = px.bar(filter_df5G, x='Date', y= 'N.User.gNodeB.RRCConn.Max', title='Max Users NR')
                st.plotly_chart(fig1, use_container_width= True)
            with st.container(border = True):
                fig2 = px.bar(filter_df5G, x='Date', y= 'N.NsaDc.SgNB.Add.Fail.Radio.License', title='License Failures')
                st.plotly_chart(fig2, use_container_width= True)

                # st download button
            st.download_button(
                label="Download Data",
                data=df5G.to_csv().encode('utf-8'),
                file_name='NR license_utilization.csv',
                mime='text/csv'
            )   












         

if __name__ == "__main__":
    main()
